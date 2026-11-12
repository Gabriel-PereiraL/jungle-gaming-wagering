import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { CreateQueueCommand, DeleteQueueCommand } from '@aws-sdk/client-sqs';
import { connect, one, transaction, type Database } from '../src/db';
import { createSqs, discoverQueues, OutboxPublisher, WagerConsumer } from '../src/messaging';
import { Wagering } from '../src/wagering';
import { appUrls, command, docker, drain, eventually, isolatedQueues, json, open, post, ready,
  reconcile, send, type Outcome } from './fixtures';

let db: Database;
let urls: string[];
const sqs = createSqs();

beforeAll(async () => {
  urls = appUrls();
  expect(new Set(urls).size).toBeGreaterThanOrEqual(3);
  await Promise.all(urls.map(ready));
  db = await connect();
}, 20_000);

afterAll(async () => {
  await db?.close();
  sqs.destroy();
});

test('50 submissões em 3 processos: um débito e um evento lógico', async () => {
  const wallet = await open(urls[0]);
  const bet = command(wallet);
  const results = await Promise.all(Array.from({ length: 50 }, (_, i) => post(urls[i % urls.length], bet)));
  expect(results.every(result => result.status === 200)).toBe(true);
  expect(new Set(results.map(result => result.body.transactionId)).size).toBe(1);
  expect(results.filter(result => !result.body.idempotentReplay)).toHaveLength(1);
  const id = results[0].body.transactionId;
  expect(await one(db.em.fork(), `
    select count(*)::int n from outbox_messages
    where event_type='WagerTransactionProcessed' and payload->'data'->>'transactionId'=?`, [id])).toEqual({ n: 1 });
  await reconcile(urls[1], wallet, '90.00', 2);
}, 25_000);

test('duas BET de 80 sobre 100: só uma aplica', async () => {
  const wallet = await open(urls[0]);
  const money = { amount: '80.00', currency: 'BRL' };
  const results = await Promise.all([post(urls[1], command(wallet, { money })), post(urls[2], command(wallet, { money }))]);
  expect(results.map(result => result.status).sort()).toEqual([200, 422]);
  expect(results.find(result => result.status === 422)?.body.failureCode).toBe('INSUFFICIENT_FUNDS');
  await reconcile(urls[0], wallet, '20.00', 2);
});

test('lock explícito de uma wallet não impede progresso de outra', async () => {
  const blocked = await open(urls[0]);
  const independent = await open(urls[0]);
  const latch = Promise.withResolvers<void>();
  const acquired = Promise.withResolvers<void>();
  const holding = transaction(db, async em => {
    await em.execute('select id from wallets where id=? for update', [blocked.id]);
    acquired.resolve();
    await latch.promise;
  });
  await acquired.promise;
  const waiting = post(urls[1], command(blocked));
  try {
    await eventually(async () => {
      const locks = await one<{ n: number }>(db.em.fork(),
        "select count(*)::int n from pg_stat_activity where wait_event_type='Lock' and query ilike '%wallets%for update%'");
      return locks?.n ? true : undefined;
    }, 2000);
    expect((await post(urls[2], command(independent))).status).toBe(200);
  } finally {
    latch.resolve();
    await holding;
    await waiting;
  }
  expect((await waiting).status).toBe(200);
  await reconcile(urls[0], blocked, '90.00', 2);
  await reconcile(urls[0], independent, '90.00', 2);
});

test('REFUND entregue primeiro na fila, depois reprocessado pelo worker', async () => {
  const queues = await discoverQueues(sqs);
  const wallet = await open(urls[0]);
  const bet = command(wallet);
  const refund = command(wallet, { kind: 'REFUND', referenceExternalTransactionId: bet.externalTransactionId });
  const path = '/providers/' + refund.providerId + '/wagering/transactions/' + refund.externalTransactionId;
  await send(sqs, queues.input, refund);
  await eventually(async () => (await json<Outcome>(urls[1], path)).body.status === 'PENDING_REFERENCE' ? true : undefined);
  await send(sqs, queues.input, bet);
  await eventually(async () => (await json<Outcome>(urls[2], path)).body.status === 'PROCESSED' ? true : undefined);
  await reconcile(urls[0], wallet, '100.00', 3);
}, 20_000);

test('pendência expira com rejeição auditável', async () => {
  const wallet = await open(urls[0]);
  const pending = await post(urls[1], command(wallet, { kind: 'ROLLBACK', referenceExternalTransactionId: randomUUID() }));
  expect(pending.status).toBe(202);
  const id = pending.body.transactionId;
  await eventually(async () => {
    const row = await one<{ attempts: number }>(db.em.fork(),
      'select attempts from wager_transactions where id=?', [id]);
    return row && row.attempts > 1 ? true : undefined;
  });
  await db.em.fork().execute(
    "update wager_transactions set expires_at=clock_timestamp()-interval '1 second',next_attempt_at=clock_timestamp() where id=?",
    [id]);
  const final = await eventually(async () => {
    const result = await json<Outcome>(urls[2], '/wagering/transactions/' + id);
    return result.body.status === 'REJECTED' ? result.body : undefined;
  });
  expect(final.failureCode).toBe('REFERENCE_NOT_FOUND');
  await reconcile(urls[0], wallet, '100.00', 1);
}, 20_000);

test('SIGKILL após commit e antes do ACK: redelivery não reaplica', async () => {
  const wallet = await open(urls[0]);
  const testQueues = await isolatedQueues(sqs);
  const cmd = command(wallet);
  const messageId = randomUUID();
  await send(sqs, testQueues.urls.input, cmd, messageId);
  docker('compose', 'pause', 'app');
  let paused = true;
  const child = Bun.spawn([process.execPath, 'test/crash-worker.ts', JSON.stringify(testQueues.urls)],
    { stdout: 'pipe', stderr: 'inherit' });
  const reader = child.stdout.getReader();
  try {
    let output = '';
    const timeout = setTimeout(() => child.kill(9), 7000);
    try {
      while (!output.includes('AFTER_COMMIT_BEFORE_ACK')) {
        const { value, done } = await reader.read();
        if (done) throw new Error('Worker exited before the commit barrier');
        output += new TextDecoder().decode(value);
      }
    } finally {
      clearTimeout(timeout);
    }
    child.kill(9);
    expect(await child.exited).not.toBe(0);
    expect(await one(db.em.fork(), 'select message_id from inbox_messages where message_id=? and processed_at is not null',
      [messageId])).toEqual({ message_id: messageId });
    expect(await one(db.em.fork(), `
      select count(*)::int n from outbox_messages o join wager_transactions t
      on o.payload->'data'->>'transactionId'=t.id::text
      where t.external_id=? and o.published_at is null`, [cmd.externalTransactionId])).toEqual({ n: 2 });
    docker('compose', 'unpause', 'app');
    paused = false;
    await eventually(async () => {
      const published = await one<{ n: number }>(db.em.fork(), `
        select count(*)::int n from outbox_messages o join wager_transactions t
        on o.payload->'data'->>'transactionId'=t.id::text
        where t.external_id=? and o.published_at is not null`, [cmd.externalTransactionId]);
      return published?.n === 2 ? true : undefined;
    });

    const replacement = new WagerConsumer(sqs, testQueues.urls, new Wagering(db));
    expect(await eventually(async () => await replacement.poll() ? true : undefined)).toBe(true);
    expect(await replacement.poll()).toBe(false);
    await reconcile(urls[0], wallet, '90.00', 2);
    const id = docker('compose', 'ps', '-q', 'app').split(/\r?\n/)[0];
    docker('restart', id);
    urls = appUrls();
    await Promise.all(urls.map(ready));
    await reconcile(urls[1], wallet, '90.00', 2);
  } finally {
    if (paused) docker('compose', 'unpause', 'app');
    reader.releaseLock();
    if (child.exitCode === null) child.kill(9);
    await child.exited;
    await testQueues.cleanup();
  }
}, 35_000);

test('publishers concorrentes recuperam lease e entregam eventos confirmados com eventId estável', async () => {
  const name = 'publisher_' + randomUUID().replaceAll('-', '');
  const base = new URL(process.env.DATABASE_URL ?? 'postgres://wager:wager@127.0.0.1:54328/wager');
  const admin = db.em.fork();
  await admin.execute('create database ' + name);
  base.pathname = '/' + name;
  const isolated = await connect(base.toString());
  let queueUrl: string | undefined;
  try {
    await isolated.getMigrator().up();
    queueUrl = (await sqs.send(new CreateQueueCommand({
      QueueName: name + '.fifo', Attributes: { FifoQueue: 'true' },
    }))).QueueUrl!;
    const service = new Wagering(isolated);
    const wallet = await service.open(randomUUID(), { currency: 'BRL', amount: '0.00' }, randomUUID());
    const win = command(wallet, { kind: 'WIN' });
    const saved = await service.submit({
      command: win, idempotencyKey: win.externalTransactionId, correlationId: randomUUID(),
    });
    const publishers = Array.from({ length: 2 }, () => new OutboxPublisher(isolated, sqs, queueUrl!));
    expect(await Promise.all(publishers.map(publisher => publisher.publishOne()))).toEqual([true, true]);
    const events = new Map<string, string>();
    await eventually(async () => {
      for (const message of await drain(sqs, queueUrl!)) {
        const event = JSON.parse(message.Body ?? '{}');
        expect(event.data.transactionId).toBe(saved.transactionId);
        events.set(event.eventId, event.eventType);
      }
      return events.size === 2 ? true : undefined;
    });
    expect([...events.values()].sort()).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);
    const eventId = [...events.keys()][0];
    const store = isolated.em.fork();
    await store.execute(`
      update outbox_messages set published_at=null, claim_token=?,lease_until=clock_timestamp()-interval '1 second'
      where id=?`, [randomUUID(), eventId]);
    const reclaimed = await Promise.all(publishers.map(publisher => publisher.publishOne()));
    expect(reclaimed.filter(Boolean)).toHaveLength(1);
    expect(await one(store, 'select attempts,(published_at is not null) as done from outbox_messages where id=?', [eventId]))
      .toEqual({ attempts: 2, done: true });
    expect((await service.reconcile(wallet.id, randomUUID())).consistent).toBe(true);
  } finally {
    if (queueUrl) await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
    await isolated.close();
    await admin.execute('drop database ' + name);
  }
}, 30_000);
