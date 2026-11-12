import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { CreateQueueCommand, DeleteQueueCommand, GetQueueAttributesCommand, SendMessageCommand } from '@aws-sdk/client-sqs';
import { connect, one, transaction, type Database } from '../src/db';
import { createSqs, discoverQueues, WagerConsumer } from '../src/messaging';
import { Wagering } from '../src/wagering';
import { appUrls, command, docker, drain, envelope, eventually, isolatedQueues, json, open, post, ready, reconcile, send,
  type Outcome, type WalletDto } from './fixtures';

let urls: string[];
let db: Database;
const sqs = createSqs();
const path = '/wagering/transactions';

beforeAll(async () => {
  urls = appUrls();
  expect(new Set(urls).size).toBe(3);
  await Promise.all(urls.map(ready));
  db = await connect();
}, 25_000);
afterAll(async () => { await db?.close(); sqs.destroy(); });

async function samples(name: string) {
  const bodies = await Promise.all(urls.map(async url => (await fetch(url + '/metrics')).text()));
  return bodies.map(body => body.split('\n')
    .filter(line => line.startsWith(name + ' ') || line.startsWith(name + '{'))
    .reduce((total, line) => total + Number(line.split(' ').at(-1)), 0));
}

async function total(name: string) {
  return (await samples(name)).reduce((sum, value) => sum + value, 0);
}

test('F01 rejeita propriedades desconhecidas, payloads grandes e JSON inválido sem ecoar conteúdo', async () => {
  const wallet = await open(urls[0]);
  const bet = command(wallet);
  const request = (body: string) => fetch(urls[0] + path, {
    method: 'POST', body, headers: { 'Content-Type': 'application/json', 'Idempotency-Key': randomUUID() },
    signal: AbortSignal.timeout(3000),
  });
  const bad = await request('{"token":sensitive-marker}');
  const badText = await bad.text();
  expect(bad.status).toBe(400);
  expect(badText).not.toContain('sensitive-marker');
  expect(badText).not.toContain('SyntaxError');
  expect(bad.headers.get('x-powered-by')).toBeNull();
  expect(bad.headers.get('cache-control')).toBe('no-store');
  expect(bad.headers.get('x-content-type-options')).toBe('nosniff');

  for (const raw of [
    JSON.stringify({ ...bet, status: 'PROCESSED' }),
    JSON.stringify({ ...bet, money: { ...bet.money, balance: '900.00' } }),
    JSON.stringify({ ...bet, walletId: 'invalid-uuid' }),
    JSON.stringify({ ...bet, providerId: 'x'.repeat(129) }),
    JSON.stringify({ ...bet, kind: 'OPENING' }),
    JSON.stringify({ ...bet, money: undefined }),
    JSON.stringify(bet).replace(/^\{/, '{"__proto__":{"polluted":true},'),
  ]) {
    const response = await request(raw);
    expect(response.status).toBe(400);
    await response.text();
  }
  const huge = await request(JSON.stringify({ ...bet, ignored: 'x'.repeat(20_000) }));
  expect(huge.status).toBe(413);
  await huge.text();
  await reconcile(urls[0], wallet, '100.00', 1);
});

test('F02 parâmetros de lookup, cursor de outra wallet e limites rejeitam entrada hostil', async () => {
  const [wallet, other] = await Promise.all([open(urls[0]), open(urls[1])]);
  const cmd = command(wallet);
  await post(urls[0], cmd);
  const ledger = '/wallets/' + wallet.id + '/ledger';
  const first = await json<{ nextCursor: string }>(urls[0], ledger + '?limit=1');
  const cursor = first.body.nextCursor;
  expect(cursor).toBeTruthy();
  expect((await json(urls[0], '/wallets/' + other.id + '/ledger?cursor=' + cursor)).status).toBe(400);
  for (const suffix of ['?limit=101', '?limit=-1', '?limit=1&limit=2', '?cursor=%27%20OR%201%3D1', '?cursor=' + 'x'.repeat(180)]) {
    expect((await json(urls[1], ledger + suffix)).status).toBe(400);
  }
  const injected = encodeURIComponent("x'/**/OR/**/true--");
  expect((await json(urls[0], '/providers/' + injected + path + '/' + cmd.externalTransactionId)).status).toBe(404);
  expect((await json(urls[0], '/wallets/' + injected)).status).toBe(400);
  const response = await fetch(urls[0] + ledger, { headers: { Origin: 'https://invalid.example', Connection: 'close' } });
  expect(response.headers.get('access-control-allow-origin')).toBeNull();
  await response.text();
  await reconcile(urls[1], wallet, '90.00', 2);
  await reconcile(urls[2], other, '100.00', 1);
});

test('F03 esgotamento do retry materializa FAILED e envia à DLQ com replay estável', async () => {
  const queues = await eventually(() => discoverQueues(sqs));
  const wallet = await open(urls[0]);
  const cmd = command(wallet);
  const msg = randomUUID();
  const hook = 'fail_' + msg.replaceAll('-', '');
  const em = db.em.fork();
  await em.execute(`
    create function ${hook}() returns trigger language plpgsql as $$
    begin
      if new.aggregate_id='${wallet.id}'::uuid and new.event_type='WagerTransactionProcessed' then
        raise exception 'audit injected failure';
      end if;
      return new;
    end $$;
    create trigger ${hook} before insert on outbox_messages for each row execute function ${hook}();
  `);
  try {
    const retriesBefore = await total('wager_retry_total');
    await send(sqs, queues.input, cmd, msg);
    const dead = await eventually(async () => (await drain(sqs, queues.dead)).find(m => {
      try { return JSON.parse(m.Body ?? '').messageId === msg; } catch { return false; }
    }), 55_000);
    const outcome = await json<Outcome>(urls[1], '/providers/' + cmd.providerId + path + '/' + cmd.externalTransactionId);
    console.log(JSON.stringify({ probe: 'F03', lookupStatus: outcome.status, wagerStatus: outcome.body.status }));
    expect(outcome.status).toBe(200);
    expect(outcome.body.status).toBe('FAILED');
    expect(await total('wager_retry_total')).toBeGreaterThanOrEqual(retriesBefore + 5);
    expect(outcome.body.failureCode).toBe('INFRASTRUCTURE_RETRIES_EXHAUSTED');
    expect(dead.MessageAttributes?.FailureCode?.StringValue).toBe(outcome.body.failureCode);
    expect(dead.MessageAttributes?.CorrelationId?.StringValue).toBe(msg);
    expect(dead.MessageAttributes?.ApplicationMessageId?.StringValue).toBe(msg);
    expect(appEvents().find(line => line.event === 'sqs_dead_letter' && line.messageId === msg))
      .toMatchObject({ correlationId: msg, walletId: wallet.id, failureCode: outcome.body.failureCode });
    const replay = await post(urls[2], cmd);
    expect(replay.status).toBe(409);
    expect(replay.body.status).toBe('FAILED');
    expect(replay.body.failureCode).toBe(outcome.body.failureCode);
    expect(replay.body.transactionId).toBe(outcome.body.transactionId);
    expect(replay.body.idempotentReplay).toBe(true);
    await expect(em.execute('delete from wager_transactions where id=?', [outcome.body.transactionId])).rejects.toThrow();
    expect(await one(em, "select event_type from outbox_messages where payload->'data'->>'transactionId'=?",
      [outcome.body.transactionId])).toEqual({ event_type: 'WagerTransactionFailed' });
    expect(await one(em, 'select processed_at is not null as done from inbox_messages where message_id=?', [msg]))
      .toEqual({ done: true });
    await reconcile(urls[0], wallet, '100.00', 1);
  } finally {
    await em.execute(`drop trigger ${hook} on outbox_messages; drop function ${hook}();`);
  }
}, 65_000);

test('F04 rajada local de apostas e criação concorrente preservam wallet, saldo e ledger', async () => {
  const playerId = randomUUID();
  const opened = await Promise.all(Array.from({ length: 12 }, (_, i) =>
    json<WalletDto>(urls[i % 3], '/wallets', { playerId, initialBalance: { currency: 'BRL', amount: '10.00' } })));
  expect(opened.filter(r => r.status === 201)).toHaveLength(1);
  expect(opened.filter(r => r.status === 409)).toHaveLength(11);
  const wallet = opened.find(r => r.status === 201)!.body;
  const commands = Array.from({ length: 24 }, () => command(wallet, { money: { currency: 'BRL', amount: '1.00' } }));
  const bets = await Promise.all(commands.map((cmd, i) => post(urls[i % 3], cmd)));
  const transient = bets.filter(result => result.status === 503).length;
  for (const [i, outcome] of bets.entries()) {
    if (outcome.status === 503) bets[i] = await post(urls[i % 3], commands[i]);
  }
  console.log(JSON.stringify({ probe: 'F04', transient }));
  expect(bets.filter(r => r.status === 200)).toHaveLength(10);
  expect(bets.filter(r => r.status === 422)).toHaveLength(14);
  await reconcile(urls[0], wallet, '0.00', 11);
}, 20_000);

test('F05 OPENING via fila vai à DLQ e logs correlacionam SQS, domínio e evento sem payload integral', async () => {
  const queues = await discoverQueues(sqs);
  const wallet = await open(urls[0], '0.00');
  const cmd = command(wallet, { kind: 'LOSS', money: { currency: 'BRL', amount: '0.00' } });
  const illegal = { ...envelope(cmd), data: { ...envelope(cmd).data, kind: 'OPENING' } };
  await sqs.send(new SendMessageCommand({
    QueueUrl: queues.input, MessageBody: JSON.stringify(illegal), MessageGroupId: wallet.id, MessageDeduplicationId: illegal.messageId,
  }));
  const dead = await eventually(async () => (await drain(sqs, queues.dead)).find(m => {
    try { return JSON.parse(m.Body ?? '').messageId === illegal.messageId; } catch { return false; }
  }));
  expect(dead.MessageAttributes?.FailureCode?.StringValue).toBe('INVALID_ENVELOPE');
  const msg = envelope(cmd);
  const correlationId = randomUUID();
  await sqs.send(new SendMessageCommand({
    QueueUrl: queues.input, MessageBody: JSON.stringify({ ...msg, correlationId }),
    MessageGroupId: wallet.id, MessageDeduplicationId: msg.messageId,
  }));
  const row = await eventually(async () => one<{ id: string; payload: Record<string, unknown> }>(
    db.em.fork(), "select id,payload from outbox_messages where payload->>'causationId'=?", [msg.messageId]));
  expect(row.payload).toMatchObject({ eventId: row.id, eventType: 'WagerTransactionProcessed',
    correlationId, causationId: msg.messageId, aggregateId: wallet.id, version: 1,
    data: { money: cmd.money, status: 'PROCESSED' } });
  const logged = await eventually(async () => {
    const lines = docker('compose','logs','--no-log-prefix','--tail','120','app').split(/\r?\n/);
    for (const line of lines) {
      try {
        const item = JSON.parse(line);
        if (item.event === 'wager_result' && item.correlationId === correlationId) return item;
      } catch { /* Compose and dependency text lines */ }
    }
  });
  expect(logged).toMatchObject({ messageId: msg.messageId, providerId: cmd.providerId, walletId: wallet.id,
    status: 'PROCESSED', severity: 'info' });
  expect(logged.at).toBeString();
  expect(logged.instanceId).toBeString();
  expect(logged.transactionId).toBeString();
  expect(logged).not.toHaveProperty('money');
  expect(logged).not.toHaveProperty('payload');
  await reconcile(urls[0], wallet, '0.00', 0);
}, 25_000);

test('F06 tráfego real incrementa contadores, histograma, duplicatas e gauge da DLQ', async () => {
  const wallet = await open(urls[0]);
  const cmd = command(wallet);
  const count = 'wager_transactions_total';
  const duplicate = 'wager_duplicate_total';
  const latency = 'wager_processing_duration_seconds_count';
  const [beforeCount, beforeDuplicate, beforeLatency] = await Promise.all([total(count), total(duplicate), total(latency)]);
  expect((await post(urls[0], cmd)).status).toBe(200);
  expect((await post(urls[1], cmd)).body.idempotentReplay).toBe(true);
  expect((await post(urls[2], command(wallet, { money: { amount: '999.00', currency: 'BRL' } }))).status).toBe(422);
  expect(await total(count)).toBeGreaterThanOrEqual(beforeCount + 2);
  expect(await total(duplicate)).toBeGreaterThanOrEqual(beforeDuplicate + 1);
  expect(await total(latency)).toBeGreaterThanOrEqual(beforeLatency + 3);

  const queues = await discoverQueues(sqs);
  const dlqMetric = 'sqs_dlq_messages';
  const beforeDlq = Math.max(...await samples(dlqMetric));
  const marker = randomUUID();
  await sqs.send(new SendMessageCommand({
    QueueUrl: queues.input, MessageGroupId: marker, MessageDeduplicationId: marker, MessageBody: marker,
  }));
  await eventually(async () => Math.max(...await samples(dlqMetric)) > beforeDlq ? true : undefined);
  await eventually(async () => (await drain(sqs, queues.dead)).find(message => message.Body === marker));
  await reconcile(urls[0], wallet, '90.00', 2);
}, 25_000);

test('F07 morte real do consumidor antes do commit: sem ACK e com um único efeito após redelivery', async () => {
  const wallet = await open(urls[0]);
  const cmd = command(wallet);
  const messageId = randomUUID();
  const queues = await isolatedQueues(sqs);
  const locked = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const lock = transaction(db, async em => {
    await em.execute('select id from wallets where id=? for update', [wallet.id]);
    locked.resolve();
    await release.promise;
  });
  let worker: ReturnType<typeof Bun.spawn> | undefined;
  try {
    await Promise.race([locked.promise, lock]);
    await send(sqs, queues.urls.input, cmd, messageId);
    worker = Bun.spawn([process.execPath, 'test/crash-worker.ts', JSON.stringify(queues.urls)],
      { stdout: 'ignore', stderr: 'inherit' });
    const em = db.em.fork();
    await eventually(async () => {
      const blocked = await one<{ n: number }>(em, `
        select count(*)::int n from pg_stat_activity
        where wait_event_type='Lock' and query ilike '%wallets%for update%'`);
      return blocked?.n ? true : undefined;
    }, 2800);
    expect(await one(em, 'select 1 from inbox_messages where message_id=?', [messageId])).toBeUndefined();
    worker.kill(9);
    await worker.exited;
    release.resolve();
    await lock;
    expect(await one(em, 'select 1 from wager_transactions where external_id=?', [cmd.externalTransactionId])).toBeUndefined();
    const replacement = new WagerConsumer(sqs, queues.urls, new Wagering(db));
    expect(await eventually(async () => await replacement.poll() ? true : undefined)).toBe(true);
    expect(await one(em, 'select processed_at is not null as done from inbox_messages where message_id=?', [messageId]))
      .toEqual({ done: true });
    await reconcile(urls[0], wallet, '90.00', 2);
  } finally {
    release.resolve();
    await lock;
    if (worker?.exitCode === null) worker.kill(9);
    await worker?.exited;
    await queues.cleanup();
  }
}, 30_000);

test('F08 PostgreSQL aborta um deadlock real e ambas as wallets ficam consistentes', async () => {
  const [a,b] = await Promise.all([open(urls[0]), open(urls[1])]);
  const [heldA, heldB] = [Promise.withResolvers<void>(), Promise.withResolvers<void>()];
  const competing = (first: string, second: string, acquired: typeof heldA, other: typeof heldA) =>
    transaction(db, async em => {
      await em.execute('select id from wallets where id=? for update', [first]);
      acquired.resolve();
      await other.promise;
      await em.execute('select id from wallets where id=? for update', [second]);
    });
  const results = await Promise.allSettled([
    competing(a.id, b.id, heldA, heldB), competing(b.id, a.id, heldB, heldA),
  ]);
  expect(results.filter(item => item.status === 'fulfilled')).toHaveLength(1);
  const aborted = results.find(item => item.status === 'rejected') as PromiseRejectedResult;
  expect(aborted.reason.code).toBe('40P01');
  expect((await post(urls[2], command(a))).status).toBe(200);
  await reconcile(urls[0], a, '90.00', 2);
  await reconcile(urls[1], b, '100.00', 1);
}, 15_000);


test('F11 um scheduler reagenda falha financeira e avança outra wallet', async () => {
  const name = 'scheduler_' + randomUUID().replaceAll('-', '');
  const url = new URL(process.env.DATABASE_URL ?? 'postgres://wager:wager@127.0.0.1:54328/wager');
  const admin = db.em.fork();
  await admin.execute('create database ' + name);
  url.pathname = '/' + name;
  const isolated = await connect(url.toString());
  try {
    await isolated.getMigrator().up();
    const service = new Wagering(isolated);
    const em = isolated.em.fork();
    async function pendingRefund() {
      const wallet = await service.open(randomUUID(), { currency: 'BRL', amount: '100.00' }, randomUUID());
      const bet = command(wallet);
      const refund = command(wallet, { kind: 'REFUND', referenceExternalTransactionId: bet.externalTransactionId });
      const submit = (cmd: typeof bet) => service.submit({
        command: cmd, idempotencyKey: cmd.externalTransactionId, correlationId: randomUUID(),
      });
      const pending = await submit(refund);
      expect(pending.status).toBe('PENDING_REFERENCE');
      expect((await submit(bet)).status).toBe('PROCESSED');
      return { walletId: wallet.id, id: pending.transactionId };
    }
    const a = await pendingRefund();
    const b = await pendingRefund();
    await em.execute(`
      create function fail_scheduler_event() returns trigger language plpgsql as $$
      begin
        if new.payload->'data'->>'transactionId' = '${a.id}' and new.event_type = 'WagerTransactionProcessed' then
          raise exception 'scheduler test fault' using errcode = 'P0001';
        end if;
        return new;
      end $$;
      create trigger fail_scheduler_event before insert on outbox_messages
        for each row execute function fail_scheduler_event();
    `);
    await em.execute(`
      update wager_transactions set attempts=4, next_attempt_at=clock_timestamp() -
        case when id=? then interval '2 minutes' else interval '1 minute' end where id in (?,?)`, [a.id, a.id, b.id]);
    const outcome = (id: string) => one(em,
      'select status, (next_attempt_at > clock_timestamp()) as deferred from wager_transactions where id=?', [id]);

    expect(await service.reprocessOne()).toBe(true);
    expect(await outcome(a.id)).toEqual({ status: 'PENDING_REFERENCE', deferred: true });
    expect(await service.reconcile(a.walletId, randomUUID())).toMatchObject({
      consistent: true, storedBalance: { amount: '90.00' }, checkedEntries: 2,
    });
    expect(await one(em, 'select count(*)::int as total from wallet_ledger_entries where transaction_id=?', [a.id]))
      .toEqual({ total: 0 });

    expect(await service.reprocessOne()).toBe(true);
    expect(await outcome(b.id)).toEqual({ status: 'PROCESSED', deferred: null });
    expect(await service.reconcile(b.walletId, randomUUID())).toMatchObject({
      consistent: true, storedBalance: { amount: '100.00' }, checkedEntries: 3,
    });

    for (let failure = 2; failure <= 5; failure++) {
      await em.execute("update wager_transactions set next_attempt_at=clock_timestamp()-interval '1s' where id=?", [a.id]);
      expect(await service.reprocessOne()).toBe(true);
      expect(await one(em, 'select status,reference_failures from wager_transactions where id=?', [a.id]))
        .toEqual({ status: failure < 5 ? 'PENDING_REFERENCE' : 'FAILED', reference_failures: failure });
    }
    expect(await outcome(a.id)).toEqual({ status: 'FAILED', deferred: null });
    expect(await one(em, "select event_type from outbox_messages where payload->'data'->>'transactionId'=? and event_type='WagerTransactionFailed'", [a.id]))
      .toEqual({ event_type: 'WagerTransactionFailed' });
    expect(await service.reconcile(a.walletId, randomUUID())).toMatchObject({
      consistent: true, storedBalance: { amount: '90.00' }, checkedEntries: 2,
    });
  } finally {
    await isolated.close();
    await admin.execute('drop database ' + name);
  }
}, 25_000);

test('F09 publisher morre após claim e antes de chamar SQS; outro processo retoma', async () => {
  const name = 'before_send_' + randomUUID().replaceAll('-', '');
  const base = new URL(process.env.DATABASE_URL ?? 'postgres://wager:wager@127.0.0.1:54328/wager');
  const em = db.em.fork();
  await em.execute('create database ' + name);
  base.pathname = '/' + name;
  const isolated = await connect(base.toString());
  const workers: Array<ReturnType<typeof Bun.spawn>> = [];
  let queueUrl: string | undefined;
  try {
    await isolated.getMigrator().up();
    queueUrl = (await sqs.send(new CreateQueueCommand({
      QueueName: name + '.fifo', Attributes: { FifoQueue: 'true' },
    }))).QueueUrl!;
    const service = new Wagering(isolated);
    const wallet = await service.open(randomUUID(), { currency: 'BRL', amount: '0.00' }, randomUUID());
    const cmd = command(wallet, { kind: 'LOSS', money: { currency: 'BRL', amount: '0.00' } });
    await service.submit({ command: cmd, correlationId: randomUUID(), idempotencyKey: cmd.externalTransactionId });
    const start = (...args: string[]) => Bun.spawn(
      [process.execPath, 'test/publisher-process.ts', queueUrl!, ...args],
      { stdout: 'pipe', stderr: 'inherit', env: { ...process.env, DATABASE_URL: base.toString() } });
    const doomed = start('--pause-before-send');
    workers.push(doomed);
    const reader = doomed.stdout.getReader();
    const timer = setTimeout(() => doomed.kill(9), 7000);
    try {
      let marker = '';
      while (!marker.includes('CLAIMED_BEFORE_SQS_SEND')) {
        const { value, done } = await reader.read();
        if (done) throw new Error('Publisher saiu antes da barreira');
        marker += new TextDecoder().decode(value);
      }
    } finally {
      clearTimeout(timer);
      reader.releaseLock();
    }
    doomed.kill(9);
    expect(await doomed.exited).not.toBe(0);
    expect(await drain(sqs, queueUrl)).toHaveLength(0);
    const store = isolated.em.fork();
    const row = await one<{ id: string; attempts: number; published_at: string | null }>(store,
      'select id,attempts,published_at from outbox_messages where claim_token is not null');
    expect(row).toMatchObject({ attempts: 1, published_at: null });
    // A09 separately waits for the natural 30-second expiry.
    await store.execute("update outbox_messages set lease_until=clock_timestamp()-interval '1s' where id=?", [row!.id]);
    const replacement = start();
    workers.push(replacement);
    expect(JSON.parse(await new Response(replacement.stdout).text())).toEqual({ claimed: true });
    expect(await replacement.exited).toBe(0);
    const message = await eventually(async () => (await drain(sqs, queueUrl!))[0]);
    expect(JSON.parse(message.Body!).eventId).toBe(row!.id);
    expect((await service.reconcile(wallet.id, randomUUID())).consistent).toBe(true);
  } finally {
    for (const worker of workers) if (worker.exitCode === null) worker.kill(9);
    await Promise.all(workers.map(worker => worker.exited));
    if (queueUrl) await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
    await isolated.close();
    await em.execute('drop database ' + name);
  }
}, 28_000);

test('F10 replay de rejeição preserva snapshot, ROLLBACK(WIN) debita e chave é por provedor', async () => {
  const wallet = await open(urls[0], '0.00');
  const bet = command(wallet);
  const original = await post(urls[0], bet);
  expect(original.status).toBe(422);
  expect(original.body.balance.amount).toBe('0.00');
  const win = command(wallet, { kind: 'WIN' });
  expect((await post(urls[1], win)).body.balance.amount).toBe('10.00');
  expect((await post(urls[2], bet)).body).toEqual({ ...original.body, idempotentReplay: true });
  const rejectedReference = await post(urls[1], command(wallet, {
    kind: 'REFUND', referenceExternalTransactionId: bet.externalTransactionId,
  }));
  expect(rejectedReference.body.failureCode).toBe('REFERENCE_NOT_PROCESSED');
  const undo = await post(urls[2], command(wallet, { kind: 'ROLLBACK', referenceExternalTransactionId: win.externalTransactionId }));
  expect(undo.status).toBe(200);
  expect(undo.body.balance.amount).toBe('0.00');
  const loss = command(wallet, { kind: 'LOSS', money: { amount: '0.00', currency: 'BRL' } });
  const results = await Promise.all([post(urls[0], loss), post(urls[1], { ...loss, providerId: 'provider-other' })]);
  expect(results.map(result => result.status)).toEqual([200, 200]);
  expect(results[0].body.transactionId).not.toBe(results[1].body.transactionId);
  await reconcile(urls[0], wallet, '0.00', 2);
}, 15_000);


test('F12 SQS preserva comandos durante quedas de PostgreSQL de 5, 15 e 30 segundos', async () => {
  const queues = await discoverQueues(sqs);
  const depth = async () => {
    const { Attributes } = await sqs.send(new GetQueueAttributesCommand({
      QueueUrl: queues.input, AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
    }));
    return Number(Attributes?.ApproximateNumberOfMessages ?? 0) +
      Number(Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0);
  };
  for (const seconds of [5, 15, 30]) {
    await eventually(async () => await depth() === 0 ? true : undefined);
    const wallet = await open(urls[0]);
    const cmd = command(wallet);
    const messageId = randomUUID();
    const url = '/providers/' + cmd.providerId + path + '/' + cmd.externalTransactionId;
    docker('compose', 'stop', 'postgres');
    try {
      await send(sqs, queues.input, cmd, messageId);
      await Bun.sleep(seconds * 1000);
      expect(await depth()).toBe(1);
    } finally {
      docker('compose', 'start', 'postgres');
      await Promise.all(urls.map(ready));
    }
    const result = await eventually(async () => {
      const response = await json<Outcome>(urls[0], url);
      return response.status === 200 ? response.body : undefined;
    }, 46_000);
    expect(result.status).toBe('PROCESSED');
    expect((await post(urls[2], cmd)).body.idempotentReplay).toBe(true);
    expect(await one(db.em.fork(), 'select processed_at is not null as done from inbox_messages where message_id=?', [messageId]))
      .toEqual({ done: true });
    await reconcile(urls[0], wallet, '90.00', 2);
    console.log(JSON.stringify({ probe: 'F12', postgresOutageSeconds: seconds, appliedAfterRecovery: true }));
  }
}, 155_000);


test('F13 três schedulers isolam a carteira defeituosa até FAILED', async () => {
  async function pendingRefund(replica: number) {
    const wallet = await open(urls[replica]);
    const bet = command(wallet);
    const refund = command(wallet, { kind: 'REFUND', referenceExternalTransactionId: bet.externalTransactionId });
    const pending = await post(urls[replica], refund);
    expect(pending.body.status).toBe('PENDING_REFERENCE');
    await db.em.fork().execute("update wager_transactions set next_attempt_at=clock_timestamp()+interval '2 minutes' where id=?",
      [pending.body.transactionId]);
    expect((await post(urls[replica], bet)).body.status).toBe('PROCESSED');
    return { wallet, refund, id: pending.body.transactionId };
  }
  const [a,b] = await Promise.all([pendingRefund(0), pendingRefund(1)]);
  const hook = 'scheduler_' + randomUUID().replaceAll('-', '');
  const em = db.em.fork();
  await em.execute(`
    create function ${hook}() returns trigger language plpgsql as $$
    begin
      if new.payload->'data'->>'transactionId' = '${a.id}' and new.event_type='WagerTransactionProcessed' then
        raise exception 'scheduler replica test fault' using errcode='P0001';
      end if;
      return new;
    end $$;
    create trigger ${hook} before insert on outbox_messages for each row execute function ${hook}();
  `);
  try {
    await em.execute(`
      update wager_transactions set next_attempt_at=clock_timestamp() -
        case when id=? then interval '2 minutes' else interval '1 minute' end where id in (?,?)`, [a.id, a.id, b.id]);
    const state = (id: string) => one<{ status: string; reference_failures: number }>(
      em, 'select status,reference_failures from wager_transactions where id=?', [id]);
    await eventually(async () => (await state(b.id))?.status === 'PROCESSED' &&
      (await state(a.id))!.reference_failures > 0 ? true : undefined);
    for (let failure = 2; failure <= 5; failure++) {
      await em.execute("update wager_transactions set next_attempt_at=clock_timestamp()-interval '1s' where id=? and status='PENDING_REFERENCE' and reference_failures < ?",
        [a.id, failure]);
      await eventually(async () => (await state(a.id))!.reference_failures >= failure ? true : undefined);
    }
    expect(await state(a.id)).toEqual({ status: 'FAILED', reference_failures: 5 });
    const replay = await post(urls[2], a.refund);
    expect(replay.status).toBe(409);
    expect(replay.body).toMatchObject({
      transactionId: a.id, status: 'FAILED', failureCode: 'INFRASTRUCTURE_RETRIES_EXHAUSTED',
      balance: { amount: '90.00' }, idempotentReplay: true,
    });
    await reconcile(urls[0], a.wallet, '90.00', 2);
    await reconcile(urls[1], b.wallet, '100.00', 3);
  } finally {
    await em.execute(`drop trigger ${hook} on outbox_messages; drop function ${hook}()`);
  }
}, 35_000);

async function scheduleBlockedRefunds(database: Database, a: string, b: string) {
  const hook = 'block_pending_' + a.replaceAll('-', '');
  // Make both rows eligible and arm the trigger in the same commit, so no worker can beat the failpoint.
  await transaction(database, async em => {
    await em.execute("update wager_transactions set next_attempt_at=clock_timestamp()-case when id=? then interval '2 minutes' else interval '1 minute' end where id in (?,?)", [a, a, b]);
    await em.execute('create function ' + hook + "() returns trigger language plpgsql as $$ begin raise exception 'scheduler UPDATE rejected' using errcode='P0001'; end $$");
    await em.execute("create trigger " + hook + " before update on wager_transactions for each row when (old.id='" + a +
      "'::uuid and old.status='PENDING_REFERENCE') execute function " + hook + "()");
  });
  return () => database.em.fork().execute('drop trigger ' + hook + ' on wager_transactions; drop function ' + hook + '()');
}

function pendingState(database: Database, id: string) {
  return one<{ status: string; reference_failures: number; movements: number }>(database.em.fork(),
    'select status,reference_failures,(select count(*)::int from wallet_ledger_entries where transaction_id=w.id) as movements from wager_transactions w where id=?',
    [id]);
}

function appEvents() {
  return docker('compose', 'logs', '--no-log-prefix', '--tail', '250', 'app').split(/\r?\n/).flatMap(line => {
    try { return [JSON.parse(line) as Record<string, unknown>]; } catch { return []; }
  });
}

test('F14 scheduler único avança outra carteira quando o UPDATE de reagendamento também falha', async () => {
  const name = 'blocked_' + randomUUID().replaceAll('-', '');
  const url = new URL(process.env.DATABASE_URL ?? 'postgres://wager:wager@127.0.0.1:54328/wager');
  const admin = db.em.fork();
  await admin.execute('create database ' + name);
  url.pathname = '/' + name;
  const isolated = await connect(url.toString());
  try {
    await isolated.getMigrator().up();
    const worker = new Wagering(isolated);
    async function pending() {
      const wallet = await worker.open(randomUUID(), { currency: 'BRL', amount: '100.00' }, randomUUID());
      const bet = command(wallet);
      const submit = (cmd: typeof bet) => worker.submit({
        command: cmd, idempotencyKey: cmd.externalTransactionId, correlationId: randomUUID(),
      });
      const refund = await submit(command(wallet, { kind: 'REFUND', referenceExternalTransactionId: bet.externalTransactionId }));
      expect(refund.status).toBe('PENDING_REFERENCE');
      expect((await submit(bet)).status).toBe('PROCESSED');
      return { wallet, id: refund.transactionId };
    }
    const a = await pending();
    const b = await pending();
    const remove = await scheduleBlockedRefunds(isolated, a.id, b.id);
    try {
      expect(await worker.reprocessOne()).toBe(true);
      expect(await worker.reprocessOne()).toBe(true);
      expect(await pendingState(isolated, a.id)).toEqual({ status: 'PENDING_REFERENCE', reference_failures: 0, movements: 0 });
      expect(await pendingState(isolated, b.id)).toMatchObject({ status: 'PROCESSED', movements: 1 });
      for (let i = 0; i < 5; i++) expect(await worker.reprocessOne()).toBe(false);
      expect(await worker.reconcile(a.wallet.id, randomUUID())).toMatchObject({
        consistent: true, storedBalance: { amount: '90.00' }, checkedEntries: 2,
      });
    } finally {
      await remove();
    }
    await eventually(async () => await worker.reprocessOne() || undefined);
    expect(await pendingState(isolated, a.id)).toMatchObject({ status: 'PROCESSED', movements: 1 });
    for (const { wallet } of [a, b]) {
      expect(await worker.reconcile(wallet.id, randomUUID())).toMatchObject({
        consistent: true, storedBalance: { amount: '100.00' }, checkedEntries: 3,
      });
    }
  } finally {
    await isolated.close();
    await admin.execute('drop database ' + name);
  }
}, 25_000);

test('F15 três schedulers limitam retentativas quando o UPDATE da pendência é recusado', async () => {
  async function pending(replica: number) {
    const wallet = await open(urls[replica]);
    const bet = command(wallet);
    const refund = await post(urls[replica],
      command(wallet, { kind: 'REFUND', referenceExternalTransactionId: bet.externalTransactionId }));
    expect(refund.body.status).toBe('PENDING_REFERENCE');
    await db.em.fork().execute("update wager_transactions set next_attempt_at=clock_timestamp()+interval '2 minutes' where id=?",
      [refund.body.transactionId]);
    expect((await post(urls[replica], bet)).body.status).toBe('PROCESSED');
    return { wallet, id: refund.body.transactionId };
  }
  const [a, b] = await Promise.all([pending(0), pending(1)]);
  const remove = await scheduleBlockedRefunds(db, a.id, b.id);
  const deferred = () => appEvents().filter(line => line.event === 'reference_write_deferred' && line.transactionId === a.id);
  try {
    await eventually(async () => (await pendingState(db, b.id))?.status === 'PROCESSED' || undefined);
    await eventually(async () => deferred().length > 0 || undefined);
    await Bun.sleep(800);
    const attempts = deferred();
    expect(attempts.length).toBeGreaterThan(0);
    expect(attempts.length).toBeLessThanOrEqual(3);
    expect(new Set(attempts.map(item => item.instanceId)).size).toBe(attempts.length);
    expect(attempts.every(item => item.errorCode === 'P0001')).toBe(true);
    expect(await pendingState(db, a.id)).toEqual({ status: 'PENDING_REFERENCE', reference_failures: 0, movements: 0 });
    await reconcile(urls[0], a.wallet, '90.00', 2);
  } finally {
    await remove();
  }
  await eventually(async () => (await pendingState(db, a.id))?.status === 'PROCESSED' || undefined);
  await reconcile(urls[1], a.wallet, '100.00', 3);
  await reconcile(urls[2], b.wallet, '100.00', 3);
}, 32_000);

test('F16 erro de INSERT financeiro omite payload nos logs PostgreSQL e correlaciona HTTP e SQS', async () => {
  const wallet = await open(urls[0]);
  const marker = 'finance-probe-' + randomUUID();
  const hook = 'log_probe_' + wallet.id.replaceAll('-', '');
  const diagnostic = 'audit outbox probe ' + wallet.id;
  const httpId = randomUUID();
  const messageId = randomUUID();
  const sqsId = randomUUID();
  const amount = '73.61';
  const http = command(wallet, { externalTransactionId: marker + '-http', money: { amount, currency: 'BRL' } });
  const queued = command(wallet, { externalTransactionId: marker + '-sqs', money: { amount, currency: 'BRL' } });
  const em = db.em.fork();
  await em.execute("create function " + hook + "() returns trigger language plpgsql as $$ begin if new.aggregate_id='" +
    wallet.id + "'::uuid and new.event_type='WagerTransactionProcessed' then raise exception '" + diagnostic +
    "' using errcode='P0001', detail=new.payload::text; end if; return new; end $$; create trigger " + hook +
    " before insert on outbox_messages for each row execute function " + hook + "()");
  try {
    const response = await fetch(urls[0] + path, {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': http.externalTransactionId,
        'X-Correlation-Id': httpId }, body: JSON.stringify(http),
    });
    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ code: 'SERVICE_UNAVAILABLE' });
    const { input } = await discoverQueues(sqs);
    await sqs.send(new SendMessageCommand({
      QueueUrl: input, MessageGroupId: wallet.id, MessageDeduplicationId: messageId,
      MessageBody: JSON.stringify({ ...envelope(queued, messageId), correlationId: sqsId }),
    }));
    const sqsLog = await eventually(async () => appEvents().find(line => line.event === 'sqs_retry' && line.messageId === messageId));
    const pg = docker('compose', 'logs', '--no-log-prefix', '--tail', '150', 'postgres');
    expect(pg.split(/\r?\n/).filter(line => line.includes('ERROR:') && line.includes(diagnostic)).length).toBeGreaterThanOrEqual(2);
    expect(pg.includes(marker)).toBe(false);
    expect(pg.includes('"amount": "' + amount + '"')).toBe(false);
    expect(docker('compose', 'logs', '--no-log-prefix', '--tail', '250', 'app').includes(marker)).toBe(false);
    expect(response.headers.get('x-correlation-id')).toBe(httpId);
    expect(appEvents().find(line => line.event === 'http_unavailable' && line.correlationId === httpId))
      .toMatchObject({ errorCode: 'P0001' });
    expect(sqsLog).toMatchObject({ correlationId: sqsId, walletId: wallet.id, errorCode: 'P0001' });
    await reconcile(urls[0], wallet, '100.00', 1);
    expect(await one(em, 'select message_id from inbox_messages where message_id=?', [messageId])).toBeUndefined();
  } finally {
    await em.execute('drop trigger ' + hook + ' on outbox_messages; drop function ' + hook + '()');
  }
  await eventually(async () => (await one<{ status: string }>(em,
    'select status from wager_transactions where external_id=?', [queued.externalTransactionId]))?.status === 'PROCESSED' || undefined, 20_000);
  await reconcile(urls[1], wallet, '26.39', 2);
}, 38_000);
