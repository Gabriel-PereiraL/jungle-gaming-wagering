import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import {
  ChangeMessageVisibilityCommand, CreateQueueCommand, DeleteQueueCommand, GetQueueAttributesCommand, ReceiveMessageCommand,
} from '@aws-sdk/client-sqs';
import { all, connect, one, transaction, type Database } from '../src/db';
import { createSqs, discoverQueues } from '../src/messaging';
import { Wagering } from '../src/wagering';
import { appUrls, command, docker, drain, eventually, json, open, post, ready, reconcile, send, type Outcome } from './fixtures';

let db: Database;
let urls: string[];
const sqs = createSqs();
const path = '/wagering/transactions';

beforeAll(async () => {
  urls = appUrls();
  expect(new Set(urls).size).toBe(3);
  await Promise.all(urls.map(ready));
  db = await connect();
}, 25_000);
afterAll(async () => { await db?.close(); sqs.destroy(); });

async function race<T>(wallets: string[], actions: (() => Promise<T>)[]): Promise<T[]> {
  const acquired = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const holder = transaction(db, async em => {
    for (const id of wallets) await em.execute('select id from wallets where id=? for update', [id]);
    acquired.resolve();
    await release.promise;
  });
  await Promise.race([acquired.promise, holder]);
  const results = Promise.all(actions.map(action => action()));
  try {
    await eventually(async () => {
      const locks = await one<{ n: number }>(db.em.fork(), `
        select count(*)::int n from pg_stat_activity
        where wait_event_type='Lock' and query ilike '%wallets%for update%'`);
      return locks && locks.n >= actions.length ? true : undefined;
    }, 1800);
  } finally {
    release.resolve();
    await holder;
    await results;
  }
  return results;
}

async function outboxRows(transactionId: string) {
  return all<{ id: string; event_type: string }>(db.em.fork(),
    "select id,event_type from outbox_messages where payload->'data'->>'transactionId'=? order by event_type",
    [transactionId]);
}

async function metric(url: string, key: string) {
  const response = await fetch(url + '/metrics', { signal: AbortSignal.timeout(1500) });
  const line = (await response.text()).split('\n').find(line => line.startsWith(key + ' '));
  return Number(line?.slice(key.length + 1) ?? 0);
}

test('A01 REFUND e ROLLBACK(REFUND) concorrentes: só um débito compensatório e replay histórico', async () => {
  const wallet = await open(urls[0]);
  const bet = command(wallet, { money: { amount: '25.00', currency: 'BRL' } });
  expect((await post(urls[0], bet)).body.balance.amount).toBe('75.00');
  const refund = command(wallet, { kind: 'REFUND', money: bet.money, referenceExternalTransactionId: bet.externalTransactionId });
  const refundResult = await post(urls[1], refund);
  expect(refundResult.body.balance.amount).toBe('100.00');
  const undo = command(wallet, { kind: 'ROLLBACK', money: bet.money, referenceExternalTransactionId: refund.externalTransactionId });
  const results = await race([wallet.id], [
    () => post(urls[0], undo),
    () => post(urls[1], { ...undo, externalTransactionId: randomUUID() }),
    () => post(urls[2], { ...undo, externalTransactionId: randomUUID() }),
  ]);
  expect(results.map(x => x.status).sort()).toEqual([200, 422, 422]);
  const accepted = results.find(x => x.status === 200)!.body;
  expect(accepted.balance.amount).toBe('75.00');
  expect(accepted.version).toBe(4);
  for (const reject of results.filter(x => x.status === 422)) {
    expect(reject.body.failureCode).toBe('REVERSAL_ALREADY_APPLIED');
    expect((await outboxRows(reject.body.transactionId)).map(e => e.event_type)).toEqual(['WagerTransactionRejected']);
  }
  expect((await outboxRows(accepted.transactionId)).map(e => e.event_type))
    .toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);
  expect((await post(urls[2], refund)).body).toEqual({ ...refundResult.body, idempotentReplay: true });
  await reconcile(urls[0], wallet, '75.00', 4);
}, 18_000);

test('A02 REFUND, REFUND e ROLLBACK(BET) disputam a mesma origem entre três processos', async () => {
  const wallet = await open(urls[0]);
  const bet = command(wallet);
  await post(urls[0], bet);
  const results = await race([wallet.id], (['REFUND','REFUND','ROLLBACK'] as const).map((kind, i) =>
    () => post(urls[i], command(wallet, { kind, referenceExternalTransactionId: bet.externalTransactionId }))));
  expect(results.map(x => x.status).sort()).toEqual([200, 422, 422]);
  expect(results.filter(x => x.body.failureCode === 'REVERSAL_ALREADY_APPLIED')).toHaveLength(2);
  await reconcile(urls[0], wallet, '100.00', 3);
}, 18_000);

test('A03 chave igual, payload divergente, carteiras distintas e cruzamento de identidades', async () => {
  const [a,b] = await Promise.all([open(urls[0]), open(urls[1])]);
  const key = randomUUID();
  const first = command(a);
  const second = command(b, { money: { amount: '20.00', currency: 'BRL' } });
  const results = await race([a.id, b.id], [() => post(urls[0], first, key), () => post(urls[1], second, key)]);
  expect(results.map(x => x.status).sort()).toEqual([200, 409]);
  const aWon = results[0].status === 200;
  const winner = aWon ? first : second;
  const loser = aWon ? second : first;
  expect((await post(urls[2], { ...winner, roundId: 'other' }, key)).status).toBe(409);
  expect((await post(urls[2], winner, randomUUID())).status).toBe(409);
  const other = await post(urls[2], loser);
  expect(other.status).toBe(200);
  expect((await post(urls[2], loser, key)).status).toBe(409);
  expect((await post(urls[2], winner, loser.externalTransactionId)).status).toBe(409);
  await reconcile(urls[0], a, '90.00', 2);
  await reconcile(urls[1], b, '80.00', 2);
}, 18_000);

test('A04 referências encadeadas chegam invertidas via SQS e aplicam uma vez cada', async () => {
  const queues = await discoverQueues(sqs);
  const wallet = await open(urls[0]);
  const bet = command(wallet);
  const refund = command(wallet, { kind: 'REFUND', referenceExternalTransactionId: bet.externalTransactionId });
  const rollback = command(wallet, { kind: 'ROLLBACK', referenceExternalTransactionId: refund.externalTransactionId });
  const lookup = (external: string) => '/providers/' + bet.providerId + path + '/' + external;
  for (const cmd of [rollback, refund]) {
    const messageId = randomUUID();
    await send(sqs, queues.input, cmd, messageId);
    await send(sqs, queues.input, cmd, messageId);
    await eventually(async () => (await json<Outcome>(urls[1], lookup(cmd.externalTransactionId)))
      .body.status === 'PENDING_REFERENCE' ? true : undefined);
  }
  await send(sqs, queues.input, bet);
  for (const cmd of [bet, refund, rollback]) {
    const result = await eventually(async () => {
      const reply = await json<Outcome>(urls[2], lookup(cmd.externalTransactionId));
      return reply.body.status === 'PROCESSED' ? reply.body : undefined;
    });
    expect((await outboxRows(result.transactionId)).map(e => e.event_type)).toEqual(cmd === bet
      ? ['WagerTransactionProcessed','WalletBalanceChanged']
      : ['WagerTransactionPendingReference','WagerTransactionProcessed','WalletBalanceChanged']);
  }
  await reconcile(urls[0], wallet, '90.00', 4);
}, 30_000);

test('A05 precisão acima de 2^53, overflow auditável, versões e validação de contrato', async () => {
  const wallet = await open(urls[0], '999999999999999999.99');
  const cent = { amount: '0.01', currency: 'BRL' };
  const overflow = await post(urls[1], command(wallet, { kind: 'WIN', money: cent }));
  expect(overflow.status).toBe(422);
  expect(overflow.body.failureCode).toBe('BALANCE_LIMIT_EXCEEDED');
  expect(overflow.body.version).toBe(1);
  const bet = await post(urls[2], command(wallet, { money: cent }));
  expect(bet.body.balance.amount).toBe('999999999999999999.98');
  for (const amount of [0.1, '0.001', '1e-1', '-0.01', 'NaN', 'Infinity']) {
    const invalid = await json(urls[0], path, { ...command(wallet), money: { ...cent, amount } }, randomUUID());
    expect(invalid.status).toBe(400);
  }
  const mismatch = await post(urls[0], command(wallet, { money: { ...cent, currency: 'USD' } }));
  expect(mismatch.body.failureCode).toBe('CURRENCY_MISMATCH');
  await reconcile(urls[0], wallet, '999999999999999999.98', 2);
});

test('A06 schema preserva operação terminal sem ledger contra DELETE e novo replay', async () => {
  const wallet = await open(urls[0], '0.00');
  const loss = command(wallet, { kind: 'LOSS', money: { amount: '0.00', currency: 'BRL' } });
  const outcome = await post(urls[0], loss);
  expect(outcome.status).toBe(200);
  const em = db.em.fork();
  const deleted = await em.execute('delete from wager_transactions where id=? returning id', [outcome.body.transactionId])
    .then(rows => ({ deleted: rows.length === 1 }), () => ({ deleted: false }));
  const replay = await post(urls[1], loss);
  console.log(JSON.stringify({ probe: 'A06', deleted: deleted.deleted,
    original: outcome.body.transactionId, replay: replay.body.transactionId, replayFlag: replay.body.idempotentReplay }));
  expect(deleted.deleted).toBe(false);
  expect(replay.body).toEqual({ ...outcome.body, idempotentReplay: true });
  await reconcile(urls[0], wallet, '0.00', 0);
});

test('A07 reconciliação detecta desvio injetado e não corrige saldo, ledger ou versão', async () => {
  const wallet = await open(urls[0]);
  await post(urls[1], command(wallet));
  const em = db.em.fork();
  const key = 'wallet_reconciliation_mismatch_total';
  const before = await metric(urls[2], key);
  await em.execute('update wallets set balance=balance+0.01 where id=?', [wallet.id]);
  try {
    const res = await json<{
      consistent: boolean; difference: { amount: string }; checkedEntries: number;
      storedBalance: { amount: string }; calculatedBalance: { amount: string };
    }>(urls[2], '/wallets/' + wallet.id + '/reconciliation', {});
    expect(res.status).toBe(200);
    expect(res.body.consistent).toBe(false);
    expect(res.body.difference.amount).toBe('0.01');
    expect(res.body.storedBalance.amount).toBe('90.01');
    expect(res.body.calculatedBalance.amount).toBe('90.00');
    expect(res.body.checkedEntries).toBe(2);
    expect(await one(em, 'select balance::text,version from wallets where id=?', [wallet.id]))
      .toEqual({ balance: '90.01', version: 2 });
    expect(await metric(urls[2], key)).toBe(before + 1);
  } finally {
    await em.execute('update wallets set balance=balance-0.01 where id=?', [wallet.id]);
  }
  await reconcile(urls[0], wallet, '90.00', 2);
});

test('A08 liveness, readiness, outbox e lag durante indisponibilidade real do SQS', async () => {
  const wallet = await open(urls[0], '0.00');
  const cmd = command(wallet);
  const key = 'outbox_lag_seconds';
  const before = await metric(urls[0], key);
  docker('compose','pause','sqs');
  try {
    const outcome = await post(urls[0], cmd);
    expect(outcome.status).toBe(422);
    expect(outcome.body.failureCode).toBe('INSUFFICIENT_FUNDS');
    expect((await json(urls[1], '/health/live')).status).toBe(200);
    expect((await json(urls[1], '/health/ready')).status).toBe(503);
    const id = outcome.body.transactionId;
    await eventually(async () => {
      const row = await one<{ n: number }>(db.em.fork(), `
        select count(*)::int n from outbox_messages where payload->'data'->>'transactionId'=? and published_at is null
        `, [id]);
      return row?.n === 1 ? true : undefined;
    });
    const observed = await eventually(async () => {
      const current = await metric(urls[0], key);
      return current > before + 0.5 ? current : undefined;
    }, 7000).catch(() => metric(urls[0], key));
    console.log(JSON.stringify({ probe: 'A08', before, during: observed }));
    expect(observed).toBeGreaterThan(before + 0.5);
    expect(outcome.body.status).toBe('REJECTED');
  } finally {
    docker('compose','unpause','sqs');
    await Promise.all(urls.map(ready));
  }
  await eventually(async () => {
    const row = await one<{ published_at: string | null }>(db.em.fork(), `
      select o.published_at from outbox_messages o join wager_transactions t
      on o.payload->'data'->>'transactionId'=t.id::text where t.external_id=?`, [cmd.externalTransactionId]);
    return row?.published_at || undefined;
  });
  await reconcile(urls[0], wallet, '0.00', 0);
}, 30_000);

test('A09 dois publishers reais, queda pós-envio, redelivery físico e dedup persistente do consumidor', async () => {
  const name = 'audit_' + randomUUID().replaceAll('-', '');
  const em = db.em.fork();
  await em.execute('create database ' + name);
  const clientUrl = new URL(process.env.DATABASE_URL ?? 'postgres://wager:wager@127.0.0.1:54328/wager');
  clientUrl.pathname = '/' + name;
  const queueUrl = (await sqs.send(new CreateQueueCommand({
    QueueName: name + '.fifo', Attributes: { FifoQueue: 'true', VisibilityTimeout: '2' },
  }))).QueueUrl!;
  const isolated = await connect(clientUrl.toString());
  const workers: Array<ReturnType<typeof Bun.spawn>> = [];
  try {
    await isolated.getMigrator().up();
    const isolatedEm = isolated.em.fork();
    const wagering = new Wagering(isolated);
    const wallet = await wagering.open(randomUUID(), { amount: '0.00', currency: 'BRL' }, randomUUID());
    const cmd = command(wallet, { kind: 'LOSS', money: { amount: '0.00', currency: 'BRL' } });
    const result = await wagering.submit({ command: cmd, idempotencyKey: cmd.externalTransactionId, correlationId: randomUUID() });
    const row = await one<{ id: string }>(isolatedEm, 'select id from outbox_messages');
    expect(row?.id).toBeDefined();

    await isolatedEm.execute('create table observed_events(event_id uuid primary key)');
    const spawn = (...args: string[]) => Bun.spawn([process.execPath, 'test/publisher-process.ts', queueUrl, ...args], {
      env: { ...process.env, DATABASE_URL: clientUrl.toString() }, stdout: 'pipe', stderr: 'inherit',
    });
    const doomed = spawn('--pause-after-send');
    workers.push(doomed);
    const physical = await eventually(async () => {
      const response = await sqs.send(new ReceiveMessageCommand({
        QueueUrl: queueUrl, WaitTimeSeconds: 1, MaxNumberOfMessages: 1,
      }));
      return response.Messages?.[0];
    });
    const event = JSON.parse(physical.Body!);
    expect(event.eventId).toBe(row!.id);
    expect(event.data.transactionId).toBe(result.transactionId);
    expect(event.version).toBe(1);
    expect(event.data.money).toEqual({ amount: '0.00', currency: 'BRL' });
    expect((await one<{ published_at: string | null }>(isolatedEm, 'select published_at from outbox_messages'))
      ?.published_at).toBeNull();
    doomed.kill(9);
    await doomed.exited;
    expect(await one(isolatedEm, 'select attempts,(published_at is null) as pending from outbox_messages'))
      .toEqual({ attempts: 1, pending: true });
    const lease = await one<{ remaining: number }>(isolatedEm,
      'select extract(epoch from lease_until-clock_timestamp())::float8 as remaining from outbox_messages');
    expect(lease!.remaining).toBeGreaterThan(0);
    await eventually(async () => {
      const state = await one<{ expired: boolean }>(isolatedEm,
        'select lease_until <= clock_timestamp() as expired from outbox_messages');
      return state?.expired ? true : undefined;
    }, 33_000);
    const contenders = [spawn(), spawn()];
    workers.push(...contenders);
    const answers = await Promise.all(contenders.map(async worker => {
      const body = await new Response(worker.stdout).text();
      expect(await worker.exited).toBe(0);
      return JSON.parse(body).claimed as boolean;
    }));
    expect(answers.sort()).toEqual([false,true]);
    expect(await one(isolatedEm, 'select attempts,(published_at is not null) as done from outbox_messages'))
      .toEqual({ attempts: 2, done: true });

    await isolatedEm.execute('insert into observed_events values (?) on conflict do nothing', [event.eventId]);
    await sqs.send(new ChangeMessageVisibilityCommand({
      QueueUrl: queueUrl, ReceiptHandle: physical.ReceiptHandle, VisibilityTimeout: 0,
    })).catch(() => undefined);
    const again = await eventually(async () => (await drain(sqs, queueUrl))[0]);
    expect(again.MessageId).toBe(physical.MessageId);
    expect(JSON.parse(again.Body!).eventId).toBe(event.eventId);
    await isolatedEm.execute('insert into observed_events values (?) on conflict do nothing', [event.eventId]);
    expect(await one(isolatedEm, 'select count(*)::int n from observed_events')).toEqual({ n: 1 });
    expect(await drain(sqs, queueUrl)).toHaveLength(0);
    expect((await wagering.reconcile(wallet.id, randomUUID())).consistent).toBe(true);
  } finally {
    for (const worker of workers) if (worker.exitCode === null) worker.kill(9);
    await Promise.all(workers.map(worker => worker.exited));
    await isolated.close();
    await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
    await em.execute('drop database ' + name);
  }
}, 55_000);

test('A10 reconciliação sob escrita concorrente lê um snapshot consistente', async () => {
  const wallet = await open(urls[0], '0.00');
  const reads = Array.from({ length: 20 }, () => json<{ consistent: boolean }>(urls[0],
    '/wallets/' + wallet.id + '/reconciliation', {}));
  const writes = Array.from({ length: 20 }, (_, index) => post(urls[index % 3], command(wallet, {
    kind: 'WIN', money: { amount: '0.01', currency: 'BRL' },
  })));
  expect((await Promise.all(reads)).every(res => res.status === 200 && res.body.consistent)).toBe(true);
  expect((await Promise.all(writes)).every(res => res.status === 200)).toBe(true);
  await reconcile(urls[0], wallet, '0.20', 20);
}, 20_000);

test('A11 PostgreSQL indisponível e reiniciado: 503 transitório, replay e saldo preservados', async () => {
  const wallet = await open(urls[0]);
  const existing = command(wallet);
  const pending = command(wallet);
  const initial = await post(urls[0], existing);
  expect(initial.body.balance.amount).toBe('90.00');
  docker('compose','stop','postgres');
  try {
    expect((await json(urls[1], '/health/live')).status).toBe(200);
    expect((await json(urls[1], '/health/ready')).status).toBe(503);
    expect((await post(urls[2], pending)).status).toBe(503);
  } finally {
    docker('compose','start','postgres');
    await Promise.all(urls.map(ready));
  }
  expect((await post(urls[2], existing)).body).toEqual({ ...initial.body, idempotentReplay: true });
  const retried = await post(urls[1], pending);
  expect(retried.status).toBe(200);
  expect(retried.body.idempotentReplay).toBe(false);
  await reconcile(urls[0], wallet, '80.00', 3);
}, 35_000);

test('A12 o schema recusa saldo negativo, ledger mutável e duplicação de identidade', async () => {
  const wallet = await open(urls[0]);
  const bet = command(wallet);
  const result = await post(urls[1], bet);
  const em = db.em.fork();
  await expect(em.execute('update wallets set balance=-0.01 where id=?', [wallet.id])).rejects.toThrow();
  await expect(em.execute('update wallet_ledger_entries set amount=amount where transaction_id=?',
    [result.body.transactionId])).rejects.toThrow();
  await expect(em.execute('delete from wallet_ledger_entries where transaction_id=?',
    [result.body.transactionId])).rejects.toThrow();
  await expect(em.execute(`
    insert into wager_transactions
      (id,provider_id,external_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,
      kind,currency,amount,status,correlation_id)
    select ?,provider_id,external_id,?,payload_hash,wallet_id,player_id,round_id,game_id,
      kind,currency,amount,'PENDING',correlation_id from wager_transactions where id=?
  `, [randomUUID(), randomUUID(), result.body.transactionId])).rejects.toThrow();
  await expect(em.execute(`
    insert into wager_transactions
      (id,provider_id,external_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,
      kind,currency,amount,status,correlation_id)
    select ?,provider_id,?,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,
      kind,currency,amount,'PENDING',correlation_id from wager_transactions where id=?
  `, [randomUUID(), randomUUID(), result.body.transactionId])).rejects.toThrow();
  await reconcile(urls[0], wallet, '90.00', 2);
});

test('A13 SIGTERM drena mensagem em voo antes de encerrar e a instância reinicia', async () => {
  const wallet = await open(urls[0]);
  const queues = await discoverQueues(sqs);
  const ids = docker('compose', 'ps', '-q', 'app').split(/\r?\n/);
  expect(ids).toHaveLength(3);
  const [target, ...others] = ids;
  const messageId = randomUUID();
  const acquired = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const holder = transaction(db, async em => {
    await em.execute('select id from wallets where id=? for update', [wallet.id]);
    acquired.resolve();
    await release.promise;
  });
  const depth = async () => {
    const state = await sqs.send(new GetQueueAttributesCommand({
      QueueUrl: queues.input, AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
    }));
    return Number(state.Attributes?.ApproximateNumberOfMessages ?? 0) +
      Number(state.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0);
  };
  let otherWorkersStopped = false;
  let signalled = false;
  try {
    await Promise.race([acquired.promise, holder]);
    await eventually(async () => await depth() === 0 ? true : undefined);
    docker('stop', ...others);
    otherWorkersStopped = true;
    // An aborted client request can still occupy the broker until its two-second long poll ends.
    await Bun.sleep(2100);
    await send(sqs, queues.input, command(wallet), messageId);
    await eventually(async () => {
      const row = await one<{ n: number }>(db.em.fork(), `
        select count(*)::int n from pg_stat_activity
        where wait_event_type='Lock' and query ilike '%wallets%for update%'`);
      return row?.n === 1 ? true : undefined;
    }, 7000);
    docker('kill', '--signal', 'SIGTERM', target);
    signalled = true;
    release.resolve();
    await holder;
    expect(docker('wait', target)).toBe('0');
    expect(docker('logs', '--tail', '20', target)).toContain('"event":"stopped"');
    expect(await depth()).toBe(0);
    expect(await one(db.em.fork(), 'select message_id from inbox_messages where message_id=? and processed_at is not null',
      [messageId])).toEqual({ message_id: messageId });
  } finally {
    release.resolve();
    await holder;
    if (otherWorkersStopped) docker('start', ...others);
    if (signalled) docker('start', target);
    urls = appUrls();
    await Promise.all(urls.map(ready));
  }
  await reconcile(urls[0], wallet, '90.00', 2);
}, 35_000);

test('A14 conflito de lock gera 503, é medido e a mesma chave pode tentar de novo', async () => {
  const wallet = await open(urls[0]);
  const cmd = command(wallet);
  const counter = 'wallet_lock_conflict_total';
  const before = await metric(urls[1], counter);
  await transaction(db, async em => {
    await em.execute('select id from wallets where id=? for update', [wallet.id]);
    expect((await post(urls[1], cmd)).status).toBe(503);
  });
  expect(await metric(urls[1], counter)).toBe(before + 1);
  const retry = await post(urls[2], cmd);
  expect(retry.status).toBe(200);
  expect(retry.body.idempotentReplay).toBe(false);
  await reconcile(urls[0], wallet, '90.00', 2);
}, 15_000);

test('A15 referência de outro provedor não é encontrada; carteira, moeda e tipo são validados', async () => {
  const [a,b] = await Promise.all([open(urls[0]), open(urls[1])]);
  const bet = command(a);
  const win = command(a, { kind: 'WIN' });
  await post(urls[0], bet);
  await post(urls[0], win);
  const wrongWallet = await post(urls[1], command(b, { kind: 'REFUND', referenceExternalTransactionId: bet.externalTransactionId }));
  expect(wrongWallet.body.failureCode).toBe('REFERENCE_SCOPE_MISMATCH');
  const wrongKind = await post(urls[1], command(a, { kind: 'REFUND', referenceExternalTransactionId: win.externalTransactionId }));
  expect(wrongKind.body.failureCode).toBe('INVALID_REFERENCE_KIND');
  const wrongPlayer = await post(urls[2], command(a, { playerId: b.playerId }));
  expect(wrongPlayer.body.failureCode).toBe('PLAYER_MISMATCH');
  const wrongCurrency = await post(urls[2], command(a, {
    kind: 'REFUND', referenceExternalTransactionId: bet.externalTransactionId, money: { ...bet.money, currency: 'USD' },
  }));
  expect(wrongCurrency.body.failureCode).toBe('CURRENCY_MISMATCH');
  const pending = await post(urls[2], command(a, {
    providerId: 'other-provider', kind: 'REFUND', referenceExternalTransactionId: bet.externalTransactionId,
  }));
  expect(pending.status).toBe(202);
  expect(pending.body.status).toBe('PENDING_REFERENCE');
  await reconcile(urls[0], a, '100.00', 3);
  await reconcile(urls[1], b, '100.00', 1);
});
