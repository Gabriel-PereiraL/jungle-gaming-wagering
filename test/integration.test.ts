import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { SendMessageCommand } from '@aws-sdk/client-sqs';
import { connect, one, type Database } from '../src/db';
import { createSqs, discoverQueues, WagerConsumer, type QueueUrls } from '../src/messaging';
import { Wagering } from '../src/wagering';
import { appUrls, command, drain, eventually, isolatedQueues, json, open, post, ready, reconcile, send,
  type Outcome } from './fixtures';

let db: Database;
let url: string;
let urls: QueueUrls;
const sqs = createSqs();
const txPath = '/wagering/transactions';

beforeAll(async () => {
  [url] = appUrls();
  await ready(url);
  db = await connect();
  urls = await discoverQueues(sqs);
}, 20_000);

afterAll(async () => {
  await db?.close();
  sqs.destroy();
});

test('migração reversível em banco isolado', async () => {
  const name = 'migration_' + randomUUID().replaceAll('-', '');
  await db.em.fork().execute(`create database ${name}`);
  const clientUrl = new URL(process.env.DATABASE_URL ?? 'postgres://wager:wager@127.0.0.1:54328/wager');
  clientUrl.pathname = '/' + name;
  try {
    const isolated = await connect(clientUrl.toString());
    try {
      await isolated.getMigrator().up();
      await isolated.getMigrator().down({ to: 0 });
      expect(await one(isolated.em.fork(), "select to_regclass('wallets') as table")).toEqual({ table: null });
      await isolated.getMigrator().up();
      expect(await one(isolated.em.fork(), "select to_regclass('wallets')::text as table")).toEqual({ table: 'wallets' });
    } finally {
      await isolated.close();
    }
  } finally {
    await db.em.fork().execute(`drop database ${name}`);
  }
}, 20_000);

test('HTTP, snapshot idempotente, paginação e constraints', async () => {
  const wallet = await open(url);
  const duplicate = await json(url, '/wallets', { playerId: wallet.playerId, initialBalance: wallet.balance });
  expect(duplicate.status).toBe(409);
  const bet = command(wallet);
  expect((await json(url, txPath, bet)).status).toBe(400);
  const first = await post(url, bet);
  expect(first.status).toBe(200);
  expect(first.body.balance.amount).toBe('90.00');
  const win = await post(url, command(wallet, { kind: 'WIN' }));
  expect(win.body.balance.amount).toBe('100.00');
  const loss = await post(url, command(wallet, { kind: 'LOSS', money: { amount: '0.00', currency: 'BRL' } }));
  expect(loss.body.version).toBe(3);
  expect((await post(url, bet)).body).toEqual({ ...first.body, idempotentReplay: true });
  expect((await post(url, { ...bet, roundId: 'different' })).status).toBe(409);
  expect((await post(url, bet, 'another-key')).status).toBe(409);

  const other = command(wallet);
  await post(url, other);
  expect((await post(url, other, bet.externalTransactionId)).status).toBe(409);
  const lookedUp = await json<Outcome>(url, '/providers/' + bet.providerId + txPath + '/' + bet.externalTransactionId);
  expect(lookedUp.body.transactionId).toBe(first.body.transactionId);
  expect((await json<Outcome>(url, txPath + '/' + first.body.transactionId)).body.balance.amount).toBe('90.00');

  type Page = { entries: { walletVersion: number }[]; nextCursor: string };
  const ledgerPath = '/wallets/' + wallet.id + '/ledger';
  const firstPage = await json<Page>(url, ledgerPath + '?limit=1');
  expect(firstPage.body.entries[0].walletVersion).toBe(4);
  const secondPage = await json<Page>(url, ledgerPath + '?limit=1&cursor=' + firstPage.body.nextCursor);
  expect(secondPage.body.entries[0].walletVersion).toBe(3);
  expect((await json(url, ledgerPath + '?cursor=bad')).status).toBe(400);
  const em = db.em.fork();
  await expect(em.execute('update wallets set balance=-1 where id=?', [wallet.id])).rejects.toThrow();
  await expect(em.execute("update wallets set balance='NaN' where id=?", [wallet.id])).rejects.toThrow();
  await expect(em.execute('delete from wallet_ledger_entries where wallet_id=?', [wallet.id])).rejects.toThrow();
  await expect(em.execute('update wager_transactions set status=status where id=?', [first.body.transactionId]))
    .rejects.toThrow();
  await reconcile(url, wallet, '90.00', 4);
  expect((await (await fetch(url + '/metrics')).text()).includes('wager_transactions_total')).toBe(true);
});

test('reversões validam escopo, saldo, valor e efeito econômico único', async () => {
  const wallet = await open(url, '0.00');
  const win = command(wallet, { kind: 'WIN' });
  await post(url, win);
  const bet = command(wallet);
  await post(url, bet);
  const rollbackWin = await post(url, command(wallet, { kind: 'ROLLBACK',
    referenceExternalTransactionId: win.externalTransactionId }));
  expect(rollbackWin.status).toBe(422);
  expect(rollbackWin.body.failureCode).toBe('REVERSAL_INSUFFICIENT_FUNDS');
  const refund = { kind: 'REFUND', referenceExternalTransactionId: bet.externalTransactionId } as const;
  expect((await post(url, command(wallet, { ...refund, roundId: 'wrong' }))).body.failureCode)
    .toBe('REFERENCE_SCOPE_MISMATCH');
  expect((await post(url, command(wallet, { ...refund, money: { amount: '1.00', currency: 'BRL' } })))
    .body.failureCode).toBe('AMOUNT_MISMATCH');
  const refundCmd = command(wallet, refund);
  expect((await post(url, refundCmd)).body.balance.amount).toBe('10.00');
  expect((await post(url, command(wallet, { ...refund, kind: 'ROLLBACK' }))).body.failureCode)
    .toBe('REVERSAL_ALREADY_APPLIED');
  const refundRollback = command(wallet, { kind: 'ROLLBACK', referenceExternalTransactionId: refundCmd.externalTransactionId });
  expect((await post(url, refundRollback)).body.balance.amount).toBe('0.00');
  for (const kind of ['REFUND', 'ROLLBACK'] as const) {
    const retryReversal = await post(url, command(wallet, { ...refund, kind }));
    expect(retryReversal.status).toBe(422);
    expect(retryReversal.body.failureCode).toBe('REVERSAL_ALREADY_APPLIED');
  }
  await reconcile(url, wallet, '0.00', 4);
});

test('inbox e chave de negócio deduplicam redelivery e entrada HTTP + fila', async () => {
  const wallet = await open(url);
  const cmd = command(wallet);
  await post(url, cmd);
  const messageId = randomUUID();
  await send(sqs, urls.input, cmd, messageId);
  await send(sqs, urls.input, cmd, messageId);
  await eventually(async () => {
    const row = await one(db.em.fork(), 'select message_id from inbox_messages where message_id=? and processed_at is not null',
      [messageId]);
    return row || undefined;
  });
  await send(sqs, urls.input, { ...cmd, money: { ...cmd.money, amount: '12.00' } }, messageId);
  const dead = await eventually(async () => (await drain(sqs, urls.dead)).find(item =>
    JSON.parse(item.Body ?? '{}').messageId === messageId));
  expect(dead.MessageAttributes?.FailureCode.StringValue).toBe('INBOX_PAYLOAD_CONFLICT');
  await reconcile(url, wallet, '90.00', 2);
}, 20_000);

test('falha antes do commit reverte inbox, saldo, ledger e outbox; retry esgotado vai para DLQ', async () => {
  const wallet = await open(url);
  const cmd = command(wallet);
  const messageId = randomUUID();
  const testQueues = await isolatedQueues(sqs);
  const hook = 'fail_event_' + wallet.id.replaceAll('-', '');
  const em = db.em.fork();
  await em.execute(`
    create function ${hook}() returns trigger language plpgsql as $$
    begin
      if new.aggregate_id = '${wallet.id}'::uuid then
        raise exception 'Forced rollback in integration test';
      end if;
      return new;
    end $$`);
  await em.execute(`create trigger ${hook} before insert on outbox_messages for each row execute function ${hook}()`);
  try {
    const consumer = new WagerConsumer(sqs, testQueues.urls, new Wagering(db));
    await send(sqs, testQueues.urls.input, cmd, messageId);
    const dead = await eventually(async () => {
      await consumer.poll();
      return (await drain(sqs, testQueues.urls.dead))[0];
    }, 36_000);
    expect(JSON.parse(dead.Body ?? '{}').messageId).toBe(messageId);
    expect(await one(em, 'select 1 from inbox_messages where message_id=?', [messageId])).toBeUndefined();
    expect(await one(em, 'select 1 from wager_transactions where external_id=?', [cmd.externalTransactionId]))
      .toBeUndefined();
    expect(await one(em, "select count(*)::int n from outbox_messages where aggregate_id=?", [wallet.id]))
      .toEqual({ n: 2 });
    await reconcile(url, wallet, '100.00', 1);
    await em.execute(`drop trigger ${hook} on outbox_messages`);
    for (let attempt = 0; attempt < 2; attempt++) {
      await sqs.send(new SendMessageCommand({
        QueueUrl: testQueues.urls.input, MessageBody: dead.Body,
        MessageGroupId: wallet.id, MessageDeduplicationId: randomUUID(),
      }));
      expect(await consumer.poll()).toBe(true);
    }
    await reconcile(url, wallet, '90.00', 2);
  } finally {
    await em.execute(`drop trigger if exists ${hook} on outbox_messages`);
    await em.execute(`drop function ${hook}()`);
    await testQueues.cleanup();
  }
}, 46_000);

test('mensagem inválida recebe código estável na DLQ', async () => {
  const marker = randomUUID();
  await sqs.send(new SendMessageCommand({
    QueueUrl: urls.input, MessageGroupId: marker, MessageDeduplicationId: marker, MessageBody: marker,
  }));
  const message = await eventually(async () => (await drain(sqs, urls.dead)).find(item => item.Body === marker));
  expect(message.MessageAttributes?.FailureCode.StringValue).toBe('INVALID_JSON');
}, 20_000);
