import { expect } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { CreateQueueCommand, DeleteMessageCommand, DeleteQueueCommand, GetQueueAttributesCommand,
  ReceiveMessageCommand, SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { Command } from '../src/domain';
import { discoverQueues, type QueueUrls } from '../src/messaging';

export function docker(...args: string[]) {
  const result = Bun.spawnSync(['docker', ...args]);
  if (result.exitCode !== 0) throw new Error(result.stderr.toString().trim());
  return result.stdout.toString().trim();
}

export function appUrls() {
  if (process.env.APP_URLS) return process.env.APP_URLS.split(',');
  return [1, 2, 3].map(index => 'http://' + docker('compose', 'port', '--index', String(index), 'app', '3000'));
}

export async function eventually<T>(fn: () => Promise<T | undefined>, timeout = 12_000): Promise<T> {
  const deadline = Date.now() + timeout;
  let lastError: unknown;
  do {
    try {
      const value = await fn();
      if (value !== undefined) return value;
    } catch (error) {
      lastError = error;
    }
    await Bun.sleep(100);
  } while (Date.now() < deadline);
  throw new Error('Timed out waiting for expected state', { cause: lastError });
}

export async function ready(url: string) {
  return eventually(async () => {
    const response = await fetch(url + '/health/ready', { signal: AbortSignal.timeout(2000) });
    return response.ok ? true : undefined;
  });
}

export async function json<T>(url: string, path: string, body?: unknown, key?: string) {
  const response = await fetch(url + path, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { Connection: 'close', 'Content-Type': 'application/json', ...(key && { 'Idempotency-Key': key }) },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(12_000),
  });
  return { status: response.status, body: await response.json() as T };
}

export interface WalletDto {
  id: string;
  playerId: string;
  balance: { amount: string; currency: string };
  version: number;
}

export interface Outcome {
  transactionId: string;
  status: string;
  balance: { amount: string; currency: string };
  version: number;
  failureCode?: string;
  idempotentReplay: boolean;
}

export async function open(url: string, amount = '100.00') {
  const response = await json<WalletDto>(url, '/wallets', {
    playerId: randomUUID(), initialBalance: { amount, currency: 'BRL' },
  });
  expect(response.status).toBe(201);
  return response.body;
}

export function command(wallet: WalletDto, extra: Partial<Command> = {}): Command {
  return {
    providerId: 'provider-test', externalTransactionId: randomUUID(), walletId: wallet.id, playerId: wallet.playerId,
    gameId: 'game', roundId: 'round', kind: 'BET', money: { amount: '10.00', currency: 'BRL' },
    ...extra,
  };
}

export const post = (url: string, cmd: Command, key = cmd.externalTransactionId) =>
  json<Outcome>(url, '/wagering/transactions', cmd, key);

export async function reconcile(url: string, wallet: WalletDto, amount: string, count: number) {
  const res = await json<{ consistent: boolean; storedBalance: { amount: string }; checkedEntries: number }>(
    url, '/wallets/' + wallet.id + '/reconciliation', {});
  expect(res.status).toBe(200);
  expect(res.body.consistent).toBe(true);
  expect(res.body.storedBalance.amount).toBe(amount);
  expect(res.body.checkedEntries).toBe(count);
}

export function envelope(cmd: Command, messageId = randomUUID() as string) {
  return { messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(),
    data: { ...cmd, idempotencyKey: cmd.externalTransactionId } };
}

export async function send(sqs: SQSClient, url: string, cmd: Command, messageId?: string) {
  await sqs.send(new SendMessageCommand({
    QueueUrl: url, MessageGroupId: cmd.walletId, MessageDeduplicationId: randomUUID(),
    MessageBody: JSON.stringify(envelope(cmd, messageId)),
  }));
}

export async function isolatedQueues(sqs: SQSClient) {
  const { events } = await discoverQueues(sqs);
  const suffix = randomUUID();
  const create = async (name: string, attrs: Record<string, string>) => {
    const response = await sqs.send(new CreateQueueCommand({
      QueueName: name + suffix + '.fifo', Attributes: { FifoQueue: 'true', ...attrs },
    }));
    if (!response.QueueUrl) throw new Error('Missing test queue URL');
    return response.QueueUrl;
  };
  const dead = await create('test-dead-', {});
  const arn = (await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dead, AttributeNames: ['QueueArn'] })))
    .Attributes?.QueueArn;
  const input = await create('test-in-', { VisibilityTimeout: '2',
    RedrivePolicy: JSON.stringify({ deadLetterTargetArn: arn, maxReceiveCount: 3 }) });
  const urls: QueueUrls = { dead, input, events };
  return {
    urls,
    cleanup: () => Promise.all([input, dead].map(QueueUrl => sqs.send(new DeleteQueueCommand({ QueueUrl })))),
  };
}

export async function drain(sqs: SQSClient, url: string) {
  const batch = (await sqs.send(new ReceiveMessageCommand({
    QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 1,
    MessageSystemAttributeNames: ['All'], MessageAttributeNames: ['All'],
  }))).Messages ?? [];
  for (const message of batch) {
    await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: message.ReceiptHandle }));
  }
  return batch;
}
