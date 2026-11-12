import { expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { CreateQueueCommand, GetQueueUrlCommand } from '@aws-sdk/client-sqs';
import { connect, one } from '../src/db';
import { createSqs, OutboxPublisher } from '../src/messaging';
import { Wagering } from '../src/wagering';
import { command, docker, drain, eventually } from './fixtures';

test('SIGKILL no broker descartável e recuperação do evento original pelo PostgreSQL', async () => {
  if (!process.env.DATABASE_URL) throw new Error('Configure o PostgreSQL isolado');
  const suffix = randomUUID().replaceAll('-', '');
  const name = 'jungle-broker-probe-' + suffix;
  const database = 'crash_' + suffix;
  const base = new URL(process.env.DATABASE_URL);
  const db = await connect();
  const em = db.em.fork();
  await em.execute('create database ' + database);
  base.pathname = '/' + database;
  const isolated = await connect(base.toString());
  const previousEndpoint = process.env.SQS_ENDPOINT;
  let sqs: ReturnType<typeof createSqs> | undefined;
  let container: string | undefined;
  try {
    container = docker('run', '-d', '--name', name, '--init', '--stop-signal', 'SIGINT',
      '-p', '127.0.0.1::4566', '-e', 'PERSIST_STATE=1',
      'ministackorg/ministack@sha256:a415d58e257931ddfd2678bfba915d6dea9508c3103df74fa90efdd6535c035c');
    const health = async () => eventually(async () => docker('inspect', '--format', '{{.State.Health.Status}}', container!) === 'healthy'
      ? true : undefined, 20_000);
    await health();
    process.env.SQS_ENDPOINT = 'http://' + docker('port', container, '4566/tcp');
    sqs = createSqs();
    const queueName = 'chaos-' + suffix + '.fifo';
    const create = async () => (await sqs!.send(new CreateQueueCommand({
      QueueName: queueName, Attributes: { FifoQueue: 'true' },
    }))).QueueUrl!;
    const firstQueue = await create();
    await isolated.getMigrator().up();
    const service = new Wagering(isolated);
    const wallet = await service.open(randomUUID(), { currency: 'BRL', amount: '0.00' }, randomUUID());
    const loss = command(wallet, { kind: 'LOSS', money: { currency: 'BRL', amount: '0.00' } });
    await service.submit({ command: loss, idempotencyKey: loss.externalTransactionId, correlationId: randomUUID() });
    expect(await new OutboxPublisher(isolated, sqs, firstQueue).publishOne()).toBe(true);
    const before = await one<{ id: string; published_at: string | null }>(isolated.em.fork(),
      'select id,published_at from outbox_messages');
    expect(before?.published_at).toBeTruthy();

    docker('kill', '--signal', 'SIGKILL', container);
    docker('start', container);
    await health();
    sqs.destroy();
    process.env.SQS_ENDPOINT = 'http://' + docker('port', container, '4566/tcp');
    sqs = createSqs();
    let existingQueue: string | undefined;
    try {
      existingQueue = (await sqs.send(new GetQueueUrlCommand({ QueueName: queueName }))).QueueUrl;
    } catch (error) {
      if (!(error instanceof Error) || !/QueueDoesNotExist|NonExistentQueue/.test(error.name)) throw error;
    }
    const preserved = existingQueue && (await drain(sqs, existingQueue))[0];
    console.log(JSON.stringify({ probe: 'BROKER_SIGKILL', brokerRetainedEvent: Boolean(preserved),
      postgresRetainedConfirmedEvent: Boolean(before?.published_at) }));
    if (preserved) {
      expect(JSON.parse(preserved.Body!).eventId).toBe(before!.id);
    } else {
      const newQueue = existingQueue ?? await create();
      await isolated.em.fork().execute(
        'update outbox_messages set published_at=null,next_attempt_at=clock_timestamp() where id=? and claim_token is null',
        [before!.id]);
      expect(await new OutboxPublisher(isolated, sqs, newQueue).publishOne()).toBe(true);
      const message = await eventually(async () => (await drain(sqs!, newQueue))[0]);
      expect(JSON.parse(message.Body!).eventId).toBe(before!.id);
      console.log('BROKER_MANUAL_REPLAY_PRESERVED_EVENT_ID');
    }
    expect((await service.reconcile(wallet.id, randomUUID())).consistent).toBe(true);
  } finally {
    if (container) docker('rm', '-f', container);
    if (previousEndpoint === undefined) delete process.env.SQS_ENDPOINT;
    else process.env.SQS_ENDPOINT = previousEndpoint;
    sqs?.destroy();
    await isolated.close();
    await em.execute('drop database ' + database);
    await db.close();
  }
}, 80_000);
