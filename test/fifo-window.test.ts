import { expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { CreateQueueCommand, DeleteQueueCommand } from '@aws-sdk/client-sqs';
import { connect, one } from '../src/db';
import { createSqs, OutboxPublisher } from '../src/messaging';
import { Wagering } from '../src/wagering';
import { command, drain, eventually } from './fixtures';

test('FIFO entrega uma segunda mensagem física após cinco minutos; consumidor deduplica eventId no PostgreSQL', async () => {
  if (!process.env.DATABASE_URL || !process.env.SQS_ENDPOINT) throw new Error('Configure dependências de teste isoladas');
  const db = await connect();
  const sqs = createSqs();
  const dbName = 'fifo_' + randomUUID().replaceAll('-', '');
  const url = new URL(process.env.DATABASE_URL);
  const em = db.em.fork();
  await em.execute('create database ' + dbName);
  url.pathname = '/' + dbName;
  const isolated = await connect(url.toString());
  let queueUrl: string | undefined;
  try {
    await isolated.getMigrator().up();
    queueUrl = (await sqs.send(new CreateQueueCommand({
      QueueName: dbName + '.fifo', Attributes: { FifoQueue: 'true' },
    }))).QueueUrl!;
    const service = new Wagering(isolated);
    const wallet = await service.open(randomUUID(), { currency: 'BRL', amount: '0.00' }, randomUUID());
    const cmd = command(wallet, { kind: 'LOSS', money: { currency: 'BRL', amount: '0.00' } });
    await service.submit({ command: cmd, idempotencyKey: cmd.externalTransactionId, correlationId: randomUUID() });
    const publisher = new OutboxPublisher(isolated, sqs, queueUrl);
    const sink = isolated.em.fork();
    await sink.execute('create table consumed_events(event_id uuid primary key)');
    const receive = async () => {
      const message = await eventually(async () => (await drain(sqs, queueUrl!))[0]);
      const body = JSON.parse(message.Body!);
      await sink.execute('insert into consumed_events values (?) on conflict do nothing', [body.eventId]);
      return { eventId: body.eventId, messageId: message.MessageId };
    };
    expect(await publisher.publishOne()).toBe(true);
    const first = await receive();
    const started = Date.now();
    console.log('FIFO_WINDOW_WAIT_STARTED');
    await Bun.sleep(305_000);
    // Recover an unconfirmed publication with the original payload and eventId.
    await sink.execute('update outbox_messages set published_at=null');
    expect(await publisher.publishOne()).toBe(true);
    const second = await receive();
    expect(second.eventId).toBe(first.eventId);
    expect(second.messageId).not.toBe(first.messageId);
    expect(await one(sink, 'select count(*)::int n from consumed_events')).toEqual({ n: 1 });
    expect((await service.reconcile(wallet.id, randomUUID())).consistent).toBe(true);
    console.log(JSON.stringify({ elapsedSeconds: Math.round((Date.now() - started) / 1000), distinctPhysicalMessages: true, consumerEffects: 1 }));
  } finally {
    if (queueUrl) await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
    await isolated.close();
    await em.execute('drop database ' + dbName);
    await db.close();
    sqs.destroy();
  }
}, 345_000);
