import { randomUUID } from 'node:crypto';
import {
  ChangeMessageVisibilityCommand, CreateQueueCommand, DeleteMessageCommand,
  GetQueueAttributesCommand, GetQueueUrlCommand, ReceiveMessageCommand,
  SendMessageCommand, SetQueueAttributesCommand, SQSClient, type Message,
} from '@aws-sdk/client-sqs';
import { queueEnvelope } from './contracts';
import { one, transaction, type Database } from './db';
import { dlq, errorCode, lag, log, retries } from './telemetry';
import { Conflict, Missing, type Wagering } from './wagering';

export const queueNames = {
  input: 'wager-transactions.fifo',
  dead: 'wager-transactions-dlq.fifo',
  events: 'wager-events.fifo',
} as const;
export type QueueUrls = Record<keyof typeof queueNames, string>;
const maxReceiveCount = 5;

export function createSqs(): SQSClient {
  const endpoint = process.env.SQS_ENDPOINT ?? 'http://127.0.0.1:4566';
  return new SQSClient({
    endpoint, region: process.env.AWS_REGION ?? 'us-east-1',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    useQueueUrlAsEndpoint: false, maxAttempts: 2,
  });
}

function required(value: string | undefined): string {
  if (!value) throw new Error('Incomplete SQS response');
  return value;
}

export async function discoverQueues(sqs: SQSClient): Promise<QueueUrls> {
  const url = async (key: keyof QueueUrls) => required((await sqs.send(
    new GetQueueUrlCommand({ QueueName: queueNames[key] }), { abortSignal: AbortSignal.timeout(3000) },
  )).QueueUrl);
  const [input, dead, events] = await Promise.all([url('input'), url('dead'), url('events')]);
  return { input, dead, events };
}

export async function setupQueues(sqs: SQSClient): Promise<void> {
  const create = async (key: keyof QueueUrls, attributes: Record<string, string> = {}) =>
    required((await sqs.send(new CreateQueueCommand({
      QueueName: queueNames[key], Attributes: { FifoQueue: 'true', ...attributes },
    }), { abortSignal: AbortSignal.timeout(4000) })).QueueUrl);
  const dead = await create('dead');
  const arn = required((await sqs.send(new GetQueueAttributesCommand({
    QueueUrl: dead, AttributeNames: ['QueueArn'],
  }), { abortSignal: AbortSignal.timeout(4000) })).Attributes?.QueueArn);
  const input = await create('input', { VisibilityTimeout: '30' });
  await sqs.send(new SetQueueAttributesCommand({
    QueueUrl: input, Attributes: { RedrivePolicy: JSON.stringify({ deadLetterTargetArn: arn, maxReceiveCount }) },
  }), { abortSignal: AbortSignal.timeout(4000) });
  await create('events');
}

export function backoff(attempt: number, cap: number) {
  return Math.max(1, Math.floor(Math.min(cap, 2 ** Math.max(0, Math.min(attempt - 1, 16))) * (0.8 + Math.random() * 0.2)));
}

export class WagerConsumer {
  constructor(
    private readonly sqs: SQSClient,
    private readonly urls: QueueUrls,
    private readonly wagering: Wagering,
    private readonly afterCommit?: () => Promise<void>,
  ) {}

  async poll(signal?: AbortSignal): Promise<boolean> {
    const received = await this.sqs.send(new ReceiveMessageCommand({
      QueueUrl: this.urls.input, MaxNumberOfMessages: 1, WaitTimeSeconds: 2,
      MessageSystemAttributeNames: ['ApproximateReceiveCount'],
    }), { abortSignal: signal ? AbortSignal.any([signal, AbortSignal.timeout(7000)]) : AbortSignal.timeout(7000) });
    const message = received.Messages?.[0];
    if (!message) return false;
    await this.handle(message);
    return true;
  }

  async handle(message: Message): Promise<void> {
    const receipt = required(message.ReceiptHandle);
    let envelope: unknown;
    try {
      envelope = JSON.parse(message.Body ?? '');
    } catch {
      return this.permanent(message, 'INVALID_JSON');
    }
    const parsed = queueEnvelope.safeParse(envelope);
    if (!parsed.success) return this.permanent(message, 'INVALID_ENVELOPE');

    const { idempotencyKey, ...command } = parsed.data.data;
    const { messageId, correlationId = messageId } = parsed.data;
    const input = { command, idempotencyKey, correlationId, messageId };
    const context = { messageId, correlationId, walletId: command.walletId, providerId: command.providerId };
    let result: Awaited<ReturnType<Wagering['submit']>>;
    try {
      result = await this.wagering.submit(input);
    } catch (error) {
      if (error instanceof Conflict || error instanceof Missing) return this.permanent(message, error.message, context);
      const receiveCount = Number(message.Attributes?.ApproximateReceiveCount ?? '1');
      retries.inc({ source: 'sqs' });
      log('sqs_retry', { messageId, correlationId, walletId: command.walletId, providerId: command.providerId,
        receiveCount, errorCode: errorCode(error) }, 'warn');
      if (receiveCount >= maxReceiveCount) {
        try {
          result = await this.wagering.submit(input, true);
        } catch (recordError) {
          if (recordError instanceof Conflict || recordError instanceof Missing) {
            return this.permanent(message, recordError.message, context);
          }
          // Leave the receipt unacknowledged so the broker can redrive it.
          log('sqs_failure_audit_unavailable', { messageId, correlationId, walletId: command.walletId,
            providerId: command.providerId, errorCode: errorCode(recordError) }, 'error');
          return;
        }
      } else {
        await this.sqs.send(new ChangeMessageVisibilityCommand({
          QueueUrl: this.urls.input, ReceiptHandle: receipt, VisibilityTimeout: Math.min(30, 4 * receiveCount),
        }), { abortSignal: AbortSignal.timeout(4000) });
        return;
      }
    }

    await this.afterCommit?.();
    if (result.status === 'FAILED') return this.permanent(message, 'INFRASTRUCTURE_RETRIES_EXHAUSTED', context);
    await this.ack(receipt);
  }

  private async permanent(message: Message, reason: string,
    context?: { messageId: string; correlationId: string; walletId: string; providerId: string }): Promise<void> {
    // A retry of SendMessage uses the same physical source ID for FIFO deduplication.
    const sourceId = required(message.MessageId);
    await this.sqs.send(new SendMessageCommand({
      QueueUrl: this.urls.dead, MessageGroupId: 'invalid-wager-command', MessageDeduplicationId: sourceId,
      MessageBody: message.Body ?? '{}',
      MessageAttributes: {
        FailureCode: { DataType: 'String', StringValue: reason },
        ...(context && {
          CorrelationId: { DataType: 'String', StringValue: context.correlationId },
          ApplicationMessageId: { DataType: 'String', StringValue: context.messageId },
        }),
      },
    }), { abortSignal: AbortSignal.timeout(4000) });
    log('sqs_dead_letter', { ...context, sqsMessageId: sourceId, failureCode: reason }, 'warn');
    await this.ack(required(message.ReceiptHandle));
  }

  private async ack(receipt: string): Promise<void> {
    await this.sqs.send(new DeleteMessageCommand({
      QueueUrl: this.urls.input, ReceiptHandle: receipt,
    }), { abortSignal: AbortSignal.timeout(4000) });
  }
}

interface OutboxClaim {
  id: string;
  aggregate_id: string;
  payload: Record<string, unknown>;
  attempts: number;
}

export class OutboxPublisher {
  constructor(private readonly db: Database, private readonly sqs: SQSClient, private readonly url: string) {}

  async publishOne(): Promise<boolean> {
    const token = randomUUID();
    const row = await transaction(this.db, em => one<OutboxClaim>(em, `
      with candidate as (
        select id from outbox_messages where published_at is null and next_attempt_at <= clock_timestamp()
          and (lease_until is null or lease_until <= clock_timestamp())
        order by next_attempt_at,occurred_at,id limit 1 for update skip locked
      )
      update outbox_messages o set claim_token=?,lease_until=clock_timestamp()+interval '30 seconds',
        attempts=attempts+1 from candidate where o.id=candidate.id
      returning o.id,o.aggregate_id,o.payload,o.attempts`, [token]));
    if (!row) return false;

    const em = this.db.em.fork();
    try {
      await this.sqs.send(new SendMessageCommand({
        QueueUrl: this.url, MessageBody: JSON.stringify(row.payload),
        MessageDeduplicationId: row.id, MessageGroupId: row.aggregate_id,
      }), { abortSignal: AbortSignal.timeout(5000) });
    } catch (error) {
      retries.inc({ source: 'outbox' });
      log('outbox_retry', { eventId: row.id, walletId: row.aggregate_id, attempts: row.attempts,
        errorCode: errorCode(error) }, 'warn');
      await em.execute(`
        update outbox_messages set claim_token=null,lease_until=null,
          next_attempt_at=clock_timestamp()+? * interval '1 second',last_error=?
        where id=? and claim_token=?`, [backoff(row.attempts, 60), errorCode(error), row.id, token]);
      return true;
    }
    await em.execute(`
      update outbox_messages set published_at=clock_timestamp(),claim_token=null,lease_until=null,last_error=null
      where id=? and claim_token=?`, [row.id, token]);
    return true;
  }
}

export async function refreshQueueMetrics(db: Database, sqs: SQSClient, urls: QueueUrls): Promise<void> {
  const observations = await Promise.allSettled([
    one<{ age: number }>(db.em.fork(), `
      select greatest(0,coalesce(extract(epoch from clock_timestamp()-min(occurred_at)),0))::float8 age
      from outbox_messages where published_at is null`).then(age => lag.set(age?.age ?? 0)),
    sqs.send(new GetQueueAttributesCommand({
      QueueUrl: urls.dead, AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
    }), { abortSignal: AbortSignal.timeout(4000) }).then(dead => {
      dlq.set(Number(dead.Attributes?.ApproximateNumberOfMessages ?? 0) +
        Number(dead.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0));
    }),
  ]);
  for (const observation of observations) {
    if (observation.status === 'rejected') throw observation.reason;
  }
}
