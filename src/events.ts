import { randomUUID } from 'node:crypto';
import type { EntityManager } from '@mikro-orm/postgresql';
import type { FailureCode, LedgerState, MoneyProps, Status, WagerTransaction } from './domain';

export interface EventContext {
  aggregateId: string;
  correlationId: string;
  causationId: string;
}

export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  readonly version = 1;
  readonly eventId = randomUUID();
  readonly occurredAt = new Date();
  protected constructor(private readonly context: EventContext, readonly data: Readonly<T>) {}

  toJSON() {
    return {
      ...this.context, eventId: this.eventId, occurredAt: this.occurredAt.toISOString(),
      eventType: this.eventType, version: this.version, data: this.data,
    };
  }
}

interface WagerEventData {
  transactionId: string;
  status: Status;
  walletId: string;
  providerId: string;
  externalTransactionId: string;
  kind: string;
  money: MoneyProps;
  failureCode?: FailureCode;
  referenceExternalTransactionId?: string;
}

export class WagerTransactionProcessed extends IntegrationEvent<WagerEventData> {
  readonly eventType = 'WagerTransactionProcessed';
  constructor(context: EventContext, data: WagerEventData) { super(context, data); }
}
export class WagerTransactionRejected extends IntegrationEvent<WagerEventData> {
  readonly eventType = 'WagerTransactionRejected';
  constructor(context: EventContext, data: WagerEventData) { super(context, data); }
}
export class WagerTransactionFailed extends IntegrationEvent<WagerEventData> {
  readonly eventType = 'WagerTransactionFailed';
  constructor(context: EventContext, data: WagerEventData) { super(context, data); }
}
export class WagerTransactionPendingReference extends IntegrationEvent<WagerEventData> {
  readonly eventType = 'WagerTransactionPendingReference';
  constructor(context: EventContext, data: WagerEventData) { super(context, data); }
}

export class WalletBalanceChanged extends IntegrationEvent<{
  walletId: string;
  walletVersion: number;
  transactionId: string;
  direction: string;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
}> {
  readonly eventType = 'WalletBalanceChanged';
  constructor(context: EventContext, state: LedgerState) {
    super(context, {
      walletId: state.walletId, walletVersion: state.walletVersion, transactionId: state.transactionId,
      direction: state.direction, money: state.money.toJSON(),
      balanceBefore: state.balanceBefore.toJSON(), balanceAfter: state.balanceAfter.toJSON(),
    });
  }
}

export function wagerEvent(context: EventContext, wager: WagerTransaction): IntegrationEvent<unknown> {
  const data = {
    transactionId: wager.id, status: wager.status, walletId: wager.command.walletId,
    providerId: wager.command.providerId, kind: wager.kind, money: wager.command.money,
    externalTransactionId: wager.command.externalTransactionId, failureCode: wager.failureCode,
    referenceExternalTransactionId: wager.command.referenceExternalTransactionId,
  };
  if (wager.status === 'PROCESSED') return new WagerTransactionProcessed(context, data);
  if (wager.status === 'REJECTED') return new WagerTransactionRejected(context, data);
  if (wager.status === 'FAILED') return new WagerTransactionFailed(context, data);
  return new WagerTransactionPendingReference(context, data);
}

export async function enqueue(em: EntityManager, event: IntegrationEvent<unknown>): Promise<void> {
  const payload = event.toJSON();
  // EntityManager.execute formats SQL with literals. Use the active Knex transaction's pg binds.
  const trx = em.getTransactionContext();
  if (!trx) throw new Error('Outbox enqueue requires an active transaction');
  await trx.raw(
    'insert into outbox_messages (id,aggregate_id,event_type,payload,occurred_at) values (?,?,?,?::jsonb,?)',
    [payload.eventId, payload.aggregateId, payload.eventType, JSON.stringify(payload), payload.occurredAt],
  );
}
