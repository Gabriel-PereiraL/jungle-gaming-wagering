import { EntitySchema, IsolationLevel, LockMode } from '@mikro-orm/core';
import { Migrator } from '@mikro-orm/migrations';
import { MikroORM, type EntityManager } from '@mikro-orm/postgresql';
import { Migration202611110001, Migration202611110002, Migration202611110003 } from './migration';
import { Money, Wallet, WagerTransaction, type FailureCode, type Kind, type Status } from './domain';

class WalletRow {
  id!: string;
  playerId!: string;
  currency!: string;
  balance!: string;
  version!: number;
}

const walletSchema = new EntitySchema({
  class: WalletRow,
  tableName: 'wallets',
  properties: {
    id: { type: 'uuid', primary: true },
    playerId: { type: 'uuid' },
    currency: { type: 'string' },
    balance: { type: 'string', columnType: 'numeric(20,2)' },
    version: { type: 'integer' },
  },
});

export const connect = (clientUrl = process.env.DATABASE_URL ?? 'postgres://wager:wager@127.0.0.1:54328/wager') => MikroORM.init({
  clientUrl,
  entities: [walletSchema],
  extensions: [Migrator],
  migrations: { migrationsList: [Migration202611110001, Migration202611110002, Migration202611110003] },
  driverOptions: { connection: { statement_timeout: 8000, lock_timeout: 3000 } },
  pool: { min: 0, max: 12, acquireTimeoutMillis: 5000 },
});

export type Database = Awaited<ReturnType<typeof connect>>;

export const transaction = <T>(db: Database, work: (em: EntityManager) => Promise<T>) =>
  db.em.fork().transactional(work, { isolationLevel: IsolationLevel.READ_COMMITTED });

export const all = <T>(em: EntityManager, sql: string, params: unknown[] = []): Promise<T[]> =>
  em.execute<T[]>(sql, params);

export async function one<T>(em: EntityManager, sql: string, params: unknown[] = []): Promise<T | undefined> {
  return (await all<T>(em, sql, params))[0];
}

export async function loadWallet(em: EntityManager, id: string, lock = false): Promise<Wallet | undefined> {
  const row = await em.findOne(WalletRow, { id }, lock ? { lockMode: LockMode.PESSIMISTIC_WRITE } : {});
  if (row) {
    return Wallet.rehydrate({
      id: row.id, playerId: row.playerId, version: row.version,
      balance: Money.rehydrate({ amount: row.balance, currency: row.currency }),
    });
  }
}

export interface WagerRow {
  id: string;
  provider_id: string;
  external_id: string;
  idempotency_key: string;
  payload_hash: string;
  wallet_id: string;
  player_id: string;
  round_id: string;
  game_id: string;
  kind: Kind;
  currency: string;
  amount: string;
  reference_external_id: string | null;
  reference_id: string | null;
  status: Status;
  failure_code: FailureCode | null;
  result_balance: string | null;
  result_currency: string | null;
  result_version: number | null;
  correlation_id: string;
  message_id: string | null;
  attempts: number;
  reference_failures: number;
  expires_at: string | Date | null;
  created_at: string | Date;
  processed_at: string | Date | null;
}

export function rehydrateWager(row: WagerRow): WagerTransaction {
  return WagerTransaction.rehydrate({
    id: row.id, status: row.status, failureCode: row.failure_code ?? undefined,
    referenceId: row.reference_id ?? undefined,
    command: {
      providerId: row.provider_id, externalTransactionId: row.external_id,
      walletId: row.wallet_id, playerId: row.player_id, roundId: row.round_id,
      gameId: row.game_id, kind: row.kind, money: { amount: row.amount, currency: row.currency },
      referenceExternalTransactionId: row.reference_external_id ?? undefined,
    },
  });
}
