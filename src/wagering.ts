import { randomUUID } from 'node:crypto';
import type { EntityManager } from '@mikro-orm/postgresql';
import { hashCommand, hashInbox } from './contracts';
import { all, loadWallet, one, rehydrateWager, transaction, type Database, type WagerRow } from './db';
import { Money, Wallet, WagerTransaction, WalletLedgerEntry, decide, type Command, type MoneyProps } from './domain';
import { enqueue, wagerEvent, WalletBalanceChanged, type EventContext } from './events';
import { duplicates, duration, errorCode, lockConflicts, log, mismatch, retries, transactions } from './telemetry';

const consumer = 'wager-command';
const maxReferenceFailures = 5;
const referenceTtlMs = Number(process.env.REFERENCE_TTL_MS ?? 600_000);
if (!Number.isSafeInteger(referenceTtlMs) || referenceTtlMs < 100) throw new Error('Invalid REFERENCE_TTL_MS');

export class Conflict extends Error {}
export class Missing extends Error {}

interface Submission {
  command: Command;
  idempotencyKey: string;
  correlationId: string;
  messageId?: string;
}

interface LedgerRow {
  wallet_version: number;
  transaction_id: string;
  direction: 'CREDIT' | 'DEBIT';
  currency: string;
  amount: string;
  balance_before: string;
  balance_after: string;
  created_at: Date;
}

export function result(row: WagerRow, idempotentReplay: boolean) {
  return {
    transactionId: row.id, status: row.status,
    balance: row.result_balance === null ? undefined : {
      amount: row.result_balance, currency: row.result_currency,
    },
    version: row.result_version ?? undefined,
    failureCode: row.failure_code ?? undefined, idempotentReplay,
  };
}

function eventContext(row: WagerRow): EventContext {
  return {
    aggregateId: row.wallet_id, correlationId: row.correlation_id,
    causationId: row.message_id ?? row.id,
  };
}

async function recordLedger(em: EntityManager, entry: WalletLedgerEntry): Promise<void> {
  const s = entry.state;
  await em.execute(`
    insert into wallet_ledger_entries
      (transaction_id,wallet_id,wallet_version,direction,currency,amount,balance_before,balance_after)
    values (?,?,?,?,?,?,?,?)`,
    [s.transactionId, s.walletId, s.walletVersion, s.direction, s.money.currency, s.money.toString(),
      s.balanceBefore.toString(), s.balanceAfter.toString()]);
}

export class Wagering {
  // Local fallback when the database also rejects the durable retry update.
  private readonly referenceWriteCooldowns = new Map<string, number>();

  constructor(private readonly db: Database) {}

  async open(playerId: string, initial: MoneyProps, correlationId: string) {
    const wallet = Wallet.open(randomUUID(), playerId, Money.from(initial));
    await transaction(this.db, async em => {
      const created = await one(em, `
        insert into wallets (id,player_id,currency,balance,version)
        values (?,?,?,?,1) on conflict (player_id,currency) do nothing returning id`,
        [wallet.id, wallet.playerId, initial.currency, initial.amount]);
      if (!created) throw new Conflict('WALLET_ALREADY_EXISTS');
      if (wallet.balance.isZero()) return;

      const id = randomUUID();
      const row = await one<WagerRow>(em, `
        insert into wager_transactions
          (id,provider_id,external_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,
           kind,currency,amount,status,result_balance,result_currency,result_version,correlation_id,processed_at)
        values (?,'__opening__',?,?,repeat('0',64),?,?,'opening','opening','OPENING',?,?,
          'PROCESSED',?,?,1,?,clock_timestamp()) returning *`,
        [id, id, id, wallet.id, playerId, initial.currency, initial.amount,
          initial.amount, initial.currency, correlationId]);
      if (!row) throw new Error('Opening transaction was not inserted');
      const entry = WalletLedgerEntry.create({
        walletId: wallet.id, walletVersion: 1, transactionId: id, direction: 'CREDIT',
        money: wallet.balance, balanceBefore: Money.zero(initial.currency), balanceAfter: wallet.balance,
      });
      await recordLedger(em, entry);
      const context = eventContext(row);
      await enqueue(em, wagerEvent(context, rehydrateWager(row)));
      await enqueue(em, new WalletBalanceChanged(context, entry.state));
    });
    if (!wallet.balance.isZero()) transactions.inc({ status: 'PROCESSED', kind: 'OPENING' });
    log('wallet_opened', { correlationId, walletId: wallet.id });
    return wallet.toJSON();
  }

  async submit(input: Submission, exhausted = false) {
    const stop = duration.startTimer({ source: input.messageId ? 'sqs' : 'http' });
    try {
      const { row, replay } = await transaction(this.db, em => this.submitInTransaction(em, input, exhausted));
      if (replay) duplicates.inc();
      else transactions.inc({ kind: row.kind, status: row.status });
      this.logResult(row, input.correlationId, input.messageId, replay);
      return result(row, replay);
    } catch (error) {
      if (['55P03', '40P01'].includes(errorCode(error))) lockConflicts.inc();
      throw error;
    } finally {
      stop();
    }
  }

  private async submitInTransaction(em: EntityManager, input: Submission, exhausted: boolean) {
    const { command, idempotencyKey, correlationId, messageId } = input;
    const hash = hashCommand(command);
    if (messageId) {
      const inboxHash = hashInbox(hash, idempotencyKey);
      const inserted = await one(em, `
        insert into inbox_messages (consumer_name,message_id,payload_hash) values (?,?,?)
        on conflict do nothing returning message_id`, [consumer, messageId, inboxHash]);
      if (!inserted) {
        const existing = await one<{ payload_hash: string; processed_at: Date }>(em,
          'select payload_hash,processed_at from inbox_messages where consumer_name=? and message_id=?',
          [consumer, messageId]);
        if (existing?.payload_hash !== inboxHash) throw new Conflict('INBOX_PAYLOAD_CONFLICT');
        if (!existing.processed_at) throw new Error('Inbox row has no committed result');
        const row = await this.findReplay(em, command.providerId, command.externalTransactionId, idempotencyKey, hash);
        if (!row) throw new Error('Inbox result missing');
        return { row, replay: true };
      }
    }

    const existing = await this.findReplay(em, command.providerId, command.externalTransactionId, idempotencyKey, hash);
    let row: WagerRow;
    let replay = Boolean(existing);
    if (existing) {
      row = existing;
    } else {
      const wallet = await loadWallet(em, command.walletId, true);
      if (!wallet) throw new Missing('WALLET_NOT_FOUND');
      const wager = WagerTransaction.create(randomUUID(), command);
      const inserted = await one<WagerRow>(em, `
        insert into wager_transactions
          (id,provider_id,external_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,
           kind,currency,amount,reference_external_id,status,correlation_id,message_id)
        values (?,?,?,?,?,?,?,?,?,?,?,?,?,'PENDING',?,?)
        on conflict do nothing returning *`,
        [wager.id, command.providerId, command.externalTransactionId, idempotencyKey, hash, wallet.id,
          command.playerId, command.roundId, command.gameId, command.kind, command.money.currency,
          command.money.amount, command.referenceExternalTransactionId ?? null, correlationId, messageId ?? null]);
      if (inserted && exhausted) {
        wager.transition('FAILED', 'INFRASTRUCTURE_RETRIES_EXHAUSTED');
        row = await this.persistTerminal(em, wallet, inserted, wager);
      } else if (inserted) row = await this.apply(em, wallet, inserted);
      else {
        const winner = await this.findReplay(em, command.providerId, command.externalTransactionId, idempotencyKey, hash);
        if (!winner) throw new Error('Conflicting wager was not found');
        row = winner;
        replay = true;
      }
    }
    if (messageId) {
      await em.execute('update inbox_messages set processed_at=clock_timestamp() where consumer_name=? and message_id=?',
        [consumer, messageId]);
    }
    return { row, replay };
  }

  private async findReplay(em: EntityManager, provider: string, external: string, key: string, hash: string) {
    const rows = await all<WagerRow>(em, `
      select * from wager_transactions where provider_id=? and (idempotency_key=? or external_id=?)`,
      [provider, key, external]);
    if (!rows.length) return;
    const [row] = rows;
    if (rows.length !== 1 || row.external_id !== external || row.idempotency_key !== key || row.payload_hash !== hash) {
      throw new Conflict('IDEMPOTENCY_CONFLICT');
    }
    return row;
  }

  private async apply(em: EntityManager, wallet: Wallet, row: WagerRow): Promise<WagerRow> {
    const wager = rehydrateWager(row);
    const source = row.reference_external_id
      ? await one<WagerRow>(em, 'select * from wager_transactions where provider_id=? and external_id=?',
        [row.provider_id, row.reference_external_id])
      : undefined;
    const reference = source && rehydrateWager(source);
    const alreadyReversed = source && wager.reversal && Boolean(await one(em, `
      select 1 from wager_transactions where reference_id=? and status='PROCESSED' and kind in ('REFUND','ROLLBACK')`,
      [source.id]));
    const now = new Date();
    const expiry = row.expires_at === null ? undefined : new Date(row.expires_at);
    const expired = expiry !== undefined && expiry <= now;
    const resolvedTooLate = expired && source?.processed_at && new Date(source.processed_at) > expiry;
    const decision = decide(wager, wallet, resolvedTooLate ? undefined : reference, expired, !!alreadyReversed);
    const context = eventContext(row);

    if ('pending' in decision) {
      const expires = expiry ?? new Date(now.getTime() + referenceTtlMs);
      const delay = Math.min(60_000, 1000 * 2 ** Math.min(row.attempts, 6));
      const next = new Date(Math.min(expires.getTime(), now.getTime() + delay * (0.75 + Math.random() * 0.25)));
      wager.transition('PENDING_REFERENCE');
      const pending = await one<WagerRow>(em, `
        update wager_transactions set status='PENDING_REFERENCE',attempts=attempts+1,reference_failures=0,next_attempt_at=?,expires_at=?,
          result_balance=?,result_currency=?,result_version=? where id=? returning *`,
        [next, expires, wallet.balance.toString(), wallet.balance.currency, wallet.version, wager.id]);
      if (row.status === 'PENDING') await enqueue(em, wagerEvent(context, wager));
      return pending!;
    }

    let entry: WalletLedgerEntry | undefined;
    if ('reject' in decision) {
      wager.transition('REJECTED', decision.reject, source?.id);
    } else {
      const before = wallet.balance;
      const failure = decision.direction && wallet.move(decision.direction, wager.money, wager.reversal);
      if (failure) wager.transition('REJECTED', failure, source?.id);
      else {
        wager.transition('PROCESSED', undefined, source?.id);
        if (decision.direction) {
          entry = WalletLedgerEntry.create({
            walletId: wallet.id, walletVersion: wallet.version, transactionId: wager.id,
            direction: decision.direction, money: wager.money, balanceBefore: before, balanceAfter: wallet.balance,
          });
          await em.execute('update wallets set balance=?,version=?,updated_at=clock_timestamp() where id=?',
            [wallet.balance.toString(), wallet.version, wallet.id]);
          await recordLedger(em, entry);
        }
      }
    }

    return this.persistTerminal(em, wallet, row, wager, entry);
  }

  private async persistTerminal(
    em: EntityManager, wallet: Wallet, row: WagerRow, wager: WagerTransaction, entry?: WalletLedgerEntry,
    referenceFailures = row.reference_failures,
  ): Promise<WagerRow> {
    const terminal = await one<WagerRow>(em, `
      update wager_transactions set status=?,failure_code=?,reference_id=?,processed_at=clock_timestamp(),
        next_attempt_at=null,result_balance=?,result_currency=?,result_version=?,reference_failures=?
      where id=? returning *`,
      [wager.status, wager.failureCode ?? null, wager.referenceId ?? null, wallet.balance.toString(),
        wallet.balance.currency, wallet.version, referenceFailures, wager.id]);
    await enqueue(em, wagerEvent(eventContext(row), wager));
    if (entry) await enqueue(em, new WalletBalanceChanged(eventContext(row), entry.state));
    return terminal!;
  }

  async reprocessOne(): Promise<boolean> {
    const now = performance.now();
    for (const [id, until] of this.referenceWriteCooldowns) {
      if (until <= now) this.referenceWriteCooldowns.delete(id);
    }
    const excluded = [...this.referenceWriteCooldowns.keys()].join(',');
    let selected: WagerRow | undefined;
    let row: WagerRow | undefined;
    try {
      row = await transaction(this.db, async em => {
        // Lock the wallet first, exactly as on the submission path.
        const candidate = await one<{ id: string }>(em, `
          select w.id from wager_transactions t join wallets w on w.id=t.wallet_id
          where t.status='PENDING_REFERENCE' and t.next_attempt_at <= clock_timestamp()
            and t.id <> all(string_to_array(?, ',')::uuid[])
          order by t.next_attempt_at,t.id limit 1 for update of w skip locked`, [excluded]);
        if (!candidate) return;
        const due = await one<WagerRow>(em, `
          select * from wager_transactions where wallet_id=? and status='PENDING_REFERENCE'
            and next_attempt_at <= clock_timestamp() and id <> all(string_to_array(?, ',')::uuid[])
          order by next_attempt_at,id limit 1`, [candidate.id, excluded]);
        if (!due) return;
        selected = due;

        try {
          // The nested transaction rolls back the attempt without releasing the wallet lock.
          return await em.transactional(async attempt => {
            const wallet = await loadWallet(attempt, candidate.id);
            if (!wallet) throw new Error('Missing wallet for pending wager');
            return this.apply(attempt, wallet, due);
          });
        } catch (error) {
          return this.handleReferenceFailure(em, due, error);
        }
      });
    } catch (error) {
      if (!selected) throw error;
      // The outer transaction has rolled back. The row stays durable and other wallets can progress.
      const delayMs = 5000;
      this.referenceWriteCooldowns.set(selected.id, performance.now() + delayMs);
      retries.inc({ source: 'reference' });
      log('reference_write_deferred', {
        transactionId: selected.id, walletId: selected.wallet_id, providerId: selected.provider_id,
        correlationId: selected.correlation_id, messageId: selected.message_id ?? undefined,
        errorCode: errorCode(error), delayMs,
      }, 'warn');
      return true;
    }
    if (!row) return false;
    retries.inc({ source: 'reference' });
    if (row.status !== 'PENDING_REFERENCE') transactions.inc({ kind: row.kind, status: row.status });
    this.logResult(row, row.correlation_id, row.message_id ?? undefined, false);
    return true;
  }

  private async handleReferenceFailure(em: EntityManager, row: WagerRow, error: unknown) {
    const failures = Math.min(row.reference_failures + 1, maxReferenceFailures);
    const context = {
      transactionId: row.id, walletId: row.wallet_id, providerId: row.provider_id,
      correlationId: row.correlation_id, messageId: row.message_id ?? undefined,
      errorCode: errorCode(error), failures,
    };
    if (failures === maxReferenceFailures) {
      try {
        return await em.transactional(async attempt => {
          const wallet = await loadWallet(attempt, row.wallet_id);
          if (!wallet) throw new Error('Missing wallet for pending wager');
          const wager = rehydrateWager(row);
          wager.transition('FAILED', 'INFRASTRUCTURE_RETRIES_EXHAUSTED');
          return this.persistTerminal(attempt, wallet, row, wager, undefined, failures);
        });
      } catch (recordError) {
        log('reference_failure_audit_unavailable', { ...context, auditErrorCode: errorCode(recordError) }, 'error');
      }
    }

    const delay = failures === maxReferenceFailures ? 60 : 2 ** failures;
    const rescheduled = await one<WagerRow>(em, `
      update wager_transactions set reference_failures=?,
        next_attempt_at=clock_timestamp()+? * interval '1 second'
      where id=? and status='PENDING_REFERENCE' returning *`, [failures, delay, row.id]);
    log('reference_retry', { ...context, delaySeconds: delay }, 'warn');
    return rescheduled;
  }

  private logResult(row: WagerRow, correlationId: string, messageId?: string, replay = false) {
    log('wager_result', {
      correlationId, messageId, transactionId: row.id, walletId: row.wallet_id,
      providerId: row.provider_id, status: row.status, failureCode: row.failure_code ?? undefined, replay,
    });
  }

  async wallet(id: string) {
    const wallet = await loadWallet(this.db.em.fork(), id);
    if (!wallet) throw new Missing('WALLET_NOT_FOUND');
    return wallet.toJSON();
  }

  async ledger(walletId: string, beforeVersion: number, limit: number) {
    await this.wallet(walletId);
    const rows = await all<LedgerRow>(this.db.em.fork(), `
      select * from wallet_ledger_entries where wallet_id=? and wallet_version < ?::bigint
      order by wallet_version desc limit ?`, [walletId, beforeVersion, limit + 1]);
    const page = rows.slice(0, limit);
    return {
      entries: page.map(row => ({
        transactionId: row.transaction_id, walletVersion: row.wallet_version, direction: row.direction,
        money: { amount: row.amount, currency: row.currency },
        balanceBefore: { amount: row.balance_before, currency: row.currency },
        balanceAfter: { amount: row.balance_after, currency: row.currency }, createdAt: row.created_at,
      })),
      nextCursor: rows.length > limit
        ? Buffer.from(JSON.stringify([walletId, page[page.length - 1].wallet_version])).toString('base64url')
        : null,
    };
  }

  async wager(query: { id: string } | { provider: string; external: string }) {
    const em = this.db.em.fork();
    const row = 'id' in query
      ? await one<WagerRow>(em, 'select * from wager_transactions where id=?', [query.id])
      : await one<WagerRow>(em, 'select * from wager_transactions where provider_id=? and external_id=?',
        [query.provider, query.external]);
    if (!row) throw new Missing('TRANSACTION_NOT_FOUND');
    const wager = rehydrateWager(row);
    return { ...wager.command, ...result(row, false), referenceTransactionId: wager.referenceId,
      createdAt: row.created_at, processedAt: row.processed_at };
  }

  async reconcile(walletId: string, correlationId: string) {
    // A single statement reads wallet and ledger under the same MVCC snapshot.
    const row = await one<{ currency: string; balance: string; calculated: string; count: number }>(
      this.db.em.fork(), `
      select w.currency,w.balance,coalesce(sum(case l.direction when 'DEBIT' then -l.amount else l.amount end),
        0)::numeric as calculated,count(l.transaction_id)::int as count
      from wallets w left join wallet_ledger_entries l on l.wallet_id=w.id where w.id=? group by w.id`, [walletId]);
    if (!row) throw new Missing('WALLET_NOT_FOUND');
    const stored = Money.rehydrate({ amount: row.balance, currency: row.currency });
    const calculated = Money.rehydrate({ amount: row.count === 0 ? '0.00' : row.calculated, currency: row.currency });
    const difference = stored.subtract(calculated);
    if (!difference.isZero()) {
      mismatch.inc();
      log('wallet_reconciliation_mismatch', { correlationId, walletId });
    }
    return {
      walletId, storedBalance: stored.toJSON(), calculatedBalance: calculated.toJSON(),
      difference: difference.toJSON(), consistent: difference.isZero(), checkedEntries: row.count,
    };
  }
}
