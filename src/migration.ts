import { Migration } from '@mikro-orm/migrations';

export class Migration202611110001 extends Migration {
  override up(): void {
    this.addSql(`
      create table wallets (
        id uuid primary key, player_id uuid not null, currency text not null,
        balance numeric(20,2) not null check (balance >= 0 and balance <> 'NaN'),
        version integer not null check (version >= 1),
        created_at timestamptz not null default clock_timestamp(),
        updated_at timestamptz not null default clock_timestamp(),
        unique (player_id, currency), check (currency ~ '^[A-Z]{3}$')
      );

      create table wager_transactions (
        id uuid primary key, provider_id text not null, external_id text not null,
        idempotency_key text not null, payload_hash char(64) not null,
        wallet_id uuid not null references wallets(id), player_id uuid not null,
        round_id text not null, game_id text not null,
        kind text not null check (kind in ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        currency text not null check (currency ~ '^[A-Z]{3}$'),
        amount numeric(20,2) not null check (amount >= 0 and amount <> 'NaN'),
        reference_external_id text, reference_id uuid references wager_transactions(id),
        status text not null check (status in ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        failure_code text, result_balance numeric(20,2), result_currency text, result_version integer,
        correlation_id text not null, message_id text,
        attempts integer not null default 0 check (attempts >= 0),
        next_attempt_at timestamptz, expires_at timestamptz,
        created_at timestamptz not null default clock_timestamp(), processed_at timestamptz,
        unique (provider_id, external_id), unique (provider_id, idempotency_key),
        unique (wallet_id, id),
        check (amount > 0 or kind = 'LOSS'),
        check ((kind = 'OPENING') = (provider_id = '__opening__')),
        check (kind not in ('REFUND','ROLLBACK') or reference_external_id is not null),
        check (status <> 'PROCESSED' or kind not in ('REFUND','ROLLBACK') or reference_id is not null),
        check ((status in ('REJECTED','FAILED')) = (failure_code is not null)),
        check ((status in ('PROCESSED','REJECTED','FAILED')) = (processed_at is not null)),
        check ((status = 'PENDING_REFERENCE') = (next_attempt_at is not null)),
        check (result_balance is null or (result_balance >= 0 and result_balance <> 'NaN'))
      );
      create unique index wager_single_reversal on wager_transactions (reference_id)
        where status = 'PROCESSED' and kind in ('REFUND','ROLLBACK');
      create index wager_pending_due on wager_transactions (next_attempt_at,id)
        where status = 'PENDING_REFERENCE';

      create table wallet_ledger_entries (
        transaction_id uuid primary key, wallet_id uuid not null,
        wallet_version integer not null check (wallet_version >= 1),
        direction text not null check (direction in ('CREDIT','DEBIT')),
        currency text not null check (currency ~ '^[A-Z]{3}$'),
        amount numeric(20,2) not null check (amount > 0 and amount <> 'NaN'),
        balance_before numeric(20,2) not null check (balance_before >= 0 and balance_before <> 'NaN'),
        balance_after numeric(20,2) not null check (balance_after >= 0 and balance_after <> 'NaN'),
        created_at timestamptz not null default clock_timestamp(),
        foreign key (wallet_id, transaction_id) references wager_transactions(wallet_id,id),
        unique (wallet_id, wallet_version),
        check (balance_after = balance_before + case direction when 'CREDIT' then amount else -amount end)
      );

      create function protect_ledger() returns trigger language plpgsql as $$
      begin
        raise exception 'Ledger is append-only' using errcode = '23514';
      end
      $$;
      create trigger ledger_append_only before update or delete on wallet_ledger_entries
        for each row execute function protect_ledger();
      create trigger ledger_no_truncate before truncate on wallet_ledger_entries
        for each statement execute function protect_ledger();

      create function protect_terminal_wager() returns trigger language plpgsql as $$
      begin
        if old.status in ('PROCESSED','REJECTED','FAILED') then
          raise exception 'Wager is terminal' using errcode = '23514';
        end if;
        return new;
      end
      $$;
      create trigger wager_terminal before update on wager_transactions
        for each row execute function protect_terminal_wager();

      create table inbox_messages (
        consumer_name text not null, message_id text not null, payload_hash char(64) not null,
        received_at timestamptz not null default clock_timestamp(), processed_at timestamptz,
        primary key (consumer_name,message_id)
      );

      create table outbox_messages (
        id uuid primary key, aggregate_id uuid not null references wallets(id),
        event_type text not null, payload jsonb not null,
        occurred_at timestamptz not null, attempts integer not null default 0,
        next_attempt_at timestamptz not null default clock_timestamp(),
        published_at timestamptz, claim_token uuid, lease_until timestamptz, last_error text,
        check ((claim_token is null) = (lease_until is null))
      );
      create index outbox_pending on outbox_messages (next_attempt_at,occurred_at,id)
        where published_at is null;
    `);
  }

  override down(): void {
    this.addSql(`
      drop table outbox_messages,inbox_messages,wallet_ledger_entries,wager_transactions,wallets;
      drop function protect_ledger();
      drop function protect_terminal_wager();
    `);
  }
}

export class Migration202611110002 extends Migration {
  override up(): void {
    this.addSql(`
      create trigger wager_terminal_no_delete before delete on wager_transactions
        for each row when (old.status in ('PROCESSED','REJECTED','FAILED'))
        execute function protect_terminal_wager();
    `);
  }

  override down(): void {
    this.addSql('drop trigger wager_terminal_no_delete on wager_transactions;');
  }
}

export class Migration202611110003 extends Migration {
  override up(): void {
    this.addSql('alter table wager_transactions add column reference_failures integer not null default 0 check (reference_failures >= 0);');
  }

  override down(): void {
    this.addSql('alter table wager_transactions drop column reference_failures;');
  }
}
