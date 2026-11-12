export type MoneyProps = { amount: string; currency: string };
export type Kind = 'BET' | 'WIN' | 'LOSS' | 'REFUND' | 'ROLLBACK' | 'OPENING';
export type Status = 'PENDING' | 'PENDING_REFERENCE' | 'PROCESSED' | 'REJECTED' | 'FAILED';
export type Direction = 'CREDIT' | 'DEBIT';
export type FailureCode =
  | 'PLAYER_MISMATCH' | 'CURRENCY_MISMATCH' | 'INSUFFICIENT_FUNDS'
  | 'REVERSAL_INSUFFICIENT_FUNDS' | 'BALANCE_LIMIT_EXCEEDED'
  | 'REFERENCE_NOT_FOUND' | 'REFERENCE_NOT_PROCESSED' | 'REFERENCE_SCOPE_MISMATCH'
  | 'INVALID_REFERENCE_KIND' | 'AMOUNT_MISMATCH' | 'REVERSAL_ALREADY_APPLIED'
  | 'INFRASTRUCTURE_RETRIES_EXHAUSTED';

export const amountPattern = /^(?:0|[1-9][0-9]{0,17})\.[0-9]{2}$/;
export const maxCents = 10n ** 20n - 1n;
const supportedCurrencies = new Set(Intl.supportedValuesOf('currency'));
export const isCurrency = (code: string): boolean => supportedCurrencies.has(code);

export class Money {
  private constructor(private readonly cents: bigint, readonly currency: string) {
    Object.freeze(this);
  }

  static from({ amount, currency }: MoneyProps): Money {
    if (!amountPattern.test(amount) || !isCurrency(currency)) {
      throw new Error('Invalid money');
    }
    return Money.rehydrate({ amount, currency });
  }

  static rehydrate({ amount, currency }: MoneyProps): Money {
    return new Money(BigInt(amount.replace('.', '')), currency);
  }

  static zero(currency: string): Money {
    return Money.from({ amount: '0.00', currency });
  }

  add(other: Money): Money {
    this.assertCurrency(other);
    return new Money(this.cents + other.cents, this.currency);
  }

  subtract(other: Money): Money {
    return this.add(other.negate());
  }

  negate(): Money {
    return new Money(-this.cents, this.currency);
  }

  equals(other: Money): boolean {
    this.assertCurrency(other);
    return this.cents === other.cents;
  }

  isNegative(): boolean { return this.cents < 0n; }
  isZero(): boolean { return this.cents === 0n; }
  exceedsLimit(): boolean { return this.cents > maxCents; }

  toString(): string {
    const digits = (this.isNegative() ? -this.cents : this.cents).toString().padStart(3, '0');
    return (this.isNegative() ? '-' : '') + digits.slice(0, -2) + '.' + digits.slice(-2);
  }

  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }

  private assertCurrency(other: Money): void {
    if (other.currency !== this.currency) throw new Error('Currency mismatch');
  }
}

export interface WalletState {
  id: string;
  playerId: string;
  balance: Money;
  version: number;
}

export class Wallet {
  private constructor(private state: WalletState) {}

  static open(id: string, playerId: string, initial: Money): Wallet {
    if (initial.isNegative() || initial.exceedsLimit()) throw new Error('Invalid opening balance');
    return new Wallet({ id, playerId, balance: initial, version: 1 });
  }

  static rehydrate(state: WalletState): Wallet { return new Wallet({ ...state }); }

  get id() { return this.state.id; }
  get playerId() { return this.state.playerId; }
  get balance() { return this.state.balance; }
  get version() { return this.state.version; }

  move(direction: Direction, amount: Money, reversal: boolean): FailureCode | undefined {
    if (amount.isNegative()) throw new Error('Invalid movement amount');
    const next = direction === 'CREDIT' ? this.balance.add(amount) : this.balance.subtract(amount);
    if (next.isNegative()) return reversal ? 'REVERSAL_INSUFFICIENT_FUNDS' : 'INSUFFICIENT_FUNDS';
    if (next.exceedsLimit()) return 'BALANCE_LIMIT_EXCEEDED';
    if (!amount.isZero()) this.state = { ...this.state, balance: next, version: this.version + 1 };
  }

  toJSON() { return { ...this.state, balance: this.balance.toJSON() }; }
}

export interface Command {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: Exclude<Kind, 'OPENING'>;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

export interface WagerState {
  id: string;
  command: Omit<Command, 'kind'> & { kind: Kind };
  status: Status;
  failureCode?: FailureCode;
  referenceId?: string;
}

export class WagerTransaction {
  private constructor(private state: WagerState) {}

  static create(id: string, command: Command): WagerTransaction {
    if ((command.kind === 'REFUND' || command.kind === 'ROLLBACK') && !command.referenceExternalTransactionId) {
      throw new Error('Missing reference');
    }
    return WagerTransaction.rehydrate({ id, command, status: 'PENDING' });
  }

  static rehydrate(state: WagerState): WagerTransaction {
    const command = Object.freeze({ ...state.command, money: Object.freeze({ ...state.command.money }) });
    return new WagerTransaction({ ...state, command });
  }

  get id() { return this.state.id; }
  get command() { return this.state.command; }
  get status() { return this.state.status; }
  get failureCode() { return this.state.failureCode; }
  get referenceId() { return this.state.referenceId; }
  get money() { return Money.rehydrate(this.command.money); }
  get kind() { return this.command.kind; }
  get reversal() { return this.kind === 'REFUND' || this.kind === 'ROLLBACK'; }

  transition(status: Exclude<Status, 'PENDING'>, failureCode?: FailureCode, referenceId?: string): void {
    if (this.status !== 'PENDING' && this.status !== 'PENDING_REFERENCE') throw new Error('Terminal wager');
    if ((status === 'REJECTED' || status === 'FAILED') !== Boolean(failureCode)) {
      throw new Error('Invalid failure code for state');
    }
    this.state = { ...this.state, status, failureCode, referenceId };
  }
}

export interface LedgerState {
  walletId: string;
  walletVersion: number;
  transactionId: string;
  direction: Direction;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
}

export class WalletLedgerEntry {
  private constructor(readonly state: Readonly<LedgerState>) {}

  static create(state: LedgerState): WalletLedgerEntry {
    const delta = state.direction === 'CREDIT' ? state.money : state.money.negate();
    if (state.money.isNegative() || state.money.isZero() || state.balanceBefore.isNegative() ||
        state.balanceAfter.isNegative() || !state.balanceBefore.add(delta).equals(state.balanceAfter)) {
      throw new Error('Unbalanced ledger entry');
    }
    return WalletLedgerEntry.rehydrate(state);
  }

  static rehydrate(state: LedgerState): WalletLedgerEntry {
    return new WalletLedgerEntry(Object.freeze({ ...state }));
  }
}

export type Decision = { pending: true } | { reject: FailureCode } | { direction?: Direction };

export function decide(
  wager: WagerTransaction,
  wallet: Wallet,
  reference: WagerTransaction | undefined,
  expired: boolean,
  alreadyReversed: boolean,
): Decision {
  const { command } = wager;
  if (wallet.playerId !== command.playerId) return { reject: 'PLAYER_MISMATCH' };
  if (wallet.balance.currency !== command.money.currency) return { reject: 'CURRENCY_MISMATCH' };
  if (command.referenceExternalTransactionId) {
    if (command.referenceExternalTransactionId === command.externalTransactionId) {
      return { reject: 'INVALID_REFERENCE_KIND' };
    }
    if (!reference) return expired ? { reject: 'REFERENCE_NOT_FOUND' } : { pending: true };
    const source = reference.command;
    if (source.walletId !== command.walletId || source.playerId !== command.playerId ||
        source.money.currency !== command.money.currency || source.roundId !== command.roundId ||
        source.providerId !== command.providerId) {
      return { reject: 'REFERENCE_SCOPE_MISMATCH' };
    }
    const allowed = wager.kind === 'ROLLBACK' ? ['BET', 'WIN', 'REFUND'] : ['BET'];
    if (!allowed.includes(reference.kind)) return { reject: 'INVALID_REFERENCE_KIND' };
    if (reference.status === 'PENDING' || reference.status === 'PENDING_REFERENCE') {
      return expired ? { reject: 'REFERENCE_NOT_FOUND' } : { pending: true };
    }
    if (reference.status !== 'PROCESSED') return { reject: 'REFERENCE_NOT_PROCESSED' };
    if (wager.reversal && !wager.money.equals(reference.money)) return { reject: 'AMOUNT_MISMATCH' };
    if (wager.reversal && alreadyReversed) return { reject: 'REVERSAL_ALREADY_APPLIED' };
  }

  switch (wager.kind) {
    case 'BET': return { direction: 'DEBIT' };
    case 'LOSS': return {};
    case 'ROLLBACK': return { direction: reference?.kind === 'BET' ? 'CREDIT' : 'DEBIT' };
    default: return { direction: 'CREDIT' };
  }
}
