import { describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { hashCommand, httpCommand } from '../src/contracts';
import { Money, Wallet, WalletLedgerEntry, WagerTransaction, decide, type Command } from '../src/domain';

const money = (amount: string, currency = 'BRL') => Money.from({ amount, currency });
const playerId = randomUUID();
const walletId = randomUUID();
const base: Command = {
  playerId, walletId, providerId: 'p', roundId: 'round', gameId: 'game', externalTransactionId: 'bet',
  kind: 'BET', money: { amount: '25.00', currency: 'BRL' },
};
const create = (override: Partial<Command> = {}) => WagerTransaction.create(randomUUID(), { ...base, ...override });

describe('Money e ledger', () => {
  test('soma, sinal, escala e valores além da precisão de number', () => {
    expect(money('0.10').add(money('0.20')).toString()).toBe('0.30');
    expect(money('999999999999999999.99').subtract(money('0.01')).toString()).toBe('999999999999999999.98');
    expect(money('0.01').negate().toJSON()).toEqual({ amount: '-0.01', currency: 'BRL' });
    expect(() => money('0.01').add(money('0.01', 'USD'))).toThrow('Currency mismatch');
  });

  test('rejeita inválidos sem arredondar', () => {
    for (const input of ['NaN', 'Infinity', '', '-1.00', '1e1', '1.1', '1.001', ' 1.00', '1.00\n',
      '1000000000000000000.00', '01.00']) {
      expect(() => money(input)).toThrow();
    }
    expect(() => money('1.00', 'ZZZ')).toThrow();
  });

  test('wallet protege saldo, preserva versão e ledger valida aritmética', () => {
    const before = money('25.00');
    const wallet = Wallet.open(walletId, playerId, before);
    expect(wallet.move('DEBIT', money('25.01'), false)).toBe('INSUFFICIENT_FUNDS');
    expect(wallet.move('DEBIT', money('25.01'), true)).toBe('REVERSAL_INSUFFICIENT_FUNDS');
    expect(wallet.version).toBe(1);
    expect(wallet.move('DEBIT', money('25.00'), false)).toBeUndefined();
    expect(wallet.version).toBe(2);
    expect(wallet.balance.toString()).toBe('0.00');
    expect(before.toString()).toBe('25.00');
    expect(() => WalletLedgerEntry.create({
      walletId, walletVersion: 2, transactionId: 't', direction: 'DEBIT',
      money: money('25.00'), balanceBefore: before, balanceAfter: before,
    })).toThrow('Unbalanced');
  });
});

describe('regras de aposta', () => {
  const wallet = Wallet.open(walletId, playerId, money('100.00'));
  const bet = create();
  bet.transition('PROCESSED');

  test('BET, WIN, LOSS, REFUND e ROLLBACK', () => {
    expect(decide(create(), wallet, undefined, false, false)).toEqual({ direction: 'DEBIT' });
    expect(decide(create({ kind: 'WIN' }), wallet, undefined, false, false)).toEqual({ direction: 'CREDIT' });
    expect(decide(create({ kind: 'LOSS' }), wallet, undefined, false, false)).toEqual({});
    const reversal = { referenceExternalTransactionId: 'bet', externalTransactionId: 'r' };
    const refund = create({ ...reversal, kind: 'REFUND' });
    expect(decide(refund, wallet, bet, false, false)).toEqual({ direction: 'CREDIT' });
    expect(decide(refund, wallet, bet, false, true)).toEqual({ reject: 'REVERSAL_ALREADY_APPLIED' });
    expect(decide(refund, wallet, undefined, false, false)).toEqual({ pending: true });
    expect(decide(refund, wallet, undefined, true, false)).toEqual({ reject: 'REFERENCE_NOT_FOUND' });
    const rollback = create({ ...reversal, kind: 'ROLLBACK' });
    expect(decide(rollback, wallet, bet, false, false)).toEqual({ direction: 'CREDIT' });
    for (const source of [refund, create({ kind: 'WIN' })]) {
      source.transition('PROCESSED');
      expect(decide(rollback, wallet, source, false, false)).toEqual({ direction: 'DEBIT' });
    }
    expect(() => bet.transition('REJECTED', 'INSUFFICIENT_FUNDS')).toThrow('Terminal wager');
  });

  test('validação e hash canônico distinguem payloads', () => {
    const { money: value, ...fields } = base;
    expect(hashCommand({ money: { currency: value.currency, amount: value.amount }, ...fields }))
      .toBe(hashCommand(base));
    expect(hashCommand({ ...base, money: { ...value, amount: '26.00' } })).not.toBe(hashCommand(base));
    expect(httpCommand.safeParse({ ...base, kind: 'OPENING' }).success).toBe(false);
    expect(httpCommand.safeParse({ ...base, kind: 'REFUND' }).success).toBe(false);
    expect(decide(create({ money: { ...value, currency: 'USD' } }), wallet, undefined, false, false))
      .toEqual({ reject: 'CURRENCY_MISMATCH' });
  });
});
