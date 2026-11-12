import { createHash } from 'node:crypto';
import { z } from 'zod';
import { amountPattern, isCurrency, type Command } from './domain';

export const identifier = z.string().min(1).max(128).regex(/^[!-~]+$/);
export const uuid = z.uuid().transform(value => value.toLowerCase());
export const money = z.strictObject({ amount: z.string().regex(amountPattern), currency: z.string().refine(isCurrency) });
const commandShape = {
  providerId: identifier.refine(value => value !== '__opening__'),
  externalTransactionId: identifier,
  walletId: uuid,
  playerId: uuid,
  roundId: identifier,
  gameId: identifier,
  kind: z.enum(['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK']),
  money,
  referenceExternalTransactionId: identifier.optional(),
};

function validCommand(command: Command): boolean {
  const requiresReference = command.kind === 'REFUND' || command.kind === 'ROLLBACK';
  const allowsReference = requiresReference || command.kind === 'WIN' || command.kind === 'LOSS';
  return (!requiresReference || !!command.referenceExternalTransactionId) &&
    (allowsReference || !command.referenceExternalTransactionId) &&
    (command.kind === 'LOSS' || command.money.amount !== '0.00');
}

export const httpCommand = z.strictObject(commandShape).refine(validCommand);
export const openWallet = z.strictObject({ playerId: uuid, initialBalance: money });
export const queueEnvelope = z.strictObject({
  messageId: identifier,
  type: z.literal('WagerTransactionRequested'),
  version: z.literal(1).optional(),
  occurredAt: z.iso.datetime({ offset: true }),
  correlationId: identifier.optional(),
  data: z.strictObject({ ...commandShape, idempotencyKey: identifier }).refine(validCommand),
});

function canonical(value: unknown): string {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value && typeof value === 'object') {
    const object = value as Record<string, unknown>;
    return '{' + Object.keys(object).sort().filter(key => object[key] !== undefined)
      .map(key => JSON.stringify(key) + ':' + canonical(object[key])).join(',') + '}';
  }
  return JSON.stringify(value);
}

export function hashCommand(command: Command): string {
  return createHash('sha256').update(canonical(command)).digest('hex');
}

export function hashInbox(payloadHash: string, idempotencyKey: string): string {
  return createHash('sha256').update(canonical([payloadHash, idempotencyKey])).digest('hex');
}
