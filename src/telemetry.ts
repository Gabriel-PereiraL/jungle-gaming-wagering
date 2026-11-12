import { Counter, Gauge, Histogram, Registry } from 'prom-client';
import { hostname } from 'node:os';

export const registry = new Registry();
export const transactions = new Counter({
  name: 'wager_transactions_total', help: 'Committed wager outcomes', labelNames: ['status', 'kind'],
  registers: [registry],
});
export const duplicates = new Counter({
  name: 'wager_duplicate_total', help: 'Idempotent replays', registers: [registry],
});
export const retries = new Counter({
  name: 'wager_retry_total', help: 'Messaging and reference retry attempts', labelNames: ['source'],
  registers: [registry],
});
export const lockConflicts = new Counter({
  name: 'wallet_lock_conflict_total', help: 'Database lock timeouts and deadlocks', registers: [registry],
});
export const duration = new Histogram({
  name: 'wager_processing_duration_seconds', help: 'Submission latency', labelNames: ['source'],
  registers: [registry],
});
export const mismatch = new Counter({
  name: 'wallet_reconciliation_mismatch_total', help: 'Reconciliation mismatches', registers: [registry],
});
export const lag = new Gauge({
  name: 'outbox_lag_seconds', help: 'Age of oldest unpublished event', registers: [registry],
});
export const dlq = new Gauge({
  name: 'sqs_dlq_messages', help: 'Approximate visible plus in-flight messages in DLQ', registers: [registry],
});

export function errorCode(error: unknown): string {
  if (!(error instanceof Error)) return 'UNKNOWN';
  const code = (error as Error & { code?: unknown }).code;
  return typeof code === 'string' ? code : error.name;
}

const instanceId = hostname();

export function log(event: string, context: Record<string, unknown> = {},
                    severity: 'info' | 'warn' | 'error' = 'info'): void {
  console.log(JSON.stringify({ ...context, event, severity, instanceId, at: new Date().toISOString() }));
}
