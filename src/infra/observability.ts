import { Counter, Gauge, Histogram, Registry } from 'prom-client';

export const registry = new Registry();

export const metrics = {
  transactions: new Counter({
    name: 'wager_transaction_total',
    help: 'Commited transaction state transitions',
    labelNames: ['status'],
    registers: [registry],
  }),
  duplicates: new Counter({
    name: 'wager_duplicates_total',
    help: 'Persistent idempotent replays',
    registers: [registry],
  }),
  retries: new Counter({
    name: 'wager_retries_total',
    help: 'Retry Attempts',
    labelNames: ['worker'],
    registers: [registry],
  }),
  dlq: new Counter({
    name: 'wager_dlq_total',
    help: 'Messages routed to DLQ',
    registers: [registry],
  }),
  lockConflicts: new Counter({
    name: 'wager_lock_conflicts_total',
    help: 'Deadlocks, serialization failures and lock timeouts',
    registers: [registry],
  }),
  outboxLag: new Gauge({
    name: 'wager_outbox_lag_seconds',
    help: 'Age of oldest unpublished event',
    registers: [registry],
  }),
  latency: new Histogram({
    name: 'wager_processing_seconds',
    help: 'Processing time including database locks',
    buckets: [0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10],
  }),
  reconciliation: new Counter({
    name: 'wager_reconciliation_mismatches_total',
    help: 'Detected ledger discrepancies',
    registers: [registry],
  }),
};

export interface LogContext {
  correlationId?: string;
  messageId?: string;
  transactionId?: string;
  walletId?: string;
  providerId?: string;
  code?: string;
  worker?: string;
}

export function log(level: 'info' | 'error' | 'warn', event: string, context: LogContext = {}) {
  const present = Object.fromEntries(Object.entries(context).filter(([, value]) => value !== undefined));
  console.log(
    JSON.stringify({
      timestamp: new Date().toISOString(),
      level,
      event,
      correlationId: null,
      messageId: null,
      transactionId: null,
      walletId: null,
      providerId: null,
      ...present,
    }),
  );
}
