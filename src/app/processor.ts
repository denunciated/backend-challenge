import type { DataSource, EntityManager } from 'typeorm';
import { AppError, DomainError, type FailureCode } from '../domain/errors';
import { Money, type MoneyProps } from '../domain/money';
import { Wallet, WalletLedgerEntry } from '../domain/wallet';
import { WagerTransaction, type Kind, type Status } from '../domain/transaction';
import { InboxMessage, OutboxMessage, backoff } from '../domain/messages';
import {
  WalletBalanceChanged,
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  type EventContext,
  type IntegrationEvent,
} from '../domain/events';
import { hashPayload, parseWager, walletSchema } from './contracts';
import { WalletEntity, databaseErrorCode } from '../infra/database';
import { config } from '../infra/config';
import { log, metrics } from '../infra/observability';

export interface Result {
  transactionId: string;
  status: Status;
  balance: MoneyProps;
  failureCode?: FailureCode;
  idempotentReplay: boolean;
}

export interface RequestContext {
  correlationId: string;
  messageId?: string;
  inboxHash?: string;
}

export interface TransactionRow {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  idempotency_key: string;
  payload_hash: string;
  wallet_id: string;
  player_id: string;
  round_id: string;
  game_id: string;
  kind: Kind;
  amount: string;
  currency: string;
  reference_external_transaction_id: string | null;
  reference_transaction_id: string | null;
  status: Status;
  failure_code: FailureCode | null;
  processed_at: Date | null;
  result: Result;
  reference_attempts: number;
  next_attempt_at: Date | null;
  correlation_id: string;
  causation_id: string | null;
}

export function rehydrateTransaction(row: TransactionRow): WagerTransaction {
  return WagerTransaction.rehydrate({
    id: row.id,
    providerId: row.provider_id,
    externalTransactionId: row.external_transaction_id,
    idempotencyKey: row.idempotency_key,
    payloadHash: row.payload_hash,
    walletId: row.wallet_id,
    playerId: row.player_id,
    roundId: row.round_id,
    gameId: row.game_id,
    kind: row.kind,
    money: Money.rehydrate({ amount: row.amount, currency: row.currency }),
    referenceExternalTransactionId: row.reference_external_transaction_id ?? undefined,
    status: row.status,
    referenceTransactionId: row.reference_transaction_id ?? undefined,
    failureCode: row.failure_code ?? undefined,
    processedAt: row.processed_at ?? undefined,
  });
}

const consumerName = 'wagering-v1';

export class WageringService {
  constructor(readonly db: DataSource) {}

  async createWallet(payload: unknown, correlationId: string) {
    const input = walletSchema.parse(payload);
    const wallet = Wallet.open({
      id: crypto.randomUUID(),
      playerId: input.playerId,
      initialBalance: Money.from(input.initialBalance),
    });
    try {
      await this.atomic(async (manager) => {
        await manager.getRepository(WalletEntity).insert({
          id: wallet.id,
          playerId: wallet.playerId,
          currency: wallet.currency,
          balance: wallet.balance.toString(),
          version: 1,
          createdAt: new Date(),
          updatedAt: new Date(),
        });
        if (wallet.balance.isPositive()) {
          const tx = WagerTransaction.create({
            id: crypto.randomUUID(),
            providerId: 'internal',
            externalTransactionId: wallet.id,
            idempotencyKey: `opening:${wallet.id}`,
            payloadHash: hashPayload(input),
            walletId: wallet.id,
            playerId: wallet.playerId,
            roundId: 'opening',
            gameId: 'opening',
            kind: 'OPENING',
            money: wallet.balance,
          });
          await this.insertTransaction(manager, tx, { correlationId });
          tx.markProcessed(undefined, new Date());
          const entry = WalletLedgerEntry.create({
            id: crypto.randomUUID(),
            walletId: wallet.id,
            transactionId: tx.props.id,
            direction: 'CREDIT',
            money: wallet.balance,
            balanceBefore: Money.zero(wallet.currency),
            balanceAfter: wallet.balance,
            createdAt: new Date(),
          });
          await this.insertLedger(manager, entry);
          await this.persistResult(manager, tx, wallet, { correlationId }, undefined, entry);
        }
      });
    } catch (error) {
      if (databaseErrorCode(error) === '23505') throw new AppError('WALLET_ALREADY_EXISTS', 409);
      throw error;
    }
    return wallet.toJSON();
  }

  async submit(payload: unknown, key: string, context: RequestContext): Promise<Result> {
    const { transaction } = parseWager(payload, key);
    const stop = metrics.latency.startTimer();
    try {
      const result = await this.atomic(async (manager) => {
        const transaction = WagerTransaction.create(parseWager(payload, key).transaction.props);

        let inbox: InboxMessage | undefined;

        if (context.messageId) {
          inbox = InboxMessage.recieve({
            messageId: context.messageId,
            consumerName,
            payloadHash: context.inboxHash ?? hashPayload({ hash: transaction.props.payloadHash, key }),
          });
          await manager.query(
            'INSERT INTO inbox(consumer_name,message_id,payload_hash) VALUES($1,$2,$3) ON CONFLICT DO NOTHING',
            [consumerName, inbox.messageId, inbox.payloadHash],
          );
          const [existing] = await manager.query(
            'SELECT * FROM inbox WHERE consumer_name=$1 AND message_id=$2 FOR UPDATE',
            [consumerName, inbox.messageId],
          );
          if (existing.payload_hash !== inbox.payloadHash) throw new AppError('INBOX_PAYLOAD_CONFLICT', 409);
          if (existing.processed_at) {
            const [row] = await manager.query('SELECT result FROM wager_transactions WHERE id=$1', [
              existing.transaction_id,
            ]);
            return { ...row.result, idempotentReplay: true } as Result;
          }
        }

        const previous: TransactionRow[] = await manager.query(
          'SELECT * FROM wager_transactions WHERE idempotency_key=$1 OR (provider_id=$2 AND external_transaction_id=$3)',
          [key, transaction.props.providerId, transaction.props.externalTransactionId],
        );
        if (previous.length) {
          const row = previous[0]!;
          if (
            previous.length !== 1 ||
            row.idempotency_key !== key ||
            !rehydrateTransaction(row).matchesPayload(transaction.props.payloadHash)
          )
            throw new AppError('IDEMPOTENCY_CONFLICT', 409);
          if (inbox) {
            inbox.markProcessed(new Date());
            await manager.query(
              'UPDATE inbox SET processed_at=$3,transaction_id=$4 WHERE consumer_name=$1 AND message_id=$2',
              [consumerName, inbox.messageId, inbox.procssedAt, row.id],
            );
          }
          return { ...row.result, idempotentReplay: true };
        }

        const wallet = await this.lockWallet(manager, transaction.props.walletId);

        const inserted = await this.insertTransaction(manager, transaction, context);

        let result: Result;

        if (!inserted) {
          const rows: TransactionRow[] = await manager.query(
            'SELECT * FROM wager_transactions WHERE idempotency_key=$1 OR (provider_id=$2 AND external_transaction_id=$3)',
            [key, transaction.props.providerId, transaction.props.externalTransactionId],
          );

          const row = rows[0];

          if (
            rows.length !== 1 ||
            !row ||
            row.idempotency_key !== key ||
            !rehydrateTransaction(row).matchesPayload(transaction.props.payloadHash)
          )
            throw new AppError('IDEMPOTENCY_CONFLICT', 409);
          result = { ...row.result, idempotentReplay: true };
        } else {
          result = await this.apply(manager, transaction, wallet, context, 0);
        }
        if (inbox) {
          inbox.markProcessed(new Date());
          await manager.query(
            'UPDATE inbox SET processed_at=$3,transaction_id=$4 WHERE consumer_name=$1 AND message_id=$2',
            [consumerName, inbox.messageId, inbox.procssedAt, result.transactionId],
          );
        }
        return result;
      });

      this.observe(result, transaction, context);
      return result;
    } finally {
      stop();
    }
  }

  async retryReference(id: string): Promise<void> {
    const [candidate]: TransactionRow[] = await this.db.query('SELECT * FROM wager_transactions WHERE id=$1', [id]);

    if (!candidate || candidate.status !== 'PENDING_REFERENCE') return;

    const result = await this.atomic(async (manager) => {
      const wallet = await this.lockWallet(manager, candidate.wallet_id);
      const [row]: TransactionRow[] = await manager.query('SELECT * FROM wager_transactions WHERE id=$1 FOR UPDATE', [
        id,
      ]);
      if (!row || row.status !== 'PENDING_REFERENCE' || (row.next_attempt_at && row.next_attempt_at > new Date()))
        return;
      const transaction = rehydrateTransaction(row);
      return this.apply(
        manager,
        transaction,
        wallet,
        { correlationId: row.correlation_id, messageId: row.causation_id ?? undefined },
        row.reference_attempts + 1,
      );
    });
    if (result) {
      metrics.retries.inc({ worker: 'references' });
      metrics.transactions.inc({ status: result.status });
    }
  }

  private async apply(
    manager: EntityManager,
    tx: WagerTransaction,
    wallet: Wallet,
    context: RequestContext,
    attempts: number,
  ): Promise<Result> {
    let reference: WagerTransaction | undefined;

    let entry: WalletLedgerEntry | undefined;

    const wasPending = tx.status === 'PENDING_REFERENCE';

    try {
      if (tx.props.playerId !== wallet.playerId) throw new DomainError('PLAYER_MISMATCH');
      if (tx.props.money.currency !== wallet.currency) throw new DomainError('CURRENCY_MISMATCH');
      if (tx.props.referenceExternalTransactionId) {
        const [row]: TransactionRow[] = await manager.query(
          'SELECT * FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2',
          [tx.props.providerId, tx.props.referenceExternalTransactionId],
        );
        if (!row || row.status === 'PENDING' || row.status === 'PENDING_REFERENCE') {
          if (attempts >= config.referenceMaxAttempts) tx.reject('REFERENCE_EXPIRED');
          else tx.markPendingReference();
          return await this.persistResult(manager, tx, wallet, context, attempts, undefined, wasPending);
        }
        reference = rehydrateTransaction(row);

        tx.validateReference(reference);

        if (tx.requiresReference()) {
          const [reversed] = await manager.query(
            "SELECT id FROM wager_transactions WHERE reference_transaction_id=$1 AND kind IN ('REFUND','ROLLBACK') AND status='PROCESSED'",
            [reference.props.id],
          );
          if (reversed) throw new DomainError('ALREADY_REVERSED');
        }
      }
      if (tx.affectsBalance()) {
        const before = wallet.balance;
        const direction = tx.ledgerDirectionFor(reference);
        if (direction === 'DEBIT') wallet.debit(tx.props.money, tx.props.kind === 'ROLLBACK');
        else wallet.credit(tx.props.money);
        entry = WalletLedgerEntry.create({
          id: crypto.randomUUID(),
          walletId: wallet.id,
          transactionId: tx.props.id,
          direction,
          money: tx.props.money,
          balanceBefore: before,
          balanceAfter: wallet.balance,
          createdAt: new Date(),
        });
      }
      tx.markProcessed(reference?.props.id, new Date());
    } catch (error) {
      if (!(error instanceof DomainError)) throw error;
      const failures: FailureCode[] = [
        'INSUFFICIENT_FUNDS',
        'REVERSAL_INSUFFICIENT_FUNDS',
        'CURRENCY_MISMATCH',
        'PLAYER_MISMATCH',
        'REFERENCE_SCOPE_MISMATCH',
        'REFERENCE_KIND_INVALID',
        'REFERENCE_NOT_PROCESSED',
        'REFERENCE_AMOUNT_MISMATCH',
        'ALREADY_REVERSED',
        'AMOUNT_OVERFLOW',
      ];
      if (!failures.includes(error.code as FailureCode)) throw error;
      tx.reject(error.code as FailureCode);
    }
    if (entry) {
      await manager
        .getRepository(WalletEntity)
        .update(wallet.id, { balance: wallet.balance.toString(), version: wallet.version, updatedAt: new Date() });
      await this.insertLedger(manager, entry);
    }
    return this.persistResult(manager, tx, wallet, context, attempts, entry, wasPending);
  }

  private async persistResult(
    manager: EntityManager,
    tx: WagerTransaction,
    wallet: Wallet,
    context: RequestContext,
    attempts = 0,
    entry?: WalletLedgerEntry,
    wasPending = false,
  ): Promise<Result> {
    const result: Result = {
      transactionId: tx.props.id,
      status: tx.status,
      balance: wallet.balance.toJSON(),
      ...(tx.failureCode ? { failureCode: tx.failureCode } : {}),
      idempotentReplay: false,
    };
    const nextAttempt =
      tx.status === 'PENDING_REFERENCE'
        ? new Date(Date.now() + backoff(attempts + 1, config.referenceBaseDelayMS))
        : null;
    await manager.query(
      'UPDATE wager_transactions SET status=$2,reference_transaction_id=$3,failure_code=$4,processed_at=$5,result=$6,reference_attempts=$7,next_attempt_at=$8 WHERE id=$1',
      [
        tx.props.id,
        tx.status,
        tx.referenceTransactionId ?? null,
        tx.failureCode ?? null,
        tx.processedAt ?? null,
        result,
        attempts,
        nextAttempt,
      ],
    );
    const eventContext: EventContext = {
      aggregateId: wallet.id,
      correlationId: context.correlationId,
      causationId: context.messageId ?? tx.props.id,
    };
    const data = {
      transactionId: tx.props.id,
      walletId: wallet.id,
      providerId: tx.props.providerId,
      status: tx.status,
      balance: wallet.balance.toJSON(),
      failureCode: tx.failureCode,
    };
    if (tx.status === 'PROCESSED') await this.enqueue(manager, WagerTransactionProcessed.from(data, eventContext));
    if (tx.status === 'REJECTED') await this.enqueue(manager, WagerTransactionRejected.from(data, eventContext));
    if (tx.status === 'PENDING_REFERENCE' && !wasPending)
      await this.enqueue(manager, WagerTransactionPendingReference.from(data, eventContext));
    if (entry)
      await this.enqueue(
        manager,
        WalletBalanceChanged.from(
          {
            walletId: wallet.id,
            transactionId: tx.props.id,
            direction: entry.direction,
            money: entry.money.toJSON(),
            balanceBefore: entry.balanceBefore.toJSON(),
            balanceAfter: entry.balanceAfter.toJSON(),
            walletVersion: wallet.version,
          },
          eventContext,
        ),
      );
    return result;
  }

  private async insertTransaction(manager: EntityManager, tx: WagerTransaction, ctx: RequestContext): Promise<boolean> {
    const p = tx.props;
    const inserted = await manager.query(
      `INSERT INTO wager_transactions(id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,reference_external_transaction_id,status,correlation_id,causation_id)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'PENDING',$14,$15) ON CONFLICT DO NOTHING RETURNING id`,
      [
        p.id,
        p.providerId,
        p.externalTransactionId,
        p.idempotencyKey,
        p.payloadHash,
        p.walletId,
        p.playerId,
        p.roundId,
        p.gameId,
        p.kind,
        p.money.toString(),
        p.money.currency,
        p.referenceExternalTransactionId ?? null,
        ctx.correlationId,
        ctx.messageId ?? null,
      ],
    );
    return inserted.length > 0;
  }
  private async insertLedger(manager: EntityManager, e: WalletLedgerEntry): Promise<void> {
    await manager.query(
      'INSERT INTO wallet_ledger(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after) VALUES($1,$2,$3,$4,$5,$6,$7,$8)',
      [
        e.id,
        e.walletId,
        e.transactionId,
        e.direction,
        e.money.toString(),
        e.money.currency,
        e.balanceBefore.toString(),
        e.balanceAfter.toString(),
      ],
    );
  }
  private async enqueue(manager: EntityManager, event: IntegrationEvent<object>): Promise<void> {
    const message = OutboxMessage.enqueue(event);
    await manager.query('INSERT INTO outbox(id,aggregate_id,event_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5)', [
      message.id,
      message.aggregateId,
      message.eventType,
      message.payload,
      message.occurredAt,
    ]);
  }
  private async lockWallet(manager: EntityManager, id: string): Promise<Wallet> {
    const record = await manager
      .getRepository(WalletEntity)
      .findOne({ where: { id }, lock: { mode: 'pessimistic_write' } });
    if (!record) throw new AppError('WALLET_NOT_FOUND', 404);
    return Wallet.rehydrate({
      ...record,
      balance: Money.rehydrate({ amount: record.balance, currency: record.currency }),
    });
  }
  private async atomic<T>(work: (manager: EntityManager) => Promise<T>): Promise<T> {
    for (let attempt = 1; ; attempt++) {
      try {
        return await this.db.transaction('READ COMMITTED', async (manager) => {
          await manager.query("SET LOCAL lock_timeout = '3s'");
          await manager.query("SET LOCAL statement_timeout = '15s'");
          return work(manager);
        });
      } catch (error) {
        if (!['40P01', '40001', '55P03'].includes(databaseErrorCode(error) ?? '')) throw error;
        metrics.lockConflicts.inc();
        if (attempt >= 3) throw new AppError('DATABASE_BUSY', 503);
        metrics.retries.inc({ worker: 'database' });
        await Bun.sleep(20 * attempt + Math.random() * 30);
      }
    }
  }
  private observe(result: Result, tx: WagerTransaction, context: RequestContext) {
    if (result.idempotentReplay) metrics.duplicates.inc();
    else metrics.transactions.inc({ status: result.status });
    log('info', result.idempotentReplay ? 'transaction_replayed' : 'transaction_committed', {
      correlationId: context.correlationId,
      messageId: context.messageId,
      transactionId: result.transactionId,
      walletId: tx.props.walletId,
      providerId: tx.props.providerId,
      code: result.failureCode,
    });
  }
}
