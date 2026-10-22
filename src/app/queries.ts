import type { DataSource } from 'typeorm';
import { z } from 'zod';
import { AppError } from '../domain/errors';
import { Money } from '../domain/money';
import { WalletEntity } from '../infra/database';
import { metrics, log } from '../infra/observability';
import type { TransactionRow } from './processor';

const sequenceSchema = z
  .string()
  .max(19)
  .regex(/^\d+$/)
  .refine((value) => BigInt(value) <= 9223372036854775807n);

const cursorSchema = z.object({ walletId: z.uuid(), after: sequenceSchema, through: sequenceSchema }).strict();

export class QueryService {
  constructor(private readonly db: DataSource) {}
  async wallet(id: string) {
    const record = await this.db.getRepository(WalletEntity).findOneBy({ id: z.uuid().parse(id) });
    if (!record) throw new AppError('WALLET_NOT_FOUND', 404);
    return {
      id: record.id,
      playerId: record.playerId,
      balance: Money.rehydrate({ amount: record.balance, currency: record.currency }).toJSON(),
      version: record.version,
    };
  }

  async transaction(id: string) {
    const [row]: TransactionRow[] = await this.db.query('SELECT * FROM wager_transactions WHERE id=$1', [
      z.uuid().parse(id),
    ]);
    return this.transactionResponse(row);
  }

  async providerTransaction(providerId: string, externalId: string) {
    const [row]: TransactionRow[] = await this.db.query(
      'SELECT * FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2',
      [providerId, externalId],
    );
    return this.transactionResponse(row);
  }

  private transactionResponse(row?: TransactionRow) {
    if (!row) throw new AppError('TRANSACTION_NOT_FOUND', 404);
    return {
      ...row.result,
      providerId: row.provider_id,
      externalTransactionId: row.external_transaction_id,
      walletId: row.wallet_id,
      kind: row.kind,
      money: Money.rehydrate({ amount: row.amount, currency: row.currency }).toJSON(),
      referenceTransactionId: row.reference_transaction_id,
      processedAt: row.processed_at,
    };
  }

  async ledger(walletId: string, cursor?: string, requestedLimit?: string) {
    await this.wallet(walletId);
    const limit = z.coerce
      .number()
      .int()
      .min(1)
      .max(200)
      .parse(requestedLimit ?? '50');
    let after = '0',
      through: string;
    if (cursor) {
      try {
        const decoded = cursorSchema.parse(JSON.parse(Buffer.from(cursor, 'base64url').toString()));

        if (decoded.walletId !== walletId || BigInt(decoded.after) > BigInt(decoded.through))
          throw new Error('Invalid cursor');
        after = decoded.after;
        through = decoded.through;
      } catch {
        throw new AppError('INVALID_CURSOR', 400);
      }
    } else {
      const [row] = await this.db.query(
        'SELECT COALESCE(max(sequence),0)::text AS last FROM wallet_ledger WHERE wallet_id=$1',
        [walletId],
      );
      through = row.last;
    }
    const rows = await this.db.query(
      'SELECT * FROM wallet_ledger WHERE wallet_id=$1 AND sequence>$2::bigint AND sequence<=$3::bigint ORDER BY sequence LIMIT $4',
      [walletId, after, through, limit + 1],
    );

    const hasMore = rows.length > limit;

    const page = rows.slice(0, limit);

    return {
      items: page.map((row: Record<string, string>) => ({
        id: row.id,
        transactionId: row.transaction_id,
        direction: row.direction,
        money: Money.rehydrate({ amount: row.amount!, currency: row.currency! }).toJSON(),
        balanceBefore: Money.rehydrate({ amount: row.balance_before!, currency: row.currency! }).toJSON(),
        balanceAfter: Money.rehydrate({ amount: row.balance_after!, currency: row.currency! }).toJSON(),
        createdAt: row.created_at,
      })),
      nextCursor: hasMore
        ? Buffer.from(JSON.stringify({ walletId, after: page.at(-1).sequence, through })).toString('base64url')
        : null,
    };
  }

  async reconcile(walletId: string, correlationId: string) {
    z.uuid().parse(walletId);
    const [row] = await this.db.query(
      `SELECT w.balance,w.currency,count(l.id)::integer AS entries,
      COALESCE(sum(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END),0)::text AS calculated
      FROM wallets w LEFT JOIN wallet_ledger l ON l.wallet_id=w.id WHERE w.id=$1 GROUP BY w.id`,
      [walletId],
    );

    if (!row) throw new AppError('WALLET_NOT_FOUND', 404);

    const stored = Money.rehydrate({ amount: row.balance, currency: row.currency });

    const calculated = Money.rehydrate({ amount: row.calculated, currency: row.currency });

    const difference = stored.subtract(calculated);

    if (!difference.isZero()) {
      metrics.reconciliation.inc();
      log('error', 'reconciliation_mismatch', { walletId, correlationId });
    }
    return {
      walletId,
      storedBalance: stored.toJSON(),
      calculatedBalance: calculated.toJSON(),
      difference: difference.toJSON(),
      consistent: difference.isZero(),
      checkedEntries: row.entries,
    };
  }
}
