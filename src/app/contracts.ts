import { createHash } from 'node:crypto';
import z from 'zod';
import { Money } from '../domain/money';
import { WagerTransaction } from '../domain/transaction';
import { AppError } from '../domain/errors';

const identifier = z
  .string()
  .min(1)
  .max(200)
  .regex(/^[a-zA-Z0-9][a-zA-Z0-9._:-]*$/);

const moneySchema = z.object({ amount: z.string().max(40), currency: z.literal('BRL') }).strict();

export const walletSchema = z.object({ playerId: z.uuid(), initialBalance: moneySchema }).strict();

export const wagerSchema = z
  .object({
    providerId: identifier.refine((value) => value !== 'internal', 'Reserved provider'),
    externalTransactionId: identifier,
    playerId: z.uuid(),
    walletId: z.uuid(),
    roundId: identifier,
    gameId: identifier,
    kind: z.enum(['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK']),
    money: moneySchema,
    referenceExternalTransactionId: identifier.optional(),
  })
  .strict();

export type WagerInput = z.infer<typeof wagerSchema>;
export const messageSchema = z
  .object({
    messageId: identifier,
    type: z.literal('WagerTransactionRequested'),
    occurredAt: z.iso.datetime(),
    data: wagerSchema.extend({ idempotencyKey: z.string().min(1).max(200) }),
  })
  .strict();

export function validateKey(value: unknown): string {
  const result = z
    .string()
    .min(1)
    .max(200)
    .regex(/^[\x21-\x7e]+$/)
    .safeParse(value);
  if (!result.success) throw new AppError('INVALID_IDEMPOTENCY_KEY', 400);
  return result.data;
}

export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value !== null && typeof value === 'object')
    return `{${Object.entries(value)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`)
      .join(',')}}`;
  return JSON.stringify(value);
}

export function hashPayload(value: unknown): string {
  return createHash('sha256').update(canonicalJson(value)).digest('hex');
}

export function parseWager(payload: unknown, key: string) {
  const input = wagerSchema.parse(payload);
  const money = Money.from(input.money);
  const normalized = { ...input, money: money.toJSON() };
  const transaction = WagerTransaction.create({
    ...input,
    id: crypto.randomUUID(),
    money,
    idempotencyKey: validateKey(key),
    payloadHash: hashPayload(normalized),
  });
  return { input: normalized, transaction };
}
