import { describe, expect, test } from 'bun:test';
import { Money } from '../../src/domain/money';
import { Wallet, WalletLedgerEntry } from '../../src/domain/wallet';
import { WagerTransaction, type Kind } from '../../src/domain/transaction';
import { parseWager, canonicalJson } from '../../src/application/contracts';
import { InboxMessage, OutboxMessage } from '../../src/domain/messages';
import { WagerTransactionProcessed } from '../../src/domain/events';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
const input = {
  providerId: 'provider-a',
  externalTransactionId: 'bet-1',
  walletId: crypto.randomUUID(),
  playerId: crypto.randomUUID(),
  roundId: 'round-1',
  gameId: 'game-1',
  kind: 'BET' as const,
  money: { amount: '25.00', currency: 'BRL' },
};
function tx(kind: Kind, amount = '25.00', reference?: string) {
  return WagerTransaction.create({
    ...input,
    kind,
    externalTransactionId: kind,
    referenceExternalTransactionId: reference,
    money: brl(amount),
    id: crypto.randomUUID(),
    idempotencyKey: kind,
    payloadHash: 'a'.repeat(64),
  });
}
describe('Money', () => {
  test('arithmetic is exact, immutable and fixed-scale, including beyond Number precision', () => {
    const original = brl('0.10');
    expect(original.add(brl('0.20')).toString()).toBe('0.30');
    expect(original.toString()).toBe('0.10');
    expect(brl('9007199254740993.99').add(brl('0.01')).toString()).toBe('9007199254740994.00');
    expect(brl('1').toJSON()).toEqual({ amount: '1.00', currency: 'BRL' });
    expect(brl('1.2').toString()).toBe('1.20');
    expect(brl('1.01').negate().toString()).toBe('-1.01');
    expect(brl('0.01').subtract(brl('1.00')).toString()).toBe('-0.99');
    expect(Money.rehydrate({ amount: '-0.10', currency: 'BRL' }).toString()).toBe('-0.10');
  });
  test.each([
    'NaN',
    'Infinity',
    '-Infinity',
    '1e2',
    '',
    ' ',
    '1.001',
    '-1',
    '+1',
    '.1',
    '1.',
    '1,00',
    ' 1.00',
    '0x10',
    '1000000000000000000000000000000000000.00',
  ])('rejects invalid input %s without rounding', (amount) => {
    expect(() => brl(amount)).toThrow();
  });
  test('rejects monetary numbers and conflicting currencies', () => {
    expect(() => brl(1 as unknown as string)).toThrow();
    const usd = Money.from({ amount: '1.00', currency: 'USD' });
    for (const operation of ['add', 'subtract', 'equals', 'isLessThan'] as const)
      expect(() => brl('1.00')[operation](usd)).toThrow('CURRENCY_MISMATCH');
  });
});
describe('Wallet and ledger', () => {
  test('version increments only on a balance change; no negative balance', () => {
    const w = Wallet.open({ id: 'wallet', playerId: 'player', initialBalance: brl('100') });
    expect(w.version).toBe(1);
    w.credit(brl('0'));
    expect(w.version).toBe(1);
    w.debit(brl('80'));
    expect(w.version).toBe(2);
    expect(() => w.debit(brl('80'))).toThrow('INSUFFICIENT_FUNDS');
    expect(() => w.debit(brl('80'), true)).toThrow('REVERSAL_INSUFFICIENT_FUNDS');
    expect(w.balance.toString()).toBe('20.00');
    expect(() => w.credit(Money.from({ amount: '1', currency: 'USD' }))).toThrow('CURRENCY_MISMATCH');
  });
  test('ledger factory verifies arithmetic', () => {
    const props = {
      id: 'entry',
      walletId: 'wallet',
      transactionId: 'tx',
      direction: 'DEBIT' as const,
      money: brl('25'),
      balanceBefore: brl('100'),
      balanceAfter: brl('75'),
      createdAt: new Date(),
    };
    expect(WalletLedgerEntry.create(props).isBalanced()).toBe(true);
    expect(() => WalletLedgerEntry.create({ ...props, balanceAfter: brl('80') })).toThrow('INVALID_LEDGER_ENTRY');
  });
});
describe('WagerTransaction', () => {
  test('directions and reversal rules', () => {
    const bet = tx('BET');
    bet.markProcessed(undefined, new Date());
    const win = tx('WIN');
    win.markProcessed(undefined, new Date());
    const refund = tx('REFUND', '25', 'BET');
    refund.validateReference(bet);
    expect(refund.ledgerDirectionFor(bet)).toBe('CREDIT');
    expect(tx('BET').ledgerDirectionFor()).toBe('DEBIT');
    expect(tx('WIN').ledgerDirectionFor()).toBe('CREDIT');
    expect(tx('ROLLBACK', '25', 'BET').ledgerDirectionFor(bet)).toBe('CREDIT');
    expect(tx('ROLLBACK', '25', 'WIN').ledgerDirectionFor(win)).toBe('DEBIT');
    expect(() => refund.validateReference(win)).toThrow('REFERENCE_KIND_INVALID');
    expect(() => tx('REFUND', '24', 'BET').validateReference(bet)).toThrow('REFERENCE_AMOUNT_MISMATCH');
    expect(tx('LOSS', '0').affectsBalance()).toBe(false);
    expect(() => tx('LOSS', '1')).toThrow('INVALID_AMOUNT');
    expect(() => tx('REFUND')).toThrow('REFERENCE_REQUIRED');
  });
  test('terminal states reject all later transitions', () => {
    for (const transition of ['processed', 'rejected', 'failed']) {
      const transaction = tx('BET');
      transaction.markPendingReference();
      if (transition === 'processed') transaction.markProcessed(undefined, new Date());
      if (transition === 'rejected') transaction.reject('INSUFFICIENT_FUNDS');
      if (transition === 'failed') transaction.fail('INFRASTRUCTURE_PERMANENT');
      expect(transaction.isTerminal()).toBe(true);
      expect(() => transaction.markPendingReference()).toThrow('INVALID_TRANSACTION_STATE');
      expect(() => transaction.markProcessed(undefined, new Date())).toThrow('INVALID_TRANSACTION_STATE');
      expect(() => transaction.reject('INSUFFICIENT_FUNDS')).toThrow('INVALID_TRANSACTION_STATE');
      expect(() => transaction.fail('INFRASTRUCTURE_PERMANENT')).toThrow('INVALID_TRANSACTION_STATE');
    }
  });
  test('same idempotency key and divergent payload have different hashes', () => {
    const a = parseWager(input, 'same').transaction;
    const b = parseWager({ ...input, money: { amount: '26.00', currency: 'BRL' } }, 'same').transaction;
    expect(a.matchesPayload(b.props.payloadHash)).toBe(false);
    expect(
      parseWager({ ...input, money: { amount: '25', currency: 'BRL' } }, 'another').transaction.props.payloadHash,
    ).toBe(a.props.payloadHash);
    expect(canonicalJson({ z: 1, a: { z: 2, a: 1 } })).toBe('{"a":{"a":1,"z":2},"z":1}');
  });
});
test('persistent message model transitions and exponential backoff', () => {
  const inbox = InboxMessage.receive({ messageId: 'm', consumerName: 'c', payloadHash: 'h' });
  expect(inbox.isProcessed()).toBe(false);
  inbox.markProcessed(new Date());
  expect(inbox.isProcessed()).toBe(true);
  const outbox = OutboxMessage.enqueue(
    WagerTransactionProcessed.from(
      { transactionId: 't', walletId: 'w', providerId: 'p', status: 'PROCESSED', balance: brl('10').toJSON() },
      { aggregateId: 'w', correlationId: 'c' },
    ),
  );
  const now = new Date();
  outbox.scheduleRetry(now);
  expect(outbox.nextAttemptAt!.getTime() - now.getTime()).toBe(1000);
  outbox.scheduleRetry(now);
  expect(outbox.nextAttemptAt!.getTime() - now.getTime()).toBe(2000);
  expect(outbox.isDue(now)).toBe(false);
  outbox.markPublished(now);
  expect(outbox.isPending()).toBe(false);
});
