import { DomainError, type FailureCode } from './errors';
import { Money } from './money';
import type { LedgerDirection } from './wallet';

export type Kind = 'OPENING' | 'BET' | 'WIN' | 'LOSS' | 'REFUND' | 'ROLLBACK';
export type Status = 'PENDING' | 'PENDING_REFERENCE' | 'PROCESSED' | 'REJECTED' | 'FAILED';

export interface TransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: Kind;
  money: Money;
  referenceExternalTransactionId?: string;
}

export interface TransactionState extends TransactionProps {
  status: Status;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date;
}

export class WagerTransaction {
  private constructor(private readonly state: TransactionState) {}

  static create(props: TransactionProps): WagerTransaction {
    if ((props.kind === 'REFUND' || props.kind === 'ROLLBACK') && !props.referenceExternalTransactionId)
      throw new DomainError('REFERENCE_REQUIRED');
    if (props.referenceExternalTransactionId === props.externalTransactionId) throw new DomainError('SELF_REFERENCE');
    if (
      props.money.isNegative() ||
      (props.kind !== 'LOSS' && !props.money.isPositive()) ||
      (props.kind === 'LOSS' && !props.money.isZero())
    )
      throw new DomainError('INVALID_AMOUNT');
    if (
      (props.kind === 'BET' || props.kind === 'LOSS' || props.kind === 'OPENING') &&
      props.referenceExternalTransactionId
    )
      throw new DomainError('REFERENCE_NOT_ALLOWED');
    return new WagerTransaction({ ...props, status: 'PENDING' });
  }

  static rehydrate(state: TransactionState): WagerTransaction {
    return new WagerTransaction({ ...state });
  }

  get props(): Readonly<TransactionProps> {
    return Object.freeze({ ...this.state });
  }

  get status() {
    return this.state.status;
  }

  get referenceTransactionId() {
    return this.state.referenceTransactionId;
  }

  get failureCode() {
    return this.state.failureCode;
  }

  get processedAt() {
    return this.state.processedAt && new Date(this.state.processedAt);
  }

  isTerminal(): boolean {
    return ['PROCESSED', 'REJECTED', 'FAILED'].includes(this.status);
  }

  affectsBalance(): boolean {
    return this.state.kind !== 'LOSS';
  }

  requiresReference(): boolean {
    return this.state.kind === 'REFUND' || this.state.kind === 'ROLLBACK';
  }

  matchesPayload(hash: string): boolean {
    return this.state.payloadHash === hash;
  }

  private assertMutable(): void {
    if (this.isTerminal()) throw new DomainError('INVALID_TRANSACTION_STATE');
  }

  markProcessed(referenceTransactionId: string | undefined, at: Date): void {
    this.assertMutable();
    this.state.status = 'PROCESSED';
    this.state.referenceTransactionId = referenceTransactionId;
    this.state.processedAt = new Date(at);
  }

  markPendingReference(): void {
    this.assertMutable();
    this.state.status = 'PENDING_REFERENCE';
  }

  reject(code: FailureCode): void {
    this.assertMutable();
    this.state.status = 'REJECTED';
    this.state.failureCode = code;
    this.state.processedAt = new Date();
  }

  validateReference(reference: WagerTransaction): void {
    const a = this.state,
      b = reference.state;
    if (
      a.providerId !== b.providerId ||
      a.playerId !== b.playerId ||
      a.walletId !== b.walletId ||
      a.roundId !== b.roundId ||
      a.money.currency !== b.money.currency
    )
      throw new DomainError('REFFERENCE_SCOPE_MISMATCH');
    const allowed = a.kind === 'ROLLBACK' ? ['BET', 'WIN', 'REFUND'] : ['BET'];
    if (!allowed.includes(b.kind)) throw new DomainError('REFERENCE_KIND_INVALID');
    if (reference.status !== 'PROCESSED') throw new DomainError('REFERENCE_NOT_PROCESSED');
    if (this.requiresReference() && !a.money.equals(b.money)) throw new DomainError('REFERENCE_AMOUNT_MISMATCH');
  }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    if (this.state.kind === 'LOSS') throw new DomainError('NO_LEDGER_FOR_LOSS');
    if (this.state.kind === 'BET') return 'DEBIT';
    if (this.state.kind === 'ROLLBACK') {
      if (!reference) throw new DomainError('REFERENCE_REQUIRED');
      return reference.state.kind === 'BET' ? 'CREDIT' : 'DEBIT';
    }
    return 'CREDIT';
  }
}
