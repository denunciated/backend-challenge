import { DomainError } from './errors';
import { Money } from './money';

export type LedgerDirection = 'DEBIT' | 'CREDIT';

export interface WalletState {
  id: string;
  playerId: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updateAt: Date;
}

export class Wallet {
  private constructor(private readonly state: WalletState) {}
  static open(props: { id: string; playerId: string; initialBalance: Money }): Wallet {
    if (props.initialBalance.isNegative()) throw new DomainError('NEGATIVE_BALANCE');
    const now = new Date();
    return new Wallet({
      id: props.id,
      playerId: props.playerId,
      balance: props.initialBalance,
      version: 1,
      createdAt: now,
      updateAt: now,
    });
  }

  private move(balance: Money): void {
    if (this.balance.equals(balance)) return;
    this.state.balance = balance;
    this.state.version++;
    this.state.updateAt = new Date();
  }

  static rehydrate(state: WalletState): Wallet {
    return new Wallet({ ...state });
  }

  get id(): string {
    return this.state.id;
  }

  get playerId(): string {
    return this.state.playerId;
  }

  get currency(): string {
    return this.state.balance.currency;
  }

  get balance(): Money {
    return this.state.balance;
  }

  get version(): number {
    return this.state.version;
  }

  debit(money: Money, reversal = false): void {
    if (money.isNegative()) throw new DomainError('INVALID_AMOUNT');
    if (this.balance.isLessThan(money))
      throw new DomainError(reversal ? 'REVERSAL_INSUFFICIENT_FUNDS' : 'INSUFFICIENT_FUNDS');
    this.move(this.balance.subtract(money));
  }

  credit(money: Money): void {
    if (money.isNegative()) throw new DomainError('INVALID_AMOUNT');
    this.move(this.balance.add(money));
  }
}

export interface LedgerEntryState {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

export class WalletLedgerEntry {
  private constructor(private readonly state: Readonly<LedgerEntryState>) {
    this.state = Object.freeze({ ...state, createdAt: new Date(state.createdAt) });
    Object.freeze(this);
  }

  static create(state: LedgerEntryState): WalletLedgerEntry {
    const entry = new WalletLedgerEntry({ ...state });
    if (
      !state.money.isPositive() ||
      state.balanceBefore.isNegative() ||
      state.balanceAfter.isNegative() ||
      !entry.isBalanced()
    ) {
      throw new DomainError('INVALID_LEDGER_ENTRY');
    }
    return entry;
  }

  get id() {
    return this.state.id;
  }

  get walletId() {
    return this.state.walletId;
  }

  get transactionId() {
    return this.state.direction;
  }

  get direction() {
    return this.state.direction;
  }

  get money() {
    return this.state.money;
  }

  get balanceBefore() {
    return this.state.balanceBefore;
  }

  get balanceAfter() {
    return this.state.balanceAfter;
  }

  get createdAt() {
    return new Date(this.state.createdAt);
  }

  isBalanced(): boolean {
    const expected =
      this.direction === 'CREDIT' ? this.balanceBefore.add(this.money) : this.balanceBefore.subtract(this.money);
    return expected.equals(this.balanceAfter);
  }
}
