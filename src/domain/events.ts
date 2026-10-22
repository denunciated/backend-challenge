import type { MoneyProps } from './money';
import type { FailureCode } from './errors';
import type { Status } from './transaction';
import type { LedgerDirection } from './wallet';

export interface EventContext {
  aggregateId: string;
  correlationId: string;
  causationId?: string;
}

export abstract class IntegrationEvent<T extends object> {
  abstract readonly eventType: string;
  abstract readonly version: number;
  readonly eventId = crypto.randomUUID();
  readonly occurredAt = new Date();
  readonly data: Readonly<T>;
  protected constructor(
    readonly context: EventContext,
    data: T,
  ) {
    this.context = Object.freeze({ ...context });
    this.data = Object.freeze(structuredClone(data));
  }
  toJSON() {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      version: this.version,
      ...this.context,
      occurredAt: this.occurredAt.toISOString(),
      data: this.data,
    };
  }
}

interface TransactionEventData {
  transactionId: string;
  walletId: string;
  providerId: string;
  status: Status;
  balance: MoneyProps;
  failureCode?: FailureCode;
}

export class WagerTransactionProcessed extends IntegrationEvent<TransactionEventData> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;
  static from(data: TransactionEventData, context: EventContext) {
    return new this(context, data);
  }
}

export class WagerTransactionRejected extends IntegrationEvent<TransactionEventData> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;
  static from(data: TransactionEventData, context: EventContext) {
    return new this(context, data);
  }
}

export class WagerTransactionPendingReference extends IntegrationEvent<TransactionEventData> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;
  static from(data: TransactionEventData, context: EventContext) {
    return new this(context, data);
  }
}

interface BalanceEventData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<BalanceEventData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;
  static from(data: BalanceEventData, context: EventContext) {
    return new this(context, data);
  }
}
