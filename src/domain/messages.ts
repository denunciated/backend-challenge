import { DomainError } from './errors';
import type { IntegrationEvent } from './events';

interface InboxState {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  recievedAt: Date;
  processedAt?: Date;
}

export class InboxMessage {
  private constructor(private readonly state: InboxState) {}

  static recieve(props: Omit<InboxState, 'recievedAt' | 'processedAt'>) {
    return new InboxMessage({ ...props, recievedAt: new Date() });
  }

  static rehydrate(state: InboxState) {
    return new InboxMessage({ ...state });
  }

  get messageId() {
    return this.state.messageId;
  }

  get consumerName() {
    return this.state.consumerName;
  }

  get payloadHash() {
    return this.state.payloadHash;
  }

  get procssedAt() {
    return this.state.processedAt;
  }

  isProcessed() {
    return this.state.processedAt !== undefined;
  }

  markProcessed(at: Date) {
    if (this.isProcessed()) throw new DomainError('INBOX_ALREADY_PROCESSED');
    this.state.processedAt = at;
  }
}

interface OutboxState {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Readonly<Record<string, unknown>>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt?: Date;
  publishedAt?: Date;
}

export class OutboxMessage {
  private constructor(private readonly state: OutboxState) {}
  static enqueue(event: IntegrationEvent<object>) {
    return new OutboxMessage({
      id: event.eventId,
      aggregateId: event.context.aggregateId,
      eventType: event.eventType,
      payload: event.toJSON(),
      occurredAt: event.occurredAt,
      attempts: 0,
    });
  }

  static rehydrate(state: OutboxState) {
    return new OutboxMessage({ ...state });
  }

  get id() {
    return this.state.id;
  }

  get aggregateId() {
    return this.state.aggregateId;
  }

  get eventType() {
    return this.state.eventType;
  }

  get payload() {
    return this.state.payload;
  }

  get attempts() {
    return this.state.attempts;
  }

  get nextAttemptAt() {
    return this.state.nextAttemptAt;
  }

  get publushedAt() {
    return this.state.publishedAt;
  }

  isPending() {
    return !this.state.publishedAt;
  }

  isDue(now: Date) {
    return this.isPending() && (!this.state.nextAttemptAt || this.state.nextAttemptAt <= now);
  }

  markPublished(at: Date) {
    this.state.publishedAt = at;
  }

  scheduleRetry(now: Date) {
    this.state.attempts++;
    this.state.nextAttemptAt = new Date(now.getTime() + backoff(this.state.attempts));
  }
}

export function backoff(attempt: number, base = 1000, cap = 30000): number {
  return Math.min(cap, base * 2 ** Math.min(attempt - 1, 20));
}
