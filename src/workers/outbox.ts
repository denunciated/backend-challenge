import type { DataSource } from 'typeorm';
import { OutboxMessage } from '../domain/messages';
import { SqsTransport } from '../infra/sqs';
import { metrics } from '../infra/observability';

export class OutboxPublisher {
  constructor(
    private readonly db: DataSource,
    private readonly sqs: Pick<SqsTransport, 'publish'>,
  ) {}

  async tick(): Promise<boolean> {
    return this.db.transaction(async (manager) => {
      const [row] = await manager.query(
        'SELECT * FROM outbox WHERE published_at IS NULL AND next_attempt<=now() ORDER BY occurred_at, id LIMIT 1 FOR UPDATE SKIP LOCKED',
      );

      if (!row) return false;

      const message = OutboxMessage.rehydrate({
        id: row.id,
        aggregateId: row.aggregate_id,
        eventType: row.event_type,
        payload: row.payload,
        occurredAt: row.occurred_at,
        attempts: row.attempts,
        nextAttemptAt: row.next_attempt_at,
        publishedAt: row.published_at ?? undefined,
      });
      try {
        await this.sqs.publish(message.payload, message.id, message.aggregateId);
        message.markPublished(new Date());
        await manager.query('UPDATE outbox SET published_at=$2 WHERE id=$1', [message.id, message.publushedAt]);
      } catch {
        message.scheduleRetry(new Date());
        metrics.retries.inc({ worker: 'outbox' });
        await manager.query('UPDATE outbox SET attempts=$2, next_atempt_at=$3 WHERE id=$1', [
          message.id,
          message.attempts,
          message.nextAttemptAt,
        ]);
      }
      return true;
    });
  }
  async measureLag(): Promise<void> {
    const [row] = await this.db.query(
      'SELECT COALESCE(EXTRACT(EPOCH FROM now()-min(occurred_at)),0)::float AS lag FROM outbox WHERE published_at IS NULL',
    );
    metrics.outboxLag.set(Math.max(0, row.lag));
  }
}
