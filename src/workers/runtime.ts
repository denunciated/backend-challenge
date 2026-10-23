import { DataSource } from 'typeorm';
import { WageringService } from '../app/processor';
import { SqsTransport } from '../infra/sqs';
import { log } from '../infra/observability';
import { OutboxPublisher } from './outbox';
import { WagerConsumer } from './consumer';

export class WorkerRuntime {
  private stopping = false;
  private tasks: Promise<void>[] = [];
  private readonly consumer: WagerConsumer;
  readonly publisher: OutboxPublisher;
  constructor(
    private readonly db: DataSource,
    private readonly service: WageringService,
    sqs: SqsTransport,
  ) {
    this.consumer = new WagerConsumer(sqs, service);
    this.publisher = new OutboxPublisher(db, sqs);
  }
  start() {
    this.tasks = [
      this.loop('sqs', () => this.consumer.tick(), 50),
      this.loop(
        'outbox',
        async () => {
          await this.publisher.tick();
        },
        25,
      ),
      this.loop(
        'references',
        async () => {
          const rows: { id: string }[] = await this.db.query(
            "SELECT id FROM wager_transactions WHERE status='PENDING_REFERENCE' and next_attempt_at<=now() ORDER BY next_attempt_at LIMIT 50",
          );
          for (const row of rows) {
            if (this.stopping) break;
            await this.service.retryReference(row.id);
          }
        },
        250,
      ),
      this.loop('outbox_metrics', () => this.publisher.measureLag(), 5000),
    ];
  }

  private async loop(worker: string, work: () => Promise<void>, interval: number) {
    while (!this.stopping) {
      try {
        await work();
      } catch {
        if (!this.stopping) {
          log('warn', 'worker_iteration_failed', { worker });
          await Bun.sleep(1000);
        }
      }
      if (!this.stopping) await Bun.sleep(interval);
    }
  }

  async stop() {
    this.stopping = true;
    this.consumer.stopPolling();
    const drained = await Promise.race([
      Promise.allSettled(this.tasks).then(() => true),
      Bun.sleep(25000).then(() => false),
    ]);
    if (!drained) await this.consumer.releaseInFlight();
  }
}
