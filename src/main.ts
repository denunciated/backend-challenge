import { createApp } from './app';
import { createDatabase } from './infra/database';
import { SqsTransport } from './infra/sqs';
import { config } from './infra/config';
import { WageringService } from './app/processor';
import { WorkerRuntime } from './workers/runtime';
import { log } from './infra/observability';

const db = await createDatabase().initialize();
const sqs = new SqsTransport();
const app = await createApp(db, sqs);
const runtime = new WorkerRuntime(db, new WageringService(db), sqs);
await app.listen(config.port, '0.0.0.0');

if (config.workers) runtime.start();

log('info', 'service_started');

let stopping = false;

async function shutdown() {
  if (stopping) return;

  stopping = true;

  log('info', 'service_stopping');

  const deadline = setTimeout(() => process.exit(1), 35000);

  deadline.unref();

  await Promise.all([app.close(), runtime.stop()]);

  sqs.close();

  await db.destroy();

  clearTimeout(deadline);
}

process.on('SIGTERM', () => {
  void shutdown();
});

process.on('SIGINT', () => {
  void shutdown();
});
