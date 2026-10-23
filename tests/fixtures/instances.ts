import { createDatabase } from '../../src/infra/database';
import { SqsTransport } from '../../src/infra/sqs';
import { createApp } from '../../src/app';

const db = await createDatabase().initialize();
const sqs = new SqsTransport();
const app = await createApp(db, sqs);

await app.listen(0, '127.0.0.1');

const address = app.getHttpServer().address() as { port: number };

console.log(`PORT:${address.port}`);

process.on('SIGTERM', async () => {
  await app.close();
  sqs.close();
  await db.destroy();
  process.exit(0);
});
