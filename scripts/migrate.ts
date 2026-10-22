import { createDatabase } from '../src/infra/database';

const db = await createDatabase().initialize();

try {
  if (process.argv[2] === 'down') await db.undoLastMigration({ transaction: 'all' });
  else if (process.argv[2] === 'up') await db.runMigrations({ transaction: 'all' });
  else throw new Error('Usage: bun scripts/migrate.ts up|down');
} finally {
  await db.destroy();
}
