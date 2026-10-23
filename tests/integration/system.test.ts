import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { DataSource } from 'typeorm';
import {
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  ChangeMessageVisibilityCommand,
} from '@aws-sdk/client-sqs';
import { createDatabase } from '../../src/infra/database';
import { SqsTransport, queueNames } from '../../src/infra/sqs';
import { WageringService, type Result } from '../../src/app/processor';
import { QueryService } from '../../src/app/queries';
import type { WagerInput } from '../../src/app/contracts';
import { OutboxPublisher } from '../../src/workers/outbox';
import { WagerConsumer } from '../../src/workers/consumer';
import { config } from '../../src/infra/config';

const databaseUrl = process.env.DATABASE_URL ?? 'postgres://wager:wager@localhost:5432/wager_test';
if (!new URL(databaseUrl).pathname.endsWith('_test'))
  throw new Error('Integration suite requires a dedicated database ending in _test');
if (process.env.SQS_QUEUE_PREFIX !== 'wager-test')
  throw new Error('Integration suite requires SQS_QUEUE_PREFIX=wager-test; use bun run test:integration');
let db: DataSource, service: WageringService, queries: QueryService;
const sqs = new SqsTransport();
const instances: { process: ReturnType<typeof Bun.spawn>; url: string }[] = [];
const context = () => ({ correlationId: crypto.randomUUID() });
const wallets: string[] = [];
async function wallet(amount = '100.00') {
  const result = await service.createWallet(
    { playerId: crypto.randomUUID(), initialBalance: { amount, currency: 'BRL' } },
    crypto.randomUUID(),
  );
  wallets.push(result.id);
  return result;
}
function wager(w: { id: string; playerId: string }, overrides: Partial<WagerInput> = {}): WagerInput {
  return {
    providerId: 'provider-a',
    externalTransactionId: crypto.randomUUID(),
    walletId: w.id,
    playerId: w.playerId,
    roundId: 'round-1',
    gameId: 'game-1',
    kind: 'BET',
    money: { amount: '25.00', currency: 'BRL' },
    ...overrides,
  };
}
async function submit(input: WagerInput, key = `${input.providerId}:${input.externalTransactionId}`) {
  return service.submit(input, key, context());
}
async function send(input: WagerInput, messageId = crypto.randomUUID()) {
  await sqs.client.send(
    new SendMessageCommand({
      QueueUrl: await sqs.url(queueNames.incoming),
      MessageBody: JSON.stringify({
        messageId,
        type: 'WagerTransactionRequested',
        occurredAt: new Date().toISOString(),
        data: { ...input, idempotencyKey: `${input.providerId}:${input.externalTransactionId}` },
      }),
      MessageGroupId: input.walletId,
      MessageDeduplicationId: crypto.randomUUID(),
    }),
  );
}
async function spawnInstance() {
  const child = Bun.spawn([process.execPath, 'tests/fixtures/instance.ts'], {
    env: { ...process.env, DATABASE_URL: databaseUrl },
    stdout: 'pipe',
    stderr: 'inherit',
  });
  const reader = child.stdout.getReader();
  let output = '';
  while (!output.includes('\n')) {
    const chunk = await reader.read();
    if (chunk.done) throw new Error('Instance exited before listening');
    output += new TextDecoder().decode(chunk.value);
  }
  const match = /PORT:(\d+)/.exec(output);
  if (!match) throw new Error(`Unexpected startup output: ${output}`);
  void (async () => {
    while (!(await reader.read()).done) {
      /* Drain structured logs. */
    }
  })();
  const instance = { process: child, url: `http://127.0.0.1:${match[1]}` };
  instances.push(instance);
  return instance;
}
async function request(index: number, input: WagerInput, key = `${input.providerId}:${input.externalTransactionId}`) {
  const response = await fetch(`${instances[index]!.url}/wagering/transactions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'idempotency-key': key },
    body: JSON.stringify(input),
  });
  return { status: response.status, body: (await response.json()) as Result };
}

beforeAll(async () => {
  // This database is disposable. The normal application database is never migrated down or truncated.
  const url = new URL(databaseUrl),
    name = url.pathname.slice(1);
  if (!/^[a-zA-Z0-9_]+_test$/.test(name)) throw new Error('Unsafe test database name');
  url.pathname = '/postgres';
  const admin = await createDatabase(url.toString()).initialize();
  try {
    if (!(await admin.query('SELECT 1 FROM pg_database WHERE datname=$1', [name])).length)
      await admin.query(`CREATE DATABASE "${name}"`);
  } finally {
    await admin.destroy();
  }
  db = await createDatabase(databaseUrl).initialize();
  if ((await db.query("SELECT to_regclass('wallets') AS existing"))[0].existing) await db.undoLastMigration();
  await db.runMigrations();
  // Verify reversibility against actual PostgreSQL before running any scenario.
  await db.undoLastMigration();
  await db.runMigrations();
  service = new WageringService(db);
  queries = new QueryService(db);
  await sqs.initialize();
  for (const name of Object.values(queueNames)) {
    for (;;) {
      const messages = await sqs.client.send(
        new ReceiveMessageCommand({ QueueUrl: await sqs.url(name), MaxNumberOfMessages: 10, WaitTimeSeconds: 0 }),
      );
      if (!messages.Messages?.length) break;
      for (const message of messages.Messages)
        await sqs.client.send(
          new DeleteMessageCommand({ QueueUrl: await sqs.url(name), ReceiptHandle: message.ReceiptHandle }),
        );
    }
  }
  await Promise.all([spawnInstance(), spawnInstance(), spawnInstance()]);
}, 120000);

afterAll(async () => {
  for (const instance of instances) instance.process.kill('SIGTERM');
  await Promise.all(instances.map((instance) => instance.process.exited));
  sqs.close();
  if (db?.isInitialized) await db.destroy();
});

describe('real PostgreSQL and three independent NestJS processes', () => {
  test('50 parallel copies produce one debit and the original balance on replay', async () => {
    const w = await wallet(),
      input = wager(w);
    const responses = await Promise.all(Array.from({ length: 50 }, (_, i) => request(i % 3, input)));
    expect(responses.every((r) => r.status === 200)).toBe(true);
    expect(new Set(responses.map((r) => r.body.transactionId)).size).toBe(1);
    expect(responses.filter((r) => !r.body.idempotentReplay)).toHaveLength(1);
    await submit(wager(w, { kind: 'WIN', money: { amount: '10.00', currency: 'BRL' } }));
    const replay = await submit(input);
    expect(replay.balance.amount).toBe('75.00');
    expect((await queries.wallet(w.id)).balance.amount).toBe('85.00');
    const [row] = await db.query(
      "SELECT count(*)::int AS n FROM wallet_ledger WHERE wallet_id=$1 AND direction='DEBIT'",
      [w.id],
    );
    expect(row.n).toBe(1);
    expect((await queries.reconcile(w.id, 'test')).consistent).toBe(true);
  });
  test('two simultaneous 80 BRL bets on 100 BRL: one processed, one rejected', async () => {
    const w = await wallet();
    const inputs = [
      wager(w, { money: { amount: '80.00', currency: 'BRL' } }),
      wager(w, { money: { amount: '80.00', currency: 'BRL' } }),
    ];
    const results = await Promise.all(inputs.map((input, index) => request(index, input)));
    expect(results.map((r) => r.status).sort()).toEqual([200, 422]);
    expect(results.find((r) => r.status === 422)!.body.failureCode).toBe('INSUFFICIENT_FUNDS');
    expect((await queries.wallet(w.id)).balance.amount).toBe('20.00');
    await Promise.all(inputs.map((input) => submit(input)));
    const [row] = await db.query(
      "SELECT count(*)::int AS n FROM wallet_ledger WHERE wallet_id=$1 AND direction='DEBIT'",
      [w.id],
    );
    expect(row.n).toBe(1);
  });
  test('different wallets are independently locked and processed in parallel', async () => {
    const first = await wallet(),
      second = await wallet();
    const runner = db.createQueryRunner();
    await runner.connect();
    await runner.startTransaction();
    try {
      await runner.query('SELECT id FROM wallets WHERE id=$1 FOR UPDATE', [first.id]);
      const result = await request(2, wager(second));
      expect(result.status).toBe(200);
      expect(result.body.balance.amount).toBe('75.00');
    } finally {
      await runner.rollbackTransaction();
      await runner.release();
    }
    const list = await Promise.all(Array.from({ length: 9 }, () => wallet()));
    const results = await Promise.all(list.map((w, index) => request(index % 3, wager(w))));
    expect(results.every((result) => result.status === 200)).toBe(true);
  });
  test('persistent payload conflicts, external identity conflicts and wallet uniqueness', async () => {
    const w = await wallet(),
      input = wager(w);
    await submit(input, 'unique-key');
    expect((await request(1, { ...input, money: { amount: '26.00', currency: 'BRL' } }, 'unique-key')).status).toBe(
      409,
    );
    expect((await request(2, input, 'different-key')).status).toBe(409);
    expect((await request(2, { ...input, walletId: crypto.randomUUID() }, 'unique-key')).status).toBe(409);
    await expect(
      service.createWallet({ playerId: w.playerId, initialBalance: w.balance }, 'duplicate'),
    ).rejects.toThrow('WALLET_ALREADY_EXISTS');
  });
  test('LOSS has no ledger or version increment; reversals require exact references', async () => {
    const w = await wallet();
    const loss = await submit(wager(w, { kind: 'LOSS', money: { amount: '0.00', currency: 'BRL' } }));
    expect(loss.status).toBe('PROCESSED');
    expect((await queries.wallet(w.id)).version).toBe(1);
    const bet = wager(w);
    await submit(bet);
    const refund = wager(w, { kind: 'REFUND', referenceExternalTransactionId: bet.externalTransactionId });
    expect((await submit(refund)).balance.amount).toBe('100.00');
    expect(
      (await submit(wager(w, { kind: 'ROLLBACK', referenceExternalTransactionId: bet.externalTransactionId })))
        .failureCode,
    ).toBe('ALREADY_REVERSED');
    const undoRefund = await submit(
      wager(w, { kind: 'ROLLBACK', referenceExternalTransactionId: refund.externalTransactionId }),
    );
    expect(undoRefund.balance.amount).toBe('75.00');
    const win = wager(w, { kind: 'WIN', money: { amount: '100.00', currency: 'BRL' } });
    await submit(win);
    await submit(wager(w, { money: { amount: '150.00', currency: 'BRL' } }));
    const rollback = await submit(
      wager(w, { kind: 'ROLLBACK', money: win.money, referenceExternalTransactionId: win.externalTransactionId }),
    );
    expect(rollback.failureCode).toBe('REVERSAL_INSUFFICIENT_FUNDS');
    const [row] = await db.query('SELECT count(*)::int AS n FROM wallet_ledger WHERE transaction_id=$1', [
      loss.transactionId,
    ]);
    expect(row.n).toBe(0);
  });
  test('concurrent reversals cannot double-credit', async () => {
    const w = await wallet(),
      bet = wager(w);
    await submit(bet);
    const results = await Promise.all([
      request(0, wager(w, { kind: 'REFUND', referenceExternalTransactionId: bet.externalTransactionId })),
      request(1, wager(w, { kind: 'ROLLBACK', referenceExternalTransactionId: bet.externalTransactionId })),
    ]);
    expect(results.map((r) => r.status).sort()).toEqual([200, 422]);
    expect((await queries.wallet(w.id)).balance.amount).toBe('100.00');
  });
  test('out-of-order REFUND and ROLLBACK survive retry and expire missing references', async () => {
    for (const kind of ['REFUND', 'ROLLBACK'] as const) {
      const w = await wallet(),
        bet = wager(w);
      const pending = await submit(wager(w, { kind, referenceExternalTransactionId: bet.externalTransactionId }));
      expect(pending.status).toBe('PENDING_REFERENCE');
      await submit(bet);
      await db.query('UPDATE wager_transactions SET next_attempt_at=now() WHERE id=$1', [pending.transactionId]);
      await Promise.all([service.retryReference(pending.transactionId), service.retryReference(pending.transactionId)]);
      expect((await queries.transaction(pending.transactionId)).status).toBe('PROCESSED');
      expect((await queries.wallet(w.id)).balance.amount).toBe('100.00');
    }
    const w = await wallet();
    const pending = await submit(wager(w, { kind: 'REFUND', referenceExternalTransactionId: 'never-arrives' }));
    for (let i = 0; i < config.referenceMaxAttempts; i++) {
      await db.query('UPDATE wager_transactions SET next_attempt_at=now() WHERE id=$1', [pending.transactionId]);
      await service.retryReference(pending.transactionId);
    }
    expect((await queries.transaction(pending.transactionId)).failureCode).toBe('REFERENCE_EXPIRED');
  });
  test('reference scope, partial reversals and rejected references are explicit failures', async () => {
    const w = await wallet(),
      bet = wager(w);
    await submit(bet);
    expect(
      (
        await submit(
          wager(w, {
            kind: 'REFUND',
            referenceExternalTransactionId: bet.externalTransactionId,
            roundId: 'other-round',
          }),
        )
      ).failureCode,
    ).toBe('REFERENCE_SCOPE_MISMATCH');
    expect(
      (
        await submit(
          wager(w, {
            kind: 'REFUND',
            referenceExternalTransactionId: bet.externalTransactionId,
            money: { amount: '1.00', currency: 'BRL' },
          }),
        )
      ).failureCode,
    ).toBe('REFERENCE_AMOUNT_MISMATCH');
    const rejected = wager(w, { money: { amount: '999.00', currency: 'BRL' } });
    await submit(rejected);
    expect(
      (
        await submit(
          wager(w, {
            kind: 'REFUND',
            referenceExternalTransactionId: rejected.externalTransactionId,
            money: rejected.money,
          }),
        )
      ).failureCode,
    ).toBe('REFERENCE_NOT_PROCESSED');
    expect((await submit(wager(w, { playerId: crypto.randomUUID() }))).failureCode).toBe('PLAYER_MISMATCH');
  });
  test('database rejects negative balance, unbacked changes, mutable ledger and terminal transitions', async () => {
    const w = await wallet();
    await expect(db.query('UPDATE wallets SET balance=-1,version=version+1 WHERE id=$1', [w.id])).rejects.toThrow();
    await expect(
      db.query('UPDATE wallets SET balance=balance+1,version=version+1 WHERE id=$1', [w.id]),
    ).rejects.toThrow('wallet and ledger diverge');
    await expect(db.query('UPDATE wallets SET version=version+1 WHERE id=$1', [w.id])).rejects.toThrow(
      'invalid wallet version',
    );
    await expect(db.query('DELETE FROM wallet_ledger WHERE wallet_id=$1', [w.id])).rejects.toThrow(
      'ledger is append only',
    );
    await expect(db.query('UPDATE wallet_ledger SET amount=amount WHERE wallet_id=$1', [w.id])).rejects.toThrow(
      'ledger is append only',
    );
    await expect(db.query('TRUNCATE wallet_ledger')).rejects.toThrow('ledger is append only');
    const result = await submit(wager(w));
    await expect(
      db.query("UPDATE wager_transactions SET status='PENDING' WHERE id=$1", [result.transactionId]),
    ).rejects.toThrow('terminal transaction is immutable');
  });
  test('a failure inserting outbox rolls back wallet, transaction, ledger and inbox together', async () => {
    const w = await wallet(),
      input = wager(w),
      messageId = crypto.randomUUID();
    await db.query(
      `CREATE FUNCTION test_fail_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected outbox failure'; END $$`,
    );
    await db.query(
      `CREATE TRIGGER test_fail_outbox BEFORE INSERT ON outbox FOR EACH ROW WHEN (NEW.aggregate_id='${w.id}'::uuid) EXECUTE FUNCTION test_fail_outbox()`,
    );
    try {
      await expect(service.submit(input, 'atomic-failure', { ...context(), messageId })).rejects.toThrow(
        'injected outbox failure',
      );
      expect((await queries.wallet(w.id)).balance.amount).toBe('100.00');
      expect(
        (
          await db.query('SELECT id FROM wager_transactions WHERE external_transaction_id=$1', [
            input.externalTransactionId,
          ])
        ).length,
      ).toBe(0);
      expect((await db.query('SELECT message_id FROM inbox WHERE message_id=$1', [messageId])).length).toBe(0);
      expect((await db.query('SELECT id FROM wallet_ledger WHERE wallet_id=$1', [w.id])).length).toBe(1);
    } finally {
      await db.query('DROP TRIGGER test_fail_outbox ON outbox');
      await db.query('DROP FUNCTION test_fail_outbox()');
    }
    const result = await service.submit(input, 'atomic-failure', { ...context(), messageId });
    expect(result.status).toBe('PROCESSED');
  });
  test('stable ledger cursor freezes its upper boundary while new entries arrive', async () => {
    const w = await wallet();
    await submit(wager(w));
    const page = await queries.ledger(w.id, undefined, '1');
    expect(page.nextCursor).not.toBeNull();
    await submit(wager(w));
    const second = await queries.ledger(w.id, page.nextCursor!, '1');
    expect(second.items).toHaveLength(1);
    expect(second.nextCursor).toBeNull();
    expect(second.items[0].id).not.toBe(page.items[0].id);
  });
  test('HTTP distinguishes invalid input, pending work, missing records and readiness', async () => {
    const base = instances[0]!.url,
      w = await wallet(),
      input = wager(w);
    const post = (body: unknown, key?: string) =>
      fetch(`${base}/wagering/transactions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...(key ? { 'idempotency-key': key } : {}) },
        body: JSON.stringify(body),
      });
    expect((await post(input)).status).toBe(400);
    expect((await post({ ...input, kind: 'OPENING' }, 'internal-forbidden')).status).toBe(400);
    expect((await post({ ...input, money: { amount: 1, currency: 'BRL' } }, 'number-forbidden')).status).toBe(400);
    expect(
      (await post({ ...input, kind: 'REFUND', referenceExternalTransactionId: 'later' }, 'pending-http')).status,
    ).toBe(202);
    expect((await fetch(`${base}/wallets/${crypto.randomUUID()}`)).status).toBe(404);
    expect((await fetch(`${base}/wallets/${w.id}/ledger?cursor=invalid`)).status).toBe(400);
    expect((await fetch(`${base}/health/live`)).status).toBe(200);
    expect((await fetch(`${base}/health/ready`)).status).toBe(200);
    expect(await (await fetch(`${base}/metrics`)).text()).toContain('wager_processing_seconds');
  });
});

describe('real SQS, crash recovery and concurrent publishers', () => {
  test('inbox deduplicates redelivery and rejects changed payload for the same message id', async () => {
    const w = await wallet(),
      input = wager(w),
      messageId = crypto.randomUUID();
    await send(input, messageId);
    const consumer = new WagerConsumer(sqs, service);
    await consumer.tick();
    await send(input, messageId);
    await consumer.tick();
    expect((await queries.wallet(w.id)).balance.amount).toBe('75.00');
    expect(
      (await db.query('SELECT * FROM inbox WHERE message_id=$1 AND processed_at IS NOT NULL', [messageId])).length,
    ).toBe(1);
    await expect(
      service.submit(
        { ...input, money: { amount: '30.00', currency: 'BRL' } },
        `${input.providerId}:${input.externalTransactionId}`,
        { ...context(), messageId },
      ),
    ).rejects.toThrow('INBOX_PAYLOAD_CONFLICT');
  });
  test('kill a separate worker after commit and before ack; redelivery does not debit again', async () => {
    const w = await wallet(),
      input = wager(w);
    await send(input);
    const child = Bun.spawn([process.execPath, 'tests/fixtures/crash-consumer.ts'], {
      env: { ...process.env, DATABASE_URL: databaseUrl },
      stdout: 'ignore',
      stderr: 'inherit',
    });
    await child.exited;
    expect(child.signalCode).toBe('SIGKILL');
    expect((await queries.wallet(w.id)).balance.amount).toBe('75.00');
    await Bun.sleep(1200);
    await new WagerConsumer(sqs, service).tick();
    expect((await queries.wallet(w.id)).balance.amount).toBe('75.00');
    const [row] = await db.query(
      "SELECT count(*)::int AS n FROM wallet_ledger WHERE wallet_id=$1 AND direction='DEBIT'",
      [w.id],
    );
    expect(row.n).toBe(1);
  });
  test('invalid message goes to DLQ; transient infrastructure failure retries then reaches DLQ', async () => {
    const url = await sqs.url(queueNames.incoming);
    await sqs.client.send(
      new SendMessageCommand({
        QueueUrl: url,
        MessageBody: '{invalid',
        MessageGroupId: 'invalid',
        MessageDeduplicationId: crypto.randomUUID(),
      }),
    );
    await new WagerConsumer(sqs, service).tick();
    let dead = await sqs.client.send(
      new ReceiveMessageCommand({
        QueueUrl: await sqs.url(queueNames.dlq),
        WaitTimeSeconds: 2,
        MessageAttributeNames: ['All'],
      }),
    );
    expect(dead.Messages?.[0]?.Body).toBe('{invalid');
    await sqs.client.send(
      new DeleteMessageCommand({
        QueueUrl: await sqs.url(queueNames.dlq),
        ReceiptHandle: dead.Messages![0]!.ReceiptHandle,
      }),
    );
    const w = await wallet();
    await send(wager(w));
    // A real closed PostgreSQL pool supplies the infrastructure failure; SQS is never mocked.
    const unavailable = await createDatabase(databaseUrl).initialize();
    await unavailable.destroy();
    const consumer = new WagerConsumer(sqs, new WageringService(unavailable));
    for (let attempt = 1; attempt <= 5; attempt++) {
      const response = await sqs.client.send(
        new ReceiveMessageCommand({
          QueueUrl: url,
          WaitTimeSeconds: 2,
          VisibilityTimeout: 1,
          MessageSystemAttributeNames: ['ApproximateReceiveCount'],
        }),
      );
      const message = response.Messages?.[0];
      if (!message) break; // Broker redrive may move it first.
      await consumer.handle(url, message);
      if (attempt < 5)
        await sqs.client.send(
          new ChangeMessageVisibilityCommand({
            QueueUrl: url,
            ReceiptHandle: message.ReceiptHandle,
            VisibilityTimeout: 0,
          }),
        );
    }
    dead = await sqs.client.send(
      new ReceiveMessageCommand({ QueueUrl: await sqs.url(queueNames.dlq), WaitTimeSeconds: 3 }),
    );
    expect(dead.Messages?.length).toBe(1);
    expect((await queries.wallet(w.id)).balance.amount).toBe('100.00');
    await sqs.client.send(
      new DeleteMessageCommand({
        QueueUrl: await sqs.url(queueNames.dlq),
        ReceiptHandle: dead.Messages![0]!.ReceiptHandle,
      }),
    );
  });
  test('two publishers claim distinct committed rows; all events reach real SQS', async () => {
    const publisher1 = new OutboxPublisher(db, sqs),
      publisher2 = new OutboxPublisher(db, sqs);
    const [before] = await db.query('SELECT count(*)::int AS n FROM outbox WHERE published_at IS NULL');
    for (let i = 0; i < before.n + 2; i++) {
      const result = await Promise.all([publisher1.tick(), publisher2.tick()]);
      if (result.every((value) => !value)) break;
    }
    expect((await db.query('SELECT id FROM outbox WHERE published_at IS NULL')).length).toBe(0);
    const eventIds = new Set<string>(),
      url = await sqs.url(queueNames.events);
    for (;;) {
      const batch = await sqs.client.send(
        new ReceiveMessageCommand({ QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }),
      );
      if (!batch.Messages?.length) break;
      for (const message of batch.Messages) {
        const event = JSON.parse(message.Body!);
        eventIds.add(event.eventId);
        expect(event.version).toBe(1);
        expect(event.correlationId).toBeDefined();
        await sqs.client.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: message.ReceiptHandle }));
      }
    }
    expect(eventIds.size).toBe(before.n);
  });
  test('unpublished committed events survive service restart and publisher retries', async () => {
    const w = await wallet(),
      input = wager(w);
    await submit(input);
    const original = instances[0]!;
    original.process.kill('SIGTERM');
    await original.process.exited;
    const restarted = await spawnInstance();
    const response = await fetch(`${restarted.url}/wallets/${w.id}/reconciliation`, { method: 'POST' });
    expect(((await response.json()) as { consistent: boolean }).consistent).toBe(true);
    const [outbox] = await db.query(
      'SELECT id FROM outbox WHERE aggregate_id=$1 AND published_at IS NULL ORDER BY occurred_at LIMIT 1',
      [w.id],
    );
    expect(outbox).toBeDefined();
    const disconnected = new SqsTransport('http://127.0.0.1:1');
    try {
      await new OutboxPublisher(db, disconnected).tick();
    } finally {
      disconnected.close();
    }
    const [retry] = await db.query('SELECT attempts,published_at FROM outbox WHERE id=$1', [outbox.id]);
    expect(retry.attempts).toBe(1);
    expect(retry.published_at).toBeNull();
    await db.query('UPDATE outbox SET next_attempt_at=now() WHERE id=$1', [outbox.id]);
    const fresh = await createDatabase(databaseUrl).initialize();
    try {
      while (await new OutboxPublisher(fresh, sqs).tick()) {
        /* recover */
      }
    } finally {
      await fresh.destroy();
    }
    expect(
      (await db.query('SELECT id FROM outbox WHERE aggregate_id=$1 AND published_at IS NULL', [w.id])).length,
    ).toBe(0);
  });
  test('all wallets reconcile after concurrency, retries, rejection and process restart', async () => {
    for (const id of wallets) expect((await queries.reconcile(id, 'final-invariant')).consistent).toBe(true);
    const mismatches = await db.query(
      `SELECT w.id FROM wallets w LEFT JOIN wallet_ledger l ON l.wallet_id=w.id GROUP BY w.id HAVING w.balance<>COALESCE(sum(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END),0)`,
    );
    expect(mismatches).toHaveLength(0);
  });
});
