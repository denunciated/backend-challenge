export const config = {
  databaseUrl: process.env.DATABASE_URL ?? 'postgres://wager:wager@localhost:5432/wager',
  sqsEndpoint: process.env.SQS_ENDPOINT ?? 'http://localhost:4566',
  region: process.env.AWS_REGION ?? 'us-east-1',
  port: Number(process.env.PORT ?? 3000),
  workers: process.env.WORKERS_ENABLED !== 'false',
  referenceMaxAttempts: Number(process.env.REFERENCE_MAX_ATTEMPTS ?? 12),
  referenceBaseDelayMS: Number(process.env.REFERENCE_BASE_DELAY_MS ?? 1000),
};
