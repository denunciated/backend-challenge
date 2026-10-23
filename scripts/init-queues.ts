import { SqsTransport } from '../src/infra/sqs';

const sqs = new SqsTransport();

try {
  await sqs.initialize();
} finally {
  sqs.close();
}
