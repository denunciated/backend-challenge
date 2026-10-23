import { ReceiveMessageCommand } from '@aws-sdk/client-sqs';
import { createDatabase } from '../../src/infra/database';
import { SqsTransport, queueNames } from '../../src/infra/sqs';
import { WageringService } from '../../src/app/processor';
import { WagerConsumer } from '../../src/workers/consumer';

const db = await createDatabase().initialize();
const sqs = new SqsTransport();
const url = await sqs.url(queueNames.incoming);
const response = await sqs.client.send(
  new ReceiveMessageCommand({
    QueueUrl: url,
    MaxNumberOfMessages: 1,
    WaitTimeSeconds: 5,
    VisibilityTimeout: 1,
    MessageSystemAttributeNames: ['ApproximateReceiveCount'],
  }),
);

if (!response.Messages?.[0]) throw new Error('Expected one message');

const consumer = new WagerConsumer(sqs, new WageringService(db), async () => {
  process.kill(process.pid, 'SIGKILL');
});

await consumer.handle(url, response.Messages[0]);

throw new Error('Process should have died after commit');
