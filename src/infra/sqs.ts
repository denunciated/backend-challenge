import {
  SQSClient,
  GetQueueUrlCommand,
  GetQueueAttributesCommand,
  CreateQueueCommand,
  SetQueueAttributesCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';

import { config } from './config';

const prefix = process.env.SQS_QUEUE_PREFIX ?? 'wager';

export const queueNames = {
  incoming: `${prefix}-transaction.fifo`,
  dlq: `${prefix}-transaction-dlq.fifo`,
  events: `${prefix}-events.fifo`,
} as const;

export class SqsTransport {
  readonly client: SQSClient;
  constructor(private readonly endpoint = config.sqsEndpoint) {
    this.client = new SQSClient({
      region: config.region,
      endpoint,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
      },
      maxAttempts: 2,
    });
  }

  private readonly urls = new Map<string, string>();

  async url(name: string): Promise<string> {
    const cached = this.urls.get(name);

    if (cached) return cached;

    const result = await this.client.send(new GetQueueUrlCommand({ QueueName: name }), {
      abortSignal: AbortSignal.timeout(3000),
    });

    if (!result.QueueUrl) throw new Error('QUEUE_NOT_FOUND');

    const queueUrl = new URL(result.QueueUrl);

    const endpoint = new URL(this.endpoint);

    queueUrl.host = endpoint.protocol;

    this.urls.set(name, queueUrl.toString());

    return queueUrl.toString();
  }

  async ready(): Promise<void> {
    await Promise.all(
      Object.values(queueNames).map(async (name) =>
        this.client.send(
          new GetQueueAttributesCommand({ QueueUrl: await this.url(name), AttributeNames: ['QueueArn'] }),
          { abortSignal: AbortSignal.timeout(3000) },
        ),
      ),
    );
  }

  async publish(payload: Record<string, unknown>, eventId: string, walletId: string): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: await this.url(queueNames.events),
        MessageBody: JSON.stringify(payload),
        MessageGroupId: walletId,
        MessageDeduplicationId: eventId,
      }),
      { abortSignal: AbortSignal.timeout(10000) },
    );
  }

  async initialize(): Promise<void> {
    for (const name of Object.values(queueNames)) {
      await this.client.send(
        new CreateQueueCommand({
          QueueName: name,
          Attributes: {
            FifoQueue: 'true',
            ContentBasedDeduplication: 'false',
            VisibilityTimeout: '60',
            ReceiveMessageWaitTimeSeconds: '10',
          },
        }),
      );
    }
    const dead = await this.client.send(
      new GetQueueAttributesCommand({ QueueUrl: await this.url(queueNames.dlq), AttributeNames: ['QueueArn'] }),
    );
    await this.client.send(
      new SetQueueAttributesCommand({
        QueueUrl: await this.url(queueNames.incoming),
        Attributes: {
          RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dead.Attributes?.QueueArn, maxReceiveCount: '5' }),
        },
      }),
    );
  }
  close() {
    this.client.destroy();
  }
}
