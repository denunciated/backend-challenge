import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type Message,
} from '@aws-sdk/client-sqs';
import { ZodError } from 'zod';
import { AppError, DomainError } from '../domain/errors';
import { hashPayload, messageSchema } from '../app/contracts';
import type { WageringService } from '../app/processor';
import { queueNames, SqsTransport } from '../infra/sqs';
import { log, metrics } from '../infra/observability';
import { backoff } from '../domain/messages';

export class WagerConsumer {
  private stopping = false;
  private readonly pollController = new AbortController();
  private inFlight: { url: string; receipt: string } | undefined;

  constructor(
    private readonly sqs: SqsTransport,
    private readonly service: WageringService,
    private readonly afterCommit?: () => Promise<void>,
  ) {}

  async tick(): Promise<void> {
    if (this.stopping) return;

    const url = await this.sqs.url(queueNames.incoming);

    const result = await this.sqs.client.send(
      new ReceiveMessageCommand({
        QueueUrl: url,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 10,
        VisibilityTimeout: 60,
        MessageSystemAttributeNames: ['ApproximateReceiveCount'],
      }),
      { abortSignal: this.pollController.signal },
    );

    for (const message of result.Messages ?? []) {
      if (this.stopping) {
        if (message.ReceiptHandle) await this.visibility(url, message.ReceiptHandle, 0);
        continue;
      }
      await this.handle(url, message);
    }
  }

  async handle(url: string, message: Message): Promise<void> {
    const receipt = message.ReceiptHandle;

    if (!receipt) return;

    this.inFlight = { url, receipt };

    const heartbeat = setInterval(() => {
      void this.visibility(url, receipt, 60).catch(() =>
        log('warn', 'visibility_extension_failed', { messageId: message.MessageId }),
      );
    }, 20000);

    let context: { messageId?: string; correlationId?: string; walletId?: string; providerId?: string } = {
      messageId: message.MessageId,
    };

    try {
      const envelope = messageSchema.parse(JSON.parse(message.Body ?? ''));

      const { idempotencyKey, ...input } = envelope.data;

      context = {
        messageId: envelope.messageId,
        correlationId: envelope.messageId,
        walletId: input.walletId,
        providerId: input.providerId,
      };

      await this.service.submit(input, idempotencyKey, {
        correlationId: envelope.messageId,
        messageId: envelope.messageId,
        inboxHash: hashPayload({ data: envelope.data, type: envelope.type }),
      });

      await this.afterCommit?.();
      await this.sqs.client.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: receipt }));
    } catch (error) {
      const attempts = Number(message.Attributes?.ApproximateReceiveCount ?? '1');

      const permanent =
        error instanceof ZodError ||
        error instanceof SyntaxError ||
        error instanceof DomainError ||
        (error instanceof AppError && error.httpStatus < 500);

      if (permanent || attempts >= 5) {
        await this.sqs.client.send(
          new SendMessageCommand({
            QueueUrl: await this.sqs.url(queueNames.dlq),
            MessageBody: message.Body ?? '{}',
            MessageDeduplicationId: message.MessageId ?? hashPayload(message.Body),
            MessageAttributes: {
              failureCode: {
                DataType: 'String',
                StringValue:
                  error instanceof AppError || error instanceof DomainError
                    ? error.code
                    : permanent
                      ? 'INVALID_MESSAGE'
                      : 'RETRY_EXHAUTED',
              },
            },
          }),
        );

        metrics.dlq.inc();
        await this.sqs.client.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: receipt }));
        log('warn', 'message_dead_lettered', context);
      } else {
        metrics.retries.inc({ worker: 'sqs' });
        await this.visibility(url, receipt, Math.ceil(backoff(attempts) / 1000));
        log('warn', 'message_retry_scheduled', context);
      }
    } finally {
      clearInterval(heartbeat);
      this.inFlight = undefined;
    }
  }

  stopPolling() {
    this.stopping = true;
    this.pollController.abort();
  }

  async releaseInFlight() {
    if (this.inFlight) await this.visibility(this.inFlight.url, this.inFlight.receipt, 0);
  }

  private async visibility(url: string, receipt: string, seconds: number) {
    await this.sqs.client.send(
      new ChangeMessageVisibilityCommand({ QueueUrl: url, ReceiptHandle: receipt, VisibilityTimeout: seconds }),
    );
  }
}
