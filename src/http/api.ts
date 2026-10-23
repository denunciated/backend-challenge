import {
  Body,
  Catch,
  Controller,
  Get,
  Headers,
  HttpCode,
  Inject,
  Injectable,
  Param,
  Post,
  Query,
  Res,
  UseGuards,
  type ArgumentsHost,
  type CanActivate,
  type ExceptionFilter,
} from '@nestjs/common';
import type { Response } from 'express';
import { ZodError } from 'zod';
import { WageringService } from '../app/processor';
import { QueryService } from '../app/queries';
import { AppError, DomainError } from '../domain/errors';
import { validateKey } from '../app/contracts';
import { log, registry } from '../infra/observability';
import { SqsTransport } from '../infra/sqs';

@Injectable()
export class ProviderIdentityGuard implements CanActivate {
  canActivate(): boolean {
    return true;
  }
}

function correlation(value?: string): string {
  return value && /^[\w.:-]{1,200}$/.test(value) ? value : crypto.randomUUID();
}

@Controller()
@UseGuards(ProviderIdentityGuard)
export class BusinessController {
  constructor(
    @Inject(WageringService) private readonly service: WageringService,
    @Inject(QueryService) private readonly queries: QueryService,
  ) {}

  @Post('wallets')
  create(@Body() body: unknown, @Headers('x-correlation-id') id?: string) {
    return this.service.createWallet(body, correlation(id));
  }

  @Get('wallets/:walletId')
  wallet(@Param('walletId') id: string) {
    return this.queries.wallet(id);
  }

  @Get('wallet/:walletId/ledger')
  ledger(@Param('walletId') id: string, @Query('cursor') cursor?: string, @Query('limit') limit?: string) {
    return this.queries.ledger(id, cursor, limit);
  }

  @Post('wallets/:walletId/reconciliation')
  @HttpCode(200)
  reconciliation(@Param('walletId') id: string, @Headers('x-correlation-id') ctx?: string) {
    return this.queries.reconcile(id, correlation(ctx));
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  providerTransaction(@Param('providerId') provider: string, @Param('externalTransactionId') external: string) {
    return this.queries.providerTransaction(provider, external);
  }

  @Post('wagering/transactions')
  async submit(
    @Body() body: unknown,
    @Headers('idempotency-key') key: unknown,
    @Headers('x-correlation-id') ctx: string | undefined,
    @Res({ passthrough: true }) response: Response,
  ) {
    const correlationId = correlation(ctx);

    response.setHeader('X-Correlation-Id', correlationId);

    const result = await this.service.submit(body, validateKey(key), { correlationId });

    response.status(
      result.status === 'REJECTED'
        ? 422
        : result.status === 'FAILED'
          ? 500
          : result.status === 'PENDING_REFERENCE' || result.status === 'PENDING'
            ? 202
            : 200,
    );
    return result;
  }
}

@Controller()
export class OperationsController {
  constructor(
    @Inject(WageringService) private readonly service: WageringService,
    @Inject(SqsTransport) private readonly sqs: SqsTransport,
  ) {}

  @Get('health/live')
  live() {
    return { status: 'ok' };
  }

  @Get('health/ready')
  async ready() {
    try {
      await Promise.all([this.service.db.query('SELECT 1'), this.sqs.ready()]);
      return { status: 'ready' };
    } catch {
      throw new AppError('DEPENDENCY_UNAVAILABLE', 503);
    }
  }

  @Get('metrics')
  async metrics(@Res() response: Response) {
    response.type(registry.contentType).send(await registry.metrics());
  }
}

@Catch()
export class ApiErrorFilter implements ExceptionFilter {
  catch(error: unknown, host: ArgumentsHost) {
    const response = host.switchToHttp().getResponse<Response>();

    const request = host.switchToHttp().getRequest<{ headers: Record<string, string | undefined> }>();

    const correlationId = correlation(request.headers['x-correlation-id']);

    let status = 503,
      code = 'INFRASTRUCTURE_UNAVAILABLE';

    if (error instanceof AppError) {
      status = error.httpStatus;
      code = error.code;
    } else if (error instanceof DomainError) {
      status = 400;
      code = error.code;
    } else if (error instanceof ZodError) {
      status = 400;
      code = 'INVALID_PAYLOAD';
    } else if (error && typeof error === 'object' && 'getStatus' in error && typeof error.getStatus === 'function') {
      status = error.getStatus() as number;
      code = status === 404 ? 'NOT_FOUND' : 'INVALID_REQUEST';
    }

    if (status >= 500) log('error', 'http_request_failed', { correlationId, code });
    response.status(status).json({
      error: { code, correlationId },
      ...(error instanceof ZodError
        ? { fields: error.issues.map((issue) => ({ path: issue.path.join('.'), code: issue.code })) }
        : {}),
    });
  }
}
