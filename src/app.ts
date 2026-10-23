import 'reflect-metadata';
import { Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { DataSource } from 'typeorm';
import { BusinessController, OperationsController, ProviderIdentityGuard, ApiErrorFilter } from './http/api';
import { WageringService } from './app/processor';
import { QueryService } from './app/queries';
import { SqsTransport } from './infra/sqs';

export async function createApp(db: DataSource, sqs: SqsTransport) {
  @Module({
    controllers: [BusinessController, OperationsController],
    providers: [
      ProviderIdentityGuard,
      { provide: WageringService, useValue: new WageringService(db) },
      { provide: QueryService, useValue: new QueryService(db) },
      { provide: SqsTransport, useValue: sqs },
    ],
  })
  class AppModule {}
  const app = await NestFactory.create(AppModule, { logger: false });
  app.useGlobalFilters(new ApiErrorFilter());
  return app;
}
