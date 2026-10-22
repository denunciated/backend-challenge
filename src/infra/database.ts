import 'reflect-metadata';
import { DataSource, EntitySchema } from 'typeorm';
import { config } from './config';
import { Initial1792540800000 } from './migrations/initial';

export interface WalletRecord {
  id: string;
  playerId: string;
  currency: string;
  balance: string;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export const WalletEntity = new EntitySchema<WalletRecord>({
  name: 'WalletRecord',
  tableName: 'wallets',
  columns: {
    id: { type: 'uuid', primary: true },
    playerId: { type: 'uuid', name: 'player_id' },
    currency: { type: 'text' },
    balance: { type: 'numeric' },
    version: { type: 'integer' },
    createdAt: { type: 'timestamptz', name: 'created_at' },
    updatedAt: { type: 'timestamptz', name: 'updated_at' },
  },
});

export function createDatabase(url = config.databaseUrl): DataSource {
  return new DataSource({
    type: 'postgres',
    url,
    entities: [WalletEntity],
    migrations: [Initial1792540800000],
    synchronize: false,
    logging: false,
    extra: { max: 20, connectionTimeoutMillis: 5000, application_name: 'wagering' },
  });
}

export function databaseErrorCode(error: unknown): string | undefined {
  if (error && typeof error === 'object') {
    const e = error as { code?: string; driveError?: { code?: string } };
    return e.driveError?.code ?? e.code;
  }
}
