import { readFileSync } from 'node:fs';
import type { MigrationInterface, QueryRunner } from 'typeorm';
export class Initial1792540800000 implements MigrationInterface {
  name = 'Initial1792540800000';
  async up(runner: QueryRunner): Promise<void> {
    await runner.query(readFileSync(new URL('./001-up.sql', import.meta.url), 'utf8'));
  }
  async down(runner: QueryRunner): Promise<void> {
    await runner.query(readFileSync(new URL('./001-down.sql', import.meta.url), 'utf8'));
  }
}
