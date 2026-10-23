# Distributed Wagering Processor

Implementação do desafio da Jungle Gaming com **Bun 1.3.3, TypeScript estrito, NestJS 11, TypeORM, PostgreSQL 17 e SQS/LocalStack**. O enunciado original está preservado em [CHALLENGE.md](CHALLENGE.md).

Valores monetários são strings nos contratos e `bigint` no domínio. Cada operação financeira confirma saldo, ledger, transação, inbox e outbox atomicamente. Locks por wallet e constraints no PostgreSQL protegem múltiplas instâncias.

## Executar com Docker

Requisitos: Docker com Compose v2, portas 3000–3002, 5432 e 4566 disponíveis.

```sh
docker compose up --build -d
curl http://localhost:3000/health/ready
```

O Compose prepara o banco, executa as migrations, cria as filas e inicia **três instâncias**, nas portas `3000`, `3001` e `3002`. Cada instância executa também os workers. O PostgreSQL persiste em um volume; o LocalStack desta configuração é descartável. Não use esta configuração local para guardar eventos de produção.

```sh
docker compose logs -f app app-2 app-3
docker compose stop
docker compose start
```

A autenticação foi omitida conforme permitido no desafio. `ProviderIdentityGuard` é o ponto explícito para integração OIDC. A API local não deve ser exposta publicamente sem essa integração. Veja as decisões em [ARCHITECTURE.md](ARCHITECTURE.md).

## Desenvolvimento com Bun

```sh
bun install --frozen-lockfile
cp .env.example .env
docker compose up -d postgres localstack
bun run migration:up
bun run queues:init
bun run dev
```

O Bun carrega `.env`. Não é necessário Node.js. Se as três aplicações do Compose já estiverem rodando, pare-as antes de usar as mesmas portas no host.

## Qualidade de código

```sh
bun run lint
bun run lint:fix
bun run format
bun run format:check
```

O ESLint verifica os arquivos TypeScript com as regras recomendadas; o Prettier aplica a formatação compartilhada do projeto.

| Variável                                     | Padrão                                        | Uso                                |
| -------------------------------------------- | --------------------------------------------- | ---------------------------------- |
| `DATABASE_URL`                               | `postgres://wager:wager@localhost:5432/wager` | PostgreSQL                         |
| `SQS_ENDPOINT`                               | `http://localhost:4566`                       | LocalStack                         |
| `AWS_REGION`                                 | `us-east-1`                                   | Região local                       |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | `test`                                        | Credenciais locais                 |
| `PORT`                                       | `3000`                                        | HTTP                               |
| `WORKERS_ENABLED`                            | `true`                                        | Consumidor, outbox e referências   |
| `REFERENCE_MAX_ATTEMPTS`                     | `12`                                          | Tentativas agendadas de referência |
| `REFERENCE_BASE_DELAY_MS`                    | `1000`                                        | Backoff, teto de 5 minutos         |
| `SQS_QUEUE_PREFIX`                           | `wager`                                       | Isolamento das filas de teste      |

## Exemplo HTTP

```sh
curl -s http://localhost:3000/wallets \
  -H 'Content-Type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"100.00","currency":"BRL"}}'
```

Copie o `id` retornado para `WALLET_ID`:

```sh
WALLET_ID='id-retornado'
curl -s http://localhost:3000/wagering/transactions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:bet-1' \
  -H 'X-Correlation-Id: exemplo-1' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"bet-1\",\"playerId\":\"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1\",\"walletId\":\"$WALLET_ID\",\"roundId\":\"round-1\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"
curl -s "http://localhost:3000/wallets/$WALLET_ID"
curl -s "http://localhost:3000/wallets/$WALLET_ID/ledger?limit=50"
curl -s -X POST "http://localhost:3000/wallets/$WALLET_ID/reconciliation"
```

Repetir a aposta, inclusive em outra instância, retorna o mesmo `transactionId` e saldo observado na execução original, com `idempotentReplay: true`. Alterar o payload mantendo a chave retorna `409`.

| Endpoint                                                                  | Finalidade                                                      |
| ------------------------------------------------------------------------- | --------------------------------------------------------------- |
| `POST /wallets`                                                           | Abertura com ledger e transação interna se o saldo for positivo |
| `GET /wallets/:walletId`                                                  | Saldo e versão                                                  |
| `GET /wallets/:walletId/ledger?cursor=...&limit=50`                       | Paginação estável, limite 1–200                                 |
| `POST /wagering/transactions`                                             | Mesmo caso de uso do consumidor SQS                             |
| `GET /wagering/transactions/:transactionId`                               | Resultado persistido                                            |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | Consulta pela identidade externa                                |
| `POST /wallets/:walletId/reconciliation`                                  | Compara saldo e soma do ledger em um snapshot                   |
| `GET /health/live`                                                        | Processo vivo                                                   |
| `GET /health/ready`                                                       | PostgreSQL e as três filas alcançáveis                          |
| `GET /metrics`                                                            | Métricas Prometheus                                             |

Status: `201` para abertura; `200` para processamento/replay; `202` para referência pendente; `400` para contrato inválido; `404` para recurso inexistente; `409` para conflito; `422` para rejeição de negócio; `503` para infraestrutura indisponível. Rejeições são persistidas com `failureCode` e publicadas na outbox.

## SQS e eventos

Filas criadas automaticamente:

- `wager-transactions.fifo`: entrada com envelope `WagerTransactionRequested` do enunciado;
- `wager-transactions-dlq.fifo`: mensagens inválidas ou com tentativas esgotadas;
- `wager-events.fifo`: eventos de integração publicados pela outbox.

Use `MessageGroupId = walletId`, um identificador de deduplicação do envio e `data.idempotencyKey`. O `messageId` do envelope é a identidade persistida na inbox. Mesmo mensagens com diferentes identificadores de transporte são deduplicadas pela identidade da transação. `OPENING` e o provedor reservado `internal` não são aceitos externamente.

O consumidor confirma apenas depois do commit. Erros transitórios usam backoff, heartbeat de visibilidade e até cinco recebimentos. Erros permanentes vão para a DLQ. `SIGTERM` interrompe novos recebimentos e permite concluir as operações em andamento. Consumidores dos eventos devem deduplicar pelo `eventId`.

## Testes

A suíte de integração usa PostgreSQL e LocalStack reais, cria três processos Bun/NestJS independentes e mata um consumidor após o commit com `SIGKILL`. Não substitui os serviços por mocks.

Tudo em containers, sem instalar Bun no host:

```sh
docker compose --profile test run --build --rm test
```

Ou no host, com Bun e os dois serviços de infraestrutura rodando:

```sh
bun run typecheck
bun run test
DATABASE_URL=postgres://wager:wager@localhost:5432/wager_test bun run test:integration
```

Para executar apenas os testes unitários: `bun run test`. `bun test` também descobre a suíte de integração; para ela, configure explicitamente um banco terminado em `_test` e `SQS_QUEUE_PREFIX=wager-test`, ou use o comando dedicado acima.

A integração recria **somente o schema do banco de teste**, verifica migration up/down, usa filas `wager-test-*` e não altera o banco `wager`. Não execute duas suítes contra o mesmo banco/filas simultaneamente.

Cobertura: precisão monetária, transições, reversões, idempotência, 50 duplicatas paralelas, disputa de saldo, wallets independentes, constraints, atomicidade com falha injetada na outbox, inbox/redelivery, referências fora de ordem, retry/DLQ, publishers concorrentes, reinício e reconciliação final de todas as wallets.

## Migrations

```sh
bun run migration:up
bun run migration:down
```

As migrations são versionadas e transacionais; `synchronize` fica desabilitado. O rollback inicial remove as tabelas e seus dados: use apenas em um banco descartável ou com estratégia de backup. Em Compose, apenas o serviço `setup` migra o schema; aplicações concorrentes não disputam migrations.

## Organização

```text
src/domain/           Money, Wallet, transações, ledger, mensagens e eventos
src/application/      processamento compartilhado HTTP/SQS e consultas
src/infrastructure/   TypeORM, migrations SQL, SQS, logs e métricas
src/http/             controllers, guard de extensão e tratamento de erros
src/workers/          consumidor, outbox, referências e shutdown
scripts/              migrations e provisionamento de filas
tests/unit/           regras de domínio
tests/integration/    PostgreSQL/SQS reais e concorrência entre processos
```

Decisões, códigos de falha e limitações estão em [ARCHITECTURE.md](ARCHITECTURE.md).
