# Arquitetura e decisões

## Fronteiras

O domínio não importa NestJS, TypeORM, SQS nem tipos monetários de bibliotecas. As entidades possuem construtores privados, factories e transições explícitas. `rehydrate` reconstrói estado persistido sem repetir regras de transição. DTOs são interfaces ou schemas Zod; eventos usam uma classe abstrata e uma subclasse por tipo/versionamento.

`WageringService.submit` é o caso de uso compartilhado por HTTP e SQS. Os adapters cuidam do transporte; somente o caso de uso decide sobre efeitos financeiros. `QueryService` fornece consultas, cursor e reconciliação. `WorkerRuntime` agenda loops independentes de entrada, outbox e referências. Os workers podem ser executados em todas as instâncias.

## Money e persistência

`Money` armazena centavos como `bigint`, é imutável e não usa `number` para dinheiro. A escala de duas casas permite aritmética inteira exata sem uma biblioteca decimal. A saída sempre contém duas casas. Entradas com mais casas são rejeitadas, **nunca arredondadas**. `1`, `1.0` e `1.00` normalizam para `1.00`; negativos só existem internamente, por subtração/negação ou reidratação. O máximo é 36 dígitos inteiros e dois fracionários. Overflow de crédito rejeita a transação como `AMOUNT_OVERFLOW`.

O domínio suporta moedas diferentes e impede operações cruzadas. Os contratos externos aceitam apenas `BRL`, simplificação permitida no enunciado. Uma transação financeira deve ter valor positivo; `LOSS` exige `0.00`; a abertura aceita zero. Isso elimina lançamentos monetários sem efeito.

Escolha de ORM: **TypeORM**, uma das opções permitidas. `EntitySchema` mapeia a wallet sem decorators no domínio; reidratação converte explicitamente o par `numeric`/moeda para `Money`. O driver PostgreSQL mantém `numeric` como string. `DataSource.transaction` delimita a unidade de trabalho, `pessimistic_write` protege o aggregate e o repositório persiste a wallet. SQL parametrizado dentro do mesmo `EntityManager` implementa ledger, inbox/outbox, índices parciais e operações específicas do PostgreSQL. Evita-se um Identity Map compartilhado entre requisições. O custo dessa abordagem é manter os mapeamentos SQL explícitos; não se pretende portabilidade para outro banco.

As colunas são `numeric` com constraints de escala e magnitude, em vez de `numeric(p,2)` que arredondaria silenciosamente uma entrada SQL com casas extras. Não há conversão monetária para ponto flutuante. Números de tentativas, versões, contagens, durações e métricas podem usar `number`.

## Concorrência e transação

O isolamento é `READ COMMITTED`, com `SELECT ... FOR UPDATE` na wallet. A sequência de escrita é inbox (quando presente), wallet, transação financeira, saldo, ledger e outbox, terminando em um único commit. Wallets diferentes não compartilham um lock global. Uma leitura inicial de identidade permite retornar replays já confirmados sem adquirir o lock da wallet; o `INSERT ... ON CONFLICT` protege a corrida entre novas submissões. Alterações pendentes sempre adquirem wallet antes da transação, incluindo o worker de referências, evitando inversão de locks.

O saldo é lido, calculado e atualizado **dentro do lock**. As duas apostas de 80 sobre 100 são serializadas: a primeira vê 100, a segunda vê 20 e é rejeitada. `version` inicia em 1, inclusive com OPENING, e só aumenta quando o saldo muda. LOSS e rejeições não incrementam a versão.

Deadlock (`40P01`), falha de serialização (`40001`) e timeout de lock (`55P03`) têm até três tentativas com jitter curto. Cada tentativa reconstrói seus aggregates: estado mutado em uma tentativa revertida não é reutilizado. O lock tem timeout de 3 segundos; statements do processamento têm timeout de 15 segundos. Não há publicação dentro da transação financeira.

### Proteções no schema

- `UNIQUE(player_id,currency)` para wallets.
- Chave idempotente global e `UNIQUE(provider_id,external_transaction_id)`.
- Saldo não negativo, precisão monetária, status/kind e referências obrigatórias em checks.
- Uma entrada por `(transaction_id,wallet_id)`, FKs de wallet/moeda e transação/wallet.
- Ledger imutável por triggers de UPDATE, DELETE e TRUNCATE, com aritmética verificada e cadeia de saldos consecutivos.
- Trigger de versão e identidade imutável da wallet.
- Identidade imutável da transação, estados terminais imutáveis e proibição de exclusão.
- Índice parcial único de reversão processada por referência.
- Constraint triggers diferidos até o commit verificam `wallet.balance = soma(ledger)` e a correspondência entre transação processada, valor, direção, escopo e ledger. Transações não financeiras não podem possuir lançamentos.

Essas proteções se aplicam também a SQL direto sob operação normal. Um administrador capaz de desabilitar triggers ou alterar o schema continua tendo poderes administrativos. Em produção, migrations e aplicação devem usar papéis separados, sem DDL para a aplicação.

O cálculo diferido da soma do ledger privilegia correção demonstrável, com custo O(n) no histórico da wallet. Para grande volume, seriam necessários particionamento, checkpoints auditáveis e uma estratégia incremental formalmente verificada. Não há alegação de throughput de produção.

## Idempotência e resultado original

O header HTTP é obrigatório; no SQS, `data.idempotencyKey` ocupa o mesmo papel. A chave recomendada é `{providerId}:{externalTransactionId}`. A chave é global: providers devem usar namespace próprio. Reutilizar a identidade externa com outra chave também é conflito.

Hash: SHA-256 do JSON canônico do DTO de negócio validado. Chaves de objetos são ordenadas recursivamente por ordem lexicográfica, arrays mantêm a ordem, campos indefinidos são omitidos e dinheiro é normalizado para duas casas. Inclui provider, identidade externa, wallet, player, round, game, kind, money e referência quando presente. Exclui header, correlationId, messageId e timestamps de transporte. Campos desconhecidos são rejeitados, não ignorados.

O resultado contém status, saldo observado, failureCode e transactionId. É persistido na mesma transação e não é recalculado a partir do saldo atual em um replay. Resultados terminais são imutáveis. Enquanto `PENDING_REFERENCE`, consultas e replays mostram o estado persistido mais recente; quando o worker termina, passam a mostrar o resultado final. Apenas `idempotentReplay` difere na resposta de replay.

A inbox tem PK `(consumer_name,message_id)` e um hash canônico de `type` e `data` do envelope. O timestamp de transporte não faz parte desse hash. Reutilizar um messageId com conteúdo diferente produz conflito e DLQ. Reentregas com outro messageId continuam protegidas pela identidade financeira.

## Referências e interpretações

Referências são resolvidas por provider e identificador externo. Devem corresponder ao mesmo provider, player, wallet, moeda e rodada. `gameId` é parte do hash, mas não é restrição adicional de escopo porque o enunciado não a exige. `WIN` pode vir sem referência; se a enviar, deve ser uma BET processada. `BET`, `LOSS` e OPENING não aceitam referências. Autorreferências são inválidas.

REFUND só reverte BET; ROLLBACK reverte BET, WIN ou REFUND. Reversões são integrais. Uma referência já rejeitada/failed causa `REFERENCE_NOT_PROCESSED`. Referências ausentes ou ainda pendentes ficam em `PENDING_REFERENCE`. Dependências cíclicas esgotam o mesmo limite de tentativas.

**Interpretação conservadora:** uma transação só pode ter **uma reversão processada no total**, seja REFUND ou ROLLBACK. Permitir uma de cada tipo sobre a mesma BET devolveria o débito duas vezes. O índice parcial protege essa decisão no banco. É possível reverter um REFUND via ROLLBACK, como explicitamente permitido, mas isso não apaga a reversão original nem permite outra restituição da mesma BET.

O worker procura referências vencidas a cada 250 ms. Tentativas e próximo horário são persistidos; o processamento readquire o lock da wallet e revalida status/horário. Múltiplas instâncias podem selecionar a mesma candidata, mas apenas a primeira realiza a tentativa devida. Backoff: `min(300s, base * 2^(tentativa-1))`; padrão de 1 segundo e 12 tentativas agendadas, aproximadamente 23,5 minutos de espera acumulada. A espera inicial não conta como tentativa agendada. Na última tentativa, uma referência já disponível ainda pode ser aplicada; se continuar ausente, ocorre `REFERENCE_EXPIRED`, com evento de rejeição. Reiniciar o processo não reinicia contadores.

## Estados e falhas

Transições permitidas:

```text
PENDING -> PENDING_REFERENCE | PROCESSED | REJECTED | FAILED
PENDING_REFERENCE -> PENDING_REFERENCE | PROCESSED | REJECTED | FAILED
PROCESSED / REJECTED / FAILED -> nenhuma
```

`FAILED` e `fail()` modelam falha permanente de infraestrutura. O fluxo automático não classifica uma indisponibilidade temporária como falha financeira terminal: reverte a transação SQL, tenta novamente e, ao esgotar o limite de transporte, preserva a mensagem na DLQ. Não promete gravar FAILED quando o próprio banco está inacessível. Uma recuperação operacional futura pode usar essa transição; não foi criado um endpoint administrativo sem autenticação.

| Código                        | Significado/ação                                                  |
| ----------------------------- | ----------------------------------------------------------------- |
| `INSUFFICIENT_FUNDS`          | BET sem saldo; resultado terminal, não repetir a mesma identidade |
| `REVERSAL_INSUFFICIENT_FUNDS` | ROLLBACK debitante deixaria saldo negativo                        |
| `CURRENCY_MISMATCH`           | Moeda incompatível com a wallet                                   |
| `PLAYER_MISMATCH`             | Player não pertence à wallet                                      |
| `REFERENCE_SCOPE_MISMATCH`    | Referência fora do escopo autorizado                              |
| `REFERENCE_KIND_INVALID`      | Tipo de referência incompatível                                   |
| `REFERENCE_NOT_PROCESSED`     | Referência terminal que não foi aplicada                          |
| `REFERENCE_AMOUNT_MISMATCH`   | Reversão parcial ou valor divergente                              |
| `ALREADY_REVERSED`            | A referência já tem reversão processada                           |
| `REFERENCE_EXPIRED`           | Referência não ficou disponível no limite de tentativas           |
| `AMOUNT_OVERFLOW`             | Crédito excederia a magnitude monetária suportada                 |
| `INFRASTRUCTURE_PERMANENT`    | Reservado para transição explícita FAILED                         |

Erros de contrato usam 400 (`INVALID_PAYLOAD`, `INVALID_AMOUNT`, `INVALID_IDEMPOTENCY_KEY`, `REFERENCE_REQUIRED`, etc.). Conflitos usam 409 (`IDEMPOTENCY_CONFLICT`, `INBOX_PAYLOAD_CONFLICT`, `WALLET_ALREADY_EXISTS`). Rejeições de negócio usam 422; pendências, 202. Recursos inexistentes usam 404. Indisponibilidade/banco ocupado usa 503, permitindo retry com a mesma chave. GET de uma transação existente retorna 200 com seu status persistido. FAILED, se persistido por uma extensão operacional, retorna 500 em replay da submissão.

## Inbox, SQS e shutdown

As filas são FIFO por wallet como otimização; a garantia financeira permanece no PostgreSQL. O consumidor recebe uma mensagem de cada vez por instância, usa o mesmo caso de uso HTTP e deleta a mensagem apenas após commit. Rejeições e pendências são resultados aceitos e recebem ack; pendências passam a ser responsabilidade do worker SQL. Mensagens inválidas e conflitos são enviados imediatamente à DLQ.

Erros transitórios alteram a visibilidade com backoff de 1, 2, 4 e 8 segundos. O quinto recebimento vai para a DLQ. Uma RedrivePolicy de cinco recebimentos também protege situações em que o worker morre antes de classificar a falha. No encaminhamento explícito, envia-se à DLQ antes de deletar a origem, com deduplicationId estável. Isso pode duplicar a entrega de auditoria, mas não perde a mensagem. O contador da aplicação cobre encaminhamentos explícitos; redrives automáticos exigem também a métrica nativa do SQS em produção.

Visibilidade inicial de 60 segundos, renovada a cada 20 segundos durante o processamento. SIGTERM cancela long polling e espera até 25 segundos pelos loops; caso não drenem, devolve a visibilidade da mensagem em andamento. O processo tem deadline global de 35 segundos e o Compose concede 40 segundos. Um crash depois de commit e antes de ack é seguro porque a inbox e a identidade financeira já estão confirmadas.

## Outbox e publicação

Eventos só ficam visíveis ao publisher após o commit financeiro. Cada publisher abre sua própria transação, seleciona uma linha devida com `FOR UPDATE SKIP LOCKED`, envia ao SQS e marca `published_at`. A rede tem timeout; falhas persistem tentativas e próximo horário com backoff até cinco minutos, sem limite que descartaria eventos confirmados. O lock da outbox não é o lock da wallet. Cada instância possui um publisher.

Crash antes do envio: o lock é liberado, outra instância assume. Crash após envio e antes de confirmar `published_at`: o evento é reenviado com o mesmo `eventId`. A deduplicação FIFO de cinco minutos ajuda, mas **não substitui uma inbox no consumidor do evento**. A entrega é at-least-once, não exactly-once. Publishers concorrentes podem publicar eventos de uma wallet fora da ordem de criação: consumidores de projeção devem usar `walletVersion` ou consultar o estado, e nunca reaplicar efeitos monetários sem idempotência.

Eventos: `WagerTransactionProcessed` (inclui LOSS e OPENING), `WagerTransactionRejected`, `WagerTransactionPendingReference` (uma vez ao entrar nesse estado) e `WalletBalanceChanged` (somente saldo alterado). O envelope gravado contém eventId, aggregateId, correlationId, causationId, occurredAt ISO, eventType e version. Valores são `MoneyProps`, nunca bigint no JSON.

O broker de saída é uma fila de integração, não um sistema de fan-out. Vários tipos de consumidores independentes em produção precisariam de SNS/EventBridge e uma fila por assinatura. Não há consumidor fictício descartando eventos publicados.

## Consultas e observabilidade

O cursor codifica em base64url walletId, última sequência e sequência máxima da primeira página. É opaco ao cliente, validado e limitado ao range de bigint do PostgreSQL. Novos lançamentos não entram nas páginas de uma consulta em andamento. Não é um token de autorização. A sequência global do ledger tem lacunas permitidas; dentro da wallet, o lock garante a ordem de inserção.

A reconciliação usa uma única consulta SQL/snapshot MVCC para saldo e soma dos lançamentos. Não corrige divergências. Responde com diferença exata e aumenta o contador/loga um evento de erro se encontrar inconsistência.

Logs JSON têm correlationId, messageId, transactionId, walletId e providerId; não registram o payload nem valores financeiros. Métricas em `/metrics`: transições por status, replays, retries por worker, DLQ, conflitos de lock, lag da outbox, latência e divergências de reconciliação. São métricas por processo, a serem agregadas pelo Prometheus. Identificadores não são labels para evitar cardinalidade alta. O estado financeiro e a auditoria continuam no banco mesmo quando o processo morre antes de incrementar uma métrica.

Liveness não consulta dependências. Readiness consulta PostgreSQL e as filas SQS. Health fica fora do guard; métricas devem ficar em rede interna numa implantação real.

## Autenticação e limitações deliberadas

Autenticação é opcional e não vale pontos no enunciado; ficou fora desta implementação. O guard no-op é explícito. A integração planejada é OIDC via Keycloak/Zitadel: verificar assinatura/JWKS, issuer, audience e expiração; vincular o claim de provedor ao providerId e limitar consultas/escritas por escopo. Não há cadastro artesanal de senhas. Mensagens SQS usam canal interno confiável, mas passam pelas mesmas validações de domínio.

Não foram incluídos ledger de partidas dobradas, OpenTelemetry, dashboard ou benchmark. Banco e filas de teste são separados da aplicação, mas a suíte é serial por namespace. A credencial local do PostgreSQL pode criar o banco de testes; uma implantação real usaria provisionamento e permissões separados. LocalStack não está configurado para persistência após recriação; reinícios de aplicação preservam o estado do PostgreSQL e do broker em execução. Nenhuma garantia de retenção de produção é atribuída ao ambiente emulador descartável.
