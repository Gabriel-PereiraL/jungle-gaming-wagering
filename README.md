# Jungle Gaming — Distributed Wagering Processor

Processador de apostas com NestJS, Bun, PostgreSQL, MikroORM e SQS FIFO via MiniStack. O Compose inicia três instâncias da mesma aplicação. Cada instância atende HTTP e participa dos workers de entrada, referências pendentes e outbox.

## Executar

Requer Docker com Compose. Para executar os testes no host, use Bun **1.4.4**.

```sh
docker compose up -d --build
docker compose ps -a
docker compose port --index 1 app 3000
```

O serviço `setup` aplica a migração e provisiona as filas. As três réplicas aguardam seu término. O PostgreSQL fica disponível no host em `127.0.0.1:54328`, e o MiniStack em `127.0.0.1:4566`. A porta HTTP de cada réplica é publicada dinamicamente em localhost.

Para iniciar outro ambiente sem tocar nos volumes do primeiro, use um nome de projeto novo e defina `PG_HOST_PORT` e `SQS_HOST_PORT` com portas locais livres. Os testes no host devem usar `DATABASE_URL` e `SQS_ENDPOINT` correspondentes, além do mesmo `COMPOSE_PROJECT_NAME`. Fora do Compose, os processos da aplicação e de setup exigem `DATABASE_URL` e `SQS_ENDPOINT` explícitos.

Exemplo em PowerShell para criar uma carteira na primeira réplica:

```powershell
$api = 'http://' + (docker compose port --index 1 app 3000)
$player = [guid]::NewGuid().ToString()
$wallet = Invoke-RestMethod -Method Post -Uri "$api/wallets" -ContentType application/json -Body (
    @{ playerId = $player; initialBalance = @{ amount = '100.00'; currency = 'BRL' } } | ConvertTo-Json
)
$bet = @{
    providerId = 'provider-demo'; externalTransactionId = 'bet-' + $player
    playerId = $player; walletId = $wallet.id; roundId = 'round-1'
    gameId = 'demo'; kind = 'BET'; money = @{ amount = '25.00'; currency = 'BRL' }
}
Invoke-RestMethod -Method Post -Uri "$api/wagering/transactions" -ContentType application/json `
    -Headers @{ 'Idempotency-Key' = $bet.externalTransactionId } -Body ($bet | ConvertTo-Json)
Invoke-RestMethod -Method Post -Uri "$api/wallets/$($wallet.id)/reconciliation"
```

A aposta deixa saldo de `75.00`, versão `2` e dois lançamentos contando a abertura. Repetir o POST com a mesma chave e o mesmo payload retorna o snapshot original com `idempotentReplay: true`.

## API

| Método e rota | Resposta |
|---|---|
| `POST /wallets` | Cria carteira, com abertura registrada quando positiva |
| `GET /wallets/:walletId` | Saldo corrente e versão |
| `GET /wallets/:walletId/ledger?limit=50&cursor=…` | Lançamentos e `nextCursor` |
| `POST /wagering/transactions` | Resultado da operação, requer `Idempotency-Key` |
| `GET /wagering/transactions/:transactionId` | Consulta por ID interno |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | Consulta pelo identificador do provedor |
| `POST /wallets/:walletId/reconciliation` | Compara saldo persistido e soma assinada dos lançamentos |
| `GET /health/live`, `GET /health/ready`, `GET /metrics` | Processo, dependências e métricas Prometheus |

Na submissão, `200` indica aplicação, `202` indica referência pendente, `422` indica rejeição de negócio, `400` indica contrato inválido e `404` indica carteira inexistente. O `409` com status `FAILED` e `failureCode` identifica uma operação terminal registrada. Um `409` com campo `code` identifica um conflito de identidade ou payload. O `503` indica indisponibilidade transitória. Valores monetários são strings com duas casas decimais. IDs de wallet e jogador são UUIDs. IDs externos e a chave têm até 128 caracteres ASCII visíveis. JSON aceita até 16 KiB e rejeita propriedades desconhecidas.

A fila `wager-transactions.fifo` recebe o envelope `WagerTransactionRequested` do [desafio](https://github.com/junglegaming/backend-challenge), com `data.idempotencyKey` e `version: 1` opcional. Use `walletId` como `MessageGroupId` e o `messageId` da aplicação como `MessageDeduplicationId`. A DLQ é `wager-transactions-dlq.fifo`. A fila `wager-events.fifo` recebe eventos com `eventId`, que o consumidor deve deduplicar de forma persistente.

No PowerShell, após o exemplo HTTP acima, envie uma segunda aposta pela fila local:

```powershell
$messageId = [guid]::NewGuid().ToString()
$bet.externalTransactionId = 'sqs-' + $messageId
$bet.idempotencyKey = $bet.externalTransactionId
$envelope = @{ messageId = $messageId; type = 'WagerTransactionRequested'
    occurredAt = [DateTimeOffset]::UtcNow.ToString('o'); data = $bet }
$queue = docker compose exec -T sqs awslocal sqs get-queue-url --queue-name wager-transactions.fifo --query QueueUrl --output text
$envelope | ConvertTo-Json -Depth 5 -Compress | docker compose exec -T sqs awslocal sqs send-message --queue-url $queue --message-body file:///dev/stdin --message-group-id $wallet.id --message-deduplication-id $messageId
```

Consulte o resultado em `GET /providers/:providerId/wagering/transactions/:externalTransactionId` e reconcilie em `POST /wallets/:walletId/reconciliation`. Use `docker compose logs -f app` para logs e `GET /metrics` em cada réplica para Prometheus. `docker compose stop` desliga o ambiente preservando os volumes.

## Verificar

Exemplo em Bash com `curl` e `jq`. Envie a mesma aposta duas vezes e confira o ledger e a reconciliação:

```bash
api="http://$(docker compose port --index 1 app 3000)"
player=$(cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen)
wallet=$(curl -fsS -H 'Content-Type: application/json' "$api/wallets" \
  -d "{\"playerId\":\"$player\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}" | jq -r .id)
payload="{\"providerId\":\"demo\",\"externalTransactionId\":\"$player\",\"playerId\":\"$player\",\"walletId\":\"$wallet\",\"roundId\":\"demo\",\"gameId\":\"demo\",\"kind\":\"BET\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"
for attempt in 1 2; do
  curl -fsS -H 'Content-Type: application/json' -H "Idempotency-Key: $player" \
    -d "$payload" "$api/wagering/transactions" | jq
done
curl -fsS "$api/wallets/$wallet/ledger?limit=20" | jq
curl -fsS -X POST "$api/wallets/$wallet/reconciliation" | jq
curl -fsS "$api/health/ready"
```

Para reproduzir concorrência com três instâncias, execute `bun run test:concurrency`. O teste disputa duas apostas de 80,00 em uma carteira de 100,00.

Com a pilha iniciada e em um ambiente local de teste:

```sh
bun install --frozen-lockfile
bun run typecheck
bun run test
bun run test:audit
bun run test:security
bun run test:load
```

`bun run test` executa `test:unit`, `test:integration` e `test:concurrency`. Rode as suítes adicionais separadamente, no mesmo projeto Compose de teste e sem outras suítes em paralelo. `test:audit` e `test:security` injetam falhas, reiniciam serviços e inspecionam logs; `test:load` exercita as três réplicas. As suítes usam carteiras novas, consomem mensagens e deixam os registros financeiros no volume.

`docker compose up -d --build` aplica as migrations via `setup`. Para rodar manualmente no host, configure `DATABASE_URL` e execute `bun run migration:up`. `bun run migration:down` reverte todas as migrations do desafio e apaga seus dados; use somente em um banco descartável. A suíte verifica `up/down/up` em um banco temporário.

`bun run test:fifo-window` espera cinco minutos reais e exige `DATABASE_URL` e `SQS_ENDPOINT` de teste. `bun run test:broker-crash` usa o PostgreSQL de teste e cria seu próprio container MiniStack descartável.

| Variável | Configuração fora do Compose |
|---|---|
| `DATABASE_URL` | Obrigatória para aplicação e setup. Exemplo local: `postgres://wager:wager@127.0.0.1:54328/wager`. |
| `SQS_ENDPOINT` | Obrigatória para aplicação e setup. Exemplo local: `http://127.0.0.1:4566`. |
| `PORT` | `3000` |
| `REFERENCE_TTL_MS` | `600000` |
| `WORKERS` | Workers ativos. Use `off` para uma instância apenas HTTP. |

Veja [ARCHITECTURE.md](ARCHITECTURE.md) para decisões e limitações, incluindo a recuperação de um evento perdido pelo emulador.
