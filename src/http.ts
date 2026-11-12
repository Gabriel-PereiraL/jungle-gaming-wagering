import {
  ArgumentsHost, BadRequestException, Body, Catch, Controller, Get, Header, Headers, HttpException,
  Inject, Param, Post, Query, Res, UseGuards,
  type CanActivate, type ExceptionFilter,
} from '@nestjs/common';
import { GetQueueAttributesCommand, type SQSClient } from '@aws-sdk/client-sqs';
import { randomUUID } from 'node:crypto';
import type { z } from 'zod';
import { httpCommand, identifier, openWallet, uuid } from './contracts';
import type { Database } from './db';
import type { QueueUrls } from './messaging';
import { errorCode, log, registry } from './telemetry';
import { Conflict, Missing, Wagering } from './wagering';

export const DB = Symbol('DB');
export const SQS = Symbol('SQS');
export const QUEUES = Symbol('QUEUES');

interface Reply {
  status(code: number): Reply;
  json(body: unknown): void;
}

class InvalidRequest extends BadRequestException {}

function parse<T>(schema: z.ZodType<T>, value: unknown): T {
  const parsed = schema.safeParse(value);
  if (!parsed.success) throw new InvalidRequest({
    code: 'INVALID_PAYLOAD',
    fields: parsed.error.issues.map(issue => issue.path.join('.')),
  });
  return parsed.data;
}

export function correlation(value: unknown) {
  return identifier.safeParse(value).data ?? randomUUID();
}

function parseCursor(cursor: string | undefined, walletId: string): number {
  if (cursor === undefined) return 2147483648;
  try {
    if (typeof cursor !== 'string' || !/^[A-Za-z0-9_-]{1,160}$/.test(cursor)) throw new Error();
    const [id, version] = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
    if (id === walletId && Number.isInteger(version) && version > 0 && version < 2147483648) return version;
  } catch {
    // Map malformed tokens to the same API error.
  }
  throw new InvalidRequest({ code: 'INVALID_CURSOR' });
}

@Catch()
export class ApiErrors implements ExceptionFilter {
  catch(error: unknown, host: ArgumentsHost): void {
    const reply = host.switchToHttp().getResponse<Reply>();
    if (error instanceof InvalidRequest) {
      reply.status(error.getStatus()).json(error.getResponse());
    } else if (error instanceof HttpException) {
      const status = error.getStatus();
      const code = { 400: 'INVALID_REQUEST', 404: 'NOT_FOUND', 413: 'PAYLOAD_TOO_LARGE',
        415: 'UNSUPPORTED_MEDIA_TYPE' }[status as 400 | 404 | 413 | 415] ?? 'HTTP_ERROR';
      reply.status(status).json({ code });
    } else if (error instanceof Conflict) {
      reply.status(409).json({ code: error.message });
    } else if (error instanceof Missing) {
      reply.status(404).json({ code: error.message });
    } else if (error instanceof Error && 'type' in error && error.type === 'entity.too.large') {
      reply.status(413).json({ code: 'PAYLOAD_TOO_LARGE' });
    } else if (error instanceof Error && 'type' in error && error.type === 'encoding.unsupported') {
      reply.status(415).json({ code: 'UNSUPPORTED_MEDIA_TYPE' });
    } else {
      const request = host.switchToHttp().getRequest<{ headers: Record<string, unknown> }>();
      log('http_unavailable', { correlationId: correlation(request.headers['x-correlation-id']),
        errorCode: errorCode(error) }, 'error');
      reply.status(503).json({ code: 'SERVICE_UNAVAILABLE' });
    }
  }
}

// Ponto de extensão para validar JWT OIDC e vincular a identidade ao providerId.
export class ProviderAuthGuard implements CanActivate {
  canActivate(): boolean { return true; }
}

@Controller()
@UseGuards(ProviderAuthGuard)
export class WagerController {
  constructor(@Inject(Wagering) private readonly wagering: Wagering) {}

  @Post('wallets')
  open(@Body() body: unknown, @Headers('x-correlation-id') requestId?: string) {
    const input = parse(openWallet, body);
    return this.wagering.open(input.playerId, input.initialBalance, correlation(requestId));
  }

  @Get('wallets/:walletId')
  wallet(@Param('walletId') walletId: string) {
    return this.wagering.wallet(parse(uuid, walletId));
  }

  @Get('wallets/:walletId/ledger')
  ledger(@Param('walletId') walletId: string, @Query('cursor') cursor?: string, @Query('limit') limit = '50') {
    const id = parse(uuid, walletId);
    if (typeof limit !== 'string' || !/^(?:[1-9][0-9]?|100)$/.test(limit)) {
      throw new InvalidRequest({ code: 'INVALID_LIMIT' });
    }
    return this.wagering.ledger(id, parseCursor(cursor, id), Number(limit));
  }

  @Post('wallets/:walletId/reconciliation')
  async reconcile(@Param('walletId') walletId: string, @Res() reply: Reply,
                  @Headers('x-correlation-id') requestId?: string) {
    reply.status(200).json(await this.wagering.reconcile(parse(uuid, walletId), correlation(requestId)));
  }

  @Post('wagering/transactions')
  async submit(
    @Body() body: unknown, @Headers('idempotency-key') key: unknown, @Res() reply: Reply,
    @Headers('x-correlation-id') requestId?: string,
  ) {
    const outcome = await this.wagering.submit({
      command: parse(httpCommand, body), idempotencyKey: parse(identifier, key),
      correlationId: correlation(requestId),
    });
    const code = outcome.status === 'PROCESSED' ? 200
      : outcome.status === 'REJECTED' ? 422 : outcome.status === 'FAILED' ? 409 : 202;
    reply.status(code).json(outcome);
  }

  @Get('wagering/transactions/:transactionId')
  wager(@Param('transactionId') id: string) {
    return this.wagering.wager({ id: parse(uuid, id) });
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  providerWager(@Param('providerId') provider: string, @Param('externalTransactionId') external: string) {
    return this.wagering.wager({ provider: parse(identifier, provider), external: parse(identifier, external) });
  }
}

@Controller()
export class OpsController {
  constructor(
    @Inject(DB) private readonly db: Database,
    @Inject(SQS) private readonly sqs: SQSClient,
    @Inject(QUEUES) private readonly queues: QueueUrls,
  ) {}

  @Get('health/live')
  live() { return { status: 'live' }; }

  @Get('health/ready')
  async ready() {
    await Promise.all([
      this.db.em.fork().execute('select 1'),
      ...Object.values(this.queues).map(QueueUrl => this.sqs.send(
        new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ['QueueArn'] }),
        { abortSignal: AbortSignal.timeout(2000) },
      )),
    ]);
    return { status: 'ready' };
  }

  @Get('metrics')
  @Header('Content-Type', registry.contentType)
  metrics() { return registry.metrics(); }
}
