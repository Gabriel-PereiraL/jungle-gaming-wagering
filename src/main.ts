import 'reflect-metadata';
import { Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { runtimeConfig } from './config';
import { connect } from './db';
import { ApiErrors, correlation, DB, OpsController, QUEUES, SQS, WagerController } from './http';
import { createSqs, discoverQueues, OutboxPublisher, refreshQueueMetrics, WagerConsumer } from './messaging';
import { errorCode, log } from './telemetry';
import { Wagering } from './wagering';

async function main() {
  const { databaseUrl, port, workers } = runtimeConfig();
  const db = await connect(databaseUrl);
  const sqs = createSqs();
  const queues = await discoverQueues(sqs);
  const wagering = new Wagering(db);

  @Module({
    controllers: [WagerController, OpsController],
    providers: [
      { provide: Wagering, useValue: wagering },
      { provide: DB, useValue: db },
      { provide: SQS, useValue: sqs },
      { provide: QUEUES, useValue: queues },
    ],
  })
  class AppModule {}

  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    logger: false, abortOnError: false, bodyParser: false,
  });
  app.disable('x-powered-by');
  app.use((req: { headers: Record<string, unknown> },
    res: { setHeader(name: string, value: string): void }, next: () => void) => {
    const correlationId = correlation(req.headers['x-correlation-id']);
    req.headers['x-correlation-id'] = correlationId;
    res.setHeader('X-Correlation-Id', correlationId);
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Cache-Control', 'no-store');
    next();
  });
  app.useBodyParser('json', { limit: '16kb', inflate: false });
  app.useGlobalFilters(new ApiErrors());
  await app.listen(port, '0.0.0.0');
  log('listening', { url: await app.getUrl() });

  const abort = new AbortController();
  const consumer = new WagerConsumer(sqs, queues, wagering);
  const publisher = new OutboxPublisher(db, sqs, queues.events);
  const jobs = !workers ? [] : [
    loop('sqs', () => consumer.poll(abort.signal), 25),
    loop('outbox', () => publisher.publishOne(), 150),
    loop('reference', () => wagering.reprocessOne(), 250),
    loop('metrics', async () => { await refreshQueueMetrics(db, sqs, queues); return false; }, 2000),
  ];

  async function loop(name: string, work: () => Promise<boolean>, idleMs: number) {
    while (!abort.signal.aborted) {
      try {
        if (!await work() && !abort.signal.aborted) await Bun.sleep(idleMs);
      } catch (error) {
        if (abort.signal.aborted) break;
        log('worker_retry', { worker: name, errorCode: errorCode(error) }, 'warn');
        await Bun.sleep(750);
      }
    }
  }

  const shutdown = async () => {
    if (abort.signal.aborted) return;
    abort.abort();
    await Promise.all([app.close(), ...jobs]);
    sqs.destroy();
    await db.close();
    log('stopped');
  };
  for (const signal of ['SIGINT', 'SIGTERM'] as const) {
    process.on(signal, () => { void shutdown().catch(() => process.exit(1)); });
  }
}

main().catch(error => {
  log('startup_failed', { errorCode: errorCode(error) }, 'error');
  process.exit(1);
});
