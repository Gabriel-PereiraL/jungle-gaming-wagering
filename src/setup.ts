import { runtimeConfig } from './config';
import { connect } from './db';
import { createSqs, setupQueues } from './messaging';
import { errorCode, log } from './telemetry';

async function main() {
  const { databaseUrl } = runtimeConfig();
  const db = await connect(databaseUrl);
  const sqs = createSqs();
  try {
    await db.getMigrator().up();
    for (let attempt = 1; ; attempt++) {
      try {
        await setupQueues(sqs);
        break;
      } catch (error) {
        if (attempt >= 25) throw error;
        log('setup_waiting_for_sqs', { attempt, errorCode: errorCode(error) }, 'warn');
        await Bun.sleep(1000);
      }
    }
  } finally {
    sqs.destroy();
    await db.close();
  }
}

main().catch(error => {
  log('setup_failed', { errorCode: errorCode(error) }, 'error');
  process.exitCode = 1;
});
