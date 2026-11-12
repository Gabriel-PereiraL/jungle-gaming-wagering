import { connect } from '../src/db';
import { createSqs, OutboxPublisher } from '../src/messaging';

const url = process.argv[2];
if (!url) throw new Error('Missing queue URL');
const db = await connect();
const sqs = createSqs();
if (process.argv.includes('--pause-after-send')) {
  sqs.middlewareStack.add(next => async args => {
    await next(args);
    process.stdout.write('SQS_ACCEPTED_BEFORE_DB_CONFIRMATION\n');
    return new Promise(() => { setInterval(() => {}, 1000); });
  }, { step: 'initialize', name: 'auditCrashBarrier' });
}
if (process.argv.includes('--pause-before-send')) {
  sqs.middlewareStack.add(() => () => {
    process.stdout.write('CLAIMED_BEFORE_SQS_SEND\n');
    return new Promise(() => { setInterval(() => {}, 1000); });
  }, { step: 'initialize', name: 'auditBeforeSendBarrier' });
}
try {
  const claimed = await new OutboxPublisher(db, sqs, url).publishOne();
  console.log(JSON.stringify({ claimed }));
} finally {
  await db.close();
  sqs.destroy();
}
