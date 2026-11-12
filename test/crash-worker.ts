import { connect } from '../src/db';
import { createSqs, WagerConsumer, type QueueUrls } from '../src/messaging';
import { Wagering } from '../src/wagering';

const db = await connect();
const sqs = createSqs();
const urls = JSON.parse(process.argv[2]) as QueueUrls;
const consumer = new WagerConsumer(sqs, urls, new Wagering(db), async () => {
  process.stdout.write('AFTER_COMMIT_BEFORE_ACK\n');
  await new Promise<void>(() => { setInterval(() => {}, 1000); });
});
try {
  while (!await consumer.poll()) { /* long polling */ }
} finally {
  sqs.destroy();
  await db.close();
}
