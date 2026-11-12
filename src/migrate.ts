import { connect } from './db';

const direction = process.argv[2];
if (direction !== 'up' && direction !== 'down') throw new Error('Use up or down');
const db = await connect();
try {
  const migrator = db.getMigrator();
  if (direction === 'up') await migrator.up();
  else await migrator.down({ to: 0 });
} finally {
  await db.close();
}
