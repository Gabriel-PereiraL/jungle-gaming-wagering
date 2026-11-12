import { expect, test } from 'bun:test';
import { appUrls, command, open, post, ready, reconcile } from './fixtures';

test('carga local limitada em três réplicas e reconciliação de todas as carteiras', async () => {
  const urls = appUrls();
  expect(new Set(urls).size).toBe(3);
  for (const url of urls) {
    if (!['127.0.0.1', 'localhost', '[::1]'].includes(new URL(url).hostname)) {
      throw new Error('O teste aceita somente URLs locais');
    }
  }
  await Promise.all(urls.map(ready));
  const wallets = await Promise.all(Array.from({ length: 6 }, (_, i) => open(urls[i % 3], '30.00')));
  const scrape = () => Promise.all(urls.map(async url => (await fetch(url + '/metrics')).text()));
  const metric = (bodies: string[], key: string) => bodies.map(body =>
    Number(body.match(new RegExp('^' + key + ' (.+)$', 'm'))?.[1] ?? 0));
  const sum = (values: number[]) => values.reduce((a, b) => a + b, 0);
  const conflicts = 'wallet_lock_conflict_total';
  const before = await scrape();
  const durations: number[] = [];
  const statuses: number[] = [];
  const started = performance.now();
  for (let batch = 0; batch < 10; batch++) {
    await Promise.all(Array.from({ length: 24 }, async (_, index) => {
      const url = urls[(batch + index) % 3];
      const wallet = wallets[index % wallets.length];
      const begin = performance.now();
      try {
        const response = await post(url, command(wallet, { money: { amount: '1.00', currency: 'BRL' } }));
        statuses.push(response.status);
      } finally {
        durations.push(performance.now() - begin);
      }
    }));
  }
  const seconds = (performance.now() - started) / 1000;
  durations.sort((a, b) => a - b);
  const quantile = (p: number) => durations[Math.ceil(p * durations.length) - 1];
  const after = await scrape();
  const applied = statuses.filter(status => status === 200).length;
  const rejected = statuses.filter(status => status === 422).length;
  const errors = statuses.length - applied - rejected;
  console.log(JSON.stringify({
    requests: statuses.length, concurrency: 24, wallets: wallets.length, seconds,
    requestsPerSecond: statuses.length / seconds, p50Ms: quantile(0.50), p95Ms: quantile(0.95), p99Ms: quantile(0.99),
    applied, rejected, errorRate: errors / statuses.length, lockConflicts: sum(metric(after, conflicts)) - sum(metric(before, conflicts)),
    outboxLagSampleSeconds: Math.max(...metric(after, 'outbox_lag_seconds')),
  }));
  expect(errors).toBe(0);
  expect(applied).toBe(180);
  expect(rejected).toBe(60);
  await Promise.all(wallets.map(wallet => reconcile(urls[0], wallet, '0.00', 31)));
}, 45_000);
