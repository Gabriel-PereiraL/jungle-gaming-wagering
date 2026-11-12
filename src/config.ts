class InvalidConfig extends Error {
  readonly code: string;
  constructor(variable: string) {
    super('Invalid runtime configuration');
    this.code = 'INVALID_' + variable;
  }
}

function requireUrl(variable: string, protocols: string[]): string {
  const value = process.env[variable];
  if (!value) throw new InvalidConfig(variable);
  try {
    const url = new URL(value);
    if (url.hostname && protocols.includes(url.protocol)) return value;
  } catch { /* Report only the name of the variable. */ }
  throw new InvalidConfig(variable);
}

export function runtimeConfig() {
  const databaseUrl = requireUrl('DATABASE_URL', ['postgres:', 'postgresql:']);
  requireUrl('SQS_ENDPOINT', ['http:', 'https:']);
  const port = Number(process.env.PORT ?? 3000);
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new InvalidConfig('PORT');
  const workers = process.env.WORKERS ?? 'on';
  if (workers !== 'on' && workers !== 'off') throw new InvalidConfig('WORKERS');
  return { databaseUrl, port, workers: workers === 'on' };
}
