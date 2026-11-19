import assert from 'node:assert/strict';
import { test } from 'node:test';
import { sql } from 'kysely';
import { Connector } from '../dist/dataquery/postgresqlkysely/connector.js';

const dsn = process.env.SA_QUERY_TLS_POSTGRES_DSN;
test('PostgreSQL connector verifies CA and keeps TLS enabled', {skip: !dsn}, async () => {
  assert.ok(process.env.SA_QUERY_TLS_BAD_CA);
  const context = {signal: () => new AbortController().signal};
  for (const trusted of [false, true]) {
    const address = new URL(dsn);
    address.searchParams.set('sslmode', 'verify-full');
    if (!trusted) address.searchParams.set('sslrootcert', process.env.SA_QUERY_TLS_BAD_CA);
    const connector = new Connector({id: 1, name: 'tls', type: 13, implementation: 'node/kysely',
      connectionString: address.toString(), connectionsCount: 1, connectTimeoutMs: 2000, operationTimeoutMs: 3000});
    try {
      if (!trusted) await assert.rejects(connector.start(context), /certificate|self.signed/i);
      else {
        await connector.start(context);
        const result = await sql`SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()`.execute(connector.db);
        assert.equal(result.rows[0].ssl, true);
      }
    } finally {await connector.stop(context);}
  }
});
