import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Kysely, PostgresDialect, sql } from 'kysely';
import { Client, Pool } from 'pg';
import Cursor from 'pg-cursor';
import { makeRowsEndpointConsumer, makeExecuteEndpointConsumer } from '../dist/dataquery/postgresqlkysely/index.js';

const dsn = process.env.SA_QUERY_TEST_POSTGRES_DSN;
test('PostgreSQL Query native lifecycle', { skip: !dsn }, async () => {
  const pool = new Pool({ connectionString: dsn, max: 1 });
  const db = new Kysely({ dialect: new PostgresDialect({ pool, cursor: Cursor, controlClient: Client }) });
  try {
    for (const mode of ['empty', 'many', 'partial', 'cancel', 'sql-error', 'inflight-cancel', 'execute']) {
      const abort = new AbortController();
      const context = { signal: () => abort.signal };
      const output = [], failures = [];
      const stream = {
        consumeResult: async (_ctx, value) => { output.push(value); },
        errorStream: () => ({ emit: async (_ctx, error) => { failures.push(error); } }),
        runtimeEnvironment: () => ({ metrics: () => ({ enabled: () => false }) }), setQueryConsumer: () => {}
      };
      let ends = 0;
      let timer;
      const handler = {
        beginRequest: (ctx) => ({ context: ctx, state: 42 }),
        buildQuery: (_ctx, state, count) => {
          assert.equal(state, 42);
          if (mode === 'inflight-cancel') {
            timer = setTimeout(() => abort.abort(new Error('cancel request')), 100);
            return sql`select pg_sleep(10)`.compile(db);
          }
          if (mode === 'sql-error') return sql`select missing_column`.compile(db);
          if (mode === 'execute') return sql`insert into query_values values (${count})`.compile(db);
          return sql`select generate_series(1, ${count}) as value`.compile(db);
        },
        handleRow: async (ctx, _state, row, out) => {
          await out.out(ctx, row.value);
          if (mode === 'partial') throw new Error('mapping failed');
          if (mode === 'cancel') abort.abort(new Error('cancel request'));
        },
        handleResult: async (ctx, _state, result, out) => { await out.out(ctx, Number(result.numAffectedRows)); },
        endRequest: async (ctx, _stream, state, error, out, errors) => {
          ends++;
          assert.equal(state, 42);
          assert.equal(pool.totalCount - pool.idleCount, 0, mode + ': unreleased connection');
          if (error) await errors.out(ctx, error);
          await out.out(ctx, 'done');
        }
      };
      if (mode === 'execute') await sql`create temp table query_values (value integer)`.execute(db);
      const consumer = mode === 'execute'
        ? makeExecuteEndpointConsumer(stream, db, handler)
        : makeRowsEndpointConsumer(stream, db, handler, 2);
      const started = performance.now();
      try { await consumer.consume(context, mode === 'empty' ? 0 : 3); }
      finally { clearTimeout(timer); }
      assert.equal(ends, 1, mode);
      assert.deepEqual(output, mode === 'many' ? [1, 2, 3, 'done'] : ['partial', 'cancel', 'execute'].includes(mode) ? [1, 'done'] : ['done'], mode);
      assert.equal(failures.length, ['empty', 'many', 'execute'].includes(mode) ? 0 : 1, mode);
      if (mode === 'inflight-cancel') assert.ok(performance.now() - started < 3000, 'query was not interrupted');
    }
  } finally { await db.destroy(); }
});

test('PostgreSQL Query configuration parsing', async () => {
  const { parseCanonicalConfig } = await import('../dist/runtime/config/parser.js');
  const config = parseCanonicalConfig({
    dataConnectors: { database: { id: 1, name: 'Database', type: 13, implementation: 'node/kysely', connectionString: 'postgresql://localhost/query_test', connectionsCount: 3 } },
    endpoints: { readRows: { id: 2, name: 'ReadRows', idDataConnector: 1, functionName: 'ReadRows', queryOperation: 'Rows', enabled: false, fetchSize: 64 } }
  });
  assert.equal(config.dataConnectors[0].connectionsCount, 3);
  assert.equal(config.endpoints[0].queryOperation, 'Rows');
  assert.equal(config.endpoints[0].enabled, false);
  assert.equal(config.endpoints[0].fetchSize, 64);
});

test('PostgreSQL connector startup cancellation', async () => {
  const { createServer } = await import('node:net');
  const { once } = await import('node:events');
  const { Connector } = await import('../dist/dataquery/postgresqlkysely/index.js');
  const { Context } = await import('../dist/runtime/context.js');
  const sockets = new Set();
  const server = createServer(socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = server.address().port;
  const connector = new Connector({ id: 1, name: 'Database', type: 13, implementation: 'node/kysely', properties: {}, connectionString: `postgresql://user:test-only@127.0.0.1:${port}/test`, connectionsCount: 1, connectTimeoutMs: 500, operationTimeoutMs: 500 });
  const abort = new AbortController();
  const context = new Context(abort.signal);
  const timer = setTimeout(() => abort.abort(new Error('startup cancelled')), 75);
  try {
    const started = performance.now();
    await assert.rejects(connector.start(context), /startup cancelled/);
    assert.ok(performance.now() - started < 450);
  } finally {
    clearTimeout(timer);
    for (const socket of sockets) socket.destroy();
    server.close();
    await connector.stop(context);
  }
});

test('PostgreSQL connector idle failure recovery', { skip: !dsn }, async () => {
  const { Connector } = await import('../dist/dataquery/postgresqlkysely/index.js');
  const { Context } = await import('../dist/runtime/context.js');
  const errors = [];
  const context = new Context();
  const logger = { error(ctx, message) { assert.equal(ctx, context); errors.push(message); } };
  const connector = new Connector({ id: 1, name: 'Database', type: 13, implementation: 'node/kysely', properties: {}, connectionString: dsn, connectionsCount: 1, connectTimeoutMs: 500, operationTimeoutMs: 1000 }, logger);
  const control = new Client({ connectionString: dsn });
  try {
    await connector.start(context);
    const { rows } = await sql`select pg_backend_pid() as pid`.execute(connector.db);
    await control.connect();
    await control.query('select pg_terminate_backend($1)', [rows[0].pid]);
    for (let attempt = 0; attempt < 50 && errors.length === 0; attempt++) await new Promise(resolve => setTimeout(resolve, 10));
    assert.equal(errors.length, 1);
    const result = await sql`select 42 as value`.execute(connector.db);
    assert.equal(result.rows[0].value, 42);
  } finally {
    await control.end();
    await connector.stop(context);
  }
});

test('PostgreSQL builder and raw RETURNING preserve configured native types', { skip: !dsn }, async () => {
  const { types } = await import('pg');
  // Keep microseconds: pg's default timestamptz Date parser has millisecond precision.
  // Configure this on the connector-owned pool, not a process-global type registry.
  const pool = new Pool({ connectionString: dsn, max: 1, types: {
    getTypeParser: (oid, format) => oid === 1184 ? value => value : types.getTypeParser(oid, format)
  } });
  const db = new Kysely({ dialect: new PostgresDialect({ pool, cursor: Cursor, controlClient: Client }) });
  try {
    await sql`create temp table query_type_values (id text primary key, optional text,
      amount numeric(38,18), large bigint, instant timestamptz, uuid uuid, body bytea, document jsonb)`.execute(db);
    let stored = 0;
    for (const raw of [false, true]) for (const fail of [false, true]) {
      const value = { id: `bound-'key-${raw}-${fail}`, optional: null,
        amount: '12345678901234567890.123456789012345678', large: '9223372036854775807',
        instant: '2026-11-19 12:34:56.123456+00', uuid: '12345678-1234-5678-1234-567812345678',
        body: Buffer.from([97, 0, 255]), document: { value: [1, null, 'quoted"'] } };
      const output = [], errors = [], outcomes = [];
      const abort = new AbortController();
      const context = { signal: () => abort.signal };
      const stream = { consumeResult: async (_ctx, result) => output.push(result),
        errorStream: () => ({ emit: async (_ctx, error) => errors.push(error) }),
        runtimeEnvironment: () => ({ metrics: () => ({ enabled: () => false }) }), setQueryConsumer: () => {} };
      const handler = {
        beginRequest: ctx => ({ context: ctx, state: null }),
        buildQuery: () => raw
          ? sql`insert into query_type_values (id,optional,amount,large,instant,uuid,body,document)
              values (${value.id},${value.optional},${value.amount},${value.large},${value.instant},${value.uuid},${value.body},${value.document}) returning *`.compile(db)
          : db.insertInto('query_type_values').values(value).returningAll().compile(),
        handleRow: async (ctx, _state, row, out) => {
          await out.out(ctx, row);
          if (fail) throw new Error('intentional mapper failure');
        },
        endRequest: async (_ctx, _stream, _state, error) => {
          outcomes.push(error);
          assert.equal(pool.totalCount - pool.idleCount, 0, 'lease still held at EndRequest');
        }
      };
      await makeRowsEndpointConsumer(stream, db, handler, 1).consume(context, value);
      assert.deepEqual(output, [value]);
      assert.equal(outcomes.length, 1);
      assert.equal(Boolean(outcomes[0]), fail);
      // Mapping errors are not permission to retry an autocommitted write.
      const rows = await db.selectFrom('query_type_values').selectAll().where('id', '=', value.id).execute();
      assert.deepEqual(rows, [value]);
      stored++;
      const count = await sql`select count(*)::int as count from query_type_values`.execute(db);
      assert.equal(count.rows[0].count, stored, 'write replayed or missing');
    }
  } finally { await db.destroy(); }
});

test('PostgreSQL large Rows waits for slow mapping before fetching another batch', { skip: !dsn, timeout: 15000 }, async () => {
  const pool = new Pool({connectionString: dsn, max: 1});
  const db = new Kysely({dialect: new PostgresDialect({pool, cursor: Cursor, controlClient: Client})});
  const { MessageContext } = await import('../dist/runtime/context.js');
  const context = new MessageContext();
  const batch = 32, total = 10000;
  let reads = 0, mapped = 0, delivered = 0, ends = 0;
  let unblock, entered;
  const blocked = new Promise(resolve => { unblock = resolve; });
  const first = new Promise(resolve => { entered = resolve; });
  const original = Cursor.prototype.read;
  Cursor.prototype.read = function(count, ...args) {
    assert.equal(count, batch);
    reads++;
    return original.call(this, count, ...args);
  };
  const stream = {
    runtimeEnvironment: () => ({metrics: () => ({enabled: () => false})}),
    setQueryConsumer() {}, errorStream: () => ({emit() {}}),
    async consumeResult(_ctx, value) {
      assert.equal(value, delivered + 1);
      delivered++;
      if (delivered === 1) { entered(); await blocked; }
    }
  };
  const handler = {
    beginRequest: ctx => ({context: ctx, state: {}}),
    buildQuery: () => sql`select generate_series(1, ${total}) as value`.compile(db),
    async handleRow(ctx, _state, row, out) { mapped++; await out.out(ctx, row.value); },
    async endRequest(_ctx, _stream, _state, error) {
      ends++;
      assert.equal(error, undefined);
      assert.equal(pool.totalCount - pool.idleCount, 0);
    }
  };
  let pending;
  try {
    const consumer = makeRowsEndpointConsumer(stream, db, handler, batch);
    pending = consumer.consume(context, null);
    await first;
    for (let turn = 0; turn < 5; turn++) await new Promise(resolve => setImmediate(resolve));
    assert.equal(mapped, 1);
    assert.equal(delivered, 1);
    assert.equal(reads, 1, 'cursor prefetched another batch while mapper was blocked');
    assert.equal(ends, 0);
    unblock();
    await pending;
    assert.equal(mapped, total);
    assert.equal(delivered, total);
    assert.equal(ends, 1);
    assert.ok(reads >= Math.ceil(total / batch));
    assert.equal((await sql`select 1 as value`.execute(db)).rows[0].value, 1);
  } finally {
    unblock();
    if (pending) await pending;
    Cursor.prototype.read = original;
    await db.destroy();
  }
});
