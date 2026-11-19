import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Pool } from 'pg';
import { S3Client } from '@aws-sdk/client-s3';
import { Connector as Postgres } from '../dist/dataquery/postgresqlkysely/connector.js';
import { Connector as S3 } from '../dist/dataquery/s3aws/connector.js';
const context = { signal: () => new AbortController().signal };
const config = { id: 91, name: 'objects', type: 18, implementation: 'node/aws', region: 'us-east-1', forcePathStyle: true, properties: {} };
test('maker pool remains configured and is closed even without a first query', async () => {
  const pool = new Pool({ max: 7 });
  const connector = new Postgres({ id: 92, name: 'db', type: 13, implementation: 'node/kysely', connectionString: 'postgres://localhost/test', connectionsCount: 2, connectTimeoutMs: 0, operationTimeoutMs: 0, properties: {} }, undefined, pool);
  assert.equal(pool.options.max, 7);
  await connector.stop(context);
  assert.equal(pool.ended, true);
});
test('maker S3 client is retained and cleaned; cancellation during factory cleans result', async () => {
  const controller = new AbortController();
  let destroyed = 0;
  const native = new S3Client({ region: 'eu-west-1' });
  const destroy = native.destroy.bind(native);
  native.destroy = () => { destroyed++; destroy(); };
  const connector = new S3(config, ctx => {
    assert.equal(ctx, context);
    return native;
  });
  await connector.start(context);
  assert.equal(connector.client(), native);
  assert.equal(await native.config.region(), 'eu-west-1');
  await connector.stop(context);
  await connector.stop(context);
  assert.equal(destroyed, 1);
  const cancelled = new S3(config, () => { controller.abort(); return native; });
  await assert.rejects(cancelled.start({ signal: () => controller.signal }));
  assert.equal(destroyed, 2);
  assert.throws(() => cancelled.client(), /not started/);
});
