import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Connector, makeConnector } from '../dist/dataquery/s3aws/index.js';
import { requireS3AWSDataConnectorConfig, requireQueryEndpointConfig } from '../dist/runtime/config/query.js';

const config = { id: 91, name: 'objects', type: 18, implementation: 'node/aws', region: 'us-east-1', forcePathStyle: true, properties: {} };
const context = { signal: () => new AbortController().signal };
test('managed S3 connector owns a single native client and rejects use outside its lifecycle', async () => {
  const entries = new Map();
  const environment = { managedDataConnectorById: id => entries.get(id), addManagedDataConnector: connector => entries.set(connector.id, connector) };
  const connector = makeConnector(environment, config);
  assert.equal(makeConnector(environment, config), connector);
  assert.throws(() => connector.client(), /not started/);
  await assert.rejects(connector.start({ signal: () => AbortSignal.abort() }));
  assert.throws(() => connector.client(), /not started/);
  await connector.start(context);
  const client = connector.client();
  assert.equal(await client.config.maxAttempts(), 1);
  await connector.start(context);
  assert.equal(connector.client(), client);
  let destroyed = 0;
  const destroy = client.destroy.bind(client);
  client.destroy = () => { destroyed++; destroy(); };
  await connector.stop(context);
  await connector.stop(context);
  assert.equal(destroyed, 1);
  assert.throws(() => connector.client(), /not started/);
  await connector.start(context);
  assert.notEqual(connector.client(), client);
  await connector.stop(context);
});
test('S3 config rejects credential URLs without exposing them and validates operation families', () => {
  assert.throws(() => new Connector({ ...config, address: 'https://private:secret@example.com' }), error => !error.message.includes('secret') && /endpoint/.test(error.message));
  assert.throws(() => requireS3AWSDataConnectorConfig({ ...config, region: '' }));
  assert.throws(() => requireS3AWSDataConnectorConfig({ ...config, implementation: 'node/kysely' }));
  const endpoint = { id: 92, name: 'query', enabled: true, fetchSize: 256 };
  for (const queryOperation of ['GetObject', 'HeadObject', 'ListObjects', 'PutObject', 'DeleteObject']) {
    assert.equal(requireQueryEndpointConfig({ ...endpoint, queryOperation }, 18).queryOperation, queryOperation);
    assert.throws(() => requireQueryEndpointConfig({ ...endpoint, queryOperation }, 13));
  }
  assert.throws(() => requireQueryEndpointConfig({ ...endpoint, queryOperation: 'Rows' }, 18));
});

test('S3 canonical configuration retains endpoint settings without treating them as custom properties', async () => {
  const { parseCanonicalConfig, RuntimeConfig } = await import('../dist/runtime/config/index.js');
  const parsed = parseCanonicalConfig({
    dataConnectors: { objects: { ...config, properties: undefined, address: 'http://127.0.0.1:9000', customSetting: 'keep' } },
    endpoints: { list: { id: 92, name: 'list', idDataConnector: config.id, queryOperation: 'ListObjects', bucket: 'catalog', enabled: false } }
  });
  new RuntimeConfig(parsed);
  assert.equal(parsed.dataConnectors[0].properties.region, undefined);
  assert.equal(parsed.dataConnectors[0].properties.forcePathStyle, undefined);
  assert.equal(parsed.dataConnectors[0].properties.customSetting, 'keep');
  assert.equal(parsed.endpoints[0].enabled, false);
  assert.equal(parsed.endpoints[0].bucket, 'catalog');
  assert.equal(parsed.endpoints[0].properties.bucket, undefined);
  assert.throws(() => new RuntimeConfig({ ...parsed, endpoints: [{ ...parsed.endpoints[0], queryOperation: 'Rows' }] }), /operation/);
});

test('runtime graph includes live Query endpoints and omits runtime connection addresses', async () => {
  const { RuntimeConfig, parseCanonicalConfig } = await import('../dist/runtime/config/index.js');
  const { runtimeToCanonicalConfig } = await import('../dist/runtime/status/graph.js');
  const parsed = parseCanonicalConfig({
    dataConnectors: {
      objects: { ...config, address: 'http://private-s3:9000' },
      database: { id: 93, name: 'database', type: 13, implementation: 'node/kysely', connectionString: 'postgres://user:secret@database/app', connectionsCount: 2 }
    },
    endpoints: {
      objects: { id: 92, name: 'objects', idDataConnector: 91, queryOperation: 'GetObject', bucket: 'catalog' },
      database: { id: 94, name: 'database', idDataConnector: 93, queryOperation: 'Rows' }
    }
  });
  const runtimeConfig = new RuntimeConfig(parsed);
  const streams = [92, 94].map((id, index) => ({ id: index + 1, config: () => ({ id: index + 1, type: 'Query', idEndpoint: id }) }));
  const environment = { runtimeConfig: () => runtimeConfig, runtimeStreams: () => streams, streamById: () => undefined, dataSources: () => [], dataSinks: () => [] };
  const graph = runtimeToCanonicalConfig(environment);
  assert.deepEqual(graph.endpoints.map(e => e.id), [92, 94]);
  assert.deepEqual(graph.dataConnectors.map(c => c.id), [91, 93]);
  assert.equal(graph.dataConnectors[0].region, 'us-east-1');
  assert.equal(graph.dataConnectors[0].address, undefined);
  assert.equal(graph.dataConnectors[1].connectionString, undefined);
  assert.ok(!JSON.stringify(graph).includes('secret'));
  assert.ok(!JSON.stringify(graph).includes('private-s3'));
  assert.equal(runtimeConfig.dataConnectorById(93).connectionString, 'postgres://user:secret@database/app');
});
