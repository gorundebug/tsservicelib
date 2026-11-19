import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createServer } from 'node:https';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { HeadObjectCommand } from '@aws-sdk/client-s3';
import { Connector } from '../dist/dataquery/s3aws/connector.js';

if (process.env.SA_QUERY_TLS_CHILD) {
  const context = { signal: () => new AbortController().signal };
  const connector = new Connector({id: 1, name: 'tls', type: 18, implementation: 'node/aws',
    region: 'us-east-1', forcePathStyle: true, address: process.env.SA_QUERY_TLS_ADDRESS});
  try {
    await connector.start(context);
    await connector.client().send(new HeadObjectCommand({Bucket: 'bucket', Key: 'key'}));
    process.stdout.write('TLS_ACCEPTED');
  } catch (error) {
    process.stdout.write(`TLS_REJECTED:${error.code || error.name}`);
    process.exitCode = 2;
  } finally { await connector.stop(context); }
} else {
  test('S3 native TLS verifies certificates and accepts NODE_EXTRA_CA_CERTS', {timeout: 15000}, async () => {
    const dir = new URL('./fixtures/query-tls/', import.meta.url);
    const server = createServer({key: await readFile(new URL('key.pem', dir)), cert: await readFile(new URL('ca.pem', dir))},
      (_req, res) => { res.setHeader('Content-Length', '0'); res.end(); });
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    try {
      for (const trusted of [false, true]) {
        const env = {...process.env, SA_QUERY_TLS_CHILD: '1', SA_QUERY_TLS_ADDRESS: `https://127.0.0.1:${server.address().port}`,
          AWS_ACCESS_KEY_ID: 'fixture', AWS_SECRET_ACCESS_KEY: 'fixture', AWS_SESSION_TOKEN: '', AWS_PROFILE: '',
          AWS_EC2_METADATA_DISABLED: 'true', AWS_CONFIG_FILE: '/nonexistent-query-config',
          AWS_SHARED_CREDENTIALS_FILE: '/nonexistent-query-credentials'};
        delete env.NODE_TLS_REJECT_UNAUTHORIZED;
        delete env.NODE_EXTRA_CA_CERTS;
        if (trusted) env.NODE_EXTRA_CA_CERTS = fileURLToPath(new URL('ca.pem', dir));
        const child = spawn(process.execPath, [fileURLToPath(import.meta.url)], {env, stdio: ['ignore','pipe','pipe']});
        let output = ''; child.stdout.on('data', chunk => { output += chunk; });
        child.stderr.resume();
        const [code] = await once(child, 'close');
        assert.equal(code, trusted ? 0 : 2, output);
        assert.match(output, trusted ? /TLS_ACCEPTED/ : /TLS_REJECTED:.*CERT/);
      }
    } finally { await new Promise(resolve => server.close(resolve)); }
  });
}
