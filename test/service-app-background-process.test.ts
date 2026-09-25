import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { test } from "node:test";

for (const kind of ["fifo", "priority", "delay"] as const) {
  for (const brokenLogger of [false, true]) {
    await test(`ServiceApp ${kind} keeps background failure fatal, brokenLogger=${String(brokenLogger)}`, () => {
      const child = spawnSync(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          `
        import { Context, MessageContext, ServiceApp, RuntimeConfig, RuntimeConfigStore,
          NoopMetricsEngine, noopLogger } from '@gorundebug/tsservicelib/runtime';
        const kind = ${JSON.stringify(kind)};
        const config = {
          services: [{ id: 1, name: 'orders', color: '#000000', properties: {},
            environment: 'test', grpcHost: '127.0.0.1', grpcPort: 0,
            httpHost: '127.0.0.1', httpPort: 0, metricsHandler: '/metrics',
            shutdownTimeout: 1000, statusHandler: '/status', startupHandler: '/health/startup',
            readinessHandler: '/health/ready', livenessHandler: '/health/live',
            kubernetesWorkloadType: 'Deployment',
            defaultCallSemantics: kind === 'fifo' ? {taskPool: {poolName: 'workers'}}
              : kind === 'priority' ? {priorityTaskPool: {poolName: 'workers', priority: 0}}
              : {functionCall: {async: false}} }],
          pools: [{name: 'workers', executorsCount: 1, queueCapacity: 0, properties: {}}],
          streams: [], dataConnectors: [], endpoints: [], links: [], modules: [], types: [], properties: {}
        };
        const logger = ${brokenLogger ? "{...noopLogger, error() { throw new Error('logger-failure'); }}" : "noopLogger"};
        const app = new ServiceApp(new RuntimeConfigStore(new RuntimeConfig(config)), 1,
          {logger, metricsEngine: new NoopMetricsEngine()});
        await app.start(Context.background());
        const callback = () => { throw new Error('service-background-original'); };
        const context = new MessageContext();
        if (kind === 'fifo') app.environment().taskPool('workers').addTask(context, callback);
        else if (kind === 'priority') app.environment().priorityTaskPool('workers').addTask(context, 0, callback);
        else app.environment().delay(context, 0, callback);
        setTimeout(() => console.log('unexpected-process-survival'), 50);
      `
        ],
        { encoding: "utf8", timeout: 3000 }
      );
      assert.equal(child.error, undefined);
      assert.equal(child.signal, null);
      assert.equal(child.status, 1, `${child.stdout}\n${child.stderr}`);
      assert.match(child.stderr, /service-background-original/);
      assert.doesNotMatch(child.stdout, /unexpected-process-survival/);
    });
  }
}
