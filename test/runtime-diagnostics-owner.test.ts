import { execFileSync } from "node:child_process";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

await test("real GC diagnostics use the current lifecycle owner and stop reporting after shutdown", () => {
  execFileSync(process.execPath, ["--expose-gc", "--input-type=module", "-e", `
    import assert from "node:assert/strict";
    import { setTimeout as delay } from "node:timers/promises";
    import { MessageContext, RuntimeTaskRegistry } from "@gorundebug/tsservicelib/runtime";
    import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";
    import { RuntimeDiagnostics } from "./dist/runtime/runtime-diagnostics.js";

    const metrics = new TestMetrics();
    const histogramOwners = [];
    const counterOwners = [];
    const originalScope = metrics.scope.bind(metrics);
    metrics.scope = (...args) => {
      const scope = originalScope(...args);
      const histogram = scope.histogram.bind(scope);
      scope.histogram = (...parameters) => {
        const instrument = histogram(...parameters);
        return { observe(context, value) {
          histogramOwners.push(context);
          instrument.observe(context, value);
        }};
      };
      const counterVec = scope.counterVec.bind(scope);
      scope.counterVec = (...parameters) => {
        const vector = counterVec(...parameters);
        return { with(labels) {
          const counter = vector.with(labels);
          return {
            inc(context) { counterOwners.push(context); counter.inc(context); },
            add(context, value) { counterOwners.push(context); counter.add(context, value); }
          };
        }};
      };
      return scope;
    };
    const diagnostics = new RuntimeDiagnostics(metrics, "gc-owner", new RuntimeTaskRegistry(), [], []);
    globalThis.gc();
    await delay(30);
    assert.equal(histogramOwners.length, 0);
    for (const name of ["first-owner", "second-owner"]) {
      const owner = new MessageContext().withStreamId(name).withSampling(true);
      const before = histogramOwners.length;
      const countersBefore = counterOwners.length;
      await diagnostics.start(owner);
      try {
        await assert.rejects(diagnostics.start(owner), /already started/);
        globalThis.gc();
        const deadline = performance.now() + 2000;
        while (histogramOwners.length === before && performance.now() < deadline) await delay(10);
        assert.ok(histogramOwners.length > before, "no real GC observations received");
        assert.ok(counterOwners.length > countersBefore);
        assert.ok(histogramOwners.slice(before).every(context => context === owner));
        assert.ok(counterOwners.slice(countersBefore).every(context => context === owner));
      } finally {
        await diagnostics.stop(owner);
      }
      const after = histogramOwners.length;
      const countersAfter = counterOwners.length;
      globalThis.gc();
      await delay(30);
      assert.equal(histogramOwners.length, after);
      assert.equal(counterOwners.length, countersAfter);
    }
    assert.ok(metrics.histogramValue("runtime_gc_pause_seconds", { service: "gc-owner" }).count >= 2);
    const disabled = new TestMetrics();
    disabled.enabled = () => false;
    disabled.scope = () => assert.fail("disabled metrics must not create instruments");
    const noop = new RuntimeDiagnostics(disabled, "disabled", new RuntimeTaskRegistry(), [], []);
    const owner = new MessageContext();
    await noop.start(owner);
    await noop.stop(owner);
  `], {
    cwd: fileURLToPath(new URL("../../", import.meta.url)),
    timeout: 10_000,
    stdio: "pipe"
  });
});
