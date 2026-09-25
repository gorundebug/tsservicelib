import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { test } from "node:test";

for (const kind of ["fifo", "priority", "delay", "detached"] as const) {
  for (const failure of ["throw", "reject"] as const) {
    await test(`${kind}: unhandled ${failure} terminates the process`, () => {
      const source = `
        import { Context, TaskPool, PriorityTaskPool, DelayPool, RuntimeTaskRegistry }
          from '@gorundebug/tsservicelib/runtime';
        const context = Context.background();
        const callback = () => {
          const error = new Error('background-failure-original');
          ${failure === "throw" ? "throw error;" : "return Promise.reject(error);"}
        };
        const kind = ${JSON.stringify(kind)};
        if (kind === 'detached') {
          new RuntimeTaskRegistry().admitDetached(callback);
        } else {
          const pool = kind === 'fifo' ? new TaskPool({name: kind, executorsCount: 1})
            : kind === 'priority' ? new PriorityTaskPool({name: kind, executorsCount: 1})
            : new DelayPool();
          await pool.start(context);
          if (kind === 'delay') pool.delay(context, 0, callback);
          else if (kind === 'priority') pool.addTask(context, 0, callback);
          else pool.addTask(context, callback);
        }
        setTimeout(() => console.log('unexpected-process-survival'), 30);
      `;
      const child = spawnSync(process.execPath, ["--input-type=module", "-e", source], {
        encoding: "utf8",
        timeout: 3000
      });
      assert.equal(child.error, undefined);
      assert.equal(child.signal, null);
      assert.equal(child.status, 1, `${child.stdout}\n${child.stderr}`);
      assert.match(child.stderr, /background-failure-original/);
      assert.doesNotMatch(child.stdout, /unexpected-process-survival/);
    });
  }
}

await test("an awaited task failure remains available to its caller without process failure", () => {
  const child = spawnSync(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `import assert from 'node:assert/strict';
       import { RuntimeTaskRegistry } from '@gorundebug/tsservicelib/runtime';
       const tasks = new RuntimeTaskRegistry();
       const error = new Error('handled failure');
       await assert.rejects(tasks.admit(() => Promise.reject(error)), value => value === error);
       await tasks.drain();`
    ],
    { encoding: "utf8", timeout: 3000 }
  );
  assert.equal(child.error, undefined);
  assert.equal(child.status, 0, child.stderr);
});
