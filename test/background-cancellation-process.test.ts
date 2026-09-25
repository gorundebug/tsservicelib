import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { test } from "node:test";

for (const kind of ["fifo", "priority", "delay", "detached"] as const) {
  for (const unrelated of [false, true]) {
    await test(`${kind}: cancellation ${unrelated ? "does not hide unrelated failure" : "does not terminate process"}`, () => {
      const source = `
        import { Context, TaskPool, PriorityTaskPool, DelayPool, RuntimeTaskRegistry }
          from '@gorundebug/tsservicelib/runtime';
        const controller = new AbortController();
        const context = Context.background().withExternalCancellation(controller.signal);
        const reason = new Error('ordinary-cancellation');
        const callback = async () => {
          controller.abort(reason);
          throw ${unrelated ? "new Error('unrelated-failure')" : "reason"};
        };
        const kind = ${JSON.stringify(kind)};
        if (kind === 'detached') {
          const tasks = new RuntimeTaskRegistry();
          tasks.admitDetached(callback, controller.signal);
          await tasks.drain();
        } else {
          const pool = kind === 'fifo' ? new TaskPool({name: kind, executorsCount: 1})
            : kind === 'priority' ? new PriorityTaskPool({name: kind, executorsCount: 1})
            : new DelayPool();
          await pool.start(context);
          if (kind === 'delay') pool.delay(context, 0, callback);
          else if (kind === 'priority') pool.addTask(context, 0, callback);
          else pool.addTask(context, callback);
          await pool.stop(Context.background());
        }
        setTimeout(() => console.log('process-survived'), 30);
      `;
      const child = spawnSync(process.execPath, ["--input-type=module", "-e", source], {
        encoding: "utf8",
        timeout: 3000
      });
      assert.equal(child.error, undefined);
      assert.equal(child.signal, null);
      assert.equal(child.status, unrelated ? 1 : 0, `${child.stdout}\n${child.stderr}`);
      if (unrelated) {
        assert.match(child.stderr, /unrelated-failure/);
        assert.doesNotMatch(child.stdout, /process-survived/);
      } else {
        assert.match(child.stdout, /process-survived/);
      }
    });
  }
}
