import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { test } from "node:test";

const environmentModule = new URL("./support/environment.js", import.meta.url).href;

for (const expiry of ["ttl", "deadline"] as const) {
  for (const failure of ["throw", "reject"] as const) {
    await test(`join storage ${expiry}: unhandled ${failure} terminates the process`, () => {
      const source = `
        import { Context, MessageContext, HashMapJoinStorage }
          from '@gorundebug/tsservicelib/runtime';
        import { makeTestEnvironment } from ${JSON.stringify(environmentModule)};
        const storage = new HashMapJoinStorage(makeTestEnvironment([]), {
          ttlMs: () => ${expiry === "ttl" ? "10" : "60_000"},
          renewTTL: () => false,
          name: () => 'expiry-failure'
        });
        storage.start(Context.background());
        const context = new MessageContext()${expiry === "deadline" ? ".bounded(10)" : ""};
        let calls = 0;
        await storage.joinValue(context, 'key', 0, 'value', () => {
          if (++calls === 1) return false;
          const error = new Error('join-expiry-original-failure');
          ${failure === "throw" ? "throw error;" : "return Promise.reject(error);"}
        });
        setTimeout(() => console.log('unexpected-process-survival'), 100);
      `;
      const child = spawnSync(process.execPath, ["--input-type=module", "-e", source], {
        encoding: "utf8",
        timeout: 3000
      });
      assert.equal(child.error, undefined);
      assert.equal(child.signal, null);
      assert.equal(child.status, 1, `${child.stdout}\n${child.stderr}`);
      assert.match(child.stderr, /join-expiry-original-failure/);
      assert.doesNotMatch(child.stdout, /unexpected-process-survival/);
    });
  }
}
