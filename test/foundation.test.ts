import assert from "node:assert/strict";
import { test } from "node:test";

import * as serviceLib from "@gorundebug/tsservicelib";

await test("public root module loads as standards-based ESM", () => {
  assert.equal(Object.getPrototypeOf(serviceLib), null);
});
