import assert from "node:assert/strict";
import { test } from "node:test";
import { whenIndex, type When } from "@gorundebug/tsservicelib/operators";

function branch(name: string): When {
  return {
    name,
    valueType: () => "boolean",
    whenConsumer: () => { throw new Error("must not inspect downstream consumer"); }
  };
}

await test("named same-typed branches preserve outcomes after reordering", () => {
  for (const names of [["accepted", "terminal"], ["terminal", "accepted"], ["unrelated", "terminal", "accepted"]]) {
    const items = names.map(branch);
    const accepted = whenIndex(items, "accepted");
    const terminal = whenIndex(items, "terminal");
    const select = (value: boolean): number => value ? terminal : accepted;
    assert.equal(items[select(false)]?.name, "accepted");
    assert.equal(items[select(true)]?.name, "terminal");
  }
});

await test("invalid branch bindings fail during construction", () => {
  assert.throws(() => whenIndex([branch("accepted")], "terminal"), /not found/);
  assert.throws(() => whenIndex([branch("terminal"), branch("terminal")], "terminal"), /ambiguous/);
  assert.throws(() => whenIndex([], ""), /must not be empty/);
});
