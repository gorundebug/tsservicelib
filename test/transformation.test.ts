import assert from "node:assert/strict";
import { test } from "node:test";

import * as operators from "@gorundebug/tsservicelib/operators";
import * as transformation from "@gorundebug/tsservicelib/transformation";

await test("transformation exposes every canonical operator maker", () => {
  const makers = [
    "makeMapStream",
    "makeFilterStream",
    "makeFlatMapStream",
    "makeFlatMapIterableStream",
    "makeProcessStream",
    "makeInputStream",
    "makeInputKVStream",
    "makeJoinStream",
    "makeKeyByStream",
    "makeLinkStream",
    "makeMergeStream",
    "makeMultiJoinStream",
    "makeMultiJoinLink",
    "makeWhenStream",
    "makeCaseStream",
    "makeSinkStream",
    "makeSinkStreamWithResult",
    "makeSplitStream",
    "makeDelayStream"
  ] as const;

  for (const maker of makers) {
    assert.equal(transformation[maker], operators[maker], maker);
  }
});
