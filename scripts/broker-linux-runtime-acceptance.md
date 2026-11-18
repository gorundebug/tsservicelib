# Packed SDK Linux native-load acceptance

`check-broker-linux-runtime.mjs` is copied into an unpacked SDK archive, so package
self-resolution must select that archive's `dist` rather than a source checkout.
It checks all eight broker exports, exact pinned direct-client versions, Linux
and the package's required Node version, then constructs and closes the actual
Pulsar native client. No broker connection is attempted.

Recorded 2026-11-18: SDK 0.2.181, Node 24.19.0, Linux ARM64, glibc 2.36.
All checks pass; `/private/tmp/sa-ts-packed-linux-runtime.log` contains the report.
Archive `/private/tmp/sa-broker-sdk-packages/gorundebug-tsservicelib-0.2.181.tgz`
SHA256: `ae87cf1d36232da9d39951a2c88c3082949a19a38faceb622560921246092b33`.
Current sources were rebuilt before packing (`sa-ts-linux-build.log`,
`sa-ts-linux-pack.log` in the same temporary root).

From the SDK repository, first run `corepack pnpm build` and
`corepack pnpm pack --pack-destination /private/tmp/sa-broker-sdk-packages`.
With the previously built local Linux acceptance image:

```sh
docker run --rm --network none \
  -v /private/tmp/sa-broker-sdk-packages:/artifacts:ro \
  -v "$PWD/scripts/check-broker-linux-runtime.mjs:/probe.mjs:ro" \
  --entrypoint sh sa-pulsar-typescript-auth:arm64 -c '
  mkdir /tmp/sdk-check &&
  tar xzf /artifacts/gorundebug-tsservicelib-0.2.181.tgz -C /tmp/sdk-check &&
  ln -s /app/.tsservicelib/node_modules /tmp/sdk-check/package/node_modules &&
  cp /probe.mjs /tmp/sdk-check/package/check-broker-linux-runtime.mjs &&
  node /tmp/sdk-check/package/check-broker-linux-runtime.mjs'
```

The disposable container reuses the image's installed Linux dependencies; it
does not reuse its older SDK implementation. This proves archive export/native
load compatibility on this platform, not clean dependency installation, AMD64,
musl, every transitive dependency identity or live broker behavior. The separate
packed-consumer check validates registry identities against the source lockfile.
Neither check publishes an artifact or alters a running service.
