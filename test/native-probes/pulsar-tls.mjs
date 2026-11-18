import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";
import https from "node:https";
import http from "node:http";
import { once } from "node:events";
export async function certificates() {
  const dir = await mkdtemp(join(tmpdir(), "pulsar-tls-"));
  const ca = join(dir, "ca.pem"),
    key = join(dir, "key.pem"),
    cert = join(dir, "cert.pem"),
    wrong = join(dir, "wrong.pem"),
    wrongKey = join(dir, "wrong-key.pem");
  const run = (args) => execFileSync("openssl", args, { stdio: "ignore" });
  run([
    "req",
    "-x509",
    "-newkey",
    "rsa:2048",
    "-nodes",
    "-keyout",
    key,
    "-out",
    ca,
    "-days",
    "1",
    "-subj",
    "/CN=localhost",
    "-addext",
    "subjectAltName=DNS:localhost"
  ]);
  // Test self-signed certificate is both trusted root and endpoint identity.
  await import("node:fs/promises").then((fs) => fs.copyFile(ca, cert));
  run([
    "req",
    "-x509",
    "-newkey",
    "rsa:2048",
    "-nodes",
    "-keyout",
    wrongKey,
    "-out",
    wrong,
    "-days",
    "1",
    "-subj",
    "/CN=wrong.invalid",
    "-addext",
    "subjectAltName=DNS:wrong.invalid"
  ]);
  const bundle = join(dir, "bundle.pem");
  await writeFile(bundle, Buffer.concat([await readFile(ca), await readFile(wrong)]));
  return {
    ca,
    key,
    cert,
    wrong,
    wrongKey,
    bundle,
    async options(mtls = false, bad = false) {
      return {
        key: await readFile(bad ? wrongKey : key),
        cert: await readFile(bad ? wrong : cert),
        ca: await readFile(ca),
        requestCert: mtls,
        rejectUnauthorized: mtls
      };
    },
    close: () => rm(dir, { recursive: true, force: true })
  };
}
export async function adminProxy(remote, ssl) {
  const sockets = new Set();
  const server = https.createServer(ssl, (req, res) => {
    const upstream = http.request(
      new URL(req.url, remote),
      { method: req.method, headers: req.headers },
      (response) => {
        res.writeHead(response.statusCode, response.headers);
        response.pipe(res);
      }
    );
    upstream.on("error", () => res.destroy());
    req.pipe(upstream);
  });
  server.on("tlsClientError", () => {});
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return {
    url: `https://localhost:${server.address().port}`,
    async close() {
      for (const s of sockets) s.destroy();
      await new Promise((resolve) => server.close(resolve));
    }
  };
}
