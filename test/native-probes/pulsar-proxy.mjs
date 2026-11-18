// Test-only transparent frame proxy. Only lookup addresses change; receipts may be withheld.
import net from "node:net";
import tls from "node:tls";
import { once } from "node:events";
function number(data, start) {
  let value = 0n,
    shift = 0n,
    i = start;
  for (; i < data.length; i++) {
    const b = data[i];
    value |= BigInt(b & 127) << shift;
    if (b < 128) return [value, i + 1];
    shift += 7n;
  }
  throw Error("truncated protobuf");
}
function varint(value) {
  let n = BigInt(value);
  const bytes = [];
  while (n > 127n) {
    bytes.push(Number(n & 127n) | 128);
    n >>= 7n;
  }
  bytes.push(Number(n));
  return Buffer.from(bytes);
}
function fields(data) {
  const out = [];
  let i = 0;
  while (i < data.length) {
    let tag;
    [tag, i] = number(data, i);
    const wire = Number(tag & 7n),
      key = Number(tag >> 3n);
    let value;
    if (wire === 0) [value, i] = number(data, i);
    else {
      let size;
      if (wire === 2) {
        let length;
        [length, i] = number(data, i);
        size = Number(length);
      } else if (wire === 1) size = 8;
      else if (wire === 5) size = 4;
      else throw Error("unsupported protobuf");
      value = data.subarray(i, i + size);
      i += size;
    }
    out.push([key, wire, value]);
  }
  return out;
}
function encode(items) {
  return Buffer.concat(
    items.flatMap(([key, wire, value]) => [
      varint(key * 8 + wire),
      ...(wire === 0 ? [varint(value)] : wire === 2 ? [varint(value.length), value] : [value])
    ])
  );
}
function size(n) {
  const b = Buffer.alloc(4);
  b.writeUInt32BE(n);
  return b;
}
export class PulsarProxy {
  sockets = new Set();
  sends = 0;
  dropReceipts = false;
  errors = [];
  constructor(remote, ssl) {
    this.remote = new URL(remote);
    this.ssl = ssl;
  }
  async start() {
    this.server = this.ssl
      ? tls.createServer(this.ssl, (s) => this.accept(s))
      : net.createServer((s) => this.accept(s));
    this.server.on("tlsClientError", () => {});
    this.server.listen(0, "127.0.0.1");
    await once(this.server, "listening");
    this.url = `${this.ssl ? "pulsar+ssl" : "pulsar"}://localhost:${this.server.address().port}`;
    return this;
  }
  accept(socket) {
    const upstream = net.connect(Number(this.remote.port || 6650), this.remote.hostname);
    for (const s of [socket, upstream]) {
      this.sockets.add(s);
      s.on("error", () => {});
      s.on("close", () => {
        this.sockets.delete(s);
        socket.destroy();
        upstream.destroy();
      });
    }
    this.forward(socket, upstream, false);
    this.forward(upstream, socket, true);
  }
  async forward(from, to, response) {
    try {
      let buffered = Buffer.alloc(0);
      for await (const chunk of from) {
        buffered = Buffer.concat([buffered, chunk]);
        while (buffered.length >= 4) {
          const length = buffered.readUInt32BE(0);
          if (length > 16 * 1024 * 1024) throw Error("frame limit");
          if (buffered.length < length + 4) break;
          let frame = buffered.subarray(4, length + 4);
          buffered = buffered.subarray(length + 4);
          const commandLength = frame.readUInt32BE(0),
            command = fields(frame.subarray(4, 4 + commandLength)),
            kind = command.find(([k]) => k === 1)?.[2];
          if (!response && kind === 6n) this.sends++;
          if (response && kind === 7n && this.dropReceipts) continue;
          if (response && kind === 24n) {
            const raw = encode(
              command.map(([k, w, v]) => [
                k,
                w,
                k === 24
                  ? encode([
                      ...fields(v).filter(([key]) => ![1, 2, 8].includes(key)),
                      [1, 2, Buffer.from(this.url)],
                      [2, 2, Buffer.from(this.url)],
                      [8, 0, 1n]
                    ])
                  : v
              ])
            );
            frame = Buffer.concat([size(raw.length), raw, frame.subarray(4 + commandLength)]);
          }
          if (!to.write(Buffer.concat([size(frame.length), frame]))) await once(to, "drain");
        }
      }
    } catch (error) {
      if (!["ECONNRESET", "ERR_STREAM_PREMATURE_CLOSE"].includes(error.code))
        this.errors.push(error);
    } finally {
      from.destroy();
      to.destroy();
    }
  }
  disconnect() {
    for (const s of this.sockets) s.destroy();
  }
  async close() {
    this.disconnect();
    await new Promise((resolve) => this.server.close(resolve));
  }
}
