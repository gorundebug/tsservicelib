import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const timeoutMs = Number.parseInt(process.env["LIFECYCLE_PROCESS_TIMEOUT_MS"] ?? "15000", 10);
if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1) {
  throw new RangeError("LIFECYCLE_PROCESS_TIMEOUT_MS must be a positive safe integer");
}

const matrices = [
  ["runtime", "service-runtime.test.js"],
  ["http-source", "http-source.test.js"],
  ["http-sink", "http-sink.test.js"],
  ["http-body", "http-body.test.js"],
  ["grpc-source", "grpc-source.test.js"],
  ["grpc-sink", "grpc-sink.test.js"],
  ["grpc-streaming-source", "grpc-streaming-source.test.js"],
  ["grpc-streaming-sink", "grpc-streaming-sink.test.js"],
  ["kafka-source", "kafka-source.test.js"],
  ["kafka-sink", "kafka-sink.test.js"],
  ["custom-source", "custom-source.test.js"],
  ["custom-sink", "custom-sink.test.js"]
];

for (const [name, file] of matrices) {
  await runMatrix(name, `dist-test/test/${file}`);
  process.stdout.write(`[lifecycle-process] PASS ${name}\n`);
}

function runMatrix(name, file) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["--test", "--enable-source-maps", file], {
      cwd: root,
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(
        new Error(
          `[lifecycle-process] ${name} did not exit within ${String(timeoutMs)}ms\n${stdout}${stderr}`
        )
      );
    }, timeoutMs);
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("exit", (code, signal) => {
      clearTimeout(timer);
      if (code === 0 && signal === null) {
        resolve();
        return;
      }
      reject(
        new Error(
          `[lifecycle-process] ${name} exited with code=${String(code)} signal=${String(signal)}\n${stdout}${stderr}`
        )
      );
    });
  });
}
