// Copyright 2021-2026 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Watches and serves the demo for local iteration on the landing page and
// UI. This dev server is plain HTTP, so WebTransport (which requires a
// secure context) won't work through it -- run the Go server in demo/go
// against a built `dist/` for full end-to-end testing.

import { context } from "esbuild";
import { cpSync, mkdirSync, rmSync, watch } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const rootDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const distDir = path.join(rootDir, "dist");

function copyStatic() {
  mkdirSync(distDir, { recursive: true });
  cpSync(path.join(rootDir, "index.html"), path.join(distDir, "index.html"));
  cpSync(path.join(rootDir, "style.css"), path.join(distDir, "style.css"));
  cpSync(path.join(rootDir, "favicon.svg"), path.join(distDir, "favicon.svg"));
}

rmSync(distDir, { recursive: true, force: true });
copyStatic();
watch(path.join(rootDir, "index.html"), copyStatic);
watch(path.join(rootDir, "style.css"), copyStatic);
watch(path.join(rootDir, "favicon.svg"), copyStatic);

const ctx = await context({
  entryPoints: [path.join(rootDir, "src/main.ts")],
  outfile: path.join(distDir, "bundle.js"),
  bundle: true,
  format: "esm",
  target: "es2022",
  sourcemap: true,
  logLevel: "info",
});

await ctx.watch();
const { host, port } = await ctx.serve({ servedir: distDir, port: 8081 });

console.log(`Demo dev server watching for changes: http://${host}:${port}`);
console.log(
  "The Connect API server (unary/WebSocket/WebTransport RPCs) runs " +
    "separately -- see demo/go (https://localhost:4433 by default).",
);
