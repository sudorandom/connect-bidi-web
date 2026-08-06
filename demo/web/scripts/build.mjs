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

// Bundles the demo into demo/web/dist: index.html, style.css, bundle.js.
// The Go demo server (demo/go) serves this directory as static files.

import { build } from "esbuild";
import { cpSync, mkdirSync, rmSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const rootDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const distDir = path.join(rootDir, "dist");

rmSync(distDir, { recursive: true, force: true });
mkdirSync(distDir, { recursive: true });

cpSync(path.join(rootDir, "index.html"), path.join(distDir, "index.html"));
cpSync(path.join(rootDir, "style.css"), path.join(distDir, "style.css"));
cpSync(path.join(rootDir, "favicon.svg"), path.join(distDir, "favicon.svg"));
// Browser/edge cache policy, honored by Workers Assets (and Pages-style hosts).
cpSync(path.join(rootDir, "_headers"), path.join(distDir, "_headers"));
// Static default for the WebTransport capability probe: correct for any
// host that can only serve static files (Cloudflare Workers included).
// The Go demo server overrides this path with {"webtransport":true}.
cpSync(
  path.join(rootDir, "capabilities.json"),
  path.join(distDir, "capabilities.json"),
);

await build({
  entryPoints: [path.join(rootDir, "src/main.ts")],
  outfile: path.join(distDir, "bundle.js"),
  bundle: true,
  format: "esm",
  target: "es2022",
  sourcemap: true,
  minify: true,
  logLevel: "info",
});
