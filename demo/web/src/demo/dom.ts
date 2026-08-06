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

/**
 * Looks up a required element by selector, throwing if it is missing.
 *
 * The demo's DOM structure is static markup shipped alongside this bundle,
 * so a missing element indicates a markup/script mismatch rather than a
 * recoverable runtime condition.
 */
export function requireElement<T extends Element>(selector: string): T {
  const element = document.querySelector<T>(selector);
  if (element === null) {
    throw new Error(`missing required element: ${selector}`);
  }
  return element;
}

export type MessageKind = "user" | "eliza" | "system";

/** Appends a chat bubble to a messages list and scrolls it into view. */
export function appendMessage(
  container: HTMLElement,
  kind: MessageKind,
  text: string,
): void {
  const bubble = document.createElement("div");
  bubble.className = `msg-bubble msg-${kind}`;
  bubble.innerText = text;
  container.append(bubble);
  container.scrollTop = container.scrollHeight;
}

/**
 * Reports whether an RPC failure looks like a rejected WebTransport
 * connection, the one failure with a documented self-service fix (the
 * browser's stricter certificate rules for locally-trusted certs).
 */
export function isWebTransportConnectionError(err: unknown): boolean {
  return String(err).includes("WebTransport");
}

/**
 * Appends a system bubble linking to the "Run it yourself" section, which
 * documents the per-browser fix for locally rejected WebTransport
 * connections. Built from DOM nodes because `appendMessage` is text-only
 * by design.
 */
export function appendWebTransportHint(container: HTMLElement): void {
  const bubble = document.createElement("div");
  bubble.className = "msg-bubble msg-system";
  bubble.append(
    "WebTransport connection rejected? On a local server, that is usually " +
      "the browser refusing the demo's locally-trusted certificate — see ",
  );
  const link = document.createElement("a");
  link.href = "#run-it-yourself";
  link.innerText = "Run it yourself";
  bubble.append(link);
  bubble.append(" below for the one-line browser tweak.");
  container.append(bubble);
  container.scrollTop = container.scrollHeight;
}
