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

import { createClient } from "@connectrpc/connect";
import { createChatView } from "./demo/chat-view.js";
import { requireElement } from "./demo/dom.js";
import { highlightCodeExamples } from "./demo/highlight.js";
import {
  isValidServerUrl,
  resolveServerUrl,
  saveServerUrl,
} from "./demo/settings.js";
import { createStreamsView } from "./demo/streams-view.js";
import { initTabs } from "./demo/tabs.js";
import { createDemoTransport } from "./demo/transport.js";
import type { StreamingTransportChoice } from "./demo/transport.js";
import { initUnaryView } from "./demo/unary-view.js";
import {
  isWebTransportSupported,
  probeServerWebTransport,
} from "./demo/webtransport-support.js";
import { SwappableTransport } from "./demo/swappable-transport.js";
import { ElizaService } from "./gen/connectbidi/eliza/v1/eliza_pb.js";

function transportLabel(
  choice: StreamingTransportChoice,
  connectionPerStream: boolean,
): string {
  if (choice === "webtransport") {
    return "WebTransport";
  }
  return connectionPerStream
    ? "WebSocket (connection per RPC)"
    : "WebSocket (multiplexed)";
}

/** Wires up the live demo: transport/server controls, tabs, and RPC views. */
function main(): void {
  highlightCodeExamples();

  // Transport tabs on the code example sections (WebSocket is the default).
  initTabs([
    {
      buttonId: "code-tab-btn-ts-websocket",
      panelId: "code-panel-ts-websocket",
    },
    {
      buttonId: "code-tab-btn-ts-webtransport",
      panelId: "code-panel-ts-webtransport",
    },
  ]);
  initTabs([
    {
      buttonId: "code-tab-btn-go-websocket",
      panelId: "code-panel-go-websocket",
    },
    {
      buttonId: "code-tab-btn-go-webtransport",
      panelId: "code-panel-go-webtransport",
    },
  ]);

  const serverInput = requireElement<HTMLInputElement>("#server-url-input");
  const serverError = requireElement<HTMLElement>("#server-url-error");
  const transportSelect = requireElement<HTMLSelectElement>(
    "#transport-select",
  );
  const unsupportedBadge = requireElement<HTMLElement>(
    "#transport-unsupported-badge",
  );
  const muxToggleRow = requireElement<HTMLElement>("#mux-toggle-row");
  const muxToggleInput = requireElement<HTMLInputElement>(
    "#mux-toggle-input",
  );

  let serverUrl = resolveServerUrl();
  serverInput.value = serverUrl;

  const webTransportOption = transportSelect.querySelector<HTMLOptionElement>(
    'option[value="webtransport"]',
  );
  const webTransportLabel = webTransportOption?.innerText ?? "WebTransport";
  const realitySection = requireElement<HTMLElement>("#webtransport-reality");

  function setWebTransportAvailable(available: boolean): void {
    if (webTransportOption !== null) {
      webTransportOption.disabled = !available;
      webTransportOption.innerText = available
        ? webTransportLabel
        : `${webTransportLabel} — not supported here`;
    }
    unsupportedBadge.classList.toggle("hidden", available);
    // The "why not supported here?" section (the badge's anchor target)
    // only appears where WebTransport is actually unavailable — e.g. the
    // Cloudflare Workers deployment.
    realitySection.classList.toggle("hidden", available);
    if (!available && transportSelect.value === "webtransport") {
      transportSelect.value = "websocket";
      applyTransportChange();
    }
  }

  /**
   * Whether WebTransport can possibly work with the current browser and
   * server URL, without asking the server. The constructor itself throws
   * synchronously on non-https URLs (e.g. `wrangler dev` on
   * http://localhost:8787), so this must be checked before ever building
   * a WebTransport-backed transport.
   */
  function webTransportPossible(): boolean {
    return isWebTransportSupported() && serverUrl.startsWith("https:");
  }

  /**
   * WebTransport needs support on both ends: the API in this browser, and
   * an HTTP/3 endpoint on the selected server (a Cloudflare Workers
   * deployment, for example, only offers WebSocket).
   */
  async function refreshWebTransportAvailability(): Promise<void> {
    if (!webTransportPossible()) {
      setWebTransportAvailable(false);
      return;
    }
    setWebTransportAvailable(await probeServerWebTransport(serverUrl));
  }

  function currentChoice(): StreamingTransportChoice {
    return transportSelect.value === "webtransport"
      ? "webtransport"
      : "websocket";
  }

  function connectionPerStream(): boolean {
    return !muxToggleInput.checked;
  }

  // The multiplexing toggle only means something on WebSocket; QUIC
  // streams make the question moot on WebTransport.
  function syncMuxToggleVisibility(): void {
    muxToggleRow.classList.toggle("hidden", currentChoice() !== "websocket");
  }

  // Pick a safe initial choice before building any transport: the select
  // may default to WebTransport, and constructing an impossible transport
  // throws synchronously, which would take the whole demo down with it.
  // The badge and dropdown state are reconciled by the
  // refreshWebTransportAvailability() call further down, once the swap
  // machinery it pokes actually exists.
  if (!webTransportPossible()) {
    transportSelect.value = "websocket";
  }

  const initial = createDemoTransport(currentChoice(), serverUrl, {
    connectionPerStream: connectionPerStream(),
  });
  syncMuxToggleVisibility();
  const swappable = new SwappableTransport(initial.transport);
  const client = createClient(ElizaService, swappable);

  const streamsView = createStreamsView(client);
  streamsView.setTransportDescription(initial.description);

  initUnaryView(client);

  const chatView = createChatView(
    client,
    requireElement<HTMLElement>("#chat-messages"),
    requireElement<HTMLElement>("#chat-input-container"),
  );
  void chatView.start();

  initTabs([
    { buttonId: "tab-btn-unary", panelId: "view-unary" },
    { buttonId: "tab-btn-chat", panelId: "view-chat" },
    { buttonId: "tab-btn-streams", panelId: "view-streams" },
  ]);

  function setServerInputInvalid(isInvalid: boolean): void {
    serverInput.classList.toggle("invalid", isInvalid);
    serverInput.setAttribute("aria-invalid", String(isInvalid));
    serverError.classList.toggle("hidden", !isInvalid);
  }

  function applyTransportChange(): void {
    const choice = currentChoice();
    syncMuxToggleVisibility();
    try {
      const next = createDemoTransport(choice, serverUrl, {
        connectionPerStream: connectionPerStream(),
      });
      swappable.swap(next.transport);
      streamsView.setTransportDescription(next.description);
      chatView.notifyTransportChanged(
        transportLabel(choice, connectionPerStream()),
      );
    } catch (err) {
      console.error("failed to switch transport:", err);
      setServerInputInvalid(true);
    }
  }

  transportSelect.addEventListener("change", applyTransportChange);
  muxToggleInput.addEventListener("change", applyTransportChange);

  void refreshWebTransportAvailability();

  serverInput.addEventListener("change", () => {
    const nextUrl = serverInput.value.trim();
    if (nextUrl.length === 0 || nextUrl === serverUrl) {
      setServerInputInvalid(false);
      return;
    }
    if (!isValidServerUrl(nextUrl)) {
      setServerInputInvalid(true);
      return;
    }
    setServerInputInvalid(false);
    serverUrl = nextUrl;
    saveServerUrl(serverUrl);
    applyTransportChange();
    void refreshWebTransportAvailability();
  });
}

main();
