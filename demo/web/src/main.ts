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
import { renderBenchmarks } from "./demo/benchmarks.js";
import { createChatView } from "./demo/chat-view.js";
import { requireElement } from "./demo/dom.js";
import { highlightCodeExamples } from "./demo/highlight.js";
import { createStreamsView } from "./demo/streams-view.js";
import { initTabs } from "./demo/tabs.js";
import { createDemoTransport } from "./demo/transport.js";
import type { StreamingTransportChoice } from "./demo/transport.js";
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
  if (choice === "auto") {
    return "Auto (degrading)";
  }
  if (choice === "webtransport") {
    return "WebTransport";
  }
  if (choice === "websocket-draft1" || choice === "websocket-draft5") {
    // Drafts 1 and 5 are always one connection per RPC; the toggle does
    // not apply.
    const draft = choice === "websocket-draft1" ? "1" : "5";
    return `WebSocket Draft ${draft} (connection per RPC)`;
  }
  const draft =
    choice === "websocket-draft4" ? "WebSocket Draft 4" : "WebSocket Draft 3";
  return connectionPerStream
    ? `${draft} (connection per RPC)`
    : `${draft} (multiplexed)`;
}

/** Wires up the live demo: transport/server controls, tabs, and RPC views. */
function main(): void {
  highlightCodeExamples();
  renderBenchmarks();

  // Transport tabs on the code example sections (WebSocket is the default).
  initTabs([
    {
      buttonId: "code-tab-btn-ts-websocket",
      panelId: "code-panel-ts-websocket",
    },
    {
      buttonId: "code-tab-btn-ts-websocket-draft4",
      panelId: "code-panel-ts-websocket-draft4",
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
      buttonId: "code-tab-btn-go-websocket-draft4",
      panelId: "code-panel-go-websocket-draft4",
    },
    {
      buttonId: "code-tab-btn-go-webtransport",
      panelId: "code-panel-go-webtransport",
    },
  ]);

  const transportSelect = requireElement<HTMLSelectElement>(
    "#transport-select",
  );
  const unsupportedBadge = requireElement<HTMLElement>(
    "#transport-unsupported-badge",
  );

  // The demo always talks to its own origin; the server address is not
  // user-configurable.
  const serverUrl = window.location.origin;

  const webTransportOption = transportSelect.querySelector<HTMLOptionElement>(
    'option[value="webtransport"]',
  );
  const webTransportLabel = webTransportOption?.innerText ?? "WebTransport";
  const realitySection = requireElement<HTMLElement>("#webtransport-reality");

  // The /capabilities.json probe's answer; undefined until it lands.
  let serverWebTransport: boolean | undefined;

  function setWebTransportAvailable(available: boolean): void {
    serverWebTransport = available;
    if (webTransportOption !== null) {
      webTransportOption.disabled = !available;
      webTransportOption.innerText = available
        ? webTransportLabel
        : "WebTransport (unavailable here)";
    }
    unsupportedBadge.classList.toggle("hidden", available);
    // The "why is WebTransport not shown?" section (the badge's anchor
    // target) only appears where WebTransport is actually unavailable, e.g.
    // the Cloudflare Workers deployment.
    realitySection.classList.toggle("hidden", available);
    if (!available && transportSelect.value === "webtransport") {
      transportSelect.value = "websocket";
      applyTransportChange();
    } else if (transportSelect.value === "auto") {
      // The Auto ladder was built before the probe answered; rebuild it so
      // a server without WebTransport doesn't cost every RPC a handshake
      // timeout on a rung that can never work.
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
    switch (transportSelect.value) {
      case "auto":
        return "auto";
      case "webtransport":
        return "webtransport";
      case "websocket-draft1":
        return "websocket-draft1";
      case "websocket-draft4":
        return "websocket-draft4";
      case "websocket-draft5":
        return "websocket-draft5";
      default:
        return "websocket-draft3";
    }
  }

  // Connection-per-RPC is a separate toggle that applies to whichever
  // WebSocket draft is selected; WebTransport and Auto ignore it (QUIC
  // streams make the question moot).
  const perStreamCheckbox = requireElement<HTMLInputElement>(
    "#connection-per-stream",
  );

  function connectionPerStream(): boolean {
    return perStreamCheckbox.checked;
  }

  function refreshPerStreamCheckbox(): void {
    const choice = currentChoice();
    perStreamCheckbox.disabled =
      choice === "auto" || choice === "webtransport";
  }

  // Pick a safe initial choice before building any transport: the select
  // may default to WebTransport, and constructing an impossible transport
  // throws synchronously, which would take the whole demo down with it.
  // "auto" needs no such guard — the degrading ladder simply starts at
  // WebSocket when WebTransport is impossible. The badge and dropdown
  // state are reconciled by the refreshWebTransportAvailability() call
  // further down, once the swap machinery it pokes actually exists.
  if (!webTransportPossible() && transportSelect.value === "webtransport") {
    transportSelect.value = "websocket";
  }

  const initial = createDemoTransport(currentChoice(), serverUrl, {
    connectionPerStream: connectionPerStream(),
    serverWebTransport,
  });
  const swappable = new SwappableTransport(initial.transport);
  const client = createClient(ElizaService, swappable);

  const streamsView = createStreamsView(client);
  streamsView.setTransportDescription(initial.description);

  const chatView = createChatView(
    client,
    requireElement<HTMLElement>("#chat-messages"),
    requireElement<HTMLElement>("#chat-input-container"),
  );
  void chatView.start();

  initTabs([
    { buttonId: "tab-btn-streams", panelId: "view-streams" },
    { buttonId: "tab-btn-chat", panelId: "view-chat" },
  ]);

  function applyTransportChange(): void {
    const choice = currentChoice();
    try {
      const next = createDemoTransport(choice, serverUrl, {
        connectionPerStream: connectionPerStream(),
        serverWebTransport,
      });
      // Running lanes hold streams on the old transport; stop them rather
      // than leaving them running against a transport that is no longer
      // selected.
      streamsView.stopStreams();
      swappable.swap(next.transport);
      streamsView.setTransportDescription(next.description);
      chatView.notifyTransportChanged(
        transportLabel(choice, connectionPerStream()),
      );
    } catch (err) {
      console.error("failed to switch transport:", err);
    }
    refreshPerStreamCheckbox();
  }

  transportSelect.addEventListener("change", applyTransportChange);
  perStreamCheckbox.addEventListener("change", applyTransportChange);
  refreshPerStreamCheckbox();

  // The only request the page makes on load. It never invokes the deployed
  // Worker: /capabilities.json is served from static assets (it isn't in
  // wrangler's run_worker_first list). Everything else — the WebSocket
  // connection, the WebTransport session, and every RPC — is dialed lazily,
  // on the visitor's first interaction with the demo.
  void refreshWebTransportAvailability();

}

main();
