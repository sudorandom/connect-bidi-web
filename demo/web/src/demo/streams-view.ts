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

import type { Client } from "@connectrpc/connect";
import type { ElizaService } from "../gen/connectbidi/eliza/v1/eliza_pb.js";
import { isWebTransportConnectionError, requireElement } from "./dom.js";

const LANE_LABELS: readonly string[] = [
  "Stream Lane #1 ⚡",
  "Stream Lane #2 🌊",
  "Stream Lane #3 🚀",
  "Stream Lane #4 🔮",
];

const MESSAGES_PER_LANE = 20;

interface Lane {
  id: number;
  label: string;
  txCount: number;
  rxCount: number;
  abortController: AbortController | undefined;
}

export interface StreamsView {
  /** Updates the blurb describing how the current transport carries lanes. */
  setTransportDescription(text: string): void;
}

/**
 * Creates the "Concurrent RPC Streams" tab: four independent `Converse`
 * bidi-streaming RPCs run at once, each on its own lane, to show that the
 * bidi transports multiplex multiple streams concurrently.
 */
export function createStreamsView(
  client: Client<typeof ElizaService>,
): StreamsView {
  const lanesContainer = requireElement<HTMLElement>("#lanes-container");
  const toggleButton = requireElement<HTMLButtonElement>(
    "#toggle-streams-btn",
  );
  const descriptionEl = requireElement<HTMLElement>(
    "#lane-transport-description",
  );
  const errorHint = requireElement<HTMLElement>("#lane-error-hint");

  const lanes: Lane[] = LANE_LABELS.map((label, index) => ({
    id: index + 1,
    label,
    txCount: 0,
    rxCount: 0,
    abortController: undefined,
  }));

  let running = false;
  renderLanes();

  toggleButton.addEventListener("click", () => {
    if (running) {
      stopAll();
    } else {
      startAll();
    }
  });

  function renderLanes(): void {
    lanesContainer.replaceChildren();
    for (const lane of lanes) {
      lanesContainer.append(renderLaneCard(lane));
    }
  }

  function renderLaneCard(lane: Lane): HTMLElement {
    const card = document.createElement("div");
    card.id = `lane-card-${lane.id}`;
    card.className = "lane-card";
    card.innerHTML = `
      <div class="lane-header">
        <div class="lane-title">
          <span>${lane.label}</span>
          <span id="lane-status-${lane.id}" class="status-badge">IDLE</span>
        </div>
        <div class="lane-stats">
          <span class="stat-tag">TX sent: <strong id="lane-tx-${lane.id}">0</strong></span>
          <span class="stat-tag">RX echo: <strong id="lane-rx-${lane.id}">0</strong></span>
        </div>
      </div>
      <div class="lane-tracks-wrapper">
        <div class="track-row">
          <span class="row-label label-tx">TX &#8594;</span>
          <div id="lane-track-tx-${lane.id}" class="lane-track"></div>
        </div>
        <div class="track-row">
          <span class="row-label label-rx">&#8592; RX</span>
          <div id="lane-track-rx-${lane.id}" class="lane-track"></div>
        </div>
      </div>
    `;
    return card;
  }

  function setLaneState(
    lane: Lane,
    state: "running" | "done" | "error",
  ): void {
    const card = document.querySelector<HTMLElement>(
      `#lane-card-${lane.id}`,
    );
    const status = document.querySelector<HTMLElement>(
      `#lane-status-${lane.id}`,
    );
    card?.classList.toggle("running", state === "running");
    status?.classList.toggle("active", state === "running");
    status?.classList.toggle("error", state === "error");
    if (status !== null) {
      status.innerText =
        state === "running"
          ? "STREAM ACTIVE"
          : state === "error"
            ? "ERROR"
            : "COMPLETED";
    }
  }

  function updateLaneStat(lane: Lane, kind: "tx" | "rx"): void {
    const el = document.querySelector<HTMLElement>(
      `#lane-${kind}-${lane.id}`,
    );
    if (el !== null) {
      el.innerText = String(kind === "tx" ? lane.txCount : lane.rxCount);
    }
  }

  function pulse(lane: Lane, kind: "tx" | "rx", value: number): void {
    const track = document.querySelector<HTMLElement>(
      `#lane-track-${kind}-${lane.id}`,
    );
    if (track === null) {
      return;
    }
    const node = document.createElement("div");
    node.className = `pulse-node pulse-${kind}`;
    node.innerText = `#${value}`;
    track.append(node);
    setTimeout(() => node.remove(), 1450);
  }

  function delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  function startAll(): void {
    running = true;
    toggleButton.classList.add("active");
    toggleButton.innerText = "Stop all streams";
    errorHint.classList.add("hidden");
    for (const lane of lanes) {
      void runLane(lane);
    }
  }

  function stopAll(): void {
    running = false;
    toggleButton.classList.remove("active");
    toggleButton.innerText = "Start all streams";
    for (const lane of lanes) {
      lane.abortController?.abort();
    }
  }

  async function runLane(lane: Lane): Promise<void> {
    lane.txCount = 0;
    lane.rxCount = 0;
    lane.abortController = new AbortController();
    const signal = lane.abortController.signal;
    let failed = false;
    setLaneState(lane, "running");

    async function* generateRequests() {
      for (let i = 1; i <= MESSAGES_PER_LANE && !signal.aborted; i++) {
        lane.txCount++;
        updateLaneStat(lane, "tx");
        pulse(lane, "tx", i);
        yield { sentence: `Message #${i} from ${lane.label}` };
        await delay(800 + Math.random() * 400);
      }
    }

    try {
      for await (const res of client.converse(generateRequests(), {
        signal,
      })) {
        void res;
        lane.rxCount++;
        updateLaneStat(lane, "rx");
        pulse(lane, "rx", lane.rxCount);
      }
    } catch (err) {
      if (!signal.aborted) {
        failed = true;
        console.error(`stream lane ${lane.id} error:`, err);
        // A rejected WebTransport handshake on a local server is almost
        // always the browser's stricter certificate rules; point at the
        // "Run it yourself" section, which documents the fix per browser.
        if (isWebTransportConnectionError(err)) {
          errorHint.classList.remove("hidden");
        }
      }
    } finally {
      setLaneState(lane, failed ? "error" : "done");
    }
  }

  function setTransportDescription(text: string): void {
    descriptionEl.innerText = text;
  }

  return { setTransportDescription };
}
