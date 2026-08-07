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
  "Lane 1",
  "Lane 2",
  "Lane 3",
  "Lane 4",
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
  /**
   * Stops any running streams. Called when the streaming transport is
   * switched: the lanes hold streams on the old transport, which would
   * otherwise keep running against it.
   */
  stopStreams(): void;
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
  let activeLanes = 0;
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
    card.className = "lane-row";
    // One slim row per lane, with a single shared track: sent messages
    // fly left-to-right in indigo, their echoes fly back right-to-left in
    // green.
    card.innerHTML = `
      <span class="lane-label">${lane.label}</span>
      <div id="lane-track-${lane.id}" class="lane-track"></div>
      <span class="lane-counts">TX <strong id="lane-tx-${lane.id}">0</strong> &#183; RX <strong id="lane-rx-${lane.id}">0</strong></span>
      <span id="lane-status-${lane.id}" class="status-badge">IDLE</span>
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
          ? "ACTIVE"
          : state === "error"
            ? "ERROR"
            : "COMPLETED";
    }
  }

  function updateLaneStat(lane: Lane, kind: "tx" | "rx"): void {
    const counter = document.querySelector<HTMLElement>(
      `#lane-${kind}-${lane.id}`,
    );
    if (counter !== null) {
      counter.innerText = String(kind === "tx" ? lane.txCount : lane.rxCount);
    }
  }

  /** Sends a dot flying along the lane's track: TX rightwards, RX back. */
  function pulse(lane: Lane, kind: "tx" | "rx"): void {
    const track = document.querySelector<HTMLElement>(
      `#lane-track-${lane.id}`,
    );
    if (track === null) {
      return;
    }
    const dot = document.createElement("div");
    dot.className = `lane-dot lane-dot-${kind}`;
    track.append(dot);
    setTimeout(() => dot.remove(), 1450);
  }

  function delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  function setToggleRunning(isRunning: boolean): void {
    running = isRunning;
    toggleButton.classList.toggle("active", isRunning);
    toggleButton.innerText = isRunning
      ? "Stop all streams"
      : "▶ Start all streams";
  }

  function startAll(): void {
    setToggleRunning(true);
    errorHint.classList.add("hidden");
    activeLanes = lanes.length;
    for (const lane of lanes) {
      void runLane(lane);
    }
  }

  function stopAll(): void {
    setToggleRunning(false);
    for (const lane of lanes) {
      lane.abortController?.abort();
    }
  }

  async function runLane(lane: Lane): Promise<void> {
    lane.txCount = 0;
    lane.rxCount = 0;
    updateLaneStat(lane, "tx");
    updateLaneStat(lane, "rx");
    lane.abortController = new AbortController();
    const signal = lane.abortController.signal;
    let failed = false;
    setLaneState(lane, "running");

    async function* generateRequests() {
      for (let i = 1; i <= MESSAGES_PER_LANE && !signal.aborted; i++) {
        lane.txCount++;
        updateLaneStat(lane, "tx");
        pulse(lane, "tx");
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
        pulse(lane, "rx");
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
      activeLanes--;
      // All lanes ran to completion (or failed) on their own: flip the
      // toggle back, since there is nothing left to stop.
      if (activeLanes === 0 && running) {
        setToggleRunning(false);
      }
    }
  }

  function setTransportDescription(text: string): void {
    descriptionEl.innerText = text;
  }

  function stopStreams(): void {
    if (running) {
      stopAll();
    }
  }

  return { setTransportDescription, stopStreams };
}
