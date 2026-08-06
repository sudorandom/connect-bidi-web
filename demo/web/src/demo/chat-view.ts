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
import {
  appendMessage,
  appendWebTransportHint,
  isWebTransportConnectionError,
} from "./dom.js";

export interface ChatView {
  /** Starts the name prompt, introduction, and the ongoing chat loop. */
  start(): Promise<void>;
  /**
   * Aborts the in-flight `Converse` call and starts a new one, so the chat
   * keeps working after the visitor switches the streaming transport.
   */
  notifyTransportChanged(transportLabel: string): void;
}

/**
 * Creates the stateful "Streaming Chat" tab, backed by one long-lived
 * `Converse` bidi-streaming RPC per transport, plus a one-shot `Introduce`
 * server-streaming RPC once the visitor's name is known.
 */
export function createChatView(
  client: Client<typeof ElizaService>,
  messages: HTMLElement,
  inputContainer: HTMLElement,
): ChatView {
  let name: string | undefined;
  let abortController = new AbortController();
  // The introduction and the conversation usually fail together (they share
  // the transport), so the certificate hint is shown at most once per
  // transport choice.
  let hintShown = false;

  function reportError(prefix: string, err: unknown): void {
    appendMessage(messages, "system", `${prefix}: ${String(err)}`);
    if (!hintShown && isWebTransportConnectionError(err)) {
      hintShown = true;
      appendWebTransportHint(messages);
    }
  }

  function promptForText(signal?: AbortSignal): Promise<string> {
    const input = document.createElement("input");
    input.className = "chat-input";
    input.setAttribute("autocomplete", "off");
    input.setAttribute("autocorrect", "off");
    input.setAttribute("autocapitalize", "off");
    input.setAttribute("spellcheck", "false");
    input.placeholder = "Type your message...";

    const sendButton = document.createElement("button");
    sendButton.type = "button";
    sendButton.className = "chat-send-btn";
    sendButton.setAttribute("aria-label", "Send message");
    sendButton.innerHTML =
      '<svg viewBox="0 0 24 24" width="18" height="18" fill="none" ' +
      'stroke="currentColor" stroke-width="2.5" stroke-linecap="round" ' +
      'stroke-linejoin="round" aria-hidden="true">' +
      '<path d="M12 19V5" /><path d="m5 12 7-7 7 7" /></svg>';

    const row = document.createElement("div");
    row.className = "chat-input-row";
    row.append(input, sendButton);
    inputContainer.replaceChildren(row);
    input.focus();

    return new Promise<string>((resolve, reject) => {
      const onAbort = (): void => {
        inputContainer.replaceChildren();
        reject(new Error("aborted"));
      };
      signal?.addEventListener("abort", onAbort, { once: true });
      const submit = (): void => {
        const value = input.value.trim();
        if (value.length === 0) {
          return;
        }
        signal?.removeEventListener("abort", onAbort);
        resolve(value);
      };
      input.addEventListener("keyup", (event) => {
        if (event.key === "Enter") {
          submit();
        }
      });
      sendButton.addEventListener("click", submit);
    });
  }

  async function runIntroduction(): Promise<void> {
    if (name === undefined) {
      return;
    }
    try {
      for await (const res of client.introduce({ name })) {
        appendMessage(messages, "eliza", res.sentence);
      }
    } catch (err) {
      reportError("Introduce error", err);
    }
  }

  async function runConversation(): Promise<void> {
    const signal = abortController.signal;

    async function* requests() {
      for (;;) {
        let sentence: string;
        try {
          sentence = await promptForText(signal);
        } catch {
          return;
        }
        appendMessage(messages, "user", sentence);
        yield { sentence };
      }
    }

    try {
      for await (const res of client.converse(requests(), { signal })) {
        appendMessage(messages, "eliza", res.sentence);
      }
    } catch (err) {
      if (!signal.aborted) {
        reportError("Converse error", err);
      }
    }
  }

  async function start(): Promise<void> {
    appendMessage(messages, "eliza", "What is your name?");
    name = await promptForText();
    appendMessage(messages, "user", name);
    await runIntroduction();
    await runConversation();
  }

  function notifyTransportChanged(transportLabel: string): void {
    appendMessage(
      messages,
      "system",
      `Switched streaming transport to ${transportLabel}.`,
    );
    // A new transport choice may well fix the connection; re-arm the hint.
    hintShown = false;
    abortController.abort();
    abortController = new AbortController();
    void runConversation();
  }

  return { start, notifyTransportChanged };
}
