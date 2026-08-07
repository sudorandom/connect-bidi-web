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
 * Creates the stateful "Streaming Chat" tab: a one-shot server-streaming
 * `Introduce` RPC greets the visitor by name, then a long-lived `Converse`
 * bidi-streaming RPC carries the conversation — two different RPC types,
 * multiplexed onto the same connection.
 */
export function createChatView(
  client: Client<typeof ElizaService>,
  messages: HTMLElement,
  inputContainer: HTMLElement,
): ChatView {
  let name: string | undefined;
  let abortController = new AbortController();
  // The certificate hint is shown at most once per transport choice.
  let hintShown = false;

  function reportError(prefix: string, err: unknown): void {
    // The server closes idle connections on purpose (see the worker's
    // idleTimeoutMs); that is not a failure worth an error message. The
    // re-armed conversation loop reconnects on the next message.
    if (String(err).includes("idle timeout")) {
      appendMessage(
        messages,
        "system",
        "Disconnected after inactivity — your next message reconnects.",
      );
      return;
    }
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

  // A one-shot server-streaming RPC: the greeting arrives as a short
  // response stream, multiplexed onto the same connection the Converse
  // stream below uses.
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

    // Wait for the visitor's next message before opening the RPC: calling
    // converse() dials the transport immediately, and an idle stream would
    // needlessly hold the connection (and, on Workers, an invocation)
    // open. This also keeps the automatic WebTransport->WebSocket fallback
    // on page load (which restarts this loop via notifyTransportChanged)
    // from opening a connection for an idle chat.
    let firstSentence: string;
    try {
      firstSentence = await promptForText(signal);
    } catch {
      return;
    }
    appendMessage(messages, "user", firstSentence);

    async function* requests() {
      yield { sentence: firstSentence };
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
    // The stream is gone: a server-side idle timeout, a dropped
    // connection, or a server restart. Re-arm the loop rather than
    // dead-ending the chat — the next message the visitor types opens a
    // fresh stream (dialing is lazy), so no input means no reconnect.
    if (!signal.aborted) {
      void runConversation();
    }
  }

  async function start(): Promise<void> {
    appendMessage(messages, "eliza", "What is your name?");
    // The name prompt is the first interaction; nothing dials before it
    // resolves.
    name = await promptForText();
    appendMessage(messages, "user", name);
    await runIntroduction();
    await runConversation();
  }

  function notifyTransportChanged(transportLabel: string): void {
    // Before the visitor has engaged (no name given yet), there is
    // nothing to restart and nothing worth announcing — this path also
    // runs on page load, when the WebTransport availability probe falls
    // back to WebSocket automatically.
    if (name === undefined) {
      return;
    }
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
