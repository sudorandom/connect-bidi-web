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
import { appendMessage, requireElement } from "./dom.js";

/**
 * Wires up the unary "Eliza Chat" tab, backed entirely by the `Say` RPC.
 *
 * This tab always talks over the unary transport (plain Connect-over-HTTP),
 * regardless of which streaming transport is selected elsewhere on the page.
 */
export function initUnaryView(client: Client<typeof ElizaService>): void {
  const messages = requireElement<HTMLElement>("#unary-messages");
  const input = requireElement<HTMLInputElement>("#unary-input");

  appendMessage(messages, "eliza", "What is your name?");
  let greeted = false;

  input.addEventListener("keyup", (event) => {
    if (event.key !== "Enter") {
      return;
    }
    const text = input.value.trim();
    if (text.length === 0) {
      return;
    }
    input.value = "";
    appendMessage(messages, "user", text);
    void sendToEliza(text);
  });

  async function sendToEliza(text: string): Promise<void> {
    try {
      const res = await client.say({ sentence: text });
      const reply = greeted ? res.sentence : `Hi ${text}! ${res.sentence}`;
      greeted = true;
      appendMessage(messages, "eliza", reply);
    } catch (err) {
      appendMessage(messages, "system", `Say error: ${String(err)}`);
    }
  }
}
