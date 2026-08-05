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

import hljs from "highlight.js/lib/core";
import bash from "highlight.js/lib/languages/bash";
import go from "highlight.js/lib/languages/go";
import typescript from "highlight.js/lib/languages/typescript";

hljs.registerLanguage("bash", bash);
hljs.registerLanguage("go", go);
hljs.registerLanguage("typescript", typescript);

/**
 * Syntax-highlights the static code examples on the page. Only the two
 * languages actually used are registered, keeping the bundle small; token
 * colors live in style.css (`.code-card .hljs-*`), not a highlight.js theme.
 */
export function highlightCodeExamples(): void {
  const blocks = document.querySelectorAll<HTMLElement>(
    'pre code[class*="language-"]',
  );
  for (const block of blocks) {
    hljs.highlightElement(block);
  }
}
