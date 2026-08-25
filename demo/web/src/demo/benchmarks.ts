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

// Renders the checked-in benchmark results (regenerated with
// `just bench-data`, which writes ../bench-go.json and ../bench-ts.json)
// into the Benchmarks section: one table per workload, transports as rows,
// with the best value in each column highlighted — so the transports are
// directly comparable at a glance.

import benchGo from "../bench-go.json";
import benchTs from "../bench-ts.json";

interface GoRow {
  case: string;
  workload: string;
  nsPerOp: number;
  nsPerRoundtrip?: number;
  rxBytesPerOp: number;
  txBytesPerOp: number;
}

interface TsRow {
  case: string;
  workload: string;
  "ms/op": number;
  "roundtrips/s": number;
  "rx B/rt": number;
  "tx B/rt": number;
}

/** One metric column: how to read it from a row, format it, and rank it. */
interface Column<T> {
  header: string;
  value(row: T): number | undefined;
  format(value: number): string;
  /** Most metrics are costs (lower wins); rates flip this. */
  higherIsBetter?: boolean;
}

const goWorkloadLabels: Record<string, string> = {
  unary_small: "Unary — tiny message",
  unary_16KiB_repetitive: "Unary — 16 KiB repetitive text (compressible)",
  unary_16KiB_random: "Unary — 16 KiB random text (incompressible)",
  bidi_100_roundtrips: "Bidi — 100 small roundtrips",
};

const tsWorkloadLabels: Record<string, string> = {
  "bidi_small x1000": "Bidi — 1,000 small roundtrips",
  "bidi_16KiB_repetitive x50": "Bidi — 50 × 16 KiB repetitive (compressible)",
  "bidi_16KiB_random x50": "Bidi — 50 × 16 KiB random (incompressible)",
};

function formatNs(ns: number): string {
  if (ns >= 1e6) {
    return `${(ns / 1e6).toFixed(1)} ms`;
  }
  if (ns >= 1e3) {
    return `${(ns / 1e3).toFixed(1)} µs`;
  }
  return `${Math.round(ns)} ns`;
}

function formatBytes(bytes: number): string {
  if (bytes >= 1024) {
    return `${(bytes / 1024).toFixed(1)} KiB`;
  }
  return `${Math.round(bytes)} B`;
}

const goColumns: Column<GoRow>[] = [
  { header: "Time/op", value: (r) => r.nsPerOp, format: formatNs },
  { header: "Per roundtrip", value: (r) => r.nsPerRoundtrip, format: formatNs },
  { header: "Client→server", value: (r) => r.rxBytesPerOp, format: formatBytes },
  { header: "Server→client", value: (r) => r.txBytesPerOp, format: formatBytes },
];

const tsColumns: Column<TsRow>[] = [
  {
    header: "Time/op",
    value: (r) => r["ms/op"],
    format: (v) => `${v.toFixed(1)} ms`,
  },
  {
    header: "Roundtrips/s",
    value: (r) => r["roundtrips/s"],
    format: (v) => Math.round(v).toLocaleString("en-US"),
    higherIsBetter: true,
  },
  {
    header: "Client→server",
    value: (r) => r["rx B/rt"],
    format: formatBytes,
  },
  {
    header: "Server→client",
    value: (r) => r["tx B/rt"],
    format: formatBytes,
  },
];

function groupByWorkload<T extends { workload: string }>(
  rows: T[],
): Map<string, T[]> {
  const groups = new Map<string, T[]>();
  for (const row of rows) {
    const group = groups.get(row.workload);
    if (group === undefined) {
      groups.set(row.workload, [row]);
    } else {
      group.push(row);
    }
  }
  return groups;
}

function renderWorkloadTables<T extends { case: string; workload: string }>(
  container: HTMLElement,
  rows: T[],
  allColumns: Column<T>[],
  workloadLabels: Record<string, string>,
): void {
  const fragments: Node[] = [];
  for (const [workload, unordered] of groupByWorkload(rows)) {
    // Group the identity variants together, then the compressed ones
    // (gzip and deflate alike), so like compares with like; transport
    // order stays stable within each block.
    const variantRank = (name: string): number =>
      name.endsWith("/identity") ? 0 : 1;
    const group = [...unordered].sort(
      (a, b) => variantRank(a.case) - variantRank(b.case),
    );

    const heading = document.createElement("h4");
    heading.textContent = workloadLabels[workload] ?? workload;
    fragments.push(heading);

    // Drop columns no row in this workload reports (e.g. per-roundtrip
    // time on unary workloads).
    const columns = allColumns.filter((column) =>
      group.some((row) => column.value(row) !== undefined),
    );

    const table = document.createElement("table");
    table.className = "support-table bench-table";
    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    for (const header of ["Transport", ...columns.map((c) => c.header)]) {
      const th = document.createElement("th");
      th.scope = "col";
      th.textContent = header;
      headRow.appendChild(th);
    }
    thead.appendChild(headRow);

    // The best value per column gets highlighted.
    const best = columns.map((column) => {
      const values = group
        .map((row) => column.value(row))
        .filter((value): value is number => value !== undefined);
      if (values.length < 2) {
        return undefined;
      }
      return column.higherIsBetter === true
        ? Math.max(...values)
        : Math.min(...values);
    });

    const tbody = document.createElement("tbody");
    let previousRank = -1;
    for (const row of group) {
      const tr = document.createElement("tr");
      const rank = variantRank(row.case);
      if (previousRank !== -1 && rank !== previousRank) {
        // Visual seam between the identity block and the compressed block.
        tr.classList.add("bench-variant-start");
      }
      previousRank = rank;
      const caseCell = document.createElement("td");
      caseCell.textContent = row.case;
      tr.appendChild(caseCell);
      columns.forEach((column, index) => {
        const td = document.createElement("td");
        const value = column.value(row);
        if (value === undefined) {
          td.textContent = "—";
        } else {
          td.textContent = column.format(value);
          if (value === best[index]) {
            td.classList.add("bench-best");
          }
        }
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    }
    table.replaceChildren(thead, tbody);

    const scroll = document.createElement("div");
    scroll.className = "table-scroll bench-table-scroll";
    scroll.appendChild(table);
    fragments.push(scroll);
  }
  container.replaceChildren(...fragments);
}

/** Fills the Benchmarks section from the checked-in result files. */
export function renderBenchmarks(): void {
  const goContainer = document.getElementById("bench-go-tables");
  const tsContainer = document.getElementById("bench-ts-tables");
  if (goContainer === null || tsContainer === null) {
    return;
  }

  renderWorkloadTables(
    goContainer,
    benchGo.rows as GoRow[],
    goColumns,
    goWorkloadLabels,
  );
  renderWorkloadTables(
    tsContainer,
    benchTs.rows as TsRow[],
    tsColumns,
    tsWorkloadLabels,
  );

  const goMeta = document.getElementById("bench-go-meta");
  if (goMeta !== null) {
    goMeta.textContent = `${benchGo.cpu}, loopback, ${benchGo.generatedAt}. Wire bytes are per operation, measured on the server socket: plaintext TCP for the WebSocket drafts, UDP datagrams (including QUIC + TLS overhead) for WebTransport. Best value per column is highlighted.`;
  }
  const tsMeta = document.getElementById("bench-ts-meta");
  if (tsMeta !== null) {
    tsMeta.textContent = `${benchTs.runtime}, loopback, ${benchTs.generatedAt}. Wire bytes are per roundtrip, measured on the server's TCP sockets. Client and server both TypeScript; WebTransport has no Node client, so it appears only in the Go tables. Best value per column is highlighted.`;
  }
}
