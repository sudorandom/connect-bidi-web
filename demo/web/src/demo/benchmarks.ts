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
  /**
   * How the connection was established. Byte counts are plaintext TCP on
   * HTTP/1.1 but include TLS on HTTP/2 and QUIC + TLS on HTTP/3, so rows
   * are only comparable within a bootstrap.
   */
  bootstrap?: string;
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

/**
 * `tab` is what the tab strip shows and has to stay short; `full` is the
 * heading inside the panel, where there is room to say what the workload
 * actually is.
 */
interface WorkloadLabel {
  tab: string;
  full: string;
}

const goWorkloadLabels: Record<string, WorkloadLabel> = {
  unary_small: { tab: "Tiny", full: "Unary — tiny message" },
  unary_16KiB_repetitive: {
    tab: "16 KiB text",
    full: "Unary — 16 KiB repetitive text (compressible)",
  },
  unary_16KiB_random: {
    tab: "16 KiB random",
    full: "Unary — 16 KiB random text (incompressible)",
  },
  bidi_100_roundtrips: {
    tab: "Bidi ×100",
    full: "Bidi — 100 small roundtrips",
  },
};

const tsWorkloadLabels: Record<string, WorkloadLabel> = {
  "bidi_small x1000": {
    tab: "Small ×1000",
    full: "Bidi — 1,000 small roundtrips",
  },
  "bidi_16KiB_repetitive x50": {
    tab: "16 KiB text",
    full: "Bidi — 50 × 16 KiB repetitive (compressible)",
  },
  "bidi_16KiB_random x50": {
    tab: "16 KiB random",
    full: "Bidi — 50 × 16 KiB random (incompressible)",
  },
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

// Exact bytes are the point of these numbers — "16.5 KiB" hides the
// difference between two variants that are 200 bytes apart. Rounding to KiB
// is held back until the digits stop being readable, which none of the
// current workloads reach; grouping carries the ones that get close.
const KIB_THRESHOLD = 100 * 1024;

function formatBytes(bytes: number): string {
  if (bytes >= KIB_THRESHOLD) {
    return `${(bytes / 1024).toFixed(1)} KiB`;
  }
  return `${Math.round(bytes).toLocaleString("en-US")} B`;
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

/** One quantity plotted on a side of the chart. */
interface Series<T> {
  label: string;
  value: (row: T) => number | undefined;
}

interface ChartConfig<T> {
  /** How each row is named down the centre column. */
  label: (row: T) => string;
  /** Unit and direction for the left side, which stacks its series. */
  leftHeader: string;
  /** Unit and direction for the right side. */
  rightHeader: string;
  /** Stacked left-hand series, outermost first. */
  left: Series<T>[];
  right: Series<T>;
  formatLeft: (n: number) => string;
  formatRight: (n: number) => string;
  /** Optional grouping label per row, rendered as a divider. */
  group?: (row: T) => string;
}

const SERIES_CLASS = ["s1", "s2", "s3"];

/**
 * A butterfly chart for one workload: variants down the middle, bytes
 * growing leftward, time per operation growing rightward.
 *
 * The halves are separate plots sharing a category column — small
 * multiples, not a dual-axis overlay — each scaled to its own maximum, so a
 * long bar on one side implies nothing about the other. That is why each
 * side states its own unit and direction in its header. The left bar is
 * stacked by direction, so its full length is total bytes on the wire and
 * the split shows which way they went.
 */
function renderButterflyChart<T extends { case: string }>(
  rows: T[],
  config: ChartConfig<T>,
): SVGSVGElement {
  const NS = "http://www.w3.org/2000/svg";
  const width = 680;
  const centerW = 196;
  const sideW = (width - centerW) / 2;
  const barH = 13;
  const rowH = 24;
  const groupH = 24;
  const headerH = 22;
  const midL = (width - centerW) / 2;
  const midR = midL + centerW;
  /** Surface gap between stacked segments, per the mark spec. */
  const segGap = 2;
  const fontSize = 10.5;
  const textY = (top: number) => top + barH / 2 + fontSize * 0.36;

  interface Line {
    kind: "group" | "row";
    text: string;
    row?: T;
    section: string;
  }
  // Each section is scaled to itself. A compressed variant and an
  // uncompressed one differ by two orders of magnitude, so on a shared
  // scale every compressed bar collapses to a sliver and the comparison
  // that matters — draft against draft, under the same conditions —
  // disappears.
  const maxima = new Map<string, { left: number; right: number }>();
  const lines: Line[] = [];
  let lastGroup: string | undefined;
  for (const row of rows) {
    const section = config.group?.(row) ?? "";
    const total = config.left.reduce((sum, s) => sum + (s.value(row) ?? 0), 0);
    const current = maxima.get(section) ?? { left: 0, right: 0 };
    maxima.set(section, {
      left: Math.max(current.left, total),
      right: Math.max(current.right, config.right.value(row) ?? 0),
    });
    if (section !== "" && section !== lastGroup) {
      lines.push({ kind: "group", text: section, section });
      lastGroup = section;
    }
    lines.push({ kind: "row", text: config.label(row), row, section });
  }
  const height =
    headerH +
    lines.reduce((h, l) => h + (l.kind === "group" ? groupH : rowH), 0) +
    6;

  const svg = document.createElementNS(NS, "svg");
  svg.setAttribute("class", "bench-chart");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("role", "img");
  svg.setAttribute(
    "aria-label",
    `Left: ${config.leftHeader}, stacked by direction. Right: ${config.rightHeader}. Each side is scaled to its own maximum. The table below has the same numbers.`,
  );

  const text = (
    cls: string,
    x: number,
    y: number,
    content: string,
    anchor?: string,
  ) => {
    const el = document.createElementNS(NS, "text");
    el.setAttribute("class", cls);
    el.setAttribute("x", String(x));
    el.setAttribute("y", String(y));
    if (anchor !== undefined) {
      el.setAttribute("text-anchor", anchor);
    }
    el.textContent = content;
    svg.appendChild(el);
  };

  const rect = (
    cls: string,
    x: number,
    y: number,
    w: number,
    h: number,
    tooltip: string,
  ) => {
    const el = document.createElementNS(NS, "rect");
    el.setAttribute("class", cls);
    el.setAttribute("x", String(x));
    el.setAttribute("y", String(y));
    el.setAttribute("width", String(w));
    el.setAttribute("height", String(h));
    el.setAttribute("rx", "2");
    const title = document.createElementNS(NS, "title");
    title.textContent = tooltip;
    el.appendChild(title);
    svg.appendChild(el);
  };

  text("bench-chart-unit", midL, 14, config.leftHeader, "end");
  text("bench-chart-unit", midR, 14, config.rightHeader, "start");

  let y = headerH;
  for (const line of lines) {
    if (line.kind === "group") {
      text("bench-chart-group", width / 2, y + 15, line.text, "middle");
      y += groupH;
      continue;
    }
    const row = line.row;
    if (row === undefined) {
      continue;
    }
    const { left: maxLeft, right: maxRight } = maxima.get(line.section) ?? {
      left: 0,
      right: 0,
    };
    const top = y + 5;
    text("bench-chart-label", midL + centerW / 2, textY(top), line.text, "middle");

    // Left: one stacked bar, growing away from the centre.
    const parts = config.left.map((s) => s.value(row) ?? 0);
    const total = parts.reduce((a, b) => a + b, 0);
    if (maxLeft > 0 && total > 0) {
      const totalW = Math.max(2, (total / maxLeft) * sideW);
      let cursor = midL;
      parts.forEach((part, i) => {
        const segW = (part / total) * totalW;
        if (segW <= 0) {
          return;
        }
        // Trim the gap off every segment but the outermost, so the stack's
        // overall length still reads as the total.
        const drawn = i === parts.length - 1 ? segW : Math.max(1, segW - segGap);
        rect(
          `bench-chart-bar ${SERIES_CLASS[i]}`,
          cursor - drawn,
          top,
          drawn,
          barH,
          `${line.text} · ${config.left[i].label} — ${config.formatLeft(part)}`,
        );
        cursor -= segW;
      });
      const insideLeft = totalW > sideW * 0.7;
      text(
        insideLeft ? "bench-chart-value inside" : "bench-chart-value",
        insideLeft ? midL - totalW + 7 : midL - totalW - 6,
        textY(top),
        config.formatLeft(total),
        insideLeft ? "start" : "end",
      );
    }

    // Right: time per operation.
    const right = config.right.value(row);
    if (right !== undefined && maxRight > 0) {
      const w = Math.max(2, (right / maxRight) * sideW);
      rect(
        "bench-chart-bar s3",
        midR,
        top,
        w,
        barH,
        `${line.text} · ${config.right.label} — ${config.formatRight(right)}`,
      );
      const insideRight = w > sideW * 0.7;
      text(
        insideRight ? "bench-chart-value inside" : "bench-chart-value",
        insideRight ? midR + w - 7 : midR + w + 6,
        textY(top),
        config.formatRight(right),
        insideRight ? "end" : "start",
      );
    }
    y += rowH;
  }
  return svg;
}

/** Swatch-and-label legend: identity is never carried by colour alone. */
function renderLegend(labels: string[]): HTMLElement {
  const legend = document.createElement("ul");
  legend.className = "bench-legend";
  labels.forEach((label, i) => {
    const item = document.createElement("li");
    const swatch = document.createElement("span");
    swatch.className = `bench-swatch ${SERIES_CLASS[i]}`;
    item.appendChild(swatch);
    item.appendChild(document.createTextNode(label));
    legend.appendChild(item);
  });
  return legend;
}

/**
 * Whether a case asked for compression, read off its variant name. This is
 * the configuration, not the outcome: draft 4 over HTTP/2 requests deflate
 * and gets none, which is exactly the finding its section makes visible.
 */
function compressionOf(caseName: string): string {
  const variant = caseName.slice(caseName.indexOf("/") + 1);
  return /gzip|deflate/.test(variant) ? "compression on" : "no compression";
}

/**
 * The comparison unit. Compressed and uncompressed runs differ by orders of
 * magnitude, so they get their own sections and their own scales; the
 * bootstrap rides in the row label instead.
 */
function sectionOf(row: { case: string }): string {
  return compressionOf(row.case);
}

/**
 * Row label: bootstrap, then the case. The `h2-` marker is dropped from the
 * variant because the prefix already says it.
 */
function rowLabel(row: { case: string; bootstrap?: string }): string {
  const prefix =
    row.bootstrap === undefined || row.bootstrap === ""
      ? ""
      : `${row.bootstrap.toLowerCase().replace("/", "")}/`;
  return prefix + row.case.replace("/h2-", "/");
}

interface Panel {
  label: string;
  node: Node;
}

function buildWorkloadPanels<T extends { case: string; workload: string }>(
  rows: T[],
  allColumns: Column<T>[],
  workloadLabels: Record<string, WorkloadLabel>,
  chart: ChartConfig<T>,
  legend: string[],
): Panel[] {
  const panels: Panel[] = [];
  for (const [workload, unordered] of groupByWorkload(rows)) {
    // Rows are blocked by bootstrap first — their byte counts are measured
    // differently, so they must never be ranked against each other — and
    // within a bootstrap, identity variants before compressed ones, so
    // like compares with like. Transport order stays stable within a block.
    const sectionKey = (row: T): string =>
      sectionOf(row as T & { bootstrap?: string });
    const bootstrapRank = (row: T): number =>
      ["HTTP/1.1", "HTTP/2", "HTTP/3"].indexOf(
        (row as { bootstrap?: string }).bootstrap ?? "",
      );
    // Uncompressed block first inside each bootstrap, so like sits with
    // like and the compressed block reads as the comparison it is.
    const compressionRank = (row: T): number =>
      compressionOf(row.case) === "no compression" ? 0 : 1;
    const group = [...unordered].sort(
      (a, b) =>
        compressionRank(a) - compressionRank(b) ||
        bootstrapRank(a) - bootstrapRank(b),
    );

    const label = workloadLabels[workload];
    const panel = document.createElement("div");
    const heading = document.createElement("h4");
    heading.textContent = label?.full ?? workload;
    panel.appendChild(heading);
    panel.appendChild(renderLegend(legend));
    const chartScroll = document.createElement("div");
    chartScroll.className = "bench-chart-scroll";
    chartScroll.appendChild(renderButterflyChart(group, chart));
    panel.appendChild(chartScroll);

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

    // The best value per column is computed *within* each bootstrap:
    // highlighting the smallest byte count across bootstraps would just
    // reward the row whose measurement excludes TLS.
    const bestBySection = new Map<string, (number | undefined)[]>();
    for (const section of new Set(group.map(sectionKey))) {
      const block = group.filter((row) => sectionKey(row) === section);
      bestBySection.set(
        section,
        columns.map((column) => {
          const values = block
            .map((row) => column.value(row))
            .filter((value): value is number => value !== undefined);
          if (values.length < 2) {
            return undefined;
          }
          return column.higherIsBetter === true
            ? Math.max(...values)
            : Math.min(...values);
        }),
      );
    }

    const tbody = document.createElement("tbody");
    let previousSection: string | undefined;
    for (const row of group) {
      const section = sectionKey(row);
      if (section !== previousSection) {
        const labelRow = document.createElement("tr");
        labelRow.className = "bench-bootstrap-row";
        const labelCell = document.createElement("th");
        labelCell.scope = "colgroup";
        labelCell.colSpan = columns.length + 1;
        labelCell.textContent = section;
        labelRow.appendChild(labelCell);
        tbody.appendChild(labelRow);
      }
      previousSection = section;
      const best = bestBySection.get(section) ?? [];
      const tr = document.createElement("tr");
      const caseCell = document.createElement("td");
      caseCell.textContent = rowLabel(row as T & { bootstrap?: string });
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

    const details = document.createElement("details");
    details.className = "bench-details";
    const summary = document.createElement("summary");
    summary.textContent = "Exact numbers";
    details.appendChild(summary);
    details.appendChild(scroll);
    panel.appendChild(details);
    panels.push({ label: label?.tab ?? workload, node: panel });
  }
  return panels;
}

/**
 * Renders panels behind a tab bar. The summary lands first and opens by
 * default; the per-workload tables sit behind it so the section is
 * skimmable instead of a wall of tables.
 */
function renderTabs(container: HTMLElement, panels: Panel[]): void {
  const nav = document.createElement("nav");
  nav.className = "tab-switcher bench-tab-switcher";
  nav.setAttribute("aria-label", "Benchmark view");
  const body = document.createElement("div");

  const buttons: HTMLButtonElement[] = [];
  const nodes: HTMLElement[] = [];
  panels.forEach((panel, index) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = index === 0 ? "code-tab-btn active" : "code-tab-btn";
    button.textContent = panel.label;
    const wrap = document.createElement("div");
    wrap.className = index === 0 ? "code-tab-panel active" : "code-tab-panel";
    wrap.appendChild(panel.node);
    button.addEventListener("click", () => {
      buttons.forEach((b, i) => {
        b.classList.toggle("active", i === index);
        b.setAttribute("aria-selected", String(i === index));
      });
      nodes.forEach((n, i) => n.classList.toggle("active", i === index));
    });
    button.setAttribute("role", "tab");
    button.setAttribute("aria-selected", String(index === 0));
    buttons.push(button);
    nodes.push(wrap);
    nav.appendChild(button);
    body.appendChild(wrap);
  });
  nav.setAttribute("role", "tablist");
  container.replaceChildren(nav, body);
}

/** Fills the Benchmarks section from the checked-in result files. */
export function renderBenchmarks(): void {
  const goContainer = document.getElementById("bench-go-tables");
  const tsContainer = document.getElementById("bench-ts-tables");
  if (goContainer === null || tsContainer === null) {
    return;
  }

  const goRows = benchGo.rows as GoRow[];
  const tsRows = benchTs.rows as TsRow[];

  const byteLegend = [
    "client → server bytes",
    "server → client bytes",
    "time per op",
  ];

  renderTabs(
    goContainer,
    buildWorkloadPanels(goRows, goColumns, goWorkloadLabels, {
      label: rowLabel,
      leftHeader: "◀ bytes on the wire · lower is better",
      rightHeader: "time per op · lower is better ▶",
      left: [
        { label: "client → server", value: (r) => r.rxBytesPerOp },
        { label: "server → client", value: (r) => r.txBytesPerOp },
      ],
      right: { label: "time per op", value: (r) => r.nsPerOp },
      formatLeft: formatBytes,
      formatRight: formatNs,
      group: sectionOf,
    }, byteLegend),
  );

  renderTabs(
    tsContainer,
    buildWorkloadPanels(tsRows, tsColumns, tsWorkloadLabels, {
      label: rowLabel,
      leftHeader: "◀ bytes per roundtrip · lower is better",
      rightHeader: "time per op · lower is better ▶",
      left: [
        { label: "client → server", value: (r) => r["rx B/rt"] },
        { label: "server → client", value: (r) => r["tx B/rt"] },
      ],
      right: { label: "time per op", value: (r) => r["ms/op"] },
      formatLeft: formatBytes,
      formatRight: (v) => `${v.toFixed(1)} ms`,
      group: sectionOf,
    }, byteLegend),
  );

  const goMeta = document.getElementById("bench-go-meta");
  if (goMeta !== null) {
    goMeta.textContent = `Every draft against every workload, Go on both ends. Bytes are counted on the server socket. Rows are grouped by compression, and each group is scaled to itself. Bootstraps sit in one group but are measured differently — plaintext TCP for http1.1, TLS for http2, QUIC for http3 — so their byte counts are not directly comparable. ${benchGo.cpu}, loopback, ${benchGo.generatedAt}.`;
  }
  const tsMeta = document.getElementById("bench-ts-meta");
  if (tsMeta !== null) {
    tsMeta.textContent = `The same drafts with TypeScript on both ends. Bytes are counted per roundtrip on the server's sockets. Node has no WebTransport client, so that row appears only in the Go tables. ${benchTs.runtime}, loopback, ${benchTs.generatedAt}.`;
  }
}
