const formatBytes = (bytes) => {
  if (!Number.isFinite(bytes)) return "--";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let idx = 0;
  let value = bytes;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx += 1;
  }
  return `${value.toFixed(value >= 10 ? 1 : 2)} ${units[idx]}`;
};

const formatNumber = (value, digits = 0) => {
  if (value === null || value === undefined || !Number.isFinite(value)) return "--";
  return value.toLocaleString(undefined, {
    maximumFractionDigits: digits,
  });
};

const formatMs = (ms) => {
  if (!Number.isFinite(ms)) return "--";
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`;
  return `${ms.toFixed(2)} ms`;
};

const formatColors = {
  parquet_zstd: "#2f4a36",
  parquet_snappy: "#e38b2c",
  parquet_uncompressed: "#4c6fa8",
  vortex_default: "#a84c6f",
};

const getFormatColor = (label) => formatColors[label] || "#6b6358";

let currentReport = null;
let lastUploadInfo = null;
let lastReportPath = null;

const _shortLineLabel = (label) => {
  const text = String(label).replace("parquet_", "pq_");
  if (text.length <= 12) return text;
  return `${text.slice(0, 10)}…`;
};

const _splitLabel = (label) => {
  const text = _shortLineLabel(label);
  if (text.length <= 10) return [text];
  const parts = text.split("_");
  if (parts.length >= 2) {
    return [parts[0], parts.slice(1).join("_")];
  }
  return [text.slice(0, 8), text.slice(8)];
};

const formatCellValue = (value) => {
  if (value === null || value === undefined) return "--";
  if (typeof value === "number" && Number.isFinite(value)) {
    return formatNumber(value, 4);
  }
  return String(value);
};

const renderCustomQueryResults = (output, data) => {
  output.innerHTML = "";
  const results = data?.results || {};
  const entries = Object.entries(results);
  if (!entries.length) {
    output.textContent = "No results returned.";
    return;
  }

  const best = data.best_format;
  const bestLine = document.createElement("div");
  bestLine.textContent = best ? `Best format: ${best}` : "Best format: n/a";
  bestLine.className = best ? "query-best" : "";
  output.appendChild(bestLine);

  const resultPanel = document.createElement("div");
  resultPanel.className = "query-result-panel";
  const resultTitle = document.createElement("div");
  resultTitle.className = "query-result-title";
  resultTitle.textContent = "Result detail";
  const resultBody = document.createElement("div");
  resultBody.className = "query-result-body";
  resultBody.textContent = "Click View to see a result.";
  resultPanel.appendChild(resultTitle);
  resultPanel.appendChild(resultBody);

  let activeName = null;
  let activeResult = null;
  let activePage = 0;
  const pageSize = 10;

  const renderActiveResult = () => {
    resultBody.innerHTML = "";
    if (!activeResult) {
      resultBody.textContent = "Click View to see a result.";
      return;
    }
    if (activeResult.error) {
      const err = document.createElement("div");
      err.className = "query-result-box";
      err.textContent = String(activeResult.error);
      resultBody.appendChild(err);
      return;
    }

    const rows = Array.isArray(activeResult.rows) ? activeResult.rows : [];
    const cols = Array.isArray(activeResult.columns) ? activeResult.columns : [];
    if (!rows.length) {
      const empty = document.createElement("div");
      empty.className = "query-result-box";
      empty.textContent = "No rows returned.";
      resultBody.appendChild(empty);
      return;
    }

    const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
    activePage = Math.min(Math.max(activePage, 0), totalPages - 1);
    const start = activePage * pageSize;
    const end = start + pageSize;
    const slice = rows.slice(start, end);

    const tableWrap = document.createElement("div");
    tableWrap.className = "table-wrap";
    const table = document.createElement("table");
    table.className = "preview-table";
    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    const headerCells = cols.length ? cols : slice[0].map((_, idx) => `col_${idx + 1}`);
    headerCells.forEach((label) => {
      const th = document.createElement("th");
      th.textContent = label;
      headRow.appendChild(th);
    });
    thead.appendChild(headRow);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    slice.forEach((row) => {
      const tr = document.createElement("tr");
      row.forEach((cell) => {
        const td = document.createElement("td");
        td.textContent = formatCellValue(cell);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    tableWrap.appendChild(table);
    resultBody.appendChild(tableWrap);

    if (rows.length > pageSize) {
      const pager = document.createElement("div");
      pager.className = "query-pagination";
      const prev = document.createElement("button");
      prev.type = "button";
      prev.className = "button is-muted";
      prev.textContent = "Prev";
      prev.disabled = activePage === 0;
      prev.addEventListener("click", () => {
        activePage = Math.max(0, activePage - 1);
        renderActiveResult();
      });
      const next = document.createElement("button");
      next.type = "button";
      next.className = "button is-muted";
      next.textContent = "Next";
      next.disabled = activePage >= totalPages - 1;
      next.addEventListener("click", () => {
        activePage = Math.min(totalPages - 1, activePage + 1);
        renderActiveResult();
      });
      const info = document.createElement("div");
      info.className = "query-page-info";
      info.textContent = `Page ${activePage + 1} of ${totalPages} · ${rows.length} rows`;
      pager.appendChild(prev);
      pager.appendChild(info);
      pager.appendChild(next);
      resultBody.appendChild(pager);
    }
  };

  const tableWrap = document.createElement("div");
  tableWrap.className = "table-wrap";
  const table = document.createElement("table");
  table.className = "preview-table";
  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  ["Format", "Median", "P95", "Result", "Runs", "Status"].forEach((label) => {
    const th = document.createElement("th");
    th.textContent = label;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  entries
    .slice()
    .sort((a, b) => {
      const aMed = typeof a[1]?.median_ms === "number" ? a[1].median_ms : Number.POSITIVE_INFINITY;
      const bMed = typeof b[1]?.median_ms === "number" ? b[1].median_ms : Number.POSITIVE_INFINITY;
      if (aMed === bMed) return a[0].localeCompare(b[0]);
      return aMed - bMed;
    })
    .forEach(([name, result]) => {
      const row = document.createElement("tr");
      const isBest = best && name === best;
      const error = result?.error;
      const hasError = Boolean(error);

      const cells = [
        name,
        hasError ? "--" : formatMs(result?.median_ms),
        hasError ? "--" : formatMs(result?.p95_ms),
      ];
      cells.forEach((text) => {
        const td = document.createElement("td");
        td.textContent = text;
        row.appendChild(td);
      });

      const resultCell = document.createElement("td");
      const viewButton = document.createElement("button");
      viewButton.type = "button";
      viewButton.className = "button is-muted";
      viewButton.textContent = "View";
      viewButton.addEventListener("click", () => {
        activeName = name;
        activeResult = result;
        activePage = 0;
        resultTitle.textContent = `Result detail (${activeName})`;
        renderActiveResult();
      });
      resultCell.appendChild(viewButton);
      row.appendChild(resultCell);

      const runsCell = document.createElement("td");
      runsCell.textContent = hasError ? "--" : String(result?.runs ?? "--");
      row.appendChild(runsCell);

      const statusCell = document.createElement("td");
      statusCell.textContent = hasError ? "error" : isBest ? "best" : "ok";
      row.appendChild(statusCell);

      tbody.appendChild(row);
    });

  table.appendChild(tbody);
  tableWrap.appendChild(table);
  output.appendChild(tableWrap);
  output.appendChild(resultPanel);
};

const createLineChart = (container, data, valueFormatter) => {
  container.innerHTML = "";
  const baseWidth = container.clientWidth || 640;
  const plotWidth = Math.max(baseWidth, data.length * 90);
  const height = 420;
  const padding = { top: 18, right: 16, bottom: 160, left: 56 };
  const maxValue = Math.max(...data.map((item) => item.value || 0), 1);
  const sizeValues = data.map((item) => item.size).filter(Number.isFinite);
  const minSize = sizeValues.length ? Math.min(...sizeValues) : null;
  const maxSize = sizeValues.length ? Math.max(...sizeValues) : null;
  const ticks = 4;
  const step = maxValue / ticks;

  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${plotWidth} ${height}`);
  svg.setAttribute("width", plotWidth);
  svg.setAttribute("height", "100%");
  container.style.overflowX = "auto";
  container.style.overflowY = "hidden";

  const tooltip = document.createElement("div");
  tooltip.className = "chart-tooltip";
  container.appendChild(tooltip);

  const chartWidth = plotWidth - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;
  const pointGap = data.length > 1 ? chartWidth / (data.length - 1) : 0;

  for (let i = 0; i <= ticks; i += 1) {
    const value = step * i;
    const y = padding.top + chartHeight - (value / maxValue) * chartHeight;
    const grid = document.createElementNS("http://www.w3.org/2000/svg", "line");
    grid.setAttribute("x1", padding.left);
    grid.setAttribute("x2", plotWidth - padding.right);
    grid.setAttribute("y1", y);
    grid.setAttribute("y2", y);
    grid.setAttribute("stroke", "rgba(79,87,79,0.2)");
    grid.setAttribute("stroke-dasharray", "3 4");
    svg.appendChild(grid);

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", padding.left - 8);
    label.setAttribute("y", y + 4);
    label.setAttribute("text-anchor", "end");
    label.setAttribute("font-size", "11");
    label.setAttribute("fill", "#4f574f");
    label.textContent = valueFormatter(value);
    svg.appendChild(label);
  }

  const points = data.map((item, index) => {
    const x = padding.left + pointGap * index;
    const y = padding.top + chartHeight - (item.value / maxValue) * chartHeight;
    return { x, y, item };
  });

  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  const d = points
    .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`)
    .join(" ");
  path.setAttribute("d", d);
  path.setAttribute("fill", "none");
  path.setAttribute("stroke", "#2f4a36");
  path.setAttribute("stroke-width", "3");
  svg.appendChild(path);

  const shouldRotate = false;
  points.forEach((point) => {
    const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    circle.setAttribute("cx", point.x);
    circle.setAttribute("cy", point.y);
    let radius = 5;
    if (Number.isFinite(point.item.size) && minSize !== null && maxSize !== null) {
      if (minSize === maxSize) {
        radius = 7;
      } else {
        const t = (point.item.size - minSize) / (maxSize - minSize);
        radius = 4 + t * 6;
      }
    }
    circle.setAttribute("r", radius.toFixed(2));
    circle.setAttribute("fill", "#e38b2c");
    circle.style.cursor = "pointer";

    circle.addEventListener("mousemove", (event) => {
      const containerRect = container.getBoundingClientRect();
      const sizeHint = point.item.sizeLabel ? ` · ${point.item.sizeLabel}` : "";
      tooltip.textContent = `${point.item.label}: ${valueFormatter(point.item.value)}${sizeHint}`;
      tooltip.style.left = `${event.clientX - containerRect.left}px`;
      tooltip.style.top = `${event.clientY - containerRect.top - 12}px`;
      tooltip.style.opacity = "1";
    });
    circle.addEventListener("mouseleave", () => {
      tooltip.style.opacity = "0";
    });

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    const lx = point.x;
    const ly = height - padding.bottom + 28;
    label.setAttribute("x", lx);
    label.setAttribute("y", ly);
    label.setAttribute("text-anchor", "middle");
    label.setAttribute("font-size", "11");
    label.setAttribute("fill", "#4f574f");
    const lines = _splitLabel(point.item.label);
    label.textContent = "";
    lines.forEach((line, idx) => {
      const tspan = document.createElementNS("http://www.w3.org/2000/svg", "tspan");
      tspan.setAttribute("x", lx);
      tspan.setAttribute("dy", idx === 0 ? "0" : "12");
      tspan.textContent = line;
      label.appendChild(tspan);
    });

    svg.appendChild(circle);
    svg.appendChild(label);
  });

  container.appendChild(svg);
};

const createMultiLineChart = (container, series, xLabels, valueFormatter) => {
  container.innerHTML = "";
  const baseWidth = container.clientWidth || 640;
  const plotWidth = Math.max(baseWidth, xLabels.length * 80);
  const height = 280;
  const padding = { top: 18, right: 24, bottom: 48, left: 56 };
  const allValues = series.flatMap((item) => item.values);
  const maxValue = Math.max(...allValues.filter(Number.isFinite), 1);
  const ticks = 4;
  const step = maxValue / ticks;

  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${plotWidth} ${height}`);
  svg.setAttribute("width", plotWidth);
  svg.setAttribute("height", "100%");
  container.style.overflowX = "auto";
  container.style.overflowY = "hidden";

  const tooltip = document.createElement("div");
  tooltip.className = "chart-tooltip";
  container.appendChild(tooltip);

  const chartWidth = plotWidth - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;
  const pointGap = xLabels.length > 1 ? chartWidth / (xLabels.length - 1) : 0;

  for (let i = 0; i <= ticks; i += 1) {
    const value = step * i;
    const y = padding.top + chartHeight - (value / maxValue) * chartHeight;
    const grid = document.createElementNS("http://www.w3.org/2000/svg", "line");
    grid.setAttribute("x1", padding.left);
    grid.setAttribute("x2", plotWidth - padding.right);
    grid.setAttribute("y1", y);
    grid.setAttribute("y2", y);
    grid.setAttribute("stroke", "rgba(79,87,79,0.2)");
    grid.setAttribute("stroke-dasharray", "3 4");
    svg.appendChild(grid);

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", padding.left - 8);
    label.setAttribute("y", y + 4);
    label.setAttribute("text-anchor", "end");
    label.setAttribute("font-size", "11");
    label.setAttribute("fill", "#4f574f");
    label.textContent = valueFormatter(value);
    svg.appendChild(label);
  }

  xLabels.forEach((labelText, index) => {
    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", padding.left + pointGap * index);
    label.setAttribute("y", height - 18);
    label.setAttribute("text-anchor", "middle");
    label.setAttribute("font-size", "12");
    label.setAttribute("fill", "#4f574f");
    label.textContent = labelText;
    svg.appendChild(label);
  });

  series.forEach((line) => {
    const points = line.values.map((value, index) => ({
      x: padding.left + pointGap * index,
      y: padding.top + chartHeight - (value / maxValue) * chartHeight,
      value,
      label: line.label,
    }));
    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    const d = points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
    path.setAttribute("d", d);
    path.setAttribute("fill", "none");
    path.setAttribute("stroke", line.color);
    path.setAttribute("stroke-width", "3");
    svg.appendChild(path);

    points.forEach((point) => {
      const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
      circle.setAttribute("cx", point.x);
      circle.setAttribute("cy", point.y);
      circle.setAttribute("r", "4.5");
      circle.setAttribute("fill", line.color);
      circle.style.cursor = "pointer";
      circle.addEventListener("mousemove", (event) => {
        const rect = container.getBoundingClientRect();
        tooltip.textContent = `${point.label}: ${valueFormatter(point.value)}`;
        tooltip.style.left = `${event.clientX - rect.left}px`;
        tooltip.style.top = `${event.clientY - rect.top - 12}px`;
        tooltip.style.opacity = "1";
      });
      circle.addEventListener("mouseleave", () => {
        tooltip.style.opacity = "0";
      });
      svg.appendChild(circle);
    });
  });

  container.appendChild(svg);
};

const _shortLabel = (label) => {
  return String(label)
    .replace("parquet_", "pq_")
    .replace("duckdb_table", "duckdb")
    .replace("vortex_default", "vortex")
    .replace("vortex_error", "vortex_err");
};

const createGroupedBarChart = (container, categories, series, valueFormatter) => {
  container.innerHTML = "";
  const baseWidth = container.clientWidth || 640;
  const plotWidth = Math.max(baseWidth, categories.length * 90);
  const height = 420;
  const padding = { top: 18, right: 40, bottom: 140, left: 64 };
  const allValues = series.flatMap((item) => item.values);
  const maxValue = Math.max(...allValues.filter(Number.isFinite), 1);
  const ticks = 4;
  const step = maxValue / ticks;
  const shouldRotate = false;

  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${plotWidth} ${height}`);
  svg.setAttribute("width", plotWidth);
  svg.setAttribute("height", "100%");
  container.style.overflowX = "auto";
  container.style.overflowY = "hidden";

  const tooltip = document.createElement("div");
  tooltip.className = "chart-tooltip";
  container.appendChild(tooltip);

  const chartWidth = plotWidth - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;
  const groupWidth = categories.length ? chartWidth / categories.length : 0;
  const barWidth = series.length ? (groupWidth * 0.7) / series.length : 0;

  for (let i = 0; i <= ticks; i += 1) {
    const value = step * i;
    const y = padding.top + chartHeight - (value / maxValue) * chartHeight;
    const grid = document.createElementNS("http://www.w3.org/2000/svg", "line");
    grid.setAttribute("x1", padding.left);
    grid.setAttribute("x2", plotWidth - padding.right);
    grid.setAttribute("y1", y);
    grid.setAttribute("y2", y);
    grid.setAttribute("stroke", "rgba(79,87,79,0.2)");
    grid.setAttribute("stroke-dasharray", "3 4");
    svg.appendChild(grid);

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", padding.left - 8);
    label.setAttribute("y", y + 4);
    label.setAttribute("text-anchor", "end");
    label.setAttribute("font-size", "11");
    label.setAttribute("fill", "#4f574f");
    label.textContent = valueFormatter(value);
    svg.appendChild(label);
  }

  categories.forEach((category, index) => {
    const xStart = padding.left + index * groupWidth + groupWidth * 0.15;
    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    let lx = padding.left + index * groupWidth + groupWidth * 0.5;
    const ly = height - 40;
    const minX = padding.left + 6;
    const maxX = plotWidth - padding.right - 6;
    lx = Math.min(Math.max(lx, minX), maxX);
    label.setAttribute("x", lx);
    label.setAttribute("y", ly);
    label.setAttribute("text-anchor", "middle");
    label.setAttribute("font-size", "11");
    label.setAttribute("fill", "#4f574f");
    const lines = _splitLabel(category);
    label.textContent = "";
    lines.forEach((line, idx) => {
      const tspan = document.createElementNS("http://www.w3.org/2000/svg", "tspan");
      tspan.setAttribute("x", lx);
      tspan.setAttribute("dy", idx === 0 ? "0" : "12");
      tspan.textContent = line;
      label.appendChild(tspan);
    });
    svg.appendChild(label);

    series.forEach((item, seriesIndex) => {
      const value = item.values[index] || 0;
      const barHeight = (value / maxValue) * chartHeight;
      const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      rect.setAttribute("x", xStart + seriesIndex * barWidth);
      rect.setAttribute("y", padding.top + chartHeight - barHeight);
      rect.setAttribute("width", barWidth * 0.9);
      rect.setAttribute("height", barHeight);
      rect.setAttribute("rx", 4);
      rect.setAttribute("fill", item.color);
      rect.style.cursor = "pointer";
      rect.addEventListener("mousemove", (event) => {
        const rectBox = container.getBoundingClientRect();
        tooltip.textContent = `${item.label}: ${valueFormatter(value)}`;
        tooltip.style.left = `${event.clientX - rectBox.left}px`;
        tooltip.style.top = `${event.clientY - rectBox.top - 12}px`;
        tooltip.style.opacity = "1";
      });
      rect.addEventListener("mouseleave", () => {
        tooltip.style.opacity = "0";
      });
      svg.appendChild(rect);
    });
  });

  container.appendChild(svg);
};

const setStatus = (message, state) => {
  const status = document.getElementById("run-status");
  if (!status) return;
  status.textContent = message;
  status.classList.remove("is-running", "is-error");
  if (state) status.classList.add(state);
};

const setError = (message) => {
  const box = document.getElementById("run-error");
  if (!box) return;
  if (message) {
    box.textContent = message;
    box.hidden = false;
  } else {
    box.textContent = "";
    box.hidden = true;
  }
};

const fillRunSummary = (dataset) => {
  if (!dataset) return;
  const totalCols = Object.values(dataset.column_type_counts || {}).reduce(
    (sum, val) => sum + (val || 0),
    0
  );
  document.getElementById("run-rows").textContent = formatNumber(dataset.rows || 0);
  document.getElementById("run-size").textContent = formatBytes(dataset.input_size_bytes || 0);
  document.getElementById("run-drop").textContent = formatNumber(dataset.dropped_rows || 0);
  document.getElementById("run-cols").textContent = formatNumber(totalCols);
  const caption = document.getElementById("run-caption");
  if (caption) {
    const label = dataset.name || "Uploaded dataset";
    caption.textContent = `${label} | ${formatNumber(dataset.rows || 0)} rows`;
  }
};

const renderPreview = (preview) => {
  const head = document.getElementById("preview-head");
  const body = document.getElementById("preview-body");
  if (!head || !body) return;
  head.innerHTML = "";
  body.innerHTML = "";
  if (!preview || !preview.columns || !preview.rows) {
    body.innerHTML = "<tr><td>No preview available.</td></tr>";
    return;
  }

  const headerRow = document.createElement("tr");
  preview.columns.forEach((col) => {
    const th = document.createElement("th");
    th.textContent = col;
    headerRow.appendChild(th);
  });
  head.appendChild(headerRow);

  preview.rows.forEach((row) => {
    const tr = document.createElement("tr");
    row.forEach((cell) => {
      const td = document.createElement("td");
      td.textContent = cell === null || cell === undefined ? "" : String(cell);
      tr.appendChild(td);
    });
    body.appendChild(tr);
  });
};

const renderDiagnostics = (report) => {
  const head = document.getElementById("diagnostics-head");
  const body = document.getElementById("diagnostics-body");
  const coldChart = document.getElementById("diagnostics-cold-chart");
  const baseChart = document.getElementById("diagnostics-baseline-chart");
  if (!head || !body) return;
  const formats = Object.entries(report.formats || {});
  if (!formats.length) return;

  head.innerHTML = "";
  body.innerHTML = "";
  const headerRow = document.createElement("tr");
  ["Format", "Full scan (ms)", "Cold (ms)", "Row groups", "Note"].forEach((label) => {
    const th = document.createElement("th");
    th.textContent = label;
    headerRow.appendChild(th);
  });
  head.appendChild(headerRow);

  formats.forEach(([name, data]) => {
    const tr = document.createElement("tr");
    const full = data.queries?.full_scan_min?.median_ms;
    const cold = data.queries?.full_scan_min?.cold_ms;
    const rowGroups = data.write?.row_group_count;
    const note = data.note || "";
    [name, formatMs(full), formatMs(cold), rowGroups ?? "--", note].forEach((val) => {
      const td = document.createElement("td");
      td.textContent = String(val ?? "--");
      tr.appendChild(td);
    });
    body.appendChild(tr);
  });

  if (coldChart) {
    const categories = formats.map(([name]) => name);
    const warm = formats.map(([, data]) => data.queries?.full_scan_min?.median_ms || 0);
    const cold = formats.map(([, data]) => data.queries?.full_scan_min?.cold_ms || 0);
    createGroupedBarChart(
      coldChart,
      categories,
      [
        { label: "Warm", color: "#2f4a36", values: warm },
        { label: "Cold", color: "#e38b2c", values: cold },
      ],
      (value) => formatMs(value)
    );
  }

  if (baseChart) {
    const categories = formats.map(([name]) => name);
    const values = formats.map(([, data]) => data.queries?.full_scan_min?.median_ms || 0);
    createGroupedBarChart(
      baseChart,
      categories,
      [{ label: "Median", color: "#4c6fa8", values }],
      (value) => formatMs(value)
    );
  }
};

const buildFormatCard = (name, data, isOverall, best) => {
  const card = document.createElement("div");
  card.className = "format-card";
  const epsilon = 1e-9;
  const isBestMax = (value, target) =>
    Number.isFinite(value) && Number.isFinite(target) && value >= target - epsilon;
  const isBestMin = (value, target) =>
    Number.isFinite(value) && Number.isFinite(target) && value <= target + epsilon;

  if (isOverall) {
    card.innerHTML = `
      <h3>${name}</h3>
      <div class="kv"><span>Compression ratio</span><strong>${formatNumber(
        data.compression_ratio_geomean,
        2
      )}</strong></div>
      <div class="kv"><span>Output size</span><strong>${formatBytes(
        data.output_size_bytes_geomean
      )}</strong></div>
      <div class="kv"><span>Compression time</span><strong>${formatNumber(
        data.compression_time_s_geomean,
        2
      )} s</strong></div>
      <div class="kv"><span>Compression speed</span><strong>${formatNumber(
        data.compression_speed_mb_s_geomean,
        2
      )} MB/s</strong></div>
      <div class="kv"><span>Full scan median</span><strong>${formatMs(
        data.query_median_ms_geomean?.full_scan_min
      )}</strong></div>
      <div class="kv"><span>Random access median</span><strong>${formatMs(
        data.query_median_ms_geomean?.random_access
      )}</strong></div>
    `;
  } else {
    card.innerHTML = `
      <h3>${name}</h3>
      <div class="kv ${isBestMax(data.compression_ratio, best.compression_ratio) ? "is-best" : ""}">
        <span>Compression ratio</span><strong>${formatNumber(data.compression_ratio, 2)}</strong>
      </div>
      <div class="kv ${isBestMin(data.write?.output_size_bytes, best.output_size_bytes) ? "is-best" : ""}">
        <span>Output size</span><strong>${formatBytes(data.write?.output_size_bytes)}</strong>
      </div>
      <div class="kv ${isBestMin(data.write?.compression_time_s, best.write_time) ? "is-best" : ""}">
        <span>Compression time</span><strong>${formatNumber(data.write?.compression_time_s, 2)} s</strong>
      </div>
      <div class="kv ${isBestMax(data.write?.compression_speed_mb_s, best.comp_speed) ? "is-best" : ""}">
        <span>Compression speed</span><strong>${formatNumber(data.write?.compression_speed_mb_s, 2)} MB/s</strong>
      </div>
      <div class="kv ${isBestMin(data.queries?.full_scan_min?.median_ms, best.full_scan) ? "is-best" : ""}">
        <span>Full scan median</span><strong>${formatMs(data.queries?.full_scan_min?.median_ms)}</strong>
      </div>
      <div class="kv ${isBestMin(data.queries?.random_access?.median_ms, best.random_access) ? "is-best" : ""}">
        <span>Random access median</span><strong>${formatMs(data.queries?.random_access?.median_ms)}</strong>
      </div>
    `;
  }

  return card;
};

const computeBestMetrics = (formats) => {
  const values = Object.values(formats || {});
  const getNums = (fn) =>
    values.map(fn).filter((value) => Number.isFinite(value));
  const min = (list) => (list.length ? Math.min(...list) : null);
  const max = (list) => (list.length ? Math.max(...list) : null);

  return {
    compression_ratio: max(getNums((item) => item.compression_ratio)),
    output_size_bytes: min(getNums((item) => item.write?.output_size_bytes)),
    write_time: min(getNums((item) => item.write?.compression_time_s)),
    comp_speed: max(getNums((item) => item.write?.compression_speed_mb_s)),
    full_scan: min(getNums((item) => item.queries?.full_scan_min?.median_ms)),
    random_access: min(getNums((item) => item.queries?.random_access?.median_ms)),
  };
};

const renderReportPreview = (data) => {
  const output = document.getElementById("report-output");
  if (!output) return;
  output.innerHTML = "";

  if (data.dataset_count && data.formats) {
    Object.entries(data.formats).forEach(([name, format]) => {
      output.appendChild(buildFormatCard(name, format, true));
    });
    return;
  }

  if (data.dataset && data.formats) {
    currentReport = data;
    fillRunSummary(data.dataset);
    const best = computeBestMetrics(data.formats || {});
    Object.entries(data.formats).forEach(([name, format]) => {
      output.appendChild(buildFormatCard(name, format, false, best));
    });
    renderUploadSelectivity(data.formats || {});
    renderUploadEncodings(data.formats || {});
    initUploadLikeSelect(data.formats || {});
  }
};

const renderPlots = (plots) => {
  const select = document.getElementById("plot-select");
  const preview = document.getElementById("plot-preview");
  if (!select) return;
  select.innerHTML = "";
  if (preview) {
    preview.src = "";
    preview.style.opacity = "0.3";
  }

  const selectPlot = (plot) => {
    if (preview) {
      preview.src = plot.url;
      preview.alt = plot.name;
      preview.style.opacity = "1";
    }
  };

  plots.forEach((plot, index) => {
    const option = document.createElement("option");
    option.value = plot.url;
    option.textContent = plot.name;
    select.appendChild(option);
    if (index === 0) {
      selectPlot(plot);
    }
  });

  select.addEventListener("change", () => {
    const selected = plots.find((plot) => plot.url === select.value);
    if (selected) selectPlot(selected);
  });
};

async function runBenchmark(file) {
  const schemaInput = document.getElementById("schema-file");
  const schemaFile = schemaInput?.files?.[0] || null;
  if (!file) {
    setStatus("Select a dataset file first.", "is-error");
    setError("");
    return;
  }

  const formData = new FormData();
  formData.append("dataset", file);
  if (schemaFile) {
    formData.append("schema", schemaFile);
  }
  const sortCol = document.getElementById("sort-col")?.value?.trim();
  if (sortCol) {
    formData.append("sort_col", sortCol);
  }
  setStatus("Processing benchmark...", "is-running");
  setError("");

  try {
    const response = await fetch("/api/run", {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      let message = "Benchmark failed";
      const errorText = await response.text();
      try {
        const data = JSON.parse(errorText);
        message = data.stderr || data.error || message;
      } catch (parseError) {
        message = errorText || message;
      }
      throw new Error(message);
    }

    const data = await response.json();
    if (data.report) {
      currentReport = data.report;
      renderReportPreview(data.report);
      renderUploadChart(data.report, document.getElementById("upload-metric")?.value);
      renderUploadSelectivity(data.report.formats || {});
      renderUploadEncodings(data.report.formats || {});
      initUploadLikeSelect(data.report.formats || {});
      renderDiagnostics(data.report);
    }
    if (data.plots) {
      renderPlots(data.plots);
    }
    if (data.preview) {
      renderPreview(data.preview);
    }
    if (data.upload) {
      lastUploadInfo = data.upload;
    }
    if (data.report_path) {
      lastReportPath = data.report_path;
    }
    if (data.manifest) {
      localStorage.setItem("latestManifest", JSON.stringify(data.manifest));
    }
    if (data.summary) {
      localStorage.setItem("latestSummary", JSON.stringify(data.summary));
    }
    setStatus("Benchmark complete. Results loaded.", "");
    setError("");
  } catch (error) {
    console.error("Benchmark run failed", error);
    const message = String(error?.message || "");
    if (message.includes("Failed to fetch") || message.includes("NetworkError")) {
      setStatus("Benchmark failed. Is the server running on port 5000?", "is-error");
      setError("Could not reach /api/run. Start the server and retry.");
      return;
    }
    setStatus("Benchmark failed. Check server logs.", "is-error");
    setError(error.message);
  }
}

const initDatasetUpload = () => {
  const input = document.getElementById("dataset-file");
  if (!input) return;

  input.addEventListener("change", (event) => {
    const file = event.target.files?.[0];
    if (!file) return;

    document.getElementById("file-name").textContent = file.name;
    document.getElementById("file-size").textContent = formatBytes(file.size);
    document.getElementById("file-type").textContent = "CSV";

    setStatus("Ready to run.", "");
    runBenchmark(file);
  });
};

const initCustomQuery = () => {
  const button = document.getElementById("custom-run");
  const sqlBox = document.getElementById("custom-sql");
  const repeatsInput = document.getElementById("custom-repeats");
  const warmupInput = document.getElementById("custom-warmup");
  const output = document.getElementById("custom-result");
  if (!button || !sqlBox || !output) return;

  button.addEventListener("click", async () => {
    if (!lastReportPath) {
      output.textContent = "Upload a dataset first.";
      return;
    }
    const sql = sqlBox.value.trim();
    if (!sql) {
      output.textContent = "Enter a SQL query.";
      return;
    }
    const repeats = Number(repeatsInput?.value || 5);
    const warmup = Number(warmupInput?.value || 1);
    output.textContent = "Running...";
    try {
      const response = await fetch("/api/query-formats", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          report_path: lastReportPath,
          sql,
          repeats,
          warmup,
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.error || "Query failed");
      }
      const data = await response.json();
      renderCustomQueryResults(output, data);
    } catch (err) {
      output.textContent = err.message || "Query failed";
    }
  });
};

const renderUploadChart = (report, metric) => {
  const container = document.getElementById("upload-chart");
  const title = document.getElementById("upload-chart-title");
  if (!container || !title) return;

  const metricMap = {
    compression_ratio: {
      label: "Compression ratio",
      getValue: (data) => data.compression_ratio,
      format: (value) => formatNumber(value, 2),
    },
    output_size: {
      label: "Output size",
      getValue: (data) => data.write?.output_size_bytes,
      format: (value) => formatBytes(value),
    },
    write_time: {
      label: "Compression time",
      getValue: (data) => data.write?.compression_time_s,
      format: (value) => `${formatNumber(value, 2)} s`,
    },
    full_scan: {
      label: "Full scan median",
      getValue: (data) => data.queries?.full_scan_min?.median_ms,
      format: (value) => formatMs(value),
    },
    selective: {
      label: "Selective predicate median",
      getValue: (data) => data.queries?.selective_predicate?.median_ms,
      format: (value) => formatMs(value),
    },
    random_access: {
      label: "Random access median",
      getValue: (data) => data.queries?.random_access?.median_ms,
      format: (value) => formatMs(value),
    },
  };

  const chosen = metricMap[metric] || metricMap.compression_ratio;
  const data = Object.entries(report.formats || {}).map(([name, values]) => ({
    label: name,
    value: chosen.getValue(values) || 0,
    size: values.write?.output_size_bytes,
    sizeLabel: values.write?.output_size_bytes
      ? `Output ${formatBytes(values.write.output_size_bytes)}`
      : "",
  }));

  title.textContent = chosen.label;
  createLineChart(container, data, chosen.format);
};

const renderUploadSelectivity = (formats) => {
  const select = document.getElementById("upload-selectivity-select");
  const container = document.getElementById("upload-selectivity-chart");
  if (!select || !container) return;
  select.innerHTML = "";
  const entries = Object.entries(formats || {});
  const first = entries[0]?.[1];
  const columns = Object.keys(first?.queries?.selectivity_by_col || {});
  if (!columns.length) {
    container.textContent = "Selectivity data not available.";
    return;
  }
  columns.forEach((col, index) => {
    const option = document.createElement("option");
    option.value = col;
    option.textContent = col;
    select.appendChild(option);
    if (index === 0) select.value = col;
  });

  const render = () => {
    const xLabels = [];
    const series = [];
    entries.forEach(([name, format]) => {
      const points = format.queries?.selectivity_by_col?.[select.value] || [];
      if (!xLabels.length) {
        points.forEach((item) => xLabels.push(`${Math.round(item.p * 100)}%`));
      }
      series.push({
        label: name,
        color: getFormatColor(name),
        values: points.map((item) => item.median_ms || 0),
      });
    });
    if (!xLabels.length) {
      container.textContent = "Selectivity data not available.";
      return;
    }
    createMultiLineChart(container, series, xLabels, (value) => formatMs(value));
  };
  render();
  select.addEventListener("change", render);
};

const renderUploadEncodings = (formats) => {
  const select = document.getElementById("upload-encoding-select");
  const container = document.getElementById("upload-encoding-list");
  if (!select || !container) return;
  const entries = Object.entries(formats || {});
  select.innerHTML = "";
  const expanded = new Set();

  const renderFormat = (name, data) => {
    const perColumn = data.encodings?.per_column || {};
    const columns = Object.entries(perColumn);
    const showAll = expanded.has(name);
    const visible = showAll ? columns : columns.slice(0, 8);
    const more = columns.length - visible.length;
    const list = visible.map(([col, enc]) => `${col}: ${enc.join(", ")}`).join("<br />");
    container.innerHTML = `
      <div class="encoding-group">
        <div class="encoding-title">${name}</div>
        <div class="encoding-body">${list || "No encoding metadata."}${
          more > 0
            ? `<button class="encoding-more" type="button">+${more} more columns</button>`
            : ""
        }</div>
      </div>
    `;
    const moreButton = container.querySelector(".encoding-more");
    if (moreButton) {
      moreButton.addEventListener("click", () => {
        expanded.add(name);
        renderFormat(name, data);
      });
    }
  };

  entries.forEach(([name], index) => {
    const option = document.createElement("option");
    option.value = name;
    option.textContent = name;
    select.appendChild(option);
    if (index === 0) select.value = name;
  });

  const render = () => {
    const match = entries.find(([name]) => name === select.value);
    if (match) renderFormat(match[0], match[1]);
  };
  render();
  select.addEventListener("change", render);
};

const renderUploadLikeChart = (formatName, format, column) => {
  const container = document.getElementById("upload-like-chart");
  if (!container) return;
  const likeByCol = format?.queries?.like_by_col || {};
  const rows = [];
  const columns = column ? [column] : Object.keys(likeByCol);
  columns.forEach((col) => {
    (likeByCol[col] || []).forEach((item) => {
      if (Number.isFinite(item.median_ms)) rows.push(item);
    });
  });
  if (!rows.length) {
    container.textContent = "No LIKE predicate data.";
    return;
  }

  const byType = rows.reduce((acc, item) => {
    const key = item.pattern_type || "unknown";
    const slot = acc[key] || { count: 0, total: 0 };
    slot.count += 1;
    slot.total += item.median_ms || 0;
    acc[key] = slot;
    return acc;
  }, {});

  const categories = ["prefix", "suffix", "contains"];
  const series = [
    {
      label: formatName,
      color: getFormatColor(formatName),
      values: categories.map((key) => byType[key]?.total / byType[key]?.count || 0),
    },
  ];
  createGroupedBarChart(container, categories, series, (value) => formatMs(value));
};

const renderUploadLikeSummary = (formats, activeFormat, activeColumn) => {
  const summary = document.getElementById("upload-like-summary");
  if (!summary) return;
  summary.innerHTML = "";
  const format = formats?.[activeFormat];
  const likeByCol = format?.queries?.like_by_col || {};
  const rows = [];
  const columns = activeColumn ? [activeColumn] : Object.keys(likeByCol);
  columns.forEach((col) => {
    (likeByCol[col] || []).forEach((item) => rows.push(item));
  });
  if (!rows.length) {
    summary.textContent = "LIKE predicates not available for this dataset.";
    return;
  }
  const byType = rows.reduce((acc, item) => {
    const key = item.pattern_type || "unknown";
    const slot = acc[key] || { count: 0, total: 0 };
    slot.count += 1;
    slot.total += item.median_ms || 0;
    acc[key] = slot;
    return acc;
  }, {});
  const lines = Object.entries(byType)
    .map(
      ([key, value]) =>
        `<div class="like-row"><span>${key}</span><strong>${value.count} patterns · ${formatMs(
          value.total / value.count
        )}</strong></div>`
    )
    .join("");
  summary.innerHTML = `<div class="like-card"><div class="like-title">${activeFormat}</div>${lines}</div>`;
};

const initUploadLikeSelect = (formats) => {
  const select = document.getElementById("upload-like-select");
  const columnSelect = document.getElementById("upload-like-column-select");
  if (!select || !columnSelect) return;
  select.innerHTML = "";
  const entries = Object.entries(formats || {});
  entries.forEach(([name], index) => {
    const option = document.createElement("option");
    option.value = name;
    option.textContent = name;
    select.appendChild(option);
    if (index === 0) select.value = name;
  });

  const render = () => {
    const match = entries.find(([name]) => name === select.value);
    if (!match) return;
    const likeByCol = match[1].queries?.like_by_col || {};
    columnSelect.innerHTML = "";
    Object.keys(likeByCol).forEach((col, index) => {
      const option = document.createElement("option");
      option.value = col;
      option.textContent = col;
      columnSelect.appendChild(option);
      if (index === 0) columnSelect.value = col;
    });
    const activeColumn = columnSelect.value;
    renderUploadLikeChart(match[0], match[1], activeColumn);
    renderUploadLikeSummary(formats, match[0], activeColumn);
  };

  render();
  select.addEventListener("change", render);
  columnSelect.addEventListener("change", render);
};

const initPlotModal = () => {
  const modal = document.getElementById("plot-modal");
  const full = document.getElementById("plot-full");
  const preview = document.getElementById("plot-preview");
  const exit = document.getElementById("plot-exit");
  const backdrop = modal?.querySelector(".modal-backdrop");
  if (!modal || !full || !preview || !exit || !backdrop) return;

  const openModal = () => {
    if (!preview.src) return;
    full.src = preview.src;
    full.alt = preview.alt || "Plot";
    modal.hidden = false;
  };

  const closeModal = () => {
    modal.hidden = true;
  };

  preview.addEventListener("click", openModal);
  exit.addEventListener("click", closeModal);
  backdrop.addEventListener("click", closeModal);
  window.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeModal();
  });
};

initDatasetUpload();
initPlotModal();
initCustomQuery();

const metricSelect = document.getElementById("upload-metric");
if (metricSelect) {
  metricSelect.addEventListener("change", () => {
    if (currentReport) {
      renderUploadChart(currentReport, metricSelect.value);
    }
  });
  window.addEventListener("resize", () => {
    if (currentReport) {
      renderUploadChart(currentReport, metricSelect.value);
    }
  });
}

const initReveal = () => {
  const targets = document.querySelectorAll(".panel, .stats-grid, .chart-card, .plot-viewer");
  targets.forEach((item, index) => {
    item.classList.add("reveal");
    item.style.transitionDelay = `${Math.min(index * 40, 240)}ms`;
  });

  const observer = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          entry.target.classList.add("is-visible");
          observer.unobserve(entry.target);
        }
      });
    },
    { threshold: 0.2 }
  );

  targets.forEach((item) => observer.observe(item));
};

initReveal();
