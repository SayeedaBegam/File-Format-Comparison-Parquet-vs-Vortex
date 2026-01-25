const DATASETS_URL = "./data/datasets.json";

const formatNumber = (value, digits = 0) => {
  if (value === null || value === undefined || !Number.isFinite(value)) return "--";
  return value.toLocaleString(undefined, {
    maximumFractionDigits: digits,
  });
};

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

const formatMs = (ms) => {
  if (!Number.isFinite(ms)) return "--";
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`;
  return `${ms.toFixed(2)} ms`;
};

let currentReport = null;

const createLineChart = (container, data, valueFormatter) => {
  container.innerHTML = "";
  const width = container.clientWidth || 640;
  const height = 280;
  const padding = { top: 18, right: 16, bottom: 48, left: 56 };
  const maxValue = Math.max(...data.map((item) => item.value || 0), 1);
  const ticks = 4;
  const step = maxValue / ticks;

  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("width", "100%");
  svg.setAttribute("height", "100%");

  const tooltip = document.createElement("div");
  tooltip.className = "chart-tooltip";
  container.appendChild(tooltip);

  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;
  const pointGap = data.length > 1 ? chartWidth / (data.length - 1) : 0;

  for (let i = 0; i <= ticks; i += 1) {
    const value = step * i;
    const y = padding.top + chartHeight - (value / maxValue) * chartHeight;
    const grid = document.createElementNS("http://www.w3.org/2000/svg", "line");
    grid.setAttribute("x1", padding.left);
    grid.setAttribute("x2", width - padding.right);
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

  points.forEach((point) => {
    const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    circle.setAttribute("cx", point.x);
    circle.setAttribute("cy", point.y);
    circle.setAttribute("r", "5");
    circle.setAttribute("fill", "#e38b2c");
    circle.style.cursor = "pointer";

    circle.addEventListener("mousemove", (event) => {
      const containerRect = container.getBoundingClientRect();
      tooltip.textContent = `${point.item.label}: ${valueFormatter(point.item.value)}`;
      tooltip.style.left = `${event.clientX - containerRect.left}px`;
      tooltip.style.top = `${event.clientY - containerRect.top - 12}px`;
      tooltip.style.opacity = "1";
    });
    circle.addEventListener("mouseleave", () => {
      tooltip.style.opacity = "0";
    });

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", point.x);
    label.setAttribute("y", height - 18);
    label.setAttribute("text-anchor", "middle");
    label.setAttribute("font-size", "12");
    label.setAttribute("fill", "#4f574f");
    label.textContent = point.item.label.replace("parquet_", "pq_");

    svg.appendChild(circle);
    svg.appendChild(label);
  });

  container.appendChild(svg);
};

const formatColors = {
  parquet_zstd: "#2f4a36",
  parquet_snappy: "#e38b2c",
  parquet_uncompressed: "#4c6fa8",
  vortex_default: "#a84c6f",
};

const getFormatColor = (label) => formatColors[label] || "#6b6358";

const createMultiLineChart = (container, series, xLabels, valueFormatter) => {
  container.innerHTML = "";
  const width = container.clientWidth || 640;
  const height = 280;
  const padding = { top: 18, right: 24, bottom: 48, left: 56 };
  const allValues = series.flatMap((item) => item.values);
  const maxValue = Math.max(...allValues.filter(Number.isFinite), 1);
  const ticks = 4;
  const step = maxValue / ticks;

  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("width", "100%");
  svg.setAttribute("height", "100%");

  const tooltip = document.createElement("div");
  tooltip.className = "chart-tooltip";
  container.appendChild(tooltip);

  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;
  const pointGap = xLabels.length > 1 ? chartWidth / (xLabels.length - 1) : 0;

  for (let i = 0; i <= ticks; i += 1) {
    const value = step * i;
    const y = padding.top + chartHeight - (value / maxValue) * chartHeight;
    const grid = document.createElementNS("http://www.w3.org/2000/svg", "line");
    grid.setAttribute("x1", padding.left);
    grid.setAttribute("x2", width - padding.right);
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

const createGroupedBarChart = (container, categories, series, valueFormatter) => {
  container.innerHTML = "";
  const width = container.clientWidth || 640;
  const height = 280;
  const padding = { top: 18, right: 24, bottom: 56, left: 56 };
  const allValues = series.flatMap((item) => item.values);
  const maxValue = Math.max(...allValues.filter(Number.isFinite), 1);
  const ticks = 4;
  const step = maxValue / ticks;

  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("width", "100%");
  svg.setAttribute("height", "100%");

  const tooltip = document.createElement("div");
  tooltip.className = "chart-tooltip";
  container.appendChild(tooltip);

  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;
  const groupWidth = categories.length ? chartWidth / categories.length : 0;
  const barWidth = series.length ? (groupWidth * 0.7) / series.length : 0;

  for (let i = 0; i <= ticks; i += 1) {
    const value = step * i;
    const y = padding.top + chartHeight - (value / maxValue) * chartHeight;
    const grid = document.createElementNS("http://www.w3.org/2000/svg", "line");
    grid.setAttribute("x1", padding.left);
    grid.setAttribute("x2", width - padding.right);
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
    label.setAttribute("x", padding.left + index * groupWidth + groupWidth * 0.5);
    label.setAttribute("y", height - 22);
    label.setAttribute("text-anchor", "middle");
    label.setAttribute("font-size", "12");
    label.setAttribute("fill", "#4f574f");
    label.textContent = category;
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

const fillDatasetSummary = (dataset) => {
  document.getElementById("ds-rows").textContent = formatNumber(dataset.rows);
  document.getElementById("ds-size").textContent = formatBytes(dataset.input_size_bytes);
  document.getElementById("ds-drop").textContent = formatNumber(dataset.dropped_rows || 0);
  const totalCols = Object.values(dataset.column_type_counts || {}).reduce(
    (sum, val) => sum + (val || 0),
    0
  );
  document.getElementById("ds-cols").textContent = formatNumber(totalCols);
};

const buildFormatCard = (name, data, best) => {
  const write = data.write || {};
  const queries = data.queries || {};
  const validation = data.validation || {};
  const epsilon = 1e-9;
  const isBestMax = (value, target) =>
    Number.isFinite(value) && Number.isFinite(target) && value >= target - epsilon;
  const isBestMin = (value, target) =>
    Number.isFinite(value) && Number.isFinite(target) && value <= target + epsilon;

  const card = document.createElement("div");
  card.className = "format-card";
  card.innerHTML = `
    <h3>${name}</h3>
    <div class="kv ${isBestMax(data.compression_ratio, best.compression_ratio) ? "is-best" : ""}">
      <span>Compression ratio</span><strong>${formatNumber(data.compression_ratio, 2)}</strong>
    </div>
    <div class="kv ${isBestMin(write.output_size_bytes, best.output_size_bytes) ? "is-best" : ""}">
      <span>Output size</span><strong>${formatBytes(write.output_size_bytes)}</strong>
    </div>
    <div class="kv ${isBestMin(write.compression_time_s, best.write_time) ? "is-best" : ""}">
      <span>Compression time</span><strong>${formatNumber(write.compression_time_s, 2)} s</strong>
    </div>
    <div class="kv ${isBestMin(queries.full_scan_min?.median_ms, best.full_scan) ? "is-best" : ""}">
      <span>Full scan median</span><strong>${formatMs(queries.full_scan_min?.median_ms)}</strong>
    </div>
    <div class="kv ${isBestMin(queries.selective_predicate?.median_ms, best.selective) ? "is-best" : ""}">
      <span>Selective median</span><strong>${formatMs(queries.selective_predicate?.median_ms)}</strong>
    </div>
    <div class="kv ${isBestMin(queries.random_access?.median_ms, best.random_access) ? "is-best" : ""}">
      <span>Random access median</span><strong>${formatMs(queries.random_access?.median_ms)}</strong>
    </div>
    <div class="kv"><span>Validation</span><strong>${
      validation.count_match === false ||
      validation.min_match === false ||
      validation.filtered_count_match === false
        ? "Mismatch"
        : "OK"
    }</strong></div>
  `;
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
    full_scan: min(getNums((item) => item.queries?.full_scan_min?.median_ms)),
    selective: min(getNums((item) => item.queries?.selective_predicate?.median_ms)),
    random_access: min(getNums((item) => item.queries?.random_access?.median_ms)),
  };
};

const renderDatasetReport = (report) => {
  const caption = document.getElementById("dataset-caption");
  const grid = document.getElementById("format-detail-grid");
  if (!caption || !grid) return;

  currentReport = report;
  const label = report.dataset?.name || "Dataset";
  caption.textContent = `${label} | ${report.dataset?.rows?.toLocaleString?.() ?? "--"} rows`;

  fillDatasetSummary(report.dataset || {});
  grid.innerHTML = "";

  const best = computeBestMetrics(report.formats || {});
  Object.entries(report.formats || {}).forEach(([name, data]) => {
    grid.appendChild(buildFormatCard(name, data, best));
  });

  renderDatasetChart(report, document.getElementById("dataset-metric")?.value);
  renderDetails(report);
};

const renderColumnTypes = (dataset) => {
  const bar = document.getElementById("col-type-bars");
  const legend = document.getElementById("col-type-legend");
  if (!bar || !legend) return;
  bar.innerHTML = "";
  legend.innerHTML = "";

  const order = [
    ["numeric", "#2f4a36"],
    ["text", "#e38b2c"],
    ["date", "#4c6fa8"],
    ["bool", "#a84c6f"],
    ["other", "#8a7e6b"],
  ];
  const counts = dataset?.column_type_counts || {};
  const total = order.reduce((sum, [key]) => sum + (counts[key] || 0), 0);
  if (!total) {
    bar.textContent = "No column type data.";
    return;
  }

  order.forEach(([key, color]) => {
    const value = counts[key] || 0;
    if (!value) return;
    const segment = document.createElement("div");
    segment.className = "stacked-segment";
    segment.style.width = `${(value / total) * 100}%`;
    segment.style.background = color;
    bar.appendChild(segment);

    const item = document.createElement("div");
    item.className = "legend-item";
    item.innerHTML = `<span class="legend-dot" style="background:${color}"></span>${key} (${value})`;
    legend.appendChild(item);
  });
};

const renderNdvRatios = (dataset) => {
  const container = document.getElementById("ndv-bars");
  if (!container) return;
  container.innerHTML = "";
  const ratios = dataset?.ndv_ratio_by_type || {};
  const items = Object.entries(ratios).filter(([, value]) => Number.isFinite(value));
  if (!items.length) {
    container.textContent = "NDV ratios not available.";
    return;
  }

  items.forEach(([key, value]) => {
    const row = document.createElement("div");
    row.className = "mini-bar-row";
    row.innerHTML = `
      <div class="mini-bar-label">${key}</div>
      <div class="mini-bar-track">
        <div class="mini-bar-fill" style="width:${Math.min(value * 100, 100)}%"></div>
      </div>
      <div class="mini-bar-value">${value.toFixed(3)}</div>
    `;
    container.appendChild(row);
  });
};

const renderRecommendations = (formats) => {
  const container = document.getElementById("recommendations");
  if (!container) return;
  container.innerHTML = "";
  const values = Object.entries(formats || {});
  if (!values.length) {
    container.textContent = "No recommendations available.";
    return;
  }

  const bestCompression = values.reduce((best, [name, data]) => {
    const value = data.compression_ratio ?? -Infinity;
    return value > best.value ? { name, value } : best;
  }, { name: "", value: -Infinity });

  const bestRandom = values.reduce((best, [name, data]) => {
    const value = data.queries?.random_access?.median_ms;
    return Number.isFinite(value) && value < best.value ? { name, value } : best;
  }, { name: "", value: Infinity });

  const bestScan = values.reduce((best, [name, data]) => {
    const value = data.queries?.full_scan_min?.median_ms;
    return Number.isFinite(value) && value < best.value ? { name, value } : best;
  }, { name: "", value: Infinity });

  const recs = [
    { label: "Storage-first", format: bestCompression.name },
    { label: "Read-latency-first", format: bestRandom.name },
    { label: "Scan-first", format: bestScan.name },
  ];

  recs.forEach((rec) => {
    const card = document.createElement("div");
    card.className = "rec-card";
    if (rec.format) card.classList.add("is-selected");
    card.innerHTML = `
      <div class="rec-label">${rec.label}</div>
      <div class="rec-value">${rec.format || "N/A"}</div>
    `;
    container.appendChild(card);
  });
};

const renderEncodings = (formats) => {
  const container = document.getElementById("encoding-list");
  const select = document.getElementById("encoding-select");
  if (!container) return;
  container.innerHTML = "";
  const entries = Object.entries(formats || {});
  if (!entries.length) {
    container.textContent = "No encoding data.";
    return;
  }

  if (select) {
    select.innerHTML = "";
  }

  const expanded = new Set();
  const renderFormat = (name, data) => {
    const group = document.createElement("div");
    group.className = "encoding-group";
    const perColumn = data.encodings?.per_column || {};
    const columns = Object.entries(perColumn);
    const showAll = expanded.has(name);
    const visible = showAll ? columns : columns.slice(0, 8);
    const more = columns.length - visible.length;
    const list = visible.map(([col, enc]) => `${col}: ${enc.join(", ")}`).join("<br />");
    group.innerHTML = `
      <div class="encoding-title">${name}</div>
      <div class="encoding-body">${list || "No encoding metadata."}${
        more > 0
          ? `<button class="encoding-more" type="button">+${more} more columns</button>`
          : ""
      }</div>
    `;
    container.innerHTML = "";
    container.appendChild(group);

    const moreButton = group.querySelector(".encoding-more");
    if (moreButton) {
      moreButton.addEventListener("click", () => {
        expanded.add(name);
        renderFormat(name, data);
      });
    }
  };

  entries.forEach(([name, data], index) => {
    if (select) {
      const option = document.createElement("option");
      option.value = name;
      option.textContent = name;
      select.appendChild(option);
      if (index === 0) {
        select.value = name;
        renderFormat(name, data);
      }
    } else if (index === 0) {
      renderFormat(name, data);
    }
  });

  if (select) {
    select.addEventListener("change", () => {
      const match = entries.find(([name]) => name === select.value);
      if (match) renderFormat(match[0], match[1]);
    });
  }
};

const renderSelectivityChart = (formats, selectCol) => {
  const container = document.getElementById("selectivity-chart");
  const note = document.getElementById("selectivity-note");
  if (!container) return;
  const entries = Object.entries(formats || {});
  if (!entries.length) return;

  const series = [];
  let xLabels = [];
  entries.forEach(([name, format]) => {
    const selectivity = format.queries?.selectivity_by_col || {};
    const points = selectivity[selectCol] || [];
    if (!xLabels.length && points.length) {
      xLabels = points.map((item) => `${Math.round(item.p * 100)}%`);
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

  if (note) {
    const list = Object.entries(formats || {})
      .map(([name, format]) => `${name}: ${format.best_select_col || "n/a"}`)
      .join(" · ");
    note.textContent = list || "Best selectivity column not available.";
  }
};

const renderLikeSummary = (formats, activeFormat, activeColumn) => {
  const summary = document.getElementById("like-summary");
  const table = document.getElementById("like-table");
  if (!summary || !table) return;
  summary.innerHTML = "";
  table.innerHTML = "";

  const entries = Object.entries(formats || {});
  if (!entries.length) {
    summary.textContent = "No LIKE predicate data.";
    return;
  }

  entries.forEach(([name, format]) => {
    if (activeFormat && name !== activeFormat) return;
    const likeByCol = format.queries?.like_by_col || {};
    const rows = [];
    const columns = activeColumn ? [activeColumn] : Object.keys(likeByCol);
    columns.forEach((col) => {
      (likeByCol[col] || []).forEach((item) => rows.push(item));
    });

    if (!rows.length) {
      const note = document.createElement("div");
      note.className = "like-note";
      note.textContent = `${name}: LIKE predicates not available.`;
      summary.appendChild(note);
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

    const card = document.createElement("div");
    card.className = "like-card";
    const lines = Object.entries(byType)
      .map(
        ([key, value]) =>
          `<div class="like-row"><span>${key}</span><strong>${value.count} patterns · ${formatMs(
            value.total / value.count
          )}</strong></div>`
      )
      .join("");
    card.innerHTML = `<div class="like-title">${name}</div>${lines}`;
    summary.appendChild(card);

    const top = rows
      .filter((item) => Number.isFinite(item.median_ms))
      .sort((a, b) => b.median_ms - a.median_ms)
      .slice(0, 5);
    const section = document.createElement("div");
    section.className = "like-table-section";
    section.innerHTML = `
      <div class="like-title">${name} · Slowest patterns</div>
      ${top
        .map(
          (item) =>
            `<div class="like-table-row"><span>${item.pattern_type} on ${item.pattern}</span><strong>${formatMs(
              item.median_ms
            )}</strong></div>`
        )
        .join("")}
    `;
    table.appendChild(section);
  });
};

const renderLikeChart = (formatName, format, column) => {
  const container = document.getElementById("like-chart");
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

const initLikeSelect = (formats) => {
  const select = document.getElementById("like-select");
  const columnSelect = document.getElementById("like-column-select");
  if (!select) return;
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
    if (match) {
      const likeByCol = match[1].queries?.like_by_col || {};
      if (columnSelect) {
        columnSelect.innerHTML = "";
        Object.keys(likeByCol).forEach((col, index) => {
          const option = document.createElement("option");
          option.value = col;
          option.textContent = col;
          columnSelect.appendChild(option);
          if (index === 0) columnSelect.value = col;
        });
      }
      const activeColumn = columnSelect?.value;
      renderLikeChart(match[0], match[1], activeColumn);
      renderLikeSummary(formats, match[0], activeColumn);
    }
  };
  render();
  select.addEventListener("change", render);
  if (columnSelect) {
    columnSelect.addEventListener("change", render);
  }
};

const initSelectivitySelect = (formats) => {
  const select = document.getElementById("selectivity-select");
  if (!select) return;
  select.innerHTML = "";
  const entries = Object.entries(formats || {});
  const first = entries[0]?.[1];
  const columns = Object.keys(first?.queries?.selectivity_by_col || {});
  columns.forEach((col, index) => {
    const option = document.createElement("option");
    option.value = col;
    option.textContent = col;
    select.appendChild(option);
    if (index === 0) select.value = col;
  });
  const render = () => {
    renderSelectivityChart(formats, select.value);
  };
  render();
  select.addEventListener("change", render);
};

const renderDetails = (report) => {
  renderColumnTypes(report.dataset || {});
  renderNdvRatios(report.dataset || {});
  renderRecommendations(report.formats || {});
  renderEncodings(report.formats || {});
  initSelectivitySelect(report.formats || {});
  initLikeSelect(report.formats || {});
};

const renderDatasetChart = (report, metric) => {
  const container = document.getElementById("dataset-chart");
  const title = document.getElementById("dataset-chart-title");
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
  }));

  title.textContent = chosen.label;
  createLineChart(container, data, chosen.format);
};

const renderSidebar = (manifest, activeName, filterText) => {
  const list = document.getElementById("dataset-list");
  if (!list) return;
  list.innerHTML = "";

  const query = filterText?.toLowerCase?.() ?? "";
  manifest.datasets
    .filter((item) => item.name.toLowerCase().includes(query))
    .forEach((item) => {
      const link = document.createElement("a");
      link.href = `./dataset.html?name=${encodeURIComponent(item.name)}`;
      link.className = `sidebar-item${item.name === activeName ? " is-active" : ""}`;
      link.innerHTML = `<div>${item.name}</div><span>report</span>`;
      list.appendChild(link);
    });
};

const renderOverallUpload = (summary) => {
  const caption = document.getElementById("upload-caption");
  const output = document.getElementById("upload-output");
  if (!caption || !output) return;

  caption.textContent = `Overall summary: ${summary.dataset_count} datasets`;
  output.innerHTML = "";

  Object.entries(summary.formats || {}).forEach(([name, data]) => {
    const card = document.createElement("div");
    card.className = "format-card";
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
      <div class="kv"><span>Full scan median</span><strong>${formatMs(
        data.query_median_ms_geomean?.full_scan_min
      )}</strong></div>
      <div class="kv"><span>Random access median</span><strong>${formatMs(
        data.query_median_ms_geomean?.random_access
      )}</strong></div>
    `;
    output.appendChild(card);
  });
};

const loadDataset = async (datasetName, manifest) => {
  const entry = manifest.datasets.find((item) => item.name === datasetName);
  if (!entry) return;
  const response = await fetch(entry.report);
  const report = await response.json();
  renderDatasetReport(report);
};

const getQueryDataset = () => {
  const params = new URLSearchParams(window.location.search);
  return params.get("name");
};

const initUpload = () => {
  const input = document.getElementById("upload-json");
  if (!input) return;
  input.addEventListener("change", async (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    const text = await file.text();
    const data = JSON.parse(text);
    if (data.dataset && data.formats) {
      renderDatasetReport(data);
    } else if (data.dataset_count && data.formats) {
      renderOverallUpload(data);
    }
  });
};

const init = async () => {
  const response = await fetch(DATASETS_URL);
  const manifest = await response.json();
  const select = document.getElementById("dataset-select");
  const loadButton = document.getElementById("load-dataset");
  const search = document.getElementById("dataset-search");
  const metricSelect = document.getElementById("dataset-metric");
  if (!select || !loadButton) return;

  select.innerHTML = "";
  manifest.datasets.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.name;
    option.textContent = item.name;
    select.appendChild(option);
  });

  const initial = getQueryDataset() || manifest.datasets[0]?.name;
  if (initial) {
    select.value = initial;
    await loadDataset(initial, manifest);
  }
  renderSidebar(manifest, initial, "");

  loadButton.addEventListener("click", async () => {
    await loadDataset(select.value, manifest);
    renderSidebar(manifest, select.value, search?.value || "");
  });

  initUpload();

  if (metricSelect) {
    metricSelect.addEventListener("change", () => {
      if (currentReport) {
        renderDatasetChart(currentReport, metricSelect.value);
      }
    });
    window.addEventListener("resize", () => {
      if (currentReport) {
        renderDatasetChart(currentReport, metricSelect.value);
      }
    });
  }

  if (search) {
    search.addEventListener("input", () => {
      renderSidebar(manifest, select.value, search.value);
    });
  }
};

init().catch((error) => {
  console.error("Failed to load dataset data", error);
});

const initInteractiveView = () => {
  const viewSelect = document.getElementById("interactive-view");
  const metricBlock = document.getElementById("metric-chart-block");
  const selectivityBlock = document.getElementById("selectivity-block");
  const likeBlock = document.getElementById("like-block");
  if (!viewSelect || !metricBlock || !selectivityBlock || !likeBlock) return;

  const updateView = () => {
    const value = viewSelect.value;
    metricBlock.style.display = value === "metric" ? "block" : "none";
    selectivityBlock.style.display = value === "selectivity" ? "grid" : "none";
    likeBlock.style.display = value === "like" ? "grid" : "none";
  };

  updateView();
  viewSelect.addEventListener("change", updateView);
};

initInteractiveView();

const initReveal = () => {
  const targets = document.querySelectorAll(
    ".sidebar, .panel, .stats-grid, .format-grid, .chart-card"
  );
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
