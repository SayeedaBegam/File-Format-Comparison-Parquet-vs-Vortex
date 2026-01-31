const DATASETS_URL = "./data/datasets.json";
const SUMMARY_URL = "../out/overall_summary.json";

const loadCached = (key) => {
  try {
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : null;
  } catch (err) {
    return null;
  }
};

const formatNumber = (value, digits = 0) => {
  if (value === null || value === undefined) return "--";
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

const formatMsWithP95 = (medianMs, p95Ms) => {
  const median = formatMs(medianMs);
  if (!Number.isFinite(p95Ms)) return median;
  const p95 = formatMs(p95Ms);
  return `${median}<span class="kv-sub">p95 ${p95}</span>`;
};

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

const createLineChart = (container, data, valueFormatter) => {
  container.innerHTML = "";
  const baseWidth = container.clientWidth || 640;
  const plotWidth = Math.max(baseWidth, data.length * 140);
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
  container.style.scrollbarGutter = "stable both-edges";

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
  points.forEach((point, index) => {
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
      const metaHint = point.item.metaLabel ? ` · ${point.item.metaLabel}` : "";
      tooltip.textContent = `${point.item.label}: ${valueFormatter(point.item.value)}${sizeHint}${metaHint}`;
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

const formatDisplayName = (name) => {
  if (name === "duckdb_table") return "duckdb_table (baseline)";
  if (name === "vortex_default") return "vortex default";
  return name;
};

const renderLeaderboard = (formats) => {
  const container = document.getElementById("format-leaderboard");
  if (!container) return;
  container.innerHTML = "";

  const entries = Object.entries(formats)
    .filter(([name]) => name !== "duckdb_table")
    .map(([name, data]) => ({
      name,
      score: data.query_median_ms_geomean?.random_access ?? null,
    }));

  const valid = entries.filter((entry) => Number.isFinite(entry.score));
  if (!valid.length) return;
  const scores = valid.map((entry) => entry.score);
  const max = Math.max(...scores);
  const min = Math.min(...scores);

  valid
    .sort((a, b) => a.score - b.score)
    .forEach((entry) => {
      const row = document.createElement("div");
      row.className = "leader-item";
      const widthPct =
        max === min ? 60 : 20 + ((max - entry.score) / (max - min)) * 80;
      row.innerHTML = `
        <div><strong>${formatDisplayName(entry.name)}</strong></div>
        <div class="leader-bar">
          <div class="leader-fill" style="width:${widthPct.toFixed(1)}%"></div>
        </div>
        <div>${formatMs(entry.score)}</div>
      `;
      container.appendChild(row);
    });
};

const computeBestOverall = (formats) => {
  const values = Object.entries(formats || {}).filter(([name]) => name !== "duckdb_table");
  const getNums = (fn) =>
    values.map(([, body]) => fn(body)).filter((value) => Number.isFinite(value));
  const min = (list) => (list.length ? Math.min(...list) : null);
  const max = (list) => (list.length ? Math.max(...list) : null);

  return {
    compression_ratio: max(getNums((item) => item.compression_ratio_geomean)),
    output_size_bytes: min(getNums((item) => item.output_size_bytes_geomean)),
    write_time: min(getNums((item) => item.compression_time_s_geomean)),
    full_scan: min(getNums((item) => item.query_median_ms_geomean?.full_scan_min)),
    selective: min(getNums((item) => item.query_median_ms_geomean?.selective_predicate)),
    random_access: min(getNums((item) => item.query_median_ms_geomean?.random_access)),
  };
};

const renderFormatCards = (formats) => {
  const grid = document.getElementById("format-grid");
  if (!grid) return;
  grid.innerHTML = "";
  const best = computeBestOverall(formats);
  const epsilon = 1e-9;
  const isBestMax = (value, target) =>
    Number.isFinite(value) && Number.isFinite(target) && value >= target - epsilon;
  const isBestMin = (value, target) =>
    Number.isFinite(value) && Number.isFinite(target) && value <= target + epsilon;

  Object.entries(formats).forEach(([name, data]) => {
    const card = document.createElement("div");
    card.className = "format-card";
    const allowBest = name !== "duckdb_table";
    card.innerHTML = `
      <h3>${formatDisplayName(name)}</h3>
      <div class="kv ${
        allowBest && isBestMax(data.compression_ratio_geomean, best.compression_ratio)
          ? "is-best"
          : ""
      }"><span>Compression ratio</span><strong>${formatNumber(
        data.compression_ratio_geomean,
        2
      )}</strong></div>
      <div class="kv ${
        allowBest && isBestMin(data.output_size_bytes_geomean, best.output_size_bytes)
          ? "is-best"
          : ""
      }"><span>Output size</span><strong>${formatBytes(
        data.output_size_bytes_geomean
      )}</strong></div>
      <div class="kv ${
        allowBest && isBestMin(data.compression_time_s_geomean, best.write_time) ? "is-best" : ""
      }"><span>Compression time</span><strong>${formatNumber(
        data.compression_time_s_geomean,
        2
      )} s</strong></div>
      <div class="kv ${
        allowBest && isBestMin(data.query_median_ms_geomean?.full_scan_min, best.full_scan)
          ? "is-best"
          : ""
      }"><span>Full scan*</span><strong>${formatMsWithP95(
        data.query_median_ms_geomean?.full_scan_min,
        undefined
      )}</strong></div>
      <div class="kv ${
        allowBest &&
        isBestMin(data.query_median_ms_geomean?.selective_predicate, best.selective)
          ? "is-best"
          : ""
      }"><span>Selective predicate*</span><strong>${formatMsWithP95(
        data.query_median_ms_geomean?.selective_predicate,
        undefined
      )}</strong></div>
      <div class="kv ${
        allowBest && isBestMin(data.query_median_ms_geomean?.random_access, best.random_access)
          ? "is-best"
          : ""
      }"><span>Random access*</span><strong>${formatMsWithP95(
        data.query_median_ms_geomean?.random_access,
        undefined
      )}</strong></div>
    `;
    grid.appendChild(card);
  });
};

const renderDatasetGrid = (datasets, manifest) => {
  const grid = document.getElementById("dataset-grid");
  if (!grid) return;
  grid.innerHTML = "";

  datasets.forEach((dataset) => {
    const match = manifest.datasets.find((item) => item.name === dataset.name);
    const link = match ? `./dataset.html?name=${encodeURIComponent(match.name)}` : "./dataset.html";
    const card = document.createElement("a");
    card.href = link;
    card.className = "dataset-card";
    card.innerHTML = `
      <div class="dataset-title">${dataset.name}</div>
      <div class="dataset-meta">${formatNumber(dataset.rows)} rows</div>
      <div class="dataset-meta">${formatBytes(dataset.input_size_bytes)} input</div>
      <div class="dataset-meta">Columns: ${Object.values(dataset.column_type_counts || {}).reduce(
        (sum, val) => sum + (val || 0),
        0
      )}</div>
    `;
    grid.appendChild(card);
  });
};

const renderOverallStats = (summary) => {
  const totalRows = summary.datasets.reduce((sum, dataset) => sum + (dataset.rows || 0), 0);
  const totalBytes = summary.datasets.reduce(
    (sum, dataset) => sum + (dataset.input_size_bytes || 0),
    0
  );
  const ndvNumeric = summary.datasets
    .map((dataset) => dataset.ndv_ratio_by_type?.numeric)
    .filter((value) => Number.isFinite(value));
  const avgNdv = ndvNumeric.length
    ? ndvNumeric.reduce((sum, value) => sum + value, 0) / ndvNumeric.length
    : null;

  document.getElementById("stat-datasets").textContent = formatNumber(summary.dataset_count);
  document.getElementById("stat-rows").textContent = formatNumber(totalRows);
  document.getElementById("stat-size").textContent = formatBytes(totalBytes);
  document.getElementById("stat-ndv").textContent = avgNdv ? avgNdv.toFixed(3) : "--";
};

const getMetricValue = (report, formatKey, metric) => {
  const format = report?.formats?.[formatKey];
  if (!format) return 0;
  switch (metric) {
    case "compression_ratio":
      return format.compression_ratio || 0;
    case "output_size":
      return format.write?.output_size_bytes || 0;
    case "write_time":
      return format.write?.compression_time_s || 0;
    case "full_scan":
      return format.queries?.full_scan_min?.median_ms || 0;
    case "selective":
      return format.queries?.selective_predicate?.median_ms || 0;
    case "random_access":
      return format.queries?.random_access?.median_ms || 0;
    default:
      return 0;
  }
};

const getDatasetColumns = (dataset) => {
  return Object.values(dataset.column_type_counts || {}).reduce(
    (sum, val) => sum + (val || 0),
    0
  );
};

const renderOverallChart = (summary, reports, formatKey, metric) => {
  const container = document.getElementById("overall-chart");
  const title = document.getElementById("overall-chart-title");
  if (!container || !title) return;

  const metricMap = {
    compression_ratio: {
      label: "Compression ratio by dataset",
      format: (value) => formatNumber(value, 2),
    },
    output_size: {
      label: "Output size by dataset",
      format: (value) => formatBytes(value),
    },
    write_time: {
      label: "Compression time by dataset",
      format: (value) => `${formatNumber(value, 2)} s`,
    },
    full_scan: {
      label: "Full scan median by dataset",
      format: (value) => formatMs(value),
    },
    selective: {
      label: "Selective predicate median by dataset",
      format: (value) => formatMs(value),
    },
    random_access: {
      label: "Random access median by dataset",
      format: (value) => formatMs(value),
    },
  };

  const chosen = metricMap[metric] || metricMap.compression_ratio;
  const data = summary.datasets.map((dataset) => {
    const columns = getDatasetColumns(dataset);
    return {
      label: dataset.name,
      value: getMetricValue(reports[dataset.name], formatKey, metric),
      size: dataset.rows,
      sizeLabel: `${formatNumber(dataset.rows)} rows`,
      metaLabel: columns ? `Columns: ${formatNumber(columns)}` : "",
    };
  });

  title.textContent = chosen.label;
  createLineChart(container, data, chosen.format);
};

const renderOverall3D = (summary, reports, formatKey, metric) => {
  const container = document.getElementById("overall-3d-chart");
  if (!container) return;
  if (!window.Plotly) {
    container.textContent = "3D chart unavailable (Plotly not loaded).";
    return;
  }

  const yMode = "rows";
  const x = [];
  const y = [];
  const z = [];
  const text = [];
  const sizes = [];

  summary.datasets.forEach((dataset, index) => {
    const metricValue = getMetricValue(reports[dataset.name], formatKey, metric);
    const columns = getDatasetColumns(dataset);
    const yValue = yMode === "columns" ? columns : dataset.rows || 0;
    if (!Number.isFinite(metricValue)) return;
    x.push(index + 1);
    y.push(yValue || 0);
    z.push(metricValue);
    sizes.push(Math.max(6, Math.min(18, Math.log10((dataset.input_size_bytes || 1) + 1) * 4)));
    text.push(
      `${dataset.name}<br>Rows: ${formatNumber(dataset.rows)}<br>Columns: ${formatNumber(
        columns
      )}<br>Metric: ${metricValue}`
    );
  });

  const trace = {
    type: "scatter3d",
    mode: "lines+markers",
    x,
    y,
    z,
    text,
    hoverinfo: "text",
    marker: {
      size: sizes,
      color: z,
      colorscale: "Viridis",
      opacity: 0.85,
    },
    line: {
      color: "#2f4a36",
      width: 3,
    },
  };

  const layout = {
    margin: { l: 0, r: 0, b: 0, t: 10 },
    scene: {
      xaxis: { title: "Dataset (index)" },
      yaxis: { title: yMode === "columns" ? "Columns" : "Rows" },
      zaxis: { title: metric },
    },
    paper_bgcolor: "transparent",
    plot_bgcolor: "transparent",
  };

  Plotly.newPlot(container, [trace], layout, { displayModeBar: false, responsive: true });
};

const loadData = async () => {
  const cacheBust = `?t=${Date.now()}`;
  let summary = null;
  let manifest = null;
  try {
    const [summaryResp, manifestResp] = await Promise.all([
      fetch(`${SUMMARY_URL}${cacheBust}`),
      fetch(`${DATASETS_URL}${cacheBust}`),
    ]);
    summary = await summaryResp.json();
    manifest = await manifestResp.json();
  } catch (err) {
    summary = loadCached("latestSummary");
    manifest = loadCached("latestManifest");
  }
  if (!summary || !manifest) {
    throw new Error("Summary or manifest not available.");
  }
  const manifestMap = Object.fromEntries(
    manifest.datasets.map((entry) => [entry.name, entry.report])
  );
  const reportEntries = await Promise.all(
    summary.datasets.map(async (dataset) => {
      const reportPath = manifestMap[dataset.name] || `../out/report_${dataset.name}.json`;
      const response = await fetch(reportPath);
      if (!response.ok) return [dataset.name, null];
      const report = await response.json();
      return [dataset.name, report];
    })
  );
  const reports = Object.fromEntries(reportEntries);

  renderOverallStats(summary);
  renderFormatCards(summary.formats);
  renderLeaderboard(summary.formats);
  renderDatasetGrid(summary.datasets, manifest);

  const metricSelect = document.getElementById("overall-metric");
  const formatSelect = document.getElementById("overall-format");
  if (metricSelect && formatSelect) {
    const firstReport = Object.values(reports).find((report) => report?.formats);
    const formatKeys = Object.keys(firstReport?.formats || {});
    formatSelect.innerHTML = "";
    formatKeys.forEach((key) => {
      const option = document.createElement("option");
      option.value = key;
      option.textContent = key;
      formatSelect.appendChild(option);
    });
    const renderChart = () => {
      if (!formatSelect.value) return;
      renderOverallChart(summary, reports, formatSelect.value, metricSelect.value);
      renderOverall3D(summary, reports, formatSelect.value, metricSelect.value);
    };
    renderChart();
    metricSelect.addEventListener("change", renderChart);
    formatSelect.addEventListener("change", renderChart);
    window.addEventListener("resize", renderChart);
  }
};

const initReveal = () => {
  const targets = document.querySelectorAll(
    ".hero, .hero-card, .stats-grid, .panel, .dataset-grid, .format-grid, .chart-card"
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

loadData().catch((error) => {
  console.error("Failed to load summary data", error);
});

initReveal();

window.addEventListener("storage", (event) => {
  if (event.key === "latestSummary" || event.key === "latestManifest") {
    loadData().catch((error) => {
      console.error("Failed to reload summary data", error);
    });
  }
});
