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

const renderLeaderboard = (formats) => {
  const container = document.getElementById("format-leaderboard");
  if (!container) return;
  container.innerHTML = "";

  const entries = Object.entries(formats).map(([name, data]) => ({
    name,
    score: data.query_median_ms_geomean?.random_access ?? null,
  }));

  const valid = entries.filter((entry) => Number.isFinite(entry.score));
  if (!valid.length) return;
  const max = Math.max(...valid.map((entry) => entry.score));

  valid
    .sort((a, b) => a.score - b.score)
    .forEach((entry) => {
      const row = document.createElement("div");
      row.className = "leader-item";
      row.innerHTML = `
        <div><strong>${entry.name}</strong></div>
        <div class="leader-bar">
          <div class="leader-fill" style="width:${Math.max(
            12,
            100 - (entry.score / max) * 100
          )}%"></div>
        </div>
        <div>${formatMs(entry.score)}</div>
      `;
      container.appendChild(row);
    });
};

const renderFormatCards = (formats) => {
  const grid = document.getElementById("format-grid");
  if (!grid) return;
  grid.innerHTML = "";

  Object.entries(formats).forEach(([name, data]) => {
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
      <div class="kv"><span>Selective median</span><strong>${formatMs(
        data.query_median_ms_geomean?.selective_predicate
      )}</strong></div>
      <div class="kv"><span>Random access median</span><strong>${formatMs(
        data.query_median_ms_geomean?.random_access
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
  const data = summary.datasets.map((dataset) => ({
    label: dataset.name,
    value: getMetricValue(reports[dataset.name], formatKey, metric),
    size: dataset.rows,
    sizeLabel: `${formatNumber(dataset.rows)} rows`,
  }));

  title.textContent = chosen.label;
  createLineChart(container, data, chosen.format);
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
