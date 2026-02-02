const DATA_URL = "../out/row_scaling/NYC_1/row_scaling_summary.json";

const formatNumber = (value, digits = 2) => {
  if (value === null || value === undefined || !Number.isFinite(value)) return "--";
  return value.toLocaleString(undefined, { maximumFractionDigits: digits });
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

const formatSeconds = (sec) => {
  if (!Number.isFinite(sec)) return "--";
  if (sec >= 60) return `${(sec / 60).toFixed(2)} min`;
  return `${sec.toFixed(2)} s`;
};

const formatRowCount = (count) => {
  if (!Number.isFinite(count)) return "--";
  if (count >= 1_000_000) return `${(count / 1_000_000).toFixed(0)}M`;
  if (count >= 1_000) return `${(count / 1_000).toFixed(0)}K`;
  return String(count);
};

const formatColors = {
  parquet_zstd: "#2f4a36",
  parquet_snappy: "#e38b2c",
  parquet_uncompressed: "#4c6fa8",
  vortex_default: "#a84c6f",
  duckdb_table: "#5c5c5c",
};

const formatDisplayName = (name) =>
  String(name)
    .replace("parquet_", "Parquet ")
    .replace("vortex_default", "Vortex")
    .replace("duckdb_table", "DuckDB table");

const createMultiLineChart = (container, series, xLabels, valueFormatter, options = {}) => {
  container.innerHTML = "";
  const baseWidth = container.clientWidth || 640;
  const plotWidth = Math.max(baseWidth, xLabels.length * 90);
  const height = 320;
  const padding = { top: 18, right: 24, bottom: 60, left: 56 };
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
    label.setAttribute("y", height - 24);
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
      xLabel: xLabels[index],
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
        tooltip.textContent = `${point.label} · ${point.xLabel}: ${valueFormatter(point.value)}`;
        tooltip.style.left = `${event.clientX - rect.left}px`;
        tooltip.style.top = `${event.clientY - rect.top - 12}px`;
        tooltip.style.opacity = "1";
      });
      circle.addEventListener("mouseleave", () => {
        tooltip.style.opacity = "0";
      });
      svg.appendChild(circle);

      if (options.showValues) {
        const valueLabel = document.createElementNS("http://www.w3.org/2000/svg", "text");
        valueLabel.setAttribute("x", point.x);
        valueLabel.setAttribute("y", Math.max(point.y - 8, padding.top + 10));
        valueLabel.setAttribute("text-anchor", "middle");
        valueLabel.setAttribute("font-size", "10");
        valueLabel.setAttribute("fill", "#4f574f");
        valueLabel.textContent = valueFormatter(point.value);
        svg.appendChild(valueLabel);
      }
    });
  });

  container.appendChild(svg);

  if (options.showLegend && series.length > 1) {
    const legend = document.createElement("div");
    legend.className = "legend";
    legend.style.justifyContent = "center";
    legend.style.marginTop = "10px";
    series.forEach((item) => {
      const row = document.createElement("div");
      row.className = "legend-item";
      row.innerHTML = `<span class="legend-dot" style="background:${item.color}"></span>${item.label}`;
      legend.appendChild(row);
    });
    container.appendChild(legend);
  }
};

const metricFormatters = {
  compression_ratio: (v) => formatNumber(v, 2),
  output_size: (v) => formatBytes(v),
  write_time: (v) => formatSeconds(v),
  compression_speed: (v) => `${formatNumber(v, 2)} MB/s`,
  decompression_time: (v) => formatSeconds(v),
  decompression_speed: (v) => `${formatNumber(v, 2)} MB/s`,
  full_scan: (v) => formatMs(v),
  selective: (v) => formatMs(v),
  random_access: (v) => formatMs(v),
  cold_full_scan: (v) => formatMs(v),
  cold_selective: (v) => formatMs(v),
  cold_random_access: (v) => formatMs(v),
};

let chartPrefs = { showLegend: false, showValues: false };

const init = async () => {
  const response = await fetch(`${DATA_URL}?t=${Date.now()}`);
  if (!response.ok) {
    throw new Error("Row scaling summary not found.");
  }
  const summary = await response.json();

  const metricSelect = document.getElementById("scaling-metric");
  const chart = document.getElementById("scaling-chart");
  const title = document.getElementById("scaling-title");
  const note = document.getElementById("scaling-note");
  const valuesHead = document.getElementById("scaling-values-head");
  const valuesBody = document.getElementById("scaling-values-body");
  const bestBody = document.getElementById("scaling-best-body");
  const legendButtons = document.querySelectorAll(".js-toggle-legend");
  const valuesButtons = document.querySelectorAll(".js-toggle-values");

  if (!metricSelect || !chart || !title || !note) return;

  metricSelect.innerHTML = "";
  summary.metrics.forEach((metric, index) => {
    const option = document.createElement("option");
    option.value = metric.key;
    option.textContent = metric.label;
    metricSelect.appendChild(option);
    if (index === 0) metricSelect.value = metric.key;
  });

  const rowLabels = summary.row_counts.map((value) => formatRowCount(value));

  const render = () => {
    const metricKey = metricSelect.value;
    const metricMeta = summary.metrics.find((item) => item.key === metricKey);
    const format = metricFormatters[metricKey] || ((v) => formatNumber(v, 2));
    const series = summary.formats.map((fmt) => ({
      label: formatDisplayName(fmt),
      color: formatColors[fmt] || "#6b6358",
      values: summary.series?.[metricKey]?.[fmt] || [],
    }));
    title.textContent = metricMeta?.label || "Metric trend";
    note.textContent = `Row counts: ${summary.row_counts.join(", ")}`;
    createMultiLineChart(chart, series, rowLabels, format, {
      showLegend: chartPrefs.showLegend,
      showValues: chartPrefs.showValues,
    });

    if (valuesHead && valuesBody) {
      valuesHead.innerHTML = "";
      valuesBody.innerHTML = "";
      const headerRow = document.createElement("tr");
      ["Row count", ...summary.formats.map((fmt) => formatDisplayName(fmt))].forEach((label) => {
        const th = document.createElement("th");
        th.textContent = label;
        headerRow.appendChild(th);
      });
      valuesHead.appendChild(headerRow);

      summary.row_counts.forEach((count, idx) => {
        const tr = document.createElement("tr");
        const countCell = document.createElement("td");
        countCell.textContent = String(count);
        tr.appendChild(countCell);
        summary.formats.forEach((fmt) => {
          const td = document.createElement("td");
          const value = summary.series?.[metricKey]?.[fmt]?.[idx];
          td.textContent = format(value);
          tr.appendChild(td);
        });
        valuesBody.appendChild(tr);
      });
    }

    if (bestBody) {
      bestBody.innerHTML = "";
      const preferHigher = metricKey === "compression_ratio" ||
        metricKey === "compression_speed" ||
        metricKey === "decompression_speed";
      summary.row_counts.forEach((count, idx) => {
        let bestFormat = null;
        let bestValue = null;
        summary.formats.forEach((fmt) => {
          const value = summary.series?.[metricKey]?.[fmt]?.[idx];
          if (!Number.isFinite(value)) return;
          if (bestValue === null) {
            bestValue = value;
            bestFormat = fmt;
            return;
          }
          if (preferHigher ? value > bestValue : value < bestValue) {
            bestValue = value;
            bestFormat = fmt;
          }
        });
        const tr = document.createElement("tr");
        const countCell = document.createElement("td");
        countCell.textContent = String(count);
        tr.appendChild(countCell);
        const formatCell = document.createElement("td");
        formatCell.textContent = bestFormat ? formatDisplayName(bestFormat) : "--";
        tr.appendChild(formatCell);
        const valueCell = document.createElement("td");
        valueCell.textContent = Number.isFinite(bestValue) ? format(bestValue) : "--";
        tr.appendChild(valueCell);
        bestBody.appendChild(tr);
      });
    }
  };

  render();
  metricSelect.addEventListener("change", render);
  window.addEventListener("resize", render);
  legendButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      chartPrefs.showLegend = !chartPrefs.showLegend;
      legendButtons.forEach((b) => b.classList.toggle("is-active", chartPrefs.showLegend));
      render();
    });
  });
  valuesButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      chartPrefs.showValues = !chartPrefs.showValues;
      valuesButtons.forEach((b) => b.classList.toggle("is-active", chartPrefs.showValues));
      render();
    });
  });
};

init().catch((error) => {
  const chart = document.getElementById("scaling-chart");
  if (chart) chart.textContent = error.message || "Unable to load row scaling data.";
});
