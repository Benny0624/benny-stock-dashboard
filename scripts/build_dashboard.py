"""查詢 DuckDB，產出一個包含指標走勢（價格/比值 + MA50/MA200）、RSI、月線 MACD
（僅 SOX）、轉折點事件標記的靜態 dashboard HTML。資料直接內嵌在檔案裡（不需要
後端），圖表庫用 CDN 載入的 ECharts 5，模式照抄 `Yahoo_fantasy_dashboard/
scripts/build_dashboard.py`。

美股/台股是兩份獨立的 html（grilling_notes.md 決定 4），用 MARKET 環境變數
決定要產哪一份，`benny-data-pipeline` 的 `us_market_daily_etl`/
`tw_market_daily_etl` 兩個 DAG 各自帶著自己的 market 呼叫這支腳本。

指標分兩種，資料形狀不完全一樣：
- 真實指標（index_id 1-11）：原始數值在 raw_stock，MA50/MA200/RSI14 在
  silver_stock。SOX（index_id=7）額外有 MACD/MACD_SIGNAL（月頻率，只有這一個
  指標有）。
- 合成指標（index_id 101+，SOX/SPX、道瓊/那指、羅素2000/SPX 三個比值）：沒有
  raw_stock 資料，比值本身（agg_type='RAW'）跟 MA50/MA200 都在 silver_stock。

轉折點（dim_triggers）用「這個 trigger 屬於哪個 index_id」直接分組疊到對應
的圖表上，不需要另外維護一份 trigger_type -> index_id 的對照表——每筆
trigger row 已經帶著自己的 index_id（見 compute_triggers.sql）。`_STATE`
這種每天都有一筆的 trigger_type 不疊圖（太密，畫面會全部蓋滿），只疊
ENTER/EXIT/BREAKOUT/BREAKDOWN/CROSS/NEW_HIGH/NEW_LOW 這種「事件當天才有一
筆」的 trigger_type。

用法：
    DUCKDB_PATH=/data/warehouse.duckdb MARKET=us python scripts/build_dashboard.py

warehouse.duckdb 放在 Docker named volume（benny-infra-duckdb-data）裡，不會
直接出現在檔案系統上，本機要跑這支腳本的話用容器掛同一顆 volume 進去執行，
例如：

    docker run --rm \\
        -v benny-infra-duckdb-data:/data \\
        -v "$(pwd):/app" -w /app \\
        python:3.11-slim \\
        bash -c "pip install duckdb --quiet && MARKET=us python scripts/build_dashboard.py"
"""
import json
import os
from datetime import date, timedelta

import duckdb

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/warehouse.duckdb")
MARKET = os.environ["MARKET"]  # 'us' 或 'tw'，兩個 DAG 各自帶自己的 market 呼叫
QUERY_WINDOW_YEARS = 2  # 決定 15：DB 層不設 retention，dashboard 查詢層設 2 年上限
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, f"dashboard_{MARKET}.html")

# 決定 15 拍板的窗口選項，'2yr' 對齊查詢上限（DB 資料以後超過 2 年也不會讓
# 頁面無限變大，需要回溯更久的冷資料 Benny 自己進 DB 查）。
WINDOW_OPTIONS = {"3mo": 90, "6mo": 180, "1yr": 365, "2yr": 730}

# 事件型 trigger（有實際發生日期才寫一筆），排除 *_STATE（每天都有一筆，
# 疊圖會整片蓋滿，不適合當標記點）。
TRIGGER_MARKER_EXCLUDE_SUFFIX = "_STATE"


def _query_window_start() -> date:
    return date.today() - timedelta(days=365 * QUERY_WINDOW_YEARS)


def fetch_real_index_levels(conn: duckdb.DuckDBPyConnection) -> dict[int, dict]:
    """真實指標的原始數值（raw_stock），回傳 {index_id: {"index_name":..., "level": [[date_str, value], ...]}}"""
    rows = conn.execute(
        """
        SELECT index_id, index_name, updated_at, index_value
        FROM stock_dashboard.raw_stock
        WHERE market = ? AND updated_at >= ?
        ORDER BY index_id, updated_at
        """,
        [MARKET, _query_window_start()],
    ).fetchall()

    indices: dict[int, dict] = {}
    for index_id, index_name, updated_at, value in rows:
        entry = indices.setdefault(index_id, {"index_name": index_name, "is_synthetic": False, "level": []})
        entry["level"].append([updated_at.isoformat(), value])
    return indices


def fetch_silver_series(conn: duckdb.DuckDBPyConnection) -> dict[int, dict]:
    """silver_stock 的所有列（MA50/MA200/RSI14/MACD/MACD_SIGNAL/合成指標的 RAW 比值）。
    合成指標的 agg_type='RAW' 直接當作 level 使用（合成指標沒有 raw_stock 資料）。
    """
    rows = conn.execute(
        """
        SELECT index_id, index_name, agg_type, updated_at, agg_value
        FROM stock_dashboard.silver_stock
        WHERE market = ? AND updated_at >= ?
        ORDER BY index_id, agg_type, updated_at
        """,
        [MARKET, _query_window_start()],
    ).fetchall()

    silver: dict[int, dict] = {}
    for index_id, index_name, agg_type, updated_at, value in rows:
        entry = silver.setdefault(index_id, {"index_name": index_name, "series": {}})
        entry["series"].setdefault(agg_type, []).append([updated_at.isoformat(), value])
    return silver


def fetch_triggers(conn: duckdb.DuckDBPyConnection) -> dict[int, list[dict]]:
    """事件型 trigger，依 index_id 分組。回傳 {index_id: [{"date":..., "type":..., "value":...}, ...]}"""
    rows = conn.execute(
        """
        SELECT index_id, trigger_type, trigger_value, updated_at
        FROM stock_dashboard.dim_triggers
        WHERE market = ? AND updated_at >= ? AND trigger_type NOT LIKE ?
        ORDER BY index_id, updated_at
        """,
        [MARKET, _query_window_start(), f"%{TRIGGER_MARKER_EXCLUDE_SUFFIX}"],
    ).fetchall()

    triggers: dict[int, list[dict]] = {}
    for index_id, trigger_type, trigger_value, updated_at in rows:
        triggers.setdefault(index_id, []).append(
            {"date": updated_at.isoformat(), "type": trigger_type, "value": trigger_value}
        )
    return triggers


def build_indices_payload(conn: duckdb.DuckDBPyConnection) -> dict:
    """把三次查詢組成前端要吃的單一結構：
    {index_id: {index_name, is_synthetic, level, ma50, ma200, rsi14, macd, macd_signal, triggers}}
    """
    real_levels = fetch_real_index_levels(conn)
    silver = fetch_silver_series(conn)
    triggers = fetch_triggers(conn)

    indices: dict[str, dict] = {}
    all_index_ids = set(real_levels) | set(silver)
    for index_id in all_index_ids:
        real = real_levels.get(index_id)
        silver_entry = silver.get(index_id, {"index_name": None, "series": {}})
        series = silver_entry["series"]

        is_synthetic = real is None
        level = real["level"] if real is not None else series.get("RAW", [])
        index_name = (real or {}).get("index_name") or silver_entry["index_name"]

        indices[str(index_id)] = {
            "index_name": index_name,
            "is_synthetic": is_synthetic,
            "level": level,
            "ma50": series.get("MA50", []),
            "ma200": series.get("MA200", []),
            "rsi14": series.get("RSI14", []),
            "macd": series.get("MACD", []),
            "macd_signal": series.get("MACD_SIGNAL", []),
            "triggers": triggers.get(index_id, []),
        }
    return indices


def latest_updated_at(conn: duckdb.DuckDBPyConnection) -> str | None:
    """最新一筆 raw_stock 資料的日期，dashboard 上用紅字標示，讓使用者自己
    判斷資料是不是舊的（決定 5——目前沒有交易日曆判斷，不試圖自動分辨「休市」
    跟「API 真的掛了」，只誠實秀出時間讓人自己看）。"""
    row = conn.execute(
        "SELECT MAX(updated_at) FROM stock_dashboard.raw_stock WHERE market = ?", [MARKET]
    ).fetchone()
    return row[0].isoformat() if row and row[0] else None


HTML_TEMPLATE = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>市場轉折監測 Dashboard (__MARKET_LABEL__)</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
  :root {
    --surface-1: #1a1a19;
    --page-plane: #0d0d0d;
    --text-primary: #ffffff;
    --text-secondary: #c3c2b7;
    --text-muted: #898781;
    --gridline: #2c2c2a;
    --baseline: #383835;
    --border: rgba(255,255,255,0.10);
    --accent: #3987e5;
    --good: #0ca30c;
    --critical: #e66767;
  }
  * { box-sizing: border-box; }
  body {
    font-family: system-ui, -apple-system, "Segoe UI", "Microsoft JhengHei", sans-serif;
    margin: 0; padding: 24px; background: var(--page-plane); color: var(--text-primary);
  }
  h1 { font-size: 20px; font-weight: 600; margin: 0 0 4px; }
  h2 { font-size: 15px; font-weight: 600; margin: 0 0 12px; color: var(--text-primary); }
  .subtitle { color: var(--text-secondary); font-size: 13px; margin-bottom: 4px; }
  .updated-at { color: var(--critical); font-size: 13px; font-weight: 600; margin-bottom: 20px; }

  .kpi-row { display: flex; gap: 12px; margin-bottom: 20px; flex-wrap: wrap; }
  .kpi-card {
    background: var(--surface-1); border: 1px solid var(--border); border-radius: 10px;
    padding: 14px 20px; min-width: 140px; flex: 1;
  }
  .kpi-card .kpi-label { font-size: 12px; color: var(--text-muted); margin-bottom: 6px; }
  .kpi-card .kpi-value {
    font-size: 26px; font-weight: 700; color: var(--text-primary);
    font-variant-numeric: tabular-nums;
  }
  .kpi-card .kpi-sub { font-size: 12px; color: var(--text-secondary); margin-top: 4px; }
  .kpi-card.accent { border-color: var(--accent); }
  .kpi-card.accent .kpi-value { color: var(--accent); }

  #filters {
    display: flex; flex-wrap: wrap; gap: 24px; align-items: flex-start; margin-bottom: 24px;
    background: var(--surface-1); border: 1px solid var(--border); border-radius: 10px; padding: 16px 20px;
  }
  .filter-block { display: flex; flex-direction: column; gap: 6px; }
  .filter-block label { font-size: 13px; font-weight: 600; color: var(--text-secondary); }
  select {
    font-size: 14px; padding: 6px 10px; background: var(--page-plane); color: var(--text-primary);
    border: 1px solid var(--border); border-radius: 6px; min-width: 220px;
  }

  .panel {
    background: var(--surface-1); border: 1px solid var(--border); border-radius: 10px;
    padding: 16px 20px; margin-bottom: 24px;
  }
  .panel .hint { font-size: 12px; color: var(--text-muted); margin-bottom: 8px; }
  .chart { width: 100%; height: 460px; }
  .chart.short { height: 220px; }
  .chart.hidden { display: none; }
  .legend-note { font-size: 12px; color: var(--text-secondary); margin-top: 8px; }
  .legend-note .dot { display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 4px; }
</style>
</head>
<body>
<h1>市場轉折監測 Dashboard</h1>
<div class="subtitle">__MARKET_LABEL__ · 指標走勢（價格/比值 + MA50/MA200）、RSI、轉折點標記</div>
<div class="updated-at" id="updated-at"></div>

<div id="filters">
  <div class="filter-block">
    <label for="index-select">指標</label>
    <select id="index-select"></select>
  </div>
  <div class="filter-block">
    <label for="window-select">時間範圍</label>
    <select id="window-select">
      <option value="3mo">近 3 個月</option>
      <option value="6mo">近 6 個月</option>
      <option value="1yr">近 1 年</option>
      <option value="2yr" selected>近 2 年</option>
    </select>
  </div>
</div>

<div class="kpi-row" id="kpi-row"></div>

<div class="panel">
  <h2 id="level-title">走勢</h2>
  <div class="hint">實線：價格/比值；虛線：MA50、MA200。三角形=進入/突破/黃金交叉/創新高，倒三角=離開/跌破/死亡交叉/破新低（方向由 trigger_type 決定，不代表多空判斷，詳見 tooltip）</div>
  <div id="level-chart" class="chart"></div>
</div>

<div class="panel" id="rsi-panel">
  <h2>RSI14</h2>
  <div class="hint">虛線標示 30 / 70 慣例參考線，本系統目前沒有定義 RSI 閾值 trigger，純供參考</div>
  <div id="rsi-chart" class="chart short"></div>
</div>

<div class="panel" id="macd-panel">
  <h2>月線 MACD（僅費城半導體指數 SOX）</h2>
  <div class="hint">月線收盤價算出的 MACD / Signal 線，三角形標記多頭/空頭轉向（compute_sox_macd task 算出來的）</div>
  <div id="macd-chart" class="chart short"></div>
</div>

<script>
const INDICES = __INDICES_JSON__;
const MARKET_LABEL = __MARKET_LABEL_JSON__;

const AXIS_TEXT_STYLE = { fontSize: 14, fontWeight: "bold", color: "#ffffff" };
const AXIS_LABEL_STYLE = { fontSize: 12, fontWeight: "bold", color: "#c3c2b7" };
const AXIS_LINE_STYLE = { lineStyle: { color: "#383835", width: 2 } };
const SPLIT_LINE_STYLE = { lineStyle: { color: "#2c2c2a" } };
const ACCENT = "#3987e5";
const GOOD = "#0ca30c";
const CRITICAL = "#e66767";
const MUTED = "#898781";

// trigger_type 尾巴代表方向：ENTER/BREAKOUT/GOLDEN_CROSS/NEW_HIGH... 是「進入/
// 達成」用向上三角，EXIT/BREAKDOWN/DEATH_CROSS/NEW_LOW... 是「離開」用向下
// 三角——純粹描述「有沒有進入某個已定義的條件」，不是多空判斷（例如
// VIX_PANIC_ENTER 是恐慌開始，通常是偏空訊號，但一樣用「進入」的向上三角），
// 實際意義看 tooltip 裡完整的 trigger_type 名稱。
const ENTER_KEYWORDS = ["ENTER", "BREAKOUT", "GOLDEN_CROSS", "BULLISH_CROSS", "NEW_HIGH"];
function isEnterEvent(triggerType) {
  return ENTER_KEYWORDS.some((kw) => triggerType.includes(kw));
}

const indexSelect = document.getElementById("index-select");
const windowSelect = document.getElementById("window-select");
const updatedAtEl = document.getElementById("updated-at");
const kpiRow = document.getElementById("kpi-row");
const levelTitle = document.getElementById("level-title");
const rsiPanel = document.getElementById("rsi-panel");
const macdPanel = document.getElementById("macd-panel");

const levelChart = echarts.init(document.getElementById("level-chart"));
const rsiChart = echarts.init(document.getElementById("rsi-chart"));
const macdChart = echarts.init(document.getElementById("macd-chart"));

function populateIndexOptions() {
  indexSelect.innerHTML = "";
  // 真實指標排前面、合成指標排後面，各自照 index_id 數字排序
  const ids = Object.keys(INDICES).sort((a, b) => Number(a) - Number(b));
  for (const id of ids) {
    const opt = document.createElement("option");
    opt.value = id;
    const info = INDICES[id];
    opt.textContent = info.index_name + (info.is_synthetic ? "（比值）" : "");
    indexSelect.appendChild(opt);
  }
}

function windowStartDate() {
  const days = { "3mo": 90, "6mo": 180, "1yr": 365, "2yr": 730 }[windowSelect.value];
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().slice(0, 10);
}

function filterByWindow(series) {
  const start = windowStartDate();
  return series.filter(([d]) => d >= start);
}

function toMarkPoints(triggers, valueByDate) {
  const start = windowStartDate();
  return triggers
    .filter((t) => t.date >= start)
    .map((t) => ({
      name: t.type,
      value: t.value,
      xAxis: t.date,
      yAxis: t.value,
      symbol: isEnterEvent(t.type) ? "triangle" : "arrow",
      symbolRotate: isEnterEvent(t.type) ? 0 : 180,
      symbolSize: 12,
      itemStyle: { color: isEnterEvent(t.type) ? GOOD : CRITICAL },
      label: { show: false },
    }));
}

function renderKpis() {
  const info = INDICES[indexSelect.value];
  const level = filterByWindow(info.level);
  const triggers = info.triggers.filter((t) => t.date >= windowStartDate());
  const latest = level.length ? level[level.length - 1][1] : null;

  kpiRow.innerHTML = "";
  const cards = [
    { label: "目前選取指標", value: info.index_name, sub: info.is_synthetic ? "合成指標（比值）" : "真實指標", accent: true },
    { label: "最新數值", value: latest != null ? latest.toFixed(4) : "—", sub: level.length ? level[level.length - 1][0] : "" },
    { label: "窗口內資料筆數", value: level.length, sub: windowSelect.value },
    { label: "窗口內轉折事件", value: triggers.length, sub: "不含 *_STATE" },
  ];
  for (const c of cards) {
    const div = document.createElement("div");
    div.className = "kpi-card" + (c.accent ? " accent" : "");
    div.innerHTML = `<div class="kpi-label">${c.label}</div><div class="kpi-value">${c.value}</div><div class="kpi-sub">${c.sub}</div>`;
    kpiRow.appendChild(div);
  }
}

function renderLevelChart() {
  const info = INDICES[indexSelect.value];
  levelTitle.textContent = info.index_name + (info.is_synthetic ? "（比值）走勢" : " 走勢");

  const level = filterByWindow(info.level);
  const ma50 = filterByWindow(info.ma50);
  const ma200 = filterByWindow(info.ma200);
  const valueByDate = Object.fromEntries(level);
  const markPoints = toMarkPoints(info.triggers, valueByDate);

  levelChart.setOption({
    backgroundColor: "transparent",
    tooltip: { trigger: "axis", backgroundColor: "#1a1a19", borderColor: "#383835", textStyle: { color: "#fff" } },
    legend: { top: 0, textStyle: { color: "#c3c2b7", fontSize: 12 } },
    grid: { containLabel: true, top: 40, bottom: 40, left: 10, right: 30 },
    xAxis: {
      type: "time", axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitLine: { show: false },
    },
    yAxis: {
      type: "value", scale: true, axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitLine: SPLIT_LINE_STYLE,
    },
    series: [
      { name: "價格/比值", type: "line", data: level, showSymbol: false, lineStyle: { color: ACCENT, width: 2 },
        markPoint: { data: markPoints, symbolSize: 12 } },
      { name: "MA50", type: "line", data: ma50, showSymbol: false, lineStyle: { color: GOOD, width: 1.5, type: "dashed" } },
      { name: "MA200", type: "line", data: ma200, showSymbol: false, lineStyle: { color: CRITICAL, width: 1.5, type: "dashed" } },
    ],
  }, true);
}

function renderRsiChart() {
  const info = INDICES[indexSelect.value];
  const hasRsi = info.rsi14.length > 0;
  rsiPanel.classList.toggle("hidden", !hasRsi);
  if (!hasRsi) return;

  const rsi = filterByWindow(info.rsi14);
  rsiChart.setOption({
    backgroundColor: "transparent",
    tooltip: { trigger: "axis", backgroundColor: "#1a1a19", borderColor: "#383835", textStyle: { color: "#fff" } },
    grid: { containLabel: true, top: 20, bottom: 30, left: 10, right: 30 },
    xAxis: { type: "time", axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitLine: { show: false } },
    yAxis: { type: "value", min: 0, max: 100, axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitLine: SPLIT_LINE_STYLE },
    series: [{
      name: "RSI14", type: "line", data: rsi, showSymbol: false, lineStyle: { color: ACCENT, width: 2 },
      markLine: {
        symbol: "none", label: { color: MUTED, fontSize: 11 },
        lineStyle: { color: MUTED, type: "dashed" },
        data: [{ yAxis: 30 }, { yAxis: 70 }],
      },
    }],
  }, true);
}

function renderMacdChart() {
  const info = INDICES[indexSelect.value];
  const hasMacd = info.macd.length > 0;
  macdPanel.classList.toggle("hidden", !hasMacd);
  if (!hasMacd) return;

  const macd = filterByWindow(info.macd);
  const signal = filterByWindow(info.macd_signal);
  const markPoints = toMarkPoints(
    info.triggers.filter((t) => t.type.includes("MACD")),
    Object.fromEntries(macd)
  );

  macdChart.setOption({
    backgroundColor: "transparent",
    tooltip: { trigger: "axis", backgroundColor: "#1a1a19", borderColor: "#383835", textStyle: { color: "#fff" } },
    legend: { top: 0, textStyle: { color: "#c3c2b7", fontSize: 12 } },
    grid: { containLabel: true, top: 30, bottom: 30, left: 10, right: 30 },
    xAxis: { type: "time", axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitLine: { show: false } },
    yAxis: { type: "value", scale: true, axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitLine: SPLIT_LINE_STYLE },
    series: [
      { name: "MACD", type: "line", data: macd, showSymbol: false, lineStyle: { color: ACCENT, width: 2 },
        markPoint: { data: markPoints, symbolSize: 12 } },
      { name: "Signal", type: "line", data: signal, showSymbol: false, lineStyle: { color: MUTED, width: 1.5, type: "dashed" } },
    ],
  }, true);
}

function renderAll() {
  renderKpis();
  renderLevelChart();
  renderRsiChart();
  renderMacdChart();
}

indexSelect.addEventListener("change", renderAll);
windowSelect.addEventListener("change", renderAll);
window.addEventListener("resize", () => { levelChart.resize(); rsiChart.resize(); macdChart.resize(); });

updatedAtEl.textContent = __UPDATED_AT_JSON__
  ? `資料更新時間：${__UPDATED_AT_JSON__}（${MARKET_LABEL}）——請自行核對是否為最新交易日，本系統目前沒有交易日曆判斷，休市日不會特別標示`
  : "尚無資料";

populateIndexOptions();
renderAll();
</script>
</body>
</html>
"""


def main() -> None:
    market_label = "美股/總經" if MARKET == "us" else "台股"

    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        indices = build_indices_payload(conn)
        updated_at = latest_updated_at(conn)
    finally:
        conn.close()

    def _dumps(obj) -> str:
        return json.dumps(obj, ensure_ascii=False, default=str)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    html = (
        HTML_TEMPLATE
        .replace("__MARKET_LABEL__", market_label)
        .replace("__INDICES_JSON__", _dumps(indices))
        .replace("__MARKET_LABEL_JSON__", _dumps(market_label))
        .replace("__UPDATED_AT_JSON__", _dumps(updated_at))
    )
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[OK] wrote {OUTPUT_PATH} ({len(indices)} indices, market={MARKET}, updated_at={updated_at})")


if __name__ == "__main__":
    main()
