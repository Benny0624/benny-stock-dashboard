"""查詢 DuckDB 的 Layer 3 回測結果四張表（backtest_runs/backtest_equity_curve/
backtest_trades/backtest_kpis），產出一個回測結果 dashboard HTML。跟
build_dashboard.py 是同一套模式（ECharts 5 CDN、深色主題、資料內嵌不用後端），
但這支只管回測結果，不碰指標/轉折點那份。

跟指標 dashboard 不同，這支**不用 MARKET 環境變數分流**——回測結果是按
(strategy_name, ticker) 分的，不是按 market 分的，一份 html 涵蓋所有已經跑過
的策略，用前端下拉選單切換，之後新增策略不需要多一份 html。

圖表元素（layer3_backtest_proposal.md 第 8 節「圖表最終定案」，6 項全做）：
1. Equity Curve（策略 vs 基準累積淨值）
2. Underwater Chart（水下回撤）
3. 月度/年度報酬熱力圖（從 equity curve 的策略淨值序列在這支腳本裡現算，
   DB 沒有另外存一張表——月度報酬不是「原始資料」，是「equity curve 的
   衍生檢視」，沒必要多開一張表跟 equity curve 的每日序列重複存兩份）
4. KPI 彙總表
5. 逐筆交易列表
6. 訊號疊在價格走勢圖上——這裡有一個跟指標 dashboard 不同的地方：指標
   dashboard 的 dim_triggers 每一列自帶 index_id，可以直接分組疊圖；但
   回測結果表（backtest_kpis 等）只知道「交易了哪個 ticker」，不知道
   「訊號來自哪個 index_id/trigger_type」——這個對應關係只存在
   benny-data-pipeline 的 backtest/strategies.py 裡，兩個 repo 沒有共用
   Python 套件機制（跟決定 12-C 指標名稱 denormalize 的理由一樣），所以
   下面 STRATEGY_SIGNAL_MAP 是手動同步的一份小對照表，新增策略時要記得
   兩邊都要更新（strategies.py 的訊號邏輯 + 這裡的顯示對照）。

用法：
    DUCKDB_PATH=/data/warehouse.duckdb python scripts/build_backtest_dashboard.py
"""
import json
import os
from collections import defaultdict

import duckdb

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/warehouse.duckdb")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "backtest_dashboard.html")

# strategy_name -> 這個策略的進出場訊號在 dim_triggers 裡對應的
# (market, index_id, entry_trigger_type, exit_trigger_type)。跟
# benny-data-pipeline/dags/stock_dashboard_etl/backtest/strategies.py 手動
# 保持同步，新增策略時兩邊都要改。
STRATEGY_SIGNAL_MAP = {
    "spx_golden_death_cross": {
        "market": "us",
        "index_id": 5,
        "entry_trigger": "SPX_GOLDEN_CROSS",
        "exit_trigger": "SPX_DEATH_CROSS",
    },
    "sox_spx_ratio_rotation": {
        "market": "us",
        "index_id": 101,
        "entry_trigger": "SOX_SPX_RATIO_NEW_HIGH_252D",
        "exit_trigger": "SOX_SPX_RATIO_NEW_LOW_252D",
    },
    "sox_macd_rotation": {
        "market": "us",
        "index_id": 7,
        "entry_trigger": "SOX_MACD_BULLISH_CROSS",
        "exit_trigger": "SOX_MACD_BEARISH_CROSS",
    },
    # extreme_fear_dip_buy_60d/_120d（方案一）故意不補進這份對照：進場是
    # VIX_EXTREME_FEAR_ENTER 疊加「同一天 SOX RSI14 < 30」的複合條件，出場
    # 是固定持有期，不是任何 trigger_type——這份對照表只吃「單一 entry
    # trigger + 單一 exit trigger」的形狀，硬塞 VIX_EXTREME_FEAR_ENTER 進來
    # 會讓疊圖顯示「每次極度恐慌都進場」，但實際上還有 RSI 條件篩掉一些，
    # 顯示會失真，不如照上面 fetch_strategy_signal_events() 的既有機制
    # 回傳空列表（KPI/Equity Curve/交易列表照常顯示，只是不疊訊號點）。
}


def fetch_runs(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    rows = conn.execute(
        "SELECT strategy_name, ticker, benchmark_ticker, start_date, end_date "
        "FROM stock_dashboard.backtest_runs ORDER BY strategy_name"
    ).fetchall()
    return [
        {
            "strategy_name": r[0], "ticker": r[1], "benchmark_ticker": r[2],
            "start_date": r[3].isoformat(), "end_date": r[4].isoformat(),
        }
        for r in rows
    ]


def _run_key(strategy_name: str, ticker: str) -> str:
    """`(strategy_name, ticker)` 是 backtest_* 表的真正 key（決定見追問一），
    同一個 strategy_name 可以對到多個 ticker（例如方案三同時跑 2330.TW/
    006208.TW）——純用 strategy_name 當 dict key 會讓後面跑的 ticker
    覆蓋掉前面的，這裡統一組成複合 key 避免這個問題。"""
    return f"{strategy_name}::{ticker}"


def fetch_kpis(conn: duckdb.DuckDBPyConnection) -> dict[str, dict]:
    cols = [d[0] for d in conn.execute(
        "SELECT * FROM stock_dashboard.backtest_kpis LIMIT 0"
    ).description]
    ticker_idx = cols.index("ticker")
    strategy_idx = cols.index("strategy_name")
    rows = conn.execute("SELECT * FROM stock_dashboard.backtest_kpis").fetchall()
    return {_run_key(row[strategy_idx], row[ticker_idx]): dict(zip(cols, row)) for row in rows}


def fetch_equity_curves(conn: duckdb.DuckDBPyConnection) -> dict[str, list[list]]:
    rows = conn.execute(
        "SELECT strategy_name, ticker, updated_at, strategy_value, benchmark_value, drawdown_pct "
        "FROM stock_dashboard.backtest_equity_curve ORDER BY strategy_name, ticker, updated_at"
    ).fetchall()
    curves: dict[str, list[list]] = defaultdict(list)
    for strategy_name, ticker, updated_at, strategy_value, benchmark_value, drawdown_pct in rows:
        curves[_run_key(strategy_name, ticker)].append([
            updated_at.isoformat(), strategy_value, benchmark_value, drawdown_pct
        ])
    return curves


def fetch_trades(conn: duckdb.DuckDBPyConnection) -> dict[str, list[dict]]:
    rows = conn.execute(
        "SELECT strategy_name, ticker, trade_seq, entry_date, exit_date, entry_price, "
        "exit_price, return_pct, holding_days "
        "FROM stock_dashboard.backtest_trades ORDER BY strategy_name, ticker, trade_seq"
    ).fetchall()
    trades: dict[str, list[dict]] = defaultdict(list)
    for strategy_name, ticker, seq, entry_date, exit_date, entry_price, exit_price, return_pct, holding_days in rows:
        trades[_run_key(strategy_name, ticker)].append({
            "trade_seq": seq,
            "entry_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat() if exit_date else None,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "return_pct": return_pct,
            "holding_days": holding_days,
        })
    return trades


def fetch_price_series(conn: duckdb.DuckDBPyConnection, ticker: str) -> list[list]:
    rows = conn.execute(
        "SELECT updated_at, close_price FROM stock_dashboard.backtest_universe "
        "WHERE ticker = ? ORDER BY updated_at",
        [ticker],
    ).fetchall()
    return [[r[0].isoformat(), r[1]] for r in rows]


def fetch_signal_events(conn: duckdb.DuckDBPyConnection, strategy_name: str) -> list[dict]:
    """用 STRATEGY_SIGNAL_MAP 查這個策略的進出場訊號實際發生日期，疊到價格圖上。
    找不到對照（新策略還沒補 STRATEGY_SIGNAL_MAP）就回傳空列表，不報錯。
    """
    signal_def = STRATEGY_SIGNAL_MAP.get(strategy_name)
    if signal_def is None:
        return []

    rows = conn.execute(
        "SELECT trigger_type, updated_at, trigger_value FROM stock_dashboard.dim_triggers "
        "WHERE market = ? AND index_id = ? AND trigger_type IN (?, ?) "
        "ORDER BY updated_at",
        [signal_def["market"], signal_def["index_id"],
         signal_def["entry_trigger"], signal_def["exit_trigger"]],
    ).fetchall()
    return [
        {
            "date": updated_at.isoformat(),
            "type": trigger_type,
            "is_entry": trigger_type == signal_def["entry_trigger"],
            "value": trigger_value,
        }
        for trigger_type, updated_at, trigger_value in rows
    ]


def compute_monthly_returns(equity_curve: list[list]) -> dict:
    """從策略每日累積淨值算月度報酬率，回傳 ECharts heatmap 要的
    {years: [...], months: [1..12], data: [[monthIdx, yearIdx, pct], ...]}。
    月度報酬 = 這個月最後一天淨值 / 上個月最後一天淨值 - 1，第一個月因為
    沒有「上個月」可比，跳過不畫。
    """
    if not equity_curve:
        return {"years": [], "data": []}

    monthly_last: dict[str, float] = {}
    for date_str, strategy_value, _, _ in equity_curve:
        month_key = date_str[:7]  # 'YYYY-MM'
        monthly_last[month_key] = strategy_value  # 同月覆蓋，保留最後一天

    sorted_months = sorted(monthly_last.keys())
    data = []
    years = sorted({m[:4] for m in sorted_months})
    for i in range(1, len(sorted_months)):
        prev_month, curr_month = sorted_months[i - 1], sorted_months[i]
        prev_value, curr_value = monthly_last[prev_month], monthly_last[curr_month]
        pct = (curr_value / prev_value - 1) * 100 if prev_value else None
        year, month = curr_month.split("-")
        month_idx = int(month) - 1
        year_idx = years.index(year)
        data.append([month_idx, year_idx, round(pct, 2) if pct is not None else None])

    return {"years": years, "data": data}


def build_strategies_payload(conn: duckdb.DuckDBPyConnection) -> dict:
    runs = fetch_runs(conn)
    kpis = fetch_kpis(conn)
    equity_curves = fetch_equity_curves(conn)
    trades = fetch_trades(conn)

    strategies = {}
    for run in runs:
        name = run["strategy_name"]
        ticker = run["ticker"]
        key = _run_key(name, ticker)
        equity_curve = equity_curves.get(key, [])
        strategies[key] = {
            "strategy_name": name,
            "ticker": ticker,
            "benchmark_ticker": run["benchmark_ticker"],
            "start_date": run["start_date"],
            "end_date": run["end_date"],
            "kpis": kpis.get(key, {}),
            "equity_curve": equity_curve,
            "monthly_returns": compute_monthly_returns(equity_curve),
            "trades": trades.get(key, []),
            "price": fetch_price_series(conn, ticker),
            "signals": fetch_signal_events(conn, name),
        }
    return strategies


HTML_TEMPLATE = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Layer 3 回測結果</title>
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
  .subtitle { color: var(--text-secondary); font-size: 13px; margin-bottom: 20px; }

  #filters {
    display: flex; flex-wrap: wrap; gap: 24px; align-items: flex-start; margin-bottom: 24px;
    background: var(--surface-1); border: 1px solid var(--border); border-radius: 10px; padding: 16px 20px;
  }
  .filter-block { display: flex; flex-direction: column; gap: 6px; }
  .filter-block label { font-size: 13px; font-weight: 600; color: var(--text-secondary); }
  select {
    font-size: 14px; padding: 6px 10px; background: var(--page-plane); color: var(--text-primary);
    border: 1px solid var(--border); border-radius: 6px; min-width: 260px;
  }

  .kpi-row { display: flex; gap: 12px; margin-bottom: 20px; flex-wrap: wrap; }
  .kpi-card {
    background: var(--surface-1); border: 1px solid var(--border); border-radius: 10px;
    padding: 14px 20px; min-width: 130px; flex: 1;
  }
  .kpi-card .kpi-label { font-size: 12px; color: var(--text-muted); margin-bottom: 6px; }
  .kpi-card .kpi-value {
    font-size: 24px; font-weight: 700; color: var(--text-primary);
    font-variant-numeric: tabular-nums;
  }
  .kpi-card.accent .kpi-value { color: var(--accent); }
  .kpi-card.good .kpi-value { color: var(--good); }
  .kpi-card.critical .kpi-value { color: var(--critical); }

  .panel {
    background: var(--surface-1); border: 1px solid var(--border); border-radius: 10px;
    padding: 16px 20px; margin-bottom: 24px;
  }
  .panel .hint { font-size: 12px; color: var(--text-muted); margin-bottom: 8px; }
  .chart { width: 100%; height: 380px; }
  .chart.short { height: 220px; }
  .chart.heatmap { height: 260px; }

  table.data-table { width: 100%; border-collapse: collapse; font-size: 13px; }
  table.data-table th, table.data-table td {
    text-align: right; padding: 6px 10px; border-bottom: 1px solid var(--border);
    font-variant-numeric: tabular-nums;
  }
  table.data-table th:first-child, table.data-table td:first-child { text-align: left; }
  table.data-table th { color: var(--text-secondary); font-weight: 600; }
  table.data-table td.positive { color: var(--good); }
  table.data-table td.negative { color: var(--critical); }
  .table-scroll { max-height: 320px; overflow-y: auto; }
  .no-data { color: var(--text-muted); font-size: 13px; padding: 20px 0; text-align: center; }
</style>
</head>
<body>
<h1>Layer 3 回測結果</h1>
<div class="subtitle">拿 dim_triggers 轉折訊號當買賣訊號，用 VectorBT 回測的結果——策略 vs 基準（buy &amp; hold）績效比較</div>

<div id="filters">
  <div class="filter-block">
    <label for="strategy-select">策略</label>
    <select id="strategy-select"></select>
  </div>
</div>

<div class="kpi-row" id="kpi-row"></div>

<div class="panel">
  <h2>Equity Curve（策略 vs 基準累積淨值）</h2>
  <div class="hint">三角形標記進出場訊號發生的日期</div>
  <div id="equity-chart" class="chart"></div>
</div>

<div class="panel">
  <h2>Underwater Chart（水下回撤）</h2>
  <div id="drawdown-chart" class="chart short"></div>
</div>

<div class="panel">
  <h2>月度報酬熱力圖</h2>
  <div class="hint">綠色=正報酬、紅色=負報酬，第一個月因為沒有上個月可比不畫</div>
  <div id="heatmap-chart" class="chart heatmap"></div>
</div>

<div class="panel">
  <h2>交易標的價格 + 訊號標記</h2>
  <div class="hint">訊號跟交易標的可能不是同一檔（例如訊號算在指數上、交易 ETF），見 tooltip 確認訊號實際來源</div>
  <div id="price-chart" class="chart"></div>
</div>

<div class="panel">
  <h2>逐筆交易列表</h2>
  <div class="table-scroll">
    <table class="data-table" id="trades-table">
      <thead>
        <tr><th>#</th><th>進場日</th><th>出場日</th><th>進場價</th><th>出場價</th><th>報酬%</th><th>持有天數</th></tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>

<script>
const STRATEGIES = __STRATEGIES_JSON__;

const AXIS_LABEL_STYLE = { fontSize: 12, fontWeight: "bold", color: "#c3c2b7" };
const AXIS_LINE_STYLE = { lineStyle: { color: "#383835", width: 2 } };
const SPLIT_LINE_STYLE = { lineStyle: { color: "#2c2c2a" } };
const ACCENT = "#3987e5";
const GOOD = "#0ca30c";
const CRITICAL = "#e66767";
const MUTED = "#898781";

const strategySelect = document.getElementById("strategy-select");
const kpiRow = document.getElementById("kpi-row");
const tradesTableBody = document.querySelector("#trades-table tbody");

const equityChart = echarts.init(document.getElementById("equity-chart"));
const drawdownChart = echarts.init(document.getElementById("drawdown-chart"));
const heatmapChart = echarts.init(document.getElementById("heatmap-chart"));
const priceChart = echarts.init(document.getElementById("price-chart"));

function populateStrategyOptions() {
  strategySelect.innerHTML = "";
  for (const key of Object.keys(STRATEGIES)) {
    const info = STRATEGIES[key];
    const opt = document.createElement("option");
    opt.value = key;
    opt.textContent = `${info.strategy_name}（${info.ticker}）`;
    strategySelect.appendChild(opt);
  }
}

function fmtPct(v, digits) {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return v.toFixed(digits === undefined ? 2 : digits) + "%";
}
function fmtNum(v, digits) {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return v.toFixed(digits === undefined ? 2 : digits);
}

function renderKpis() {
  const info = STRATEGIES[strategySelect.value];
  const k = info.kpis || {};
  kpiRow.innerHTML = "";
  const cards = [
    { label: "總報酬", value: fmtPct(k.total_return_pct), cls: k.total_return_pct >= 0 ? "good" : "critical" },
    { label: "CAGR（年化報酬）", value: fmtPct(k.cagr_pct), cls: "accent" },
    { label: "Sharpe Ratio", value: fmtNum(k.sharpe_ratio) },
    { label: "Sortino Ratio", value: fmtNum(k.sortino_ratio) },
    { label: "最大回撤", value: fmtPct(k.max_drawdown_pct), cls: "critical" },
    { label: "最大回撤天數", value: k.max_drawdown_days ?? "—" },
    { label: "勝率", value: fmtPct(k.win_rate_pct) },
    { label: "Profit Factor", value: fmtNum(k.profit_factor) },
    { label: "交易次數", value: k.num_trades ?? 0 },
    { label: "Calmar Ratio", value: fmtNum(k.calmar_ratio) },
    { label: "Alpha（年化）", value: fmtPct(k.alpha) },
    { label: "Beta", value: fmtNum(k.beta) },
  ];
  for (const c of cards) {
    const div = document.createElement("div");
    div.className = "kpi-card" + (c.cls ? " " + c.cls : "");
    div.innerHTML = `<div class="kpi-label">${c.label}</div><div class="kpi-value">${c.value}</div>`;
    kpiRow.appendChild(div);
  }
}

function signalMarkPoints(signals, valueLookup) {
  return signals.map((s) => ({
    name: s.type,
    xAxis: s.date,
    yAxis: valueLookup(s.date),
    symbol: s.is_entry ? "triangle" : "arrow",
    symbolRotate: s.is_entry ? 0 : 180,
    symbolSize: 12,
    itemStyle: { color: s.is_entry ? GOOD : CRITICAL },
    label: { show: false },
  })).filter((p) => p.yAxis !== undefined && p.yAxis !== null);
}

function renderEquityChart() {
  const info = STRATEGIES[strategySelect.value];
  const dates = info.equity_curve.map((r) => r[0]);
  const strategyValue = info.equity_curve.map((r) => [r[0], r[1]]);
  const benchmarkValue = info.equity_curve.map((r) => [r[0], r[2]]);
  const valueByDate = Object.fromEntries(info.equity_curve.map((r) => [r[0], r[1]]));
  const markPoints = signalMarkPoints(info.signals, (d) => valueByDate[d]);

  equityChart.setOption({
    backgroundColor: "transparent",
    tooltip: { trigger: "axis", backgroundColor: "#1a1a19", borderColor: "#383835", textStyle: { color: "#fff" } },
    legend: { top: 0, textStyle: { color: "#c3c2b7", fontSize: 12 } },
    grid: { containLabel: true, top: 40, bottom: 30, left: 10, right: 30 },
    xAxis: { type: "time", axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitLine: { show: false } },
    yAxis: { type: "value", scale: true, axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitLine: SPLIT_LINE_STYLE },
    series: [
      { name: "策略", type: "line", data: strategyValue, showSymbol: false, lineStyle: { color: ACCENT, width: 2 },
        markPoint: { data: markPoints, symbolSize: 12 } },
      { name: "基準（buy & hold）", type: "line", data: benchmarkValue, showSymbol: false, lineStyle: { color: MUTED, width: 1.5, type: "dashed" } },
    ],
  }, true);
}

function renderDrawdownChart() {
  const info = STRATEGIES[strategySelect.value];
  const drawdown = info.equity_curve.map((r) => [r[0], r[3]]);

  drawdownChart.setOption({
    backgroundColor: "transparent",
    tooltip: { trigger: "axis", backgroundColor: "#1a1a19", borderColor: "#383835", textStyle: { color: "#fff" } },
    grid: { containLabel: true, top: 20, bottom: 30, left: 10, right: 30 },
    xAxis: { type: "time", axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitLine: { show: false } },
    yAxis: { type: "value", max: 0, axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitLine: SPLIT_LINE_STYLE },
    series: [{
      name: "回撤%", type: "line", data: drawdown, showSymbol: false,
      lineStyle: { color: CRITICAL, width: 1.5 },
      areaStyle: { color: CRITICAL, opacity: 0.15 },
    }],
  }, true);
}

function renderHeatmapChart() {
  const info = STRATEGIES[strategySelect.value];
  const mr = info.monthly_returns;
  const months = ["1月","2月","3月","4月","5月","6月","7月","8月","9月","10月","11月","12月"];
  const hasData = mr.years && mr.years.length > 0 && mr.data.length > 0;

  if (!hasData) {
    heatmapChart.clear();
    heatmapChart.setOption({
      backgroundColor: "transparent",
      title: { text: "資料不足一個月，還沒有月度報酬可畫", left: "center", top: "middle", textStyle: { color: MUTED, fontSize: 13, fontWeight: "normal" } },
    }, true);
    return;
  }

  const values = mr.data.map((d) => d[2]).filter((v) => v !== null);
  const maxAbs = Math.max(1, ...values.map((v) => Math.abs(v)));

  heatmapChart.setOption({
    backgroundColor: "transparent",
    tooltip: {
      backgroundColor: "#1a1a19", borderColor: "#383835", textStyle: { color: "#fff" },
      formatter: (p) => `${mr.years[p.data[1]]}年 ${months[p.data[0]]}<br/>報酬：${p.data[2]}%`,
    },
    grid: { containLabel: true, top: 10, bottom: 30, left: 10, right: 10 },
    xAxis: { type: "category", data: months, axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitArea: { show: false } },
    yAxis: { type: "category", data: mr.years, axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitArea: { show: false } },
    visualMap: {
      min: -maxAbs, max: maxAbs, show: false,
      inRange: { color: [CRITICAL, "#2c2c2a", GOOD] },
    },
    series: [{
      type: "heatmap", data: mr.data,
      itemStyle: { borderColor: "#0d0d0d", borderWidth: 2 },
      label: { show: true, color: "#fff", fontSize: 11, formatter: (p) => p.data[2] === null ? "" : p.data[2] + "%" },
    }],
  }, true);
}

function renderPriceChart() {
  const info = STRATEGIES[strategySelect.value];
  const price = info.price;
  const valueByDate = Object.fromEntries(price);
  const markPoints = signalMarkPoints(info.signals, (d) => valueByDate[d]);

  priceChart.setOption({
    backgroundColor: "transparent",
    tooltip: { trigger: "axis", backgroundColor: "#1a1a19", borderColor: "#383835", textStyle: { color: "#fff" } },
    grid: { containLabel: true, top: 20, bottom: 30, left: 10, right: 30 },
    xAxis: { type: "time", axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitLine: { show: false } },
    yAxis: { type: "value", scale: true, axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitLine: SPLIT_LINE_STYLE },
    series: [{
      name: info.ticker, type: "line", data: price, showSymbol: false, lineStyle: { color: ACCENT, width: 1.5 },
      markPoint: { data: markPoints, symbolSize: 12 },
    }],
  }, true);
}

function renderTradesTable() {
  const info = STRATEGIES[strategySelect.value];
  tradesTableBody.innerHTML = "";
  if (!info.trades.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="7" class="no-data">還沒有任何交易</td>`;
    tradesTableBody.appendChild(tr);
    return;
  }
  for (const t of info.trades) {
    const tr = document.createElement("tr");
    const returnCls = t.return_pct >= 0 ? "positive" : "negative";
    tr.innerHTML = `
      <td>${t.trade_seq}</td>
      <td>${t.entry_date}</td>
      <td>${t.exit_date ?? "尚未出場"}</td>
      <td>${fmtNum(t.entry_price)}</td>
      <td>${fmtNum(t.exit_price)}</td>
      <td class="${returnCls}">${fmtPct(t.return_pct)}</td>
      <td>${t.holding_days ?? "—"}</td>
    `;
    tradesTableBody.appendChild(tr);
  }
}

function renderAll() {
  renderKpis();
  renderEquityChart();
  renderDrawdownChart();
  renderHeatmapChart();
  renderPriceChart();
  renderTradesTable();
}

strategySelect.addEventListener("change", renderAll);
window.addEventListener("resize", () => {
  equityChart.resize(); drawdownChart.resize(); heatmapChart.resize(); priceChart.resize();
});

populateStrategyOptions();
renderAll();
</script>
</body>
</html>
"""


def main() -> None:
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        strategies = build_strategies_payload(conn)
    finally:
        conn.close()

    def _dumps(obj) -> str:
        return json.dumps(obj, ensure_ascii=False, default=str)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    html = HTML_TEMPLATE.replace("__STRATEGIES_JSON__", _dumps(strategies))
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[OK] wrote {OUTPUT_PATH} ({len(strategies)} strategies)")


if __name__ == "__main__":
    main()
