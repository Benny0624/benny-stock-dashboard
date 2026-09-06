"""查詢 DuckDB，產出一份合併美股/總經 + 台股的 dashboard HTML：指標走勢
（價格/比值 + MA50/MA200）、RSI、月線 MACD（僅 SOX）、轉折點事件標記、
持續性狀態背景著色。資料直接內嵌在檔案裡（不需要後端），圖表庫用 CDN 載入
的 ECharts 5，模式照抄 `Yahoo_fantasy_dashboard/scripts/build_dashboard.py`。

2026-09-06 改版（layer3_backtest_proposal.md 追問三 + 第 10 節）：
- 美股/台股不再各自產一份 html（拿掉 MARKET 環境變數），改成一份
  `output/dashboard.html`，兩個 DAG（us/tw）誰觸發都查全部資料、產同一份
  檔案——因為每次都是查當下 DB 的完整狀態，誰先誰後跑完結果一樣，不會
  互相覆蓋壞掉彼此的資料。
- 指標 filter 從單選 `<select>` 改成多選 checklist：選 1 個指標維持原本的
  深度檢視（MA/RSI/MACD/trigger markPoint）；選 2 個以上切換成「比較模式」
  ——每個指標的價格/比值以窗口起點為基準重新指數化成 100，疊在同一張圖上，
  各自的事件型 trigger 改成整張圖的垂直虛線（markLine），方便對照「A 指標
  轉折當天，B 指標/個股在做什麼」。
- 新增 `_STATE` 背景著色（markArea）：`_STATE` trigger 沒有現成的 is_active
  欄位，只有 `trigger_value`（原始數值），要不要著色靠 STATE_ACTIVE_RULES
  這份手動維護的門檻對照表重新判斷 active 與否——門檻**必須**跟
  `benny-data-pipeline/dags/stock_dashboard_etl/templates/compute_triggers.sql`
  裡每個 `*_state` CTE 的 `is_active` 條件保持一致，SQL 那邊改門檻這裡要
  記得跟著改，不會有機制自動提醒（跟 `STRATEGY_SIGNAL_MAP` 是同一種
  已知限制，兩個 repo 沒有共用 Python 套件機制）。
- 新增指標定義 glossary（GLOSSARY dict）：跟 index_name 一樣的邏輯，直接
  寫死一份小 dict 放在這支腳本裡（決定 12-C 的既有慣例，不 import
  `benny-data-pipeline` 的 `constants/index_map.py`）。
- 新增交易日曆判斷（`pandas_market_calendars`）：美股查 NYSE、台股查 XTAI
  日曆，算出「以今天為準，預期最新應該有資料的交易日」，跟 DB 實際最新
  `updated_at` 比較，只有「今天該有新資料但沒有」才標紅，休市日不會誤報
  （grilling_notes.md 決定 9，取代原本「永遠顯示但不判斷」的作法）。

指標分兩種，資料形狀不完全一樣：
- 真實指標（index_id 1-11）：原始數值在 raw_stock，MA50/MA200/RSI14 在
  silver_stock。SOX（index_id=7）額外有 MACD/MACD_SIGNAL（月頻率，只有這一個
  指標有）。
- 合成指標（index_id 101+，SOX/SPX、道瓊/那指、羅素2000/SPX 三個比值）：沒有
  raw_stock 資料，比值本身（agg_type='RAW'）跟 MA50/MA200 都在 silver_stock。

轉折點（dim_triggers）用「這個 trigger 屬於哪個 index_id」直接分組疊到對應
的圖表上，不需要另外維護一份 trigger_type -> index_id 的對照表——每筆
trigger row 已經帶著自己的 index_id（見 compute_triggers.sql）。

用法：
    DUCKDB_PATH=/data/warehouse.duckdb python scripts/build_dashboard.py

warehouse.duckdb 放在 Docker named volume（benny-infra-duckdb-data）裡，不會
直接出現在檔案系統上，本機要跑這支腳本的話用容器掛同一顆 volume 進去執行，
例如：

    docker run --rm \\
        -v benny-infra-duckdb-data:/data \\
        -v "$(pwd):/app" -w /app \\
        python:3.11-slim \\
        bash -c "pip install duckdb pandas-market-calendars --quiet && python scripts/build_dashboard.py"
"""
import json
import os
from datetime import date, timedelta
from itertools import groupby

import duckdb
import pandas_market_calendars as mcal

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/warehouse.duckdb")
QUERY_WINDOW_YEARS = 2  # 決定 15：DB 層不設 retention，dashboard 查詢層設 2 年上限
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "dashboard.html")

# 決定 15 拍板的窗口選項，'2yr' 對齊查詢上限（DB 資料以後超過 2 年也不會讓
# 頁面無限變大，需要回溯更久的冷資料 Benny 自己進 DB 查）。
WINDOW_OPTIONS = {"3mo": 90, "6mo": 180, "1yr": 365, "2yr": 730}

# 事件型 trigger（有實際發生日期才寫一筆），排除 *_STATE（每天都有一筆，
# 走另一條路線變成背景著色，不當標記點）。
TRIGGER_MARKER_EXCLUDE_SUFFIX = "_STATE"

MARKET_LABELS = {"us": "美股/總經", "tw": "台股"}
MARKET_CALENDARS = {"us": "NYSE", "tw": "XTAI"}

# 指標說明 glossary——跟 index_name 一樣不 import benny-data-pipeline 的
# constants/index_map.py（決定 12-C），直接維護一份小 dict。key 是 index_id。
GLOSSARY: dict[int, str] = {
    1: "美國政府 10 年期公債殖利率，反映市場對長期經濟成長與通膨的預期，"
       "常被當作無風險利率的長天期參考值。",
    2: "美國政府 2 年期公債殖利率，對聯準會短期貨幣政策（升息/降息）最敏感，"
       "跟 10 年期利差常被拿來判斷殖利率曲線形狀。",
    3: "10 年期減 2 年期公債殖利率（利差）。數值轉負（倒掛）是常見的經濟"
       "衰退領先指標，歷史上多次倒掛後 1-2 年內出現衰退。",
    4: "VIX 恐慌指數：由 S&P 500 選擇權價格反推出的隱含波動率，反映市場對"
       "未來 30 天的波動預期。數值越高代表恐慌情緒越重——低於 12 通常代表"
       "市場過度樂觀（自滿），高於 30 代表恐慌，高於 35 代表極度恐慌。",
    5: "S&P 500 指數：涵蓋美國 500 家大型上市公司市值，是美股大盤最常用的"
       "參考基準。",
    6: "Nasdaq 綜合指數：那斯達克交易所全部上市公司組成，成分股偏重科技股，"
       "對利率變化與成長股情緒較敏感。",
    7: "費城半導體指數（SOX）：追蹤半導體產業龍頭股表現，常被視為全球科技/"
       "半導體景氣循環的領先指標。",
    8: "道瓊工業指數（DJI）：美國 30 檔藍籌股組成的價格加權指數，成分股偏向"
       "傳統產業與價值股，跟科技股權重高的 Nasdaq 走勢常出現分歧。",
    9: "羅素 2000 指數（RUT）：追蹤美國中小型股表現，對經濟成長與利率環境"
       "變化較敏感，常被當成市場「風險偏好」的指標。",
    10: "台灣證券交易所加權股價指數，反映台股大盤整體表現。",
    11: "台積電（2330.TW）：全球最大晶圓代工廠，台股市值最大權值股，走勢對"
        "加權指數影響力極大。",
    101: "SOX/SPX Ratio：半導體指數相對大盤的相對強弱。比值上升代表半導體股"
         "領漲大盤（資金偏向成長/景氣循環股），下降代表資金轉向防禦。",
    102: "道瓊/那指 Ratio：傳產價值股相對科技成長股的相對強弱。比值上升代表"
         "資金從高 Beta 科技股轉向防禦型價值股。",
    103: "羅素2000/SPX Ratio：中小型股相對大型股的相對強弱，常被視為市場"
         "風險偏好指標，比值上升代表資金偏好中小型股（risk-on）。",
}

# `_STATE` trigger 沒有存 is_active 欄位，只有 trigger_value（原始數值），
# 要不要著色靠這份門檻對照表重新判斷——**門檻必須跟 compute_triggers.sql
# 裡對應 CTE 的 is_active 條件保持一致**，SQL 改門檻這裡要記得跟著改。
TRIGGER_GLOSSARY: dict[str, str] = {
    "YIELD_INVERSION_ENTER": "10年期-2年期公債殖利率利差由正轉負（開始倒掛），常見的經濟衰退領先訊號。",
    "YIELD_INVERSION_EXIT": "殖利率利差由負轉正（倒掛結束/解倒掛）。",
    "VIX_PANIC_ENTER": "VIX 站上 30，市場進入恐慌狀態。",
    "VIX_PANIC_EXIT": "VIX 跌破 30，恐慌狀態解除。",
    "VIX_EXTREME_FEAR_ENTER": "VIX 站上 35，市場進入極度恐慌。",
    "VIX_EXTREME_FEAR_EXIT": "VIX 跌破 35，極度恐慌解除。",
    "VIX_COMPLACENCY_ENTER": "VIX 跌破 12，市場過度樂觀（自滿）。",
    "VIX_COMPLACENCY_EXIT": "VIX 站上 12，自滿狀態解除。",
    "SPX_200SMA_BREAKOUT": "S&P 500 股價站上 200 日均線（長期多頭訊號）。",
    "SPX_200SMA_BREAKDOWN": "S&P 500 股價跌破 200 日均線（長期空頭訊號）。",
    "SPX_GOLDEN_CROSS": "S&P 500 的 50 日均線由下往上穿越 200 日均線（黃金交叉，中長期偏多訊號）。",
    "SPX_DEATH_CROSS": "S&P 500 的 50 日均線由上往下穿越 200 日均線（死亡交叉，中長期偏空訊號）。",
    "SPX_50SMA_BREAKOUT": "S&P 500 股價站上 50 日均線（短中期多頭訊號）。",
    "SPX_50SMA_BREAKDOWN": "S&P 500 股價跌破 50 日均線（短中期空頭訊號）。",
    "RUT_SPX_200SMA_BREAKOUT": "羅素2000/SPX 相對強弱比值站上 200 日均線（中小型股開始相對強勢）。",
    "RUT_SPX_200SMA_BREAKDOWN": "羅素2000/SPX 相對強弱比值跌破 200 日均線（中小型股轉弱）。",
    "DJI_IXIC_ROC_ENTER": "道瓊/那指比值 20 個交易日漲幅超過 3%（資金從科技股轉向傳產價值股）。",
    "DJI_IXIC_ROC_EXIT": "道瓊/那指比值 20 日動能降回 3% 以下。",
    "SOX_SPX_RATIO_NEW_HIGH_252D": "SOX/SPX 相對強弱比值創 252 個交易日（約 52 週）新高，半導體相對大盤走強。",
    "SOX_SPX_RATIO_NEW_LOW_252D": "SOX/SPX 相對強弱比值創 252 個交易日新低，半導體相對大盤走弱。",
    "SOX_MACD_BULLISH_CROSS": "費城半導體指數月線 MACD 黃金交叉（中長期偏多轉向訊號）。",
    "SOX_MACD_BEARISH_CROSS": "費城半導體指數月線 MACD 死亡交叉（中長期偏空轉向訊號）。",
}

STATE_ACTIVE_RULES: dict[str, dict] = {
    "YIELD_INVERSION_STATE": {"is_active": lambda v: v < 0, "label": "殖利率倒掛", "color": "rgba(230,103,103,0.16)"},
    "VIX_PANIC_STATE": {"is_active": lambda v: v >= 30, "label": "VIX 恐慌 (>=30)", "color": "rgba(230,103,103,0.16)"},
    "VIX_EXTREME_FEAR_STATE": {"is_active": lambda v: v >= 35, "label": "VIX 極度恐慌 (>=35)", "color": "rgba(200,40,40,0.22)"},
    "VIX_COMPLACENCY_STATE": {"is_active": lambda v: v < 12, "label": "VIX 自滿 (<12)", "color": "rgba(12,163,12,0.14)"},
    "SPX_200SMA_STATE": {"is_active": lambda v: v >= 0, "label": "SPX 站上 200SMA", "color": "rgba(12,163,12,0.10)"},
    "SPX_MA_CROSS_STATE": {"is_active": lambda v: v >= 0, "label": "SPX 多頭排列 (MA50>=MA200)", "color": "rgba(12,163,12,0.10)"},
    "SPX_50SMA_STATE": {"is_active": lambda v: v >= 0, "label": "SPX 站上 50SMA", "color": "rgba(12,163,12,0.08)"},
    "RUT_SPX_200SMA_STATE": {"is_active": lambda v: v >= 0, "label": "RUT/SPX 站上 200SMA", "color": "rgba(57,135,229,0.12)"},
    "DJI_IXIC_ROC_STATE": {"is_active": lambda v: v > 0.03, "label": "DJI/IXIC 20日動能 > 3%", "color": "rgba(57,135,229,0.12)"},
}


def _query_window_start() -> date:
    return date.today() - timedelta(days=365 * QUERY_WINDOW_YEARS)


def fetch_real_index_levels(conn: duckdb.DuckDBPyConnection) -> dict[int, dict]:
    """真實指標的原始數值（raw_stock），回傳
    {index_id: {"index_name":..., "market":..., "level": [[date_str, value], ...]}}"""
    rows = conn.execute(
        """
        SELECT index_id, index_name, market, updated_at, index_value
        FROM stock_dashboard.raw_stock
        WHERE updated_at >= ?
        ORDER BY index_id, updated_at
        """,
        [_query_window_start()],
    ).fetchall()

    indices: dict[int, dict] = {}
    for index_id, index_name, market, updated_at, value in rows:
        entry = indices.setdefault(
            index_id, {"index_name": index_name, "market": market, "is_synthetic": False, "level": []}
        )
        entry["level"].append([updated_at.isoformat(), value])
    return indices


def fetch_silver_series(conn: duckdb.DuckDBPyConnection) -> dict[int, dict]:
    """silver_stock 的所有列（MA50/MA200/RSI14/MACD/MACD_SIGNAL/合成指標的 RAW 比值）。
    合成指標的 agg_type='RAW' 直接當作 level 使用（合成指標沒有 raw_stock 資料）。
    """
    rows = conn.execute(
        """
        SELECT index_id, index_name, market, agg_type, updated_at, agg_value
        FROM stock_dashboard.silver_stock
        WHERE updated_at >= ?
        ORDER BY index_id, agg_type, updated_at
        """,
        [_query_window_start()],
    ).fetchall()

    silver: dict[int, dict] = {}
    for index_id, index_name, market, agg_type, updated_at, value in rows:
        entry = silver.setdefault(index_id, {"index_name": index_name, "market": market, "series": {}})
        entry["series"].setdefault(agg_type, []).append([updated_at.isoformat(), value])
    return silver


def fetch_triggers(conn: duckdb.DuckDBPyConnection) -> dict[int, list[dict]]:
    """事件型 trigger，依 index_id 分組。回傳 {index_id: [{"date":..., "type":..., "value":...}, ...]}"""
    rows = conn.execute(
        """
        SELECT index_id, trigger_type, trigger_value, updated_at
        FROM stock_dashboard.dim_triggers
        WHERE updated_at >= ? AND trigger_type NOT LIKE ?
        ORDER BY index_id, updated_at
        """,
        [_query_window_start(), f"%{TRIGGER_MARKER_EXCLUDE_SUFFIX}"],
    ).fetchall()

    triggers: dict[int, list[dict]] = {}
    for index_id, trigger_type, trigger_value, updated_at in rows:
        triggers.setdefault(index_id, []).append(
            {"date": updated_at.isoformat(), "type": trigger_type, "value": trigger_value}
        )
    return triggers


def fetch_state_ranges(conn: duckdb.DuckDBPyConnection) -> dict[int, dict[str, list[list[str]]]]:
    """`_STATE` trigger 收斂成連續區間，回傳
    {index_id: {trigger_type: [[start_date, end_date], ...]}}，只收
    STATE_ACTIVE_RULES 裡有定義門檻的 trigger_type（目前 9 種都涵蓋）。"""
    rows = conn.execute(
        """
        SELECT index_id, trigger_type, updated_at, trigger_value
        FROM stock_dashboard.dim_triggers
        WHERE updated_at >= ? AND trigger_type LIKE ?
        ORDER BY index_id, trigger_type, updated_at
        """,
        [_query_window_start(), f"%{TRIGGER_MARKER_EXCLUDE_SUFFIX}"],
    ).fetchall()

    ranges: dict[int, dict[str, list]] = {}
    for (index_id, trigger_type), group in groupby(rows, key=lambda r: (r[0], r[1])):
        rule = STATE_ACTIVE_RULES.get(trigger_type)
        if rule is None:
            continue
        run_start = None
        run_end = None
        out: list[list[str]] = []
        for _, _, updated_at, trigger_value in group:
            active = trigger_value is not None and rule["is_active"](trigger_value)
            if active:
                if run_start is None:
                    run_start = updated_at
                run_end = updated_at
            elif run_start is not None:
                out.append([run_start.isoformat(), run_end.isoformat()])
                run_start, run_end = None, None
        if run_start is not None:
            out.append([run_start.isoformat(), run_end.isoformat()])
        if out:
            ranges.setdefault(index_id, {})[trigger_type] = out
    return ranges


def build_indices_payload(conn: duckdb.DuckDBPyConnection) -> dict:
    """把查詢結果組成前端要吃的單一結構：
    {index_id: {index_name, market, is_synthetic, level, ma50, ma200, rsi14,
                macd, macd_signal, triggers, state_ranges, glossary}}
    """
    real_levels = fetch_real_index_levels(conn)
    silver = fetch_silver_series(conn)
    triggers = fetch_triggers(conn)
    state_ranges = fetch_state_ranges(conn)

    indices: dict[str, dict] = {}
    all_index_ids = set(real_levels) | set(silver)
    for index_id in all_index_ids:
        real = real_levels.get(index_id)
        silver_entry = silver.get(index_id, {"index_name": None, "market": None, "series": {}})
        series = silver_entry["series"]

        is_synthetic = real is None
        level = real["level"] if real is not None else series.get("RAW", [])
        index_name = (real or {}).get("index_name") or silver_entry["index_name"]
        market = (real or {}).get("market") or silver_entry["market"]

        indices[str(index_id)] = {
            "index_name": index_name,
            "market": market,
            "is_synthetic": is_synthetic,
            "level": level,
            "ma50": series.get("MA50", []),
            "ma200": series.get("MA200", []),
            "rsi14": series.get("RSI14", []),
            "macd": series.get("MACD", []),
            "macd_signal": series.get("MACD_SIGNAL", []),
            "triggers": triggers.get(index_id, []),
            "state_ranges": state_ranges.get(index_id, {}),
            "glossary": GLOSSARY.get(index_id, ""),
        }
    return indices


def _expected_latest_trading_day(market: str, as_of: date) -> date | None:
    """用交易日曆算「以 as_of 為準，預期最新應該有資料的交易日」（決定 9）。
    抓過去 10 天的日曆窗口找最後一個交易日，10 天足夠涵蓋任何連假。"""
    cal = mcal.get_calendar(MARKET_CALENDARS[market])
    valid_days = cal.valid_days(start_date=as_of - timedelta(days=10), end_date=as_of)
    if len(valid_days) == 0:
        return None
    return valid_days[-1].date()


def build_data_status(conn: duckdb.DuckDBPyConnection) -> dict[str, dict]:
    """每個 market 各自的資料新鮮度狀態，取代舊版「永遠顯示但不判斷」的
    紅字——只有「今天該有新資料但沒有」才標記 is_stale=True，休市日不誤報。"""
    latest_by_market = dict(
        conn.execute(
            "SELECT market, MAX(updated_at) FROM stock_dashboard.raw_stock GROUP BY market"
        ).fetchall()
    )
    today = date.today()
    status: dict[str, dict] = {}
    for market in MARKET_CALENDARS:
        latest = latest_by_market.get(market)
        expected = _expected_latest_trading_day(market, today)
        is_stale = expected is not None and (latest is None or latest < expected)
        status[market] = {
            "latest": latest.isoformat() if latest else None,
            "expected": expected.isoformat() if expected else None,
            "is_stale": is_stale,
        }
    return status


HTML_TEMPLATE = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>市場轉折監測 Dashboard</title>
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
  .status-row { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 20px; }
  .status-pill { font-size: 13px; font-weight: 600; padding: 4px 10px; border-radius: 6px; }
  .status-pill.good { color: var(--good); background: rgba(12,163,12,0.10); }
  .status-pill.critical { color: var(--critical); background: rgba(230,103,103,0.12); }

  .kpi-row { display: flex; gap: 12px; margin-bottom: 20px; flex-wrap: wrap; }
  .kpi-card {
    background: var(--surface-1); border: 1px solid var(--border); border-radius: 10px;
    padding: 14px 20px; min-width: 140px; flex: 1;
  }
  .kpi-card .kpi-label { font-size: 12px; color: var(--text-muted); margin-bottom: 6px; }
  .kpi-card .kpi-value {
    font-size: 22px; font-weight: 700; color: var(--text-primary);
    font-variant-numeric: tabular-nums;
  }
  .kpi-card .kpi-sub { font-size: 12px; color: var(--text-secondary); margin-top: 4px; }
  .kpi-card.accent { border-color: var(--accent); }
  .kpi-card.accent .kpi-value { color: var(--accent); }

  #filters {
    display: flex; flex-wrap: wrap; gap: 24px; align-items: flex-start; margin-bottom: 16px;
    background: var(--surface-1); border: 1px solid var(--border); border-radius: 10px; padding: 16px 20px;
  }
  .filter-block { display: flex; flex-direction: column; gap: 6px; }
  .filter-block label { font-size: 13px; font-weight: 600; color: var(--text-secondary); }
  select {
    font-size: 14px; padding: 6px 10px; background: var(--page-plane); color: var(--text-primary);
    border: 1px solid var(--border); border-radius: 6px; min-width: 160px;
  }
  .checklist { display: flex; flex-direction: column; gap: 2px; min-width: 280px; max-height: 220px; overflow-y: auto; }
  .checklist .group-label { font-size: 11px; font-weight: 700; color: var(--text-muted); margin: 6px 0 2px; text-transform: uppercase; }
  .checklist label { display: flex; align-items: center; gap: 6px; font-size: 13px; font-weight: 400; color: var(--text-primary); cursor: pointer; padding: 2px 0; }
  .checklist input { cursor: pointer; }
  .checklist .synthetic-tag { color: var(--text-muted); font-size: 11px; }

  .panel {
    background: var(--surface-1); border: 1px solid var(--border); border-radius: 10px;
    padding: 16px 20px; margin-bottom: 24px;
  }
  .panel .hint { font-size: 12px; color: var(--text-muted); margin-bottom: 8px; }
  .chart { width: 100%; height: 460px; }
  .chart.short { height: 220px; }
  .chart.hidden { display: none; }
  .panel.hidden { display: none; }
  .glossary-item { font-size: 13px; color: var(--text-secondary); margin-bottom: 10px; line-height: 1.5; }
  .glossary-item b { color: var(--text-primary); }
  .glossary-empty { font-size: 13px; color: var(--text-muted); }
  #trigger-legend { margin-top: 12px; padding-top: 12px; border-top: 1px solid var(--border); }
  #trigger-legend .trigger-legend-title { font-size: 12px; font-weight: 700; color: var(--text-muted); margin-bottom: 6px; text-transform: uppercase; }
  #trigger-legend .trigger-item { font-size: 12px; color: var(--text-secondary); margin-bottom: 4px; line-height: 1.5; }
  #trigger-legend .trigger-item .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
  #trigger-legend .trigger-item code { color: var(--text-primary); font-size: 11px; background: rgba(255,255,255,0.06); padding: 1px 5px; border-radius: 4px; margin-right: 6px; }
</style>
</head>
<body>
<h1>市場轉折監測 Dashboard</h1>
<div class="subtitle">美股/總經 + 台股合併檢視 · 指標走勢、RSI、轉折點標記與持續性狀態</div>
<div class="status-row" id="status-row"></div>

<div id="filters">
  <div class="filter-block">
    <label>指標（可複選比較，選 2 個以上自動切換成標準化比較模式）</label>
    <div id="index-checklist" class="checklist"></div>
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

<div class="panel" id="glossary-panel">
  <h2>指標說明</h2>
  <div id="glossary-body"></div>
</div>

<div class="kpi-row" id="kpi-row"></div>

<div class="panel">
  <h2 id="level-title">走勢</h2>
  <div class="hint" id="level-hint"></div>
  <div id="level-chart" class="chart"></div>
  <div id="trigger-legend"></div>
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
const DATA_STATUS = __DATA_STATUS_JSON__;
const MARKET_LABELS = { us: "美股/總經", tw: "台股" };
const STATE_META = __STATE_META_JSON__;
const TRIGGER_GLOSSARY = __TRIGGER_GLOSSARY_JSON__;

const AXIS_TEXT_STYLE = { fontSize: 14, fontWeight: "bold", color: "#ffffff" };
const AXIS_LABEL_STYLE = { fontSize: 12, fontWeight: "bold", color: "#c3c2b7" };
const AXIS_LINE_STYLE = { lineStyle: { color: "#383835", width: 2 } };
const SPLIT_LINE_STYLE = { lineStyle: { color: "#2c2c2a" } };
const ACCENT = "#3987e5";
const GOOD = "#0ca30c";
const CRITICAL = "#e66767";
const MUTED = "#898781";
const PALETTE = ["#3987e5", "#e6a83f", "#0ca30c", "#e66767", "#b98ae6", "#4fc3c3", "#e677b0", "#a3c93f"];

// trigger_type 尾巴代表方向：ENTER/BREAKOUT/GOLDEN_CROSS/NEW_HIGH... 是「進入/
// 達成」用向上三角，EXIT/BREAKDOWN/DEATH_CROSS/NEW_LOW... 是「離開」用向下
// 三角——純粹描述「有沒有進入某個已定義的條件」，不是多空判斷（例如
// VIX_PANIC_ENTER 是恐慌開始，通常是偏空訊號，但一樣用「進入」的向上三角），
// 實際意義看 tooltip 裡完整的 trigger_type 名稱。
const ENTER_KEYWORDS = ["ENTER", "BREAKOUT", "GOLDEN_CROSS", "BULLISH_CROSS", "NEW_HIGH"];
function isEnterEvent(triggerType) {
  return ENTER_KEYWORDS.some((kw) => triggerType.includes(kw));
}

const checklistEl = document.getElementById("index-checklist");
const windowSelect = document.getElementById("window-select");
const statusRow = document.getElementById("status-row");
const kpiRow = document.getElementById("kpi-row");
const levelTitle = document.getElementById("level-title");
const levelHint = document.getElementById("level-hint");
const rsiPanel = document.getElementById("rsi-panel");
const macdPanel = document.getElementById("macd-panel");
const glossaryBody = document.getElementById("glossary-body");
const triggerLegend = document.getElementById("trigger-legend");

const levelChart = echarts.init(document.getElementById("level-chart"));
const rsiChart = echarts.init(document.getElementById("rsi-chart"));
const macdChart = echarts.init(document.getElementById("macd-chart"));

function selectedIds() {
  return Array.from(checklistEl.querySelectorAll("input:checked")).map((el) => el.value);
}

function populateIndexChecklist() {
  checklistEl.innerHTML = "";
  const ids = Object.keys(INDICES).sort((a, b) => Number(a) - Number(b));
  const byMarket = { us: [], tw: [] };
  for (const id of ids) byMarket[INDICES[id].market].push(id);

  for (const market of ["us", "tw"]) {
    if (byMarket[market].length === 0) continue;
    const heading = document.createElement("div");
    heading.className = "group-label";
    heading.textContent = MARKET_LABELS[market];
    checklistEl.appendChild(heading);
    for (const id of byMarket[market]) {
      const info = INDICES[id];
      const label = document.createElement("label");
      label.title = info.glossary || "";
      const input = document.createElement("input");
      input.type = "checkbox";
      input.value = id;
      input.checked = id === ids[0];
      input.addEventListener("change", renderAll);
      label.appendChild(input);
      label.append(info.index_name + (info.is_synthetic ? " " : ""));
      if (info.is_synthetic) {
        const tag = document.createElement("span");
        tag.className = "synthetic-tag";
        tag.textContent = "（比值）";
        label.appendChild(tag);
      }
      checklistEl.appendChild(label);
    }
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

function toMarkPoints(triggers) {
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

function toMarkLines(indicesInfo) {
  const start = windowStartDate();
  const lines = [];
  for (const info of indicesInfo) {
    for (const t of info.triggers) {
      if (t.date < start) continue;
      const color = isEnterEvent(t.type) ? GOOD : CRITICAL;
      lines.push({
        xAxis: t.date,
        label: { formatter: t.type, color, fontSize: 10, rotate: 90, position: "insideEndTop" },
        lineStyle: { color, type: "dashed", width: 1 },
      });
    }
  }
  return lines;
}

function toMarkAreas(indicesInfo) {
  const start = windowStartDate();
  const areas = [];
  for (const info of indicesInfo) {
    for (const [triggerType, ranges] of Object.entries(info.state_ranges || {})) {
      const meta = STATE_META[triggerType] || { label: triggerType, color: "rgba(150,150,150,0.12)" };
      for (const [rangeStart, rangeEnd] of ranges) {
        if (rangeEnd < start) continue;
        areas.push([
          { xAxis: rangeStart, itemStyle: { color: meta.color }, name: meta.label },
          { xAxis: rangeEnd },
        ]);
      }
    }
  }
  return areas;
}

function normalizeTo100(series) {
  const filtered = filterByWindow(series);
  if (!filtered.length) return [];
  const base = filtered[0][1];
  if (!base) return filtered.map(([d]) => [d, null]);
  return filtered.map(([d, v]) => [d, (v / base) * 100]);
}

function renderStatusRow() {
  statusRow.innerHTML = "";
  for (const market of ["us", "tw"]) {
    const s = DATA_STATUS[market];
    const pill = document.createElement("div");
    let text, cls;
    if (!s.latest) {
      text = `${MARKET_LABELS[market]}：尚無資料`;
      cls = "critical";
    } else if (s.is_stale) {
      text = `${MARKET_LABELS[market]}：資料可能過期，最新 ${s.latest}，預期至少到 ${s.expected}（交易日曆判斷）`;
      cls = "critical";
    } else {
      text = `${MARKET_LABELS[market]}：資料最新，${s.latest}`;
      cls = "good";
    }
    pill.className = "status-pill " + cls;
    pill.textContent = text;
    statusRow.appendChild(pill);
  }
}

function renderGlossary(ids) {
  glossaryBody.innerHTML = "";
  if (!ids.length) {
    glossaryBody.innerHTML = '<div class="glossary-empty">勾選左側指標查看說明。</div>';
    return;
  }
  for (const id of ids) {
    const info = INDICES[id];
    const div = document.createElement("div");
    div.className = "glossary-item";
    div.innerHTML = `<b>${info.index_name}</b>${info.is_synthetic ? "（比值）" : ""}：${info.glossary || "（尚無說明）"}`;
    glossaryBody.appendChild(div);
  }
}

function renderTriggerLegend(ids) {
  const start = windowStartDate();
  const seen = new Set();
  const rows = [];
  for (const id of ids) {
    for (const t of INDICES[id].triggers) {
      if (t.date < start || seen.has(t.type)) continue;
      seen.add(t.type);
      const isEnter = isEnterEvent(t.type);
      rows.push({
        type: t.type,
        color: isEnter ? GOOD : CRITICAL,
        desc: TRIGGER_GLOSSARY[t.type] || "（尚無說明）",
      });
    }
  }
  if (!rows.length) {
    triggerLegend.innerHTML = "";
    return;
  }
  rows.sort((a, b) => a.type.localeCompare(b.type));
  const items = rows.map((r) =>
    `<div class="trigger-item"><span class="dot" style="background:${r.color}"></span><code>${r.type}</code>${r.desc}</div>`
  ).join("");
  triggerLegend.innerHTML = `<div class="trigger-legend-title">本圖轉折點說明（綠點=進入/達成，紅點=離開/相反方向）</div>${items}`;
}

function renderKpis(ids) {
  kpiRow.innerHTML = "";
  const start = windowStartDate();
  let cards;
  if (ids.length === 1) {
    const info = INDICES[ids[0]];
    const level = filterByWindow(info.level);
    const triggers = info.triggers.filter((t) => t.date >= start);
    const latest = level.length ? level[level.length - 1][1] : null;
    cards = [
      { label: "目前選取指標", value: info.index_name, sub: info.is_synthetic ? "合成指標（比值）" : "真實指標", accent: true },
      { label: "最新數值", value: latest != null ? latest.toFixed(4) : "—", sub: level.length ? level[level.length - 1][0] : "" },
      { label: "窗口內資料筆數", value: level.length, sub: windowSelect.value },
      { label: "窗口內轉折事件", value: triggers.length, sub: "不含 *_STATE" },
    ];
  } else {
    const totalTriggers = ids.reduce(
      (sum, id) => sum + INDICES[id].triggers.filter((t) => t.date >= start).length, 0
    );
    cards = [
      { label: "比較模式：已選指標數", value: ids.length, sub: ids.map((id) => INDICES[id].index_name).join("、"), accent: true },
      { label: "時間範圍", value: windowSelect.value, sub: "價格/比值已指數化為 100 起算" },
      { label: "窗口內轉折事件（合計）", value: totalTriggers, sub: "疊成垂直虛線，不含 *_STATE" },
    ];
  }
  for (const c of cards) {
    const div = document.createElement("div");
    div.className = "kpi-card" + (c.accent ? " accent" : "");
    div.innerHTML = `<div class="kpi-label">${c.label}</div><div class="kpi-value">${c.value}</div><div class="kpi-sub">${c.sub}</div>`;
    kpiRow.appendChild(div);
  }
}

function renderLevelChart(ids) {
  const indicesInfo = ids.map((id) => INDICES[id]);

  if (ids.length === 1) {
    const info = indicesInfo[0];
    levelTitle.textContent = info.index_name + (info.is_synthetic ? "（比值）走勢" : " 走勢");
    levelHint.textContent = "實線：價格/比值；虛線：MA50、MA200。三角形=進入/突破/黃金交叉/創新高，倒三角=離開/跌破/死亡交叉/破新低。底色區塊=持續性狀態（見圖上 tooltip）";

    const level = filterByWindow(info.level);
    const ma50 = filterByWindow(info.ma50);
    const ma200 = filterByWindow(info.ma200);
    const markPoints = toMarkPoints(info.triggers);
    const markAreas = toMarkAreas(indicesInfo);

    levelChart.setOption({
      backgroundColor: "transparent",
      tooltip: { trigger: "axis", backgroundColor: "#1a1a19", borderColor: "#383835", textStyle: { color: "#fff" } },
      legend: { top: 0, textStyle: { color: "#c3c2b7", fontSize: 12 } },
      grid: { containLabel: true, top: 40, bottom: 40, left: 10, right: 30 },
      xAxis: { type: "time", axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitLine: { show: false } },
      yAxis: { type: "value", scale: true, axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitLine: SPLIT_LINE_STYLE },
      series: [
        { name: "價格/比值", type: "line", data: level, showSymbol: false, lineStyle: { color: ACCENT, width: 2 },
          markPoint: { data: markPoints, symbolSize: 12 },
          markArea: { silent: true, data: markAreas } },
        { name: "MA50", type: "line", data: ma50, showSymbol: false, lineStyle: { color: GOOD, width: 1.5, type: "dashed" } },
        { name: "MA200", type: "line", data: ma200, showSymbol: false, lineStyle: { color: CRITICAL, width: 1.5, type: "dashed" } },
      ],
    }, true);
    return;
  }

  levelTitle.textContent = "多指標比較（指數化，窗口起點 = 100）";
  levelHint.textContent = "每條線以目前窗口的起點重新指數化成 100，方便跨量級（利率/指數/股價）比較走勢；垂直虛線=各指標的事件型轉折點；底色區塊=持續性狀態";

  const markLines = toMarkLines(indicesInfo);
  const markAreas = toMarkAreas(indicesInfo);
  const series = indicesInfo.map((info, i) => ({
    name: info.index_name + (info.is_synthetic ? "（比值）" : "") + `[${MARKET_LABELS[info.market]}]`,
    type: "line",
    data: normalizeTo100(info.level),
    showSymbol: false,
    lineStyle: { color: PALETTE[i % PALETTE.length], width: 2 },
    ...(i === 0 ? { markLine: { silent: false, symbol: "none", data: markLines }, markArea: { silent: true, data: markAreas } } : {}),
  }));

  levelChart.setOption({
    backgroundColor: "transparent",
    tooltip: { trigger: "axis", backgroundColor: "#1a1a19", borderColor: "#383835", textStyle: { color: "#fff" } },
    legend: { top: 0, textStyle: { color: "#c3c2b7", fontSize: 12 }, type: "scroll" },
    grid: { containLabel: true, top: 40, bottom: 40, left: 10, right: 30 },
    xAxis: { type: "time", axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitLine: { show: false } },
    yAxis: { type: "value", scale: true, axisLabel: AXIS_LABEL_STYLE, axisLine: AXIS_LINE_STYLE, splitLine: SPLIT_LINE_STYLE },
    series,
  }, true);
}

function renderRsiChart(ids) {
  if (ids.length !== 1) {
    rsiPanel.classList.add("hidden");
    return;
  }
  const info = INDICES[ids[0]];
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

function renderMacdChart(ids) {
  if (ids.length !== 1) {
    macdPanel.classList.add("hidden");
    return;
  }
  const info = INDICES[ids[0]];
  const hasMacd = info.macd.length > 0;
  macdPanel.classList.toggle("hidden", !hasMacd);
  if (!hasMacd) return;

  const macd = filterByWindow(info.macd);
  const signal = filterByWindow(info.macd_signal);
  const markPoints = toMarkPoints(info.triggers.filter((t) => t.type.includes("MACD")));

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
  const ids = selectedIds().sort((a, b) => Number(a) - Number(b));
  renderGlossary(ids);
  renderKpis(ids);
  renderLevelChart(ids);
  renderTriggerLegend(ids);
  renderRsiChart(ids);
  renderMacdChart(ids);
}

windowSelect.addEventListener("change", renderAll);
window.addEventListener("resize", () => { levelChart.resize(); rsiChart.resize(); macdChart.resize(); });

renderStatusRow();
populateIndexChecklist();
renderAll();
</script>
</body>
</html>
"""


def main() -> None:
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        indices = build_indices_payload(conn)
        data_status = build_data_status(conn)
    finally:
        conn.close()

    state_meta = {k: {"label": v["label"], "color": v["color"]} for k, v in STATE_ACTIVE_RULES.items()}
    trigger_glossary = TRIGGER_GLOSSARY

    def _dumps(obj) -> str:
        return json.dumps(obj, ensure_ascii=False, default=str)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    html = (
        HTML_TEMPLATE
        .replace("__INDICES_JSON__", _dumps(indices))
        .replace("__DATA_STATUS_JSON__", _dumps(data_status))
        .replace("__STATE_META_JSON__", _dumps(state_meta))
        .replace("__TRIGGER_GLOSSARY_JSON__", _dumps(trigger_glossary))
    )
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[OK] wrote {OUTPUT_PATH} ({len(indices)} indices, data_status={data_status})")


if __name__ == "__main__":
    main()
