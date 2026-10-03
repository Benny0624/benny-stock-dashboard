# Layer 3 策略組合排行榜 — 開發規格書

這份文件取代舊版的「回測規劃草案」。舊版逐輪討論過程（方案一~五的訊號盤點、
schema 草稿、圖表定案等）已完工，內容濃縮進 `grilling_notes.md` 的「已完成
的開發」章節；本檔只保留**接下來要動工的部分**——把已跑通的 Layer 3 回測
pipeline，從「開發者手動觸發 6 個寫死策略」升級成「使用者自選策略組合的
排行榜」。逐輪問答細節可從 git 舊版找回：
`git show b681f4d:documents/layer3_backtest_proposal.md`——程式碼註解裡
引用的「追問一~四」「方案 B」「第 N 節」都是指這個舊版的章節編號。

**執行環境**：全部跑在地端電腦（WSL2 + Docker Desktop），DuckDB 在 Docker
volume `benny-infra-duckdb-data`。各 repo README 寫的 EC2/Lightsail 是
「之後正式上線」的規劃，**目前沒有雲端主機**；本檔說的「主機」都是指這台
地端電腦。

## 0. 已完工基礎（本次開發直接建立在這之上，不重複列）

- `stock_dashboard.backtest_universe`/`backtest_runs`/`backtest_equity_curve`/
  `backtest_trades`/`backtest_kpis` 五張表已在 `benny-data-infra` 的
  `init_schema.sql`。
- `benny-data-pipeline` 的 `backtest/engine.py`/`strategies.py`/`db_writer.py`、
  `layer3_backtest_etl` DAG、6 個寫死策略（方案一/三×2/四/五）已跑通並驗證。
- `benny-stock-dashboard` 的 `scripts/build_backtest_dashboard.py` 已產出含
  6 項圖表（Equity Curve、Underwater Chart、月度熱力圖、KPI 表、逐筆交易、
  訊號疊價格圖）的靜態 html，`dataZoom`、手機版面都已做。

## 1. 範圍界定（承接 `grilling_notes.md` 的 Phase 1 拍板）

- 使用者自選「進場條件 + 出場條件 + 標的」，觸發即時計算，結果寫入排行榜。
  **不開放自選時間區間**——固定用全部可用歷史（目前 10 年），理由：挑歷史
  區間績效好等於自爽，排行榜要每天有人上有人下才好玩。
- 條件從現有 `dim_triggers` 約 30 種 `trigger_type` 挑，**不做自訂數值運算式
  引擎**（`VIX > 30` 這種使用者自打數字的通用引擎留給之後 APP 化）。
  只開放 `ENTRY`/`EXIT` 類，兩類在進場、出場兩邊都能選（可組「VIX 恐慌
  解除時進場」這種反向玩法）；**排除 `STATE` 類**——STATE 每天寫一筆，當
  進場條件等於天天進場。
- **允許跨市場組合**（例如美股 VIX 訊號配標的 2330.TW），進場日對齊規則：
  - 同市場：訊號日收盤進場（跟現有 6 個策略一致）；訊號日不是標的交易日
    就往後延到下一個交易日。
  - 跨市場：進場日是標的市場**嚴格晚於訊號日**的第一個交易日。美股 T 日
    收盤的訊號，台股 T 日收盤時還不存在，同日進場等於偷看未來。目前
    `dim_triggers` 全是美股指標，台股標的的組合都套這條。
- 可選的 `trigger_type` 由 `constants/` 新增的 trigger 清單常數定義（見
  3.1 節），不從 `dim_triggers` 動態撈。
- 進場/出場各自最多 2 個 `trigger_type`，用 AND（交集）或 OR（聯集）組合。
  出場另有「固定持有 N 個交易日」選項（非 trigger）。
- 標的仍是開發者手動加進 `backtest_tickers.py`，不開放使用者自訂任意
  ticker。
- **不做快取**——VectorBT 單組合幾秒內跑完，每次都重算；但同一組合算過
  一次就記錄下來，使用者再送同組合直接回吐，不重複觸發 Airflow。
- **排名每日更新**：另有每日重算 DAG（3.3 節），Layer 2 有新資料就把全部
  組合重算一次、覆寫結果，排名才會隨新資料變動（「每天有人上有人下」）。
  使用者觸發只負責補從沒算過的組合。
- **使用者**：Benny + 私下拿到共用 token 的少數朋友，不做限流、不顯示提交者。
- **上榜時機**：排行榜 html 只在每日重算跑完時 push，新組合等下次重算才
  上榜。還沒結果或計算失敗的組合不顯示（不做「計算中/失敗」UI）；失敗的
  由每日重算自動再跑，仍失敗由 Benny 在 Airflow UI 處理。組合在標的上
  0 次訊號不算失敗，當「0 次交易」寫入結果照常上榜。
- Phase 1 只做 GitHub Pages 靜態前端，不做即時輪詢/loading 體驗；APP、社群
  排行、帳號系統都是長期願景，這次不設計。

## 2. DB Schema 新增（`benny-data-infra/sql/stock_dashboard/init_schema.sql`）

兩張新表，沿用既有慣例（單一 `stock_dashboard` schema、自然 key、無 FK
constraint）：

```sql
-- 策略組合定義：使用者選出的「進場條件+出場條件+標的」這個組合本身
-- combo_key 是組合內容序列化後的字串，當自然 key 用來判斷「這個組合以前
-- 有沒有人選過」，避免同一個組合被存成兩筆不同的 strategy_def_id。
CREATE TABLE IF NOT EXISTS stock_dashboard.backtest_strategy_defs (
    strategy_def_id     INTEGER      NOT NULL,  -- 由 SEQUENCE 產生
    combo_key           VARCHAR      NOT NULL,  -- 見下方「combo_key 產生規則」
    entry_trigger_types VARCHAR[]    NOT NULL,  -- 1~2 個 trigger_type
    entry_logic         VARCHAR,                -- 'AND' / 'OR' / NULL（只有1個時）
    exit_mode           VARCHAR      NOT NULL,  -- 'TRIGGER' / 'FIXED_HOLD'
    exit_trigger_types  VARCHAR[],              -- exit_mode='TRIGGER' 才有值
    exit_logic          VARCHAR,                -- 同 entry_logic
    fixed_holding_days  INTEGER,                -- exit_mode='FIXED_HOLD' 才有值
    ticker              VARCHAR      NOT NULL,
    created_at          TIMESTAMP    NOT NULL DEFAULT current_timestamp
);
CREATE SEQUENCE IF NOT EXISTS stock_dashboard.seq_backtest_strategy_def_id;
CREATE UNIQUE INDEX IF NOT EXISTS uq_backtest_strategy_defs_combo
    ON stock_dashboard.backtest_strategy_defs(combo_key);

-- 組合績效結果，一個 strategy_def_id 一列，重跑覆寫（比照 backtest_kpis 慣例）
CREATE TABLE IF NOT EXISTS stock_dashboard.backtest_strategy_results (
    strategy_def_id    INTEGER NOT NULL,
    total_return_pct   DOUBLE,
    cagr_pct           DOUBLE,
    sharpe_ratio       DOUBLE,
    sortino_ratio      DOUBLE,
    max_drawdown_pct   DOUBLE,
    win_rate_pct       DOUBLE,
    num_trades         INTEGER,
    calmar_ratio       DOUBLE,
    alpha              DOUBLE,
    beta               DOUBLE,
    processed_at       TIMESTAMP NOT NULL DEFAULT current_timestamp
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_backtest_strategy_results
    ON stock_dashboard.backtest_strategy_results(strategy_def_id);
```

**`combo_key` 產生規則**（觸發服務跟 Airflow task 都要用同一份邏輯，寫成
共用函式，不要兩邊各自兜一份）：把 `entry_trigger_types`（先排序）、
`entry_logic`、`exit_mode`、`exit_trigger_types`（先排序）、`exit_logic`、
`fixed_holding_days`、`ticker` 依固定順序串成字串（例如
`|` 分隔各欄位、`,` 分隔陣列內元素），直接當 `combo_key` 存文字，不用再
另外算 hash——欄位數量少、長度可控，存明文字串比存 hash 更好除錯（出問題
時直接看得懂這個 key 代表哪個組合）。

**重複送出不用擋**：兩人送出同一組合、或同一人連按兩次，第一次還沒算完時
results 查不到，會觸發兩次 DAG。`combo_key` unique index + `INSERT OR IGNORE`
保證 defs 只有一列，兩次 DAG 算同一件事、後者覆寫前者，只浪費幾秒，結果正確。

**逐筆交易/淨值曲線先不建新表**：Phase 1 排行榜頁面的核心是「排名 + KPI」，
不是像 `backtest_dashboard.html` 那樣給單一策略看細節圖。若之後要幫每個
組合也做 Equity Curve/逐筆交易，屆時再仿照 `backtest_equity_curve`/
`backtest_trades` 的結構加，用 `strategy_def_id` 當 key 即可，現在不用先做。

## 3. Airflow 異動（`benny-data-pipeline`）

### 3.1 回測引擎通用化（`backtest/engine.py`）

**年化修正（先做）**：現有 `from_signals(..., freq="D")` 讓 vectorbt 一年
算 365 列，但資料只有交易日（一年約 252 列），Sharpe 高估約 √(365/252)≈1.2
倍、CAGR 被放大（真實 10% 顯示約 14.8%）。改設 vectorbt
`year_freq='252 days'`（年化一律用交易日數），修完重跑現有 6 個策略更新
`backtest_kpis`。

**trigger 清單常數**：`dim_triggers` 沒有欄位標 ENTRY/EXIT/STATE，名稱後綴
也不統一（`SPX_GOLDEN_CROSS`、`*_BREAKOUT`）。新增
`constants/combo_triggers.py`：列出可選 trigger（排除 `*_STATE` 共 9 個，
其餘約 22 個），每筆含 `trigger_type`、中文顯示名、訊號所屬 `market`。
觸發服務驗證、前端下拉、組合描述文字、跨市場判斷都用這一份。

現有 6 個策略函式都是「寫死查一個 trigger_type」，要新增可重用的組合函式：

```python
def resolve_condition_dates(conn, trigger_types: list[str], logic: str | None,
                            ticker_trading_days: pd.DatetimeIndex,
                            ticker_market: str) -> set[date]:
    """查 1~2 個 trigger_type 的事件日期，先把每個 trigger 的日期對齊到標的
    交易日，再依 logic='AND' 取交集、'OR' 取聯集、None（只有1個
    trigger_type）直接回傳。先對齊再交集，避免美股/台股訊號日差一天就永遠
    交集不到。對齊規則（第 1 節）：
    - trigger 市場 == ticker_market：取 >= 訊號日的第一個標的交易日
    - trigger 市場 != ticker_market：取 >  訊號日的第一個標的交易日
    trigger 市場查 combo_triggers.py；不在清單內（含 STATE 類）直接 raise。"""

def combo_backtest(conn, strategy_def: dict) -> BacktestResult:
    """讀 backtest_strategy_defs 一列，組出 entries/exits 布林序列，
    exit_mode='FIXED_HOLD' 時複用既有的 _fixed_holding_exits()，
    'TRIGGER' 時用 resolve_condition_dates() 算出場日期，
    餵進既有的 vbt.Portfolio.from_signals 那段共用邏輯（跟現有 6 個策略
    共用同一段 VectorBT 呼叫，不要重寫一份）。
    0 次進場訊號時回傳 num_trades=0 的正常結果，不 raise。"""
```

benchmark 沿用現有 `engine.py` 的「同一檔標的 buy & hold」，alpha/beta
算法不變。

### 3.2 新 DAG：`layer3_strategy_combo_backtest`

- **觸發方式**：外部呼叫 Airflow REST API `POST /dags/{dag_id}/dagRuns`，
  `conf` 帶 `{"strategy_def_id": <int>}`——DAG 本身不接受 trigger_type 等
  原始參數，一律先由觸發服務（見第 4 節）寫進 `backtest_strategy_defs`
  拿到 `strategy_def_id` 再觸發，DAG 只認這一個 ID，查表拿完整定義。
- **task chain**：只有一個 task `run_combo_backtest`（讀 `strategy_def_id`
  → 查 `backtest_strategy_defs` → 呼叫 `combo_backtest()` → 寫
  `backtest_strategy_results`）。**不 push html**——push 統一由 3.3 節的
  每日重算 DAG 負責，避免多個觸發同時 git push 互相衝突。
- **不掛在排程上**，只能被動觸發（`schedule=None`），比照 `layer3_backtest_etl`
  現有的一次性性質。task 掛 `pool="duckdb_writer"`。

### 3.3 新 DAG：`layer3_strategy_leaderboard_daily`（每日重算 + 發布）

- **上游先補每日價格**：`backtest_universe`（交易標的收盤價）原本只靠
  一次性 `backfill_backtest_universe.py` 寫入，每日 DAG 不更新——不補的話
  每日重算一直用舊價格，排名不會動。`_build_market_dag` 加
  `fetch_backtest_universe` task，依 `BacktestTicker.market` 只抓該市場的
  標的（us：SPY/SOXX；tw：2330.TW/006208.TW），沿用 `fetch.py` 既有的抓取
  函式，掛 `pool="duckdb_writer"`。標的照既有慣例放同一張表、用 `market`
  欄位分台美。
- **觸發方式**：Airflow Dataset 排程（data-aware scheduling），不用 cron、
  不用 `TriggerDagRunOperator`。`us_market_daily_etl`/`tw_market_daily_etl`
  在 Layer 2（含 `dim_triggers`）與 `fetch_backtest_universe` 都寫完後，
  由最後一個 task 各自宣告 `outlets=[ds_us_silver]`/`outlets=[ds_tw_silver]`，
  本 DAG：

  ```python
  from airflow.datasets import Dataset

  ds_us_silver = Dataset("duckdb://stock_dashboard/silver/us")
  ds_tw_silver = Dataset("duckdb://stock_dashboard/silver/tw")

  with DAG(
      "layer3_strategy_leaderboard_daily",
      schedule=(ds_us_silver | ds_tw_silver),  # OR：任一市場更新就跑
      max_active_runs=1,
      ...
  ):
  ```

  用 OR 不用 AND：某一市場休市那天，該 DAG 不會發 Dataset 事件，AND 會卡住
  重算、另一市場的新資料延遲上榜。OR 一天最多跑兩次，全部組合重算只要
  幾分鐘，成本可接受；`max_active_runs=1` 避免兩次重疊。Dataset 物件定義
  放 `constants/` 共用，上下游 import 同一份，不要各自寫 URI 字串。
  Dataset 是 Airflow 2.4+、`|` 條件式是 2.9+ 功能，目前 image
  `apache/airflow:2.11.0` 都支援；之後升 Airflow 3 要把 `Dataset` 改名
  `Asset`（import 改 `airflow.sdk`）。
- **task chain**：`recompute_all_combos`（`backtest_strategy_defs` 全部列逐一
  跑 `combo_backtest()`，覆寫 `backtest_strategy_results`；單一組合失敗只
  log 不中斷其他組合）→ `update_leaderboard_dashboard`（第 7 節，查 DB →
  產 html → git push）。兩個 task 都掛 `pool="duckdb_writer"`。
- 這是排行榜 html **唯一的 push 來源**，所以不會有 push 衝突。

### 3.4 標的/指標擴充

維持現況，不用改——`constants/index_map.py`/`backtest_tickers.py` 加一筆
+ 重跑 backfill 就能擴充，這次開發不動這塊。

## 4. 觸發服務（新元件，待定專案位置——建議獨立小服務，不要塞進既有 DAG 程式碼）

一支很薄的 FastAPI 服務，職責只有「接使用者的組合選擇、寫 defs 表、觸發
Airflow、回覆前端」：

```
POST /api/backtest/trigger
Header: X-Api-Token: <共用密鑰>
Body: {
  "entry_trigger_types": ["VIX_EXTREME_FEAR_ENTER"],
  "entry_logic": null,
  "exit_mode": "FIXED_HOLD",
  "fixed_holding_days": 60,
  "ticker": "SPY"
}
```

處理邏輯：

1. 檢查 `X-Api-Token` 跟環境變數裡存的共用密鑰是否一致，不符回 401。
   檢查 body：每邊 1~2 個 `trigger_type` 且都在 `combo_triggers.py` 清單內
   （STATE 類不在清單，自然擋掉）、`ticker` 在 `backtest_tickers.py` 名單內，
   不符回 422。
2. 算出 `combo_key`（3.1 節共用邏輯，這支服務跟 Airflow task 都要 import
   同一份，不要各自兜一份字串拼接邏輯）。
3. 查 `backtest_strategy_results`（透過 `backtest_strategy_defs` join）：
   - 已經有結果 → 直接回傳現有結果，不觸發 Airflow（Phase 1 拍板的
     「算過的不重算」）。
   - 沒有 → `INSERT OR IGNORE` 進 `backtest_strategy_defs` 拿
     `strategy_def_id`，呼叫 Airflow REST API 觸發 `layer3_strategy_combo_backtest`
     帶上這個 ID，回傳 `{"status": "triggered", "strategy_def_id": ...}`。
4. Airflow REST API 的帳密（或 API token）存在這支服務自己的環境變數，
   **不會出現在前端或瀏覽器**——這是整個觸發服務存在的唯一理由（瀏覽器
   JS 不能直接帶 Airflow 帳密打 Airflow API）。

**DuckDB 單寫者處理**：DuckDB 檔案同時只能一個 process 讀寫開啟，Airflow
task 寫入期間這支服務開不了連線。拍板做法是**遇 lock 重試/排隊**（例如
指數退避重試數次，仍失敗回 503 請使用者晚點再送），不改架構——寫入頻率
很低（每日重算一天最多兩次 + 零星使用者觸發），撞 lock 機率低。連線只在
處理單一 request 期間開啟，用完立刻關，不常駐持有。DuckDB 檔案在 named
volume `duckdb-data:/data`，這個 container 要掛同一個 volume、沿用
`DUCKDB_PATH` 環境變數。Airflow REST API 已開 `basic_auth`
（`AIRFLOW__API__AUTH_BACKENDS`），不用改。

**放哪裡**：建議在 `benny-data-pipeline` 開一個新的 `services/backtest_trigger/`
資料夾（獨立於 `dags/`，用同一個 repo 方便共用 `combo_key`/DB 連線邏輯），
用 `docker-compose` 另開一個 container 常駐執行（跟 Airflow webserver/
scheduler 平行的服務，不是 DAG 的一部分）。

## 5. Cloudflare Tunnel + 網域設定（Benny 需要動手的部分）

現況：服務跑在地端電腦（WSL2 + Docker Desktop），家用網路沒有固定 IP、
也不該在路由器開 port 轉發。第 4 節的觸發服務要讓 GitHub Pages 前端打得到，
用 Cloudflare Tunnel（`cloudflared` 只主動連出去，不用開任何 inbound port，
也不需要固定 IP）。

**限制**：電腦關機或 Docker Desktop 沒開時，Tunnel 跟觸發服務都不在線，
朋友送出組合會失敗（前端顯示連線失敗）；每日重算也不會跑。這跟現有每日
DAG「電腦沒開就不觸發」是同一個限制，之後搬到常駐主機才會消失。

你目前沒有網域也不想用臨時 Tunnel（網址隨機、重啟就換），所以完整流程
包含買網域：

### Step 1：買一個網域

- 去 Cloudflare Dashboard（`dash.cloudflare.com`，用你現有帳號）左側選
  **Domain Registration → Register Domain**，直接在 Cloudflare 上買
  （`.com` 一年約 USD 9~10，Cloudflare 賣網域不加價，比多數註冊商便宜）。
  這個路徑買到的網域**自動掛在 Cloudflare 底下**，不用再做「轉 DNS」
  這一步，最省事。
- 如果你想在別的地方買（Namecheap/GoDaddy 便宜促銷），可以，但買完要
  多做「Step 2：把網域交給 Cloudflare 管理」。**建議直接在 Cloudflare 買**，
  省掉這一步。

### Step 2（只有在別處買網域才需要）：把網域交給 Cloudflare 管理

- Cloudflare Dashboard → **Add a Site**，輸入你的網域，選免費方案。
- Cloudflare 會給你兩組 Nameserver（例如 `xxx.ns.cloudflare.com`）。
- 回到原本買網域的註冊商後台，把網域的 Nameserver 改成 Cloudflare 給的
  這兩組（每家註冊商介面不同，找「Nameservers」或「DNS 設定」欄位）。
- 等待生效（通常幾分鐘到 24 小時），Cloudflare Dashboard 上該網域狀態會
  從 "Pending Nameserver Update" 變成 "Active"。

### Step 3：建立 Named Tunnel

- Cloudflare Dashboard → **Zero Trust**（左側選單，免費方案就有）→
  **Networks → Tunnels → Create a tunnel**。
- Tunnel 類型選 **Cloudflared**，取個名字（例如 `benny-stock-backend`）。
- 建立後會給你一段**安裝指令**，裡面包含一組 tunnel token（一長串亂碼），
  **把這串 token 存起來**，下一步 docker-compose 要用。
- 這個畫面先不要關，之後還要回來設定 Public Hostname（Step 5）。

### Step 4：地端 docker-compose 加 `cloudflared` container

在 `benny-data-pipeline/local/airflow.docker-compose.yaml`（跟 Airflow、
觸發服務同一份）加一個新 service：

```yaml
services:
  cloudflared:
    image: cloudflare/cloudflared:latest
    restart: unless-stopped
    command: tunnel run
    environment:
      - TUNNEL_TOKEN=<Step 3 拿到的 token>
```

token 不要寫死在 yaml，放 `benny-data-pipeline/.env`（`TUNNEL_TOKEN=...`，
不進 git），yaml 用 `${TUNNEL_TOKEN}` 帶入。`make start` 會一起啟動。這個
container 只會主動連出去接 Cloudflare 邊緣網路，不用開任何 port。

### Step 5：設定 Public Hostname（回到 Step 3 那個畫面）

- 在 Tunnel 詳情頁的 **Public Hostname** 分頁，點 **Add a public hostname**。
- **Subdomain**：例如 `api`。
- **Domain**：選你 Step 1/2 掛好的網域，組合起來就是
  `api.你的網域.com`。
- **Service**：Type 選 `HTTP`，URL 填第 4 節觸發服務在主機內部監聽的
  位址（例如 `http://backtest-trigger:8000`，用 docker-compose 的
  service name，不是 `localhost`，因為 `cloudflared` 是另一個 container，
  要靠 Docker 內部網路連到觸發服務那個 container）。
- 存檔，Cloudflare 會自動幫你把這個網域的 DNS 指到 Tunnel（你不用手動
  加 DNS record）。

### Step 6：驗證

從家用網路外面測（例如手機關 Wi-Fi 用行動網路開終端機 app，或請朋友打），
測的就是「外部連得到嗎」；在同一台電腦上測不算：

```bash
curl https://api.你的網域.com/api/backtest/trigger -X POST \
  -H "X-Api-Token: <你設的共用密鑰>" \
  -H "Content-Type: application/json" \
  -d '{"entry_trigger_types":["SPX_GOLDEN_CROSS"],"exit_mode":"TRIGGER","exit_trigger_types":["SPX_DEATH_CROSS"],"ticker":"SPY"}'
```

收到 JSON 回應（不是連線逾時/被拒絕）就代表 Tunnel 全部設定正確。

## 6. 共用密鑰設定

觸發服務用一個環境變數（例如 `BACKTEST_TRIGGER_API_TOKEN`）存一組你自己
選的隨機字串（`openssl rand -hex 16` 產生即可），存進
`benny-data-pipeline/.env`（比照現有 `FRED_API_KEY`/`GITHUB_PAT` 的存法，
不進 git；`.env.example` 加一行空值 + 註解）。前端頁面
（第 7 節）會有一個輸入框讓你貼這組字串，存在瀏覽器 `localStorage`（別人
不知道這組字串就打不了你的 API，公開網址本身不是秘密，這組 token 才是）。

## 7. 前端（`benny-stock-dashboard`）新增頁面

新增一份靜態頁面（例如 `output/strategy_lab.html`），兩個區塊：

1. **組合建立表單**：下拉選單選進場/出場 `trigger_type`（只列 ENTRY/EXIT
   類，最多各 2 個 + AND/OR 切換）、出場模式（trigger 組合 or 固定持有
   天數輸入框）、標的下拉、token 輸入框，按下「送出」打第 4 節的
   `POST /api/backtest/trigger`（網址就是 Step 5 設定好的
   `https://api.你的網域.com/...`）。送出後顯示「已送出，下次排行榜更新後
   上榜」，不做 loading/輪詢。
2. **排行榜表格**：讀 `backtest_strategy_results` join `backtest_strategy_defs`，
   欄位：組合描述（把 defs 的條件欄位組成一句人看得懂的文字，例如
   「VIX 極度恐慌 進場 / 固定持有60天 出場 / 標的 SPY」）、CAGR、Sharpe、
   最大回撤、勝率、交易次數，總報酬當參考欄位。**預設依 Sharpe 排序**
   （依 CAGR 排會偏好高風險組合；各標的歷史長度不同，也不比總報酬），
   表頭可點擊改依 CAGR 等欄位排序（前端 JS 排序，不重查 DB）。不放算術
   年均報酬（與 CAGR 重複）。這張表由 3.3 節的 `update_leaderboard_dashboard`
   task 產生+推送，跟現有 `update_dashboard`/`update_backtest_dashboard`
   同一套「查 DB → 產 html → git push」模式，不用新開發布機制。

## 8. 開發順序建議

1. DB schema（第 2 節）先加進 `init_schema.sql`。
2. `engine.py` 年化修正（`year_freq='252 days'`），重跑現有 6 個策略更新
   `backtest_kpis`。
3. `combo_triggers.py` 清單 + `engine.py` 通用化（3.1 節），先用現有 6 個
   寫死策略反向驗證 `combo_backtest()` 算出來的結果跟原本手寫策略函式一致
   （例如拿 `spx_golden_death_cross` 的邏輯改用 `combo_backtest` 重新跑
   一次），確保重構沒有改變行為。既有 `_signal_series()` 會丟掉不在交易日
   的訊號、新規則改往後延，所以可能有少數差異——逐筆確認差異都來自這條
   規則，不能只比相等。同時驗證跨市場對齊（美股 T 日訊號配台股標的，進場日
   嚴格晚於 T）與 0 次訊號組合回傳 `num_trades=0`。
4. 新 DAG（3.2 節）+ 觸發服務（第 4 節），先在本機/內網測試（不經過
   Cloudflare Tunnel），確認 defs 表寫入、Airflow 觸發、結果查詢整條路
   通了；含 DuckDB lock 時服務會重試、最後回 503。
5. 每日重算 DAG（3.3 節）：us/tw DAG 加 `fetch_backtest_universe` 與
   `outlets`，確認 `backtest_universe` 每天有新價格、任一邊跑完都會觸發
   重算、push 一次。
6. 排行榜前端頁面（第 7 節）串接本機服務測試。
7. 最後才做 Cloudflare Tunnel + 網域（第 5 節）——這步是「讓外部連得到」，
   跟前面 1-6 步「功能本身對不對」是獨立的兩件事，先確保功能正確，最後
   再處理對外曝露，避免除錯時把「網路連不通」跟「邏輯寫錯」混在一起。
