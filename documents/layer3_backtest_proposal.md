# Layer 3 策略組合排行榜 — 開發規格書

這份文件取代舊版的「回測規劃草案」。舊版逐輪討論過程（方案一~五的訊號盤點、
schema 草稿、圖表定案等）已完工，內容濃縮進 `grilling_notes.md` 的「已完成
的開發」章節；本檔只保留**接下來要動工的部分**——把已跑通的 Layer 3 回測
pipeline，從「開發者手動觸發 6 個寫死策略」升級成「使用者自選策略組合的
排行榜」。逐輪問答細節可從 git 舊版找回。

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
- 進場/出場各自最多 2 個 `trigger_type`，用 AND（交集）或 OR（聯集）組合。
  出場另有「固定持有 N 個交易日」選項（非 trigger）。
- 標的仍是開發者手動加進 `backtest_tickers.py`，不開放使用者自訂任意
  ticker。
- **不做快取**——VectorBT 單組合幾秒內跑完，每次都重算；但同一組合算過
  一次就記錄下來，下次同組合直接回吐，不重複觸發 Airflow。
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

**逐筆交易/淨值曲線先不建新表**：Phase 1 排行榜頁面的核心是「排名 + KPI」，
不是像 `backtest_dashboard.html` 那樣給單一策略看細節圖。若之後要幫每個
組合也做 Equity Curve/逐筆交易，屆時再仿照 `backtest_equity_curve`/
`backtest_trades` 的結構加，用 `strategy_def_id` 當 key 即可，現在不用先做。

## 3. Airflow 異動（`benny-data-pipeline`）

### 3.1 回測引擎通用化（`backtest/engine.py`）

現有 6 個策略函式都是「寫死查一個 trigger_type」，要新增可重用的組合函式：

```python
def resolve_condition_dates(conn, trigger_types: list[str], logic: str | None) -> set[date]:
    """查 1~2 個 trigger_type 的事件日期，logic='AND' 取交集、'OR' 取聯集、
    None（只有1個 trigger_type）直接回傳該 trigger 的日期集合。"""

def combo_backtest(conn, strategy_def: dict) -> BacktestResult:
    """讀 backtest_strategy_defs 一列，組出 entries/exits 布林序列，
    exit_mode='FIXED_HOLD' 時複用既有的 _fixed_holding_exits()，
    'TRIGGER' 時用 resolve_condition_dates() 算出場日期，
    餵進既有的 vbt.Portfolio.from_signals 那段共用邏輯（跟現有 6 個策略
    共用同一段 VectorBT 呼叫，不要重寫一份）。"""
```

### 3.2 新 DAG：`layer3_strategy_combo_backtest`

- **觸發方式**：外部呼叫 Airflow REST API `POST /dags/{dag_id}/dagRuns`，
  `conf` 帶 `{"strategy_def_id": <int>}`——DAG 本身不接受 trigger_type 等
  原始參數，一律先由觸發服務（見第 4 節）寫進 `backtest_strategy_defs`
  拿到 `strategy_def_id` 再觸發，DAG 只認這一個 ID，查表拿完整定義。
- **task chain**：`run_combo_backtest`（讀 `strategy_def_id` → 查
  `backtest_strategy_defs` → 呼叫 `combo_backtest()` → 寫
  `backtest_strategy_results`）→ `update_leaderboard_dashboard`（第 5 節）。
- **不掛在排程上**，只能被動觸發（`schedule=None`），比照 `layer3_backtest_etl`
  現有的一次性性質。

### 3.3 標的/指標擴充

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

**放哪裡**：建議在 `benny-data-pipeline` 開一個新的 `services/backtest_trigger/`
資料夾（獨立於 `dags/`，用同一個 repo 方便共用 `combo_key`/DB 連線邏輯），
用 `docker-compose` 另開一個 container 常駐執行（跟 Airflow webserver/
scheduler 平行的服務，不是 DAG 的一部分）。

## 5. Cloudflare Tunnel + 網域設定（Benny 需要動手的部分）

現況：Lightsail 主機刻意不開任何 inbound port。第 4 節的觸發服務要讓
GitHub Pages 前端打得到，需要對外露出，但不推翻「防火牆全關」的原則——
用 Cloudflare Tunnel（`cloudflared` 只主動連出去，不用開 port）。

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

### Step 4：Lightsail 主機上加 `cloudflared` container

在現有 `docker-compose.yaml`（跟 Airflow/DuckDB 服務同一份）加一個新
service：

```yaml
services:
  cloudflared:
    image: cloudflare/cloudflared:latest
    restart: unless-stopped
    command: tunnel run
    environment:
      - TUNNEL_TOKEN=<Step 3 拿到的 token>
```

`docker compose up -d cloudflared` 啟動。這個 container 只會主動連出去
接 Cloudflare 邊緣網路，主機防火牆 inbound 規則完全不用動。

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

主機外面（例如你自己的筆電，不要在 Lightsail 上測，測的就是「外部連得到
嗎」）：

```bash
curl https://api.你的網域.com/api/backtest/trigger -X POST \
  -H "X-Api-Token: <你設的共用密鑰>" \
  -H "Content-Type: application/json" \
  -d '{"entry_trigger_types":["SPX_GOLDEN_CROSS"],"exit_mode":"TRIGGER","exit_trigger_types":["SPX_DEATH_CROSS"],"ticker":"SPY"}'
```

收到 JSON 回應（不是連線逾時/被拒絕）就代表 Tunnel 全部設定正確。

## 6. 共用密鑰設定

觸發服務用一個環境變數（例如 `BACKTEST_TRIGGER_API_TOKEN`）存一組你自己
選的隨機字串（`openssl rand -hex 16` 產生即可），存進 Lightsail 主機的
`.env`（比照現有 `FRED_API_KEY`/`GITHUB_PAT` 的存法，不進 git）。前端頁面
（第 7 節）會有一個輸入框讓你貼這組字串，存在瀏覽器 `localStorage`（別人
不知道這組字串就打不了你的 API，公開網址本身不是秘密，這組 token 才是）。

## 7. 前端（`benny-stock-dashboard`）新增頁面

新增一份靜態頁面（例如 `output/strategy_lab.html`），兩個區塊：

1. **組合建立表單**：下拉選單選進場/出場 `trigger_type`（最多各 2 個 +
   AND/OR 切換）、出場模式（trigger 組合 or 固定持有天數輸入框）、標的
   下拉、token 輸入框，按下「送出」打第 4 節的 `POST /api/backtest/trigger`
   （網址就是 Step 5 設定好的 `https://api.你的網域.com/...`）。送出後
   顯示「已送出，稍後回來看排行榜」，不做 loading/輪詢。
2. **排行榜表格**：讀 `backtest_strategy_results` join `backtest_strategy_defs`，
   依 `total_return_pct` 排序，欄位至少含：組合描述（把 defs 的條件欄位
   組成一句人看得懂的文字，例如「VIX 極度恐慌 進場 / 固定持有60天 出場 /
   標的 SPY」）、總報酬、Sharpe、最大回撤、勝率、交易次數。這張表由既有的
   `update_leaderboard_dashboard` task（3.2 節）產生+推送，跟現有
   `update_dashboard`/`update_backtest_dashboard` 同一套「查 DB → 產 html →
   git push」模式，不用新開發布機制。

## 8. 開發順序建議

1. DB schema（第 2 節）先加進 `init_schema.sql`。
2. `engine.py` 通用化（3.1 節），先用現有 6 個寫死策略反向驗證
   `combo_backtest()` 算出來的結果跟原本手寫策略函式一致（例如拿
   `spx_golden_death_cross` 的邏輯改用 `combo_backtest` 重新跑一次，
   兩者結果要對得上），確保重構沒有改變行為。
3. 新 DAG（3.2 節）+ 觸發服務（第 4 節），先在本機/內網測試（不經過
   Cloudflare Tunnel），確認 defs 表寫入、Airflow 觸發、結果查詢整條路
   通了。
4. 排行榜前端頁面（第 7 節）串接本機服務測試。
5. 最後才做 Cloudflare Tunnel + 網域（第 5 節）——這步是「讓外部連得到」，
   跟前面 1-4 步「功能本身對不對」是獨立的兩件事，先確保功能正確，最後
   再處理對外曝露，避免除錯時把「網路連不通」跟「邏輯寫錯」混在一起。
