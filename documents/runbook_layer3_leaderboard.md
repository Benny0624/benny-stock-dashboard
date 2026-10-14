# Runbook：套用 Layer 3 排行榜 schema + 重跑回測（2026-10-04）

> **狀態：2026-10-07 已在地端執行完成。** 驗證結果：`stock_dashboard` 共
> 10 張表（含兩張新表）；方案四 Sharpe 0.83 → 0.69（= 0.83 × √(252/365)），
> 修正後 KPI 見 `grilling_notes.md`「實作進度」。之後新增表（例如 Q15 快照表）
> 可以照 Step 4～7 再跑一次。

照順序一步一步做，每一步都有「做什麼」「打什麼指令」「應該看到什麼」。
看到的跟寫的不一樣就**停下來**，不要往下做，把畫面貼給 Claude。

這次要完成兩件事：

1. **Schema migration**：在地端 DuckDB 建立策略組合排行榜的兩張新表
   `backtest_strategy_defs`、`backtest_strategy_results`。
2. **重算 6 個策略的 KPI**：`engine.py` 年化從 365 天改成 252 個交易日，
   舊的 `backtest_kpis` 數字是高估的，手動觸發一次 `layer3_backtest_etl`
   覆寫（觸發一次就好，不用補跑，見 Step 9）。

兩張都是**新表**，不改任何既有表，所以不需要手寫 `ALTER TABLE`；
`make start` 的 `CREATE TABLE IF NOT EXISTS` 就會建出來。

> 還沒包含的：Q15（排名變化快照表）還沒拍板。如果之後選要做，會多一張新表，
> 照本文件 Step 6～7 再跑一次即可。

---

## Step 0：確認三個 PR 都 merge 了

| repo | 分支 | 合進 |
|---|---|---|
| `benny-data-infra` | `feature/layer3-leaderboard-schema` | `master` |
| `benny-data-pipeline` | `feature/layer3-leaderboard` | `master` |
| `benny-stock-dashboard` | `feature/layer3-leaderboard-docs` | `master`（2026-10-04 從 `main` 改名） |

到 GitHub 各 repo 的 Pull requests 頁面，三個都顯示紫色 **Merged** 才往下。

---

## Step 1：打開 Docker Desktop

1. Windows 開始選單搜尋 **Docker Desktop**，打開。
2. 等左下角變成綠色 **Engine running**（大約 30 秒～1 分鐘）。

**注意**：Airflow 的 container 設了 `restart: always`，Docker 一開它們就會
自己跑起來。下一步要先把它們關掉，避免備份/migration 時有 DAG 正在寫 DuckDB。

---

## Step 2：進 WSL，關掉 Airflow

開 Windows Terminal（或 PowerShell），輸入：

```bash
wsl -d Ubuntu-22.04
```

提示字元變成 `benny0624@...:~$` 這種樣子就是進到 WSL 了。接著：

```bash
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
make down
```

**應該看到**：一串 `Container benny-data-pipeline-... Stopped/Removed`。

確認真的都停了：

```bash
docker ps --format "{{.Names}}"
```

**應該看到**：沒有任何 `benny-data-pipeline-` 開頭的名字（可能完全空白）。

---

## Step 3：三個 repo 拉最新程式碼

```bash
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-infra
git checkout master && git pull

cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
git checkout master && git pull

cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-stock-dashboard
git checkout master && git pull
```

**確認拉到了**（各自應該印出一行）：

```bash
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-infra
grep -c "backtest_strategy_results" sql/stock_dashboard/init_schema.sql
# 應該印出 4

cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
grep -n 'YEAR_FREQ = "252 days"' dags/stock_dashboard_etl/backtest/engine.py
# 應該印出一行，例如 25:YEAR_FREQ = "252 days"
```

印出 `0` 或什麼都沒印 = 沒拉到，回 Step 0 確認 PR 有 merge。

---

## Step 4：備份 DuckDB（出事可以還原）

```bash
mkdir -p ~/duckdb-backup && cd ~/duckdb-backup
docker run --rm \
  -v benny-infra-duckdb-data:/data \
  -v "$PWD":/backup \
  alpine tar czf /backup/warehouse-20261004.tgz -C /data .
ls -lh ~/duckdb-backup
```

**應該看到**：`warehouse-20261004.tgz`，大小不是 0（幾 MB～幾十 MB）。

---

## Step 5：記下舊的 KPI（之後拿來對照）

```bash
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
make duckdb-shell
```

**應該看到**：`D ` 開頭的提示字元（DuckDB 的命令列）。貼上：

```sql
SELECT strategy_name, ticker,
       round(cagr_pct, 2)     AS cagr_pct,
       round(sharpe_ratio, 2) AS sharpe
FROM stock_dashboard.backtest_kpis
ORDER BY strategy_name, ticker;
```

**應該看到**：6 列（方案四/三×2/一×2/五）。**截圖或把結果複製存起來。**
方案四（`spx_golden_death_cross`、SPY）的 sharpe 應該是 0.83 左右。

離開 DuckDB（**一定要離開**，開著會鎖住 DuckDB，後面的步驟會失敗）：

```
.exit
```

---

## Step 6：套用 schema migration

```bash
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-infra
make start
```

**應該看到**：最後幾行有

```
[OK] Schema initialized at /data/warehouse.duckdb
```

然後 container 自己結束（`exited with code 0`）。

---

## Step 7：確認新表建出來了

```bash
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
make duckdb-shell
```

貼上：

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'stock_dashboard'
ORDER BY table_name;
```

**應該看到 10 張表**，其中要有這兩張新的：

```
backtest_strategy_defs
backtest_strategy_results
```

再確認舊資料沒被動到（數字要跟 Step 5 一模一樣）：

```sql
SELECT count(*) FROM stock_dashboard.backtest_kpis;
-- 應該是 6
```

離開：

```
.exit
```

---

## Step 8：啟動 Airflow

```bash
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
make start
```

這次 `pyproject.toml`/`Dockerfile` 沒有改，**不需要** `make build`
（DAG 程式碼是 bind mount 進 container，改了自動生效）。

打開瀏覽器 http://localhost:8080 ，帳號 `admin` 密碼 `admin`。

**應該看到**：DAG 列表裡有 `layer3_backtest_etl`，沒有紅色的
「Broken DAG」錯誤訊息在頁面上方。

---

## Step 9：重算 6 個策略的 KPI（觸發一次，不用補跑）

**這步不是補跑歷史資料**：`layer3_backtest_etl` 是 `schedule_interval=None`
（不排程），沒有「每天一個 run」，也不會 catchup。觸發一次只會產生
**1 個 DAG run**：

- 6 個 `compute_*` task 各跑一個策略：從 DuckDB 讀出 9/5 已經 backfill 好的
  10 年 `dim_triggers`/`backtest_universe`，VectorBT 一次算完整段 10 年，
  `INSERT OR REPLACE` 覆寫 `backtest_kpis` 等表。
- `update_backtest_dashboard` 產 html、push 一次。
- 全程**不抓任何新資料**（不打 FRED/yfinance），幾分鐘就跑完。

資料本身沒錯，錯的是 KPI 年化公式；觸發一次就是用新公式把 KPI 重算覆寫。

1. 在 Airflow 網頁找到 `layer3_backtest_etl`，看最左邊的開關：
   **是灰色（暫停）就點一下變藍色**。暫停中的 DAG 觸發了也不會跑。
2. 回到 WSL 終端機：

```bash
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
make trigger dag=layer3_backtest_etl
```

**應該看到**：`Created <DagRun layer3_backtest_etl @ ...>`。

3. 回 Airflow 網頁，點進 `layer3_backtest_etl` → **Grid**。等 7 個 task
   （6 個 `compute_*` + `update_backtest_dashboard`）全部變**深綠色**
   （大約幾分鐘）。

**有任何一格變紅色**：點那一格 → **Logs**，把錯誤訊息貼給 Claude，不要往下做。

---

## Step 10：確認 KPI 已經修正

**先等 Step 9 全部綠色**（task 還在跑時開 DuckDB 會互相卡住）。

```bash
make duckdb-shell
```

貼上跟 Step 5 一樣的 SQL：

```sql
SELECT strategy_name, ticker,
       round(cagr_pct, 2)     AS cagr_pct,
       round(sharpe_ratio, 2) AS sharpe,
       processed_at
FROM stock_dashboard.backtest_kpis
ORDER BY strategy_name, ticker;
```

**應該看到**：

- ~~`processed_at` 全部是今天~~（**寫錯了**，2026-10-07 更正）：
  `processed_at` 會維持第一次寫入的日期（9/4、9/6），不會變成今天——
  `processed_at` 的定義就是第一次寫入時間，覆寫時刻意保留。最後更新時間
  之後看 `_updated_at`（Part 2 加的系統欄位）。判斷有沒有重算請看下面的
  sharpe。
- 每一列的 **sharpe 都比 Step 5 小**，大約是舊值 ÷ 1.2
  （方案四應該從 0.83 左右變成 0.69 左右）。
- 每一列的 **cagr_pct 也比 Step 5 小**（總報酬不變，年化方式變了）。

數字沒變 = engine 沒吃到新程式碼，回 Step 3 確認 `YEAR_FREQ` 那行。

離開：

```
.exit
```

---

## Step 11：確認網頁更新

`update_backtest_dashboard` task 會把新的 `backtest_dashboard.html` push
到 `benny-stock-dashboard`。

1. 到 https://github.com/Benny0624/benny-stock-dashboard/commits/master ，
   最新一筆應該是剛剛 DAG push 的 commit。
2. 等 1～2 分鐘（GitHub Pages 更新），打開 backtest dashboard 頁面，
   KPI 卡片的 Sharpe/CAGR 要跟 Step 10 一致。

---

## 完成後

全部做完，告訴 Claude「runbook 跑完了」，接著做開發順序第 3 步
（`combo_triggers.py` + `combo_backtest()`，用地端真實資料做反向驗證）。

備份檔 `~/duckdb-backup/warehouse-20261004.tgz` 留著，確認排行榜開發一陣子
沒問題再刪。

---

## 出事了怎麼還原

只有在 Step 6～10 出現無法解決的錯誤、想回到做之前的狀態時才用：

```bash
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
make down

cd ~/duckdb-backup
docker run --rm \
  -v benny-infra-duckdb-data:/data \
  -v "$PWD":/backup \
  alpine sh -c "rm -rf /data/* && tar xzf /backup/warehouse-20261004.tgz -C /data"

cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
make start
```

這會把 DuckDB 整個蓋回 Step 4 備份時的樣子（Step 4 之後寫進去的資料都會
消失，包括這段期間每日 DAG 抓的新資料，之後重跑每日 DAG 就會補回來）。

---

# Part 2：加系統欄位 `_updated_at`（2026-10-12 已執行完成）

設計見 `grilling_notes.md`「第五輪拍板」Q16~18、「第六輪拍板」Q19~20。
這次跟 Part 1 不一樣：**會改到既有的表**（9 張表各加一欄 `_updated_at`、
寫入改成條件式 upsert、`raw_stock` 改存未還原價），而且 **infra 跟 pipeline
一定要一起上線**——只做一邊，每日 DAG 就會壞掉。所以全程 Airflow 要停著。

## P2-0：兩個 PR 都 merge

| repo | 分支 | 合進 |
|---|---|---|
| `benny-data-infra` | `feature/system-updated-at` | `master` |
| `benny-data-pipeline` | `feature/system-updated-at` | `master` |
| `benny-stock-dashboard` | `feature/system-updated-at` | `master` |

**infra、pipeline 兩個都 Merged 才往下**（dashboard 那個只是 UI 說明文字跟
文件，晚點 merge 不影響）。只 merge 一個就停在這，不要開 Airflow。

## P2-1：開 Docker Desktop，關 Airflow

跟 Part 1 的 Step 1～2 一樣：開 Docker Desktop 等 **Engine running**，進 WSL：

```bash
wsl -d Ubuntu-22.04
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
make down
docker ps --format "{{.Names}}"
```

**應該看到**：沒有 `benny-data-pipeline-` 開頭的名字。

## P2-2：拉最新程式碼並確認

```bash
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-infra
git checkout master && git pull
grep -c "ADD COLUMN IF NOT EXISTS _updated_at" sql/stock_dashboard/init_schema.sql
# 應該印出 9

cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
git checkout master && git pull
grep -c "AS _updated_at" dags/stock_dashboard_etl/templates/compute_triggers.sql
# 應該印出 29
grep -c "ON CONFLICT" dags/stock_dashboard_etl/templates/compute_triggers.sql
# 應該印出 1
grep -c "auto_adjust=False" dags/stock_dashboard_etl/etl/fetch.py
# 應該印出 1
```

任何一個印出 `0` = 那個 repo 沒拉到，回 P2-0。

## P2-3：備份

```bash
mkdir -p ~/duckdb-backup && cd ~/duckdb-backup
docker run --rm \
  -v benny-infra-duckdb-data:/data \
  -v "$PWD":/backup \
  alpine tar czf /backup/warehouse-before-updated-at.tgz -C /data .
ls -lh ~/duckdb-backup
```

**應該看到**：`warehouse-before-updated-at.tgz`，大小不是 0。

## P2-4：套用 migration

```bash
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-infra
make start
```

**應該看到**：`[OK] Schema initialized at /data/warehouse.duckdb`。

## P2-5：確認欄位加上了、舊資料有補值

```bash
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
make duckdb-shell
```

貼上：

```sql
SELECT table_name
FROM information_schema.columns
WHERE table_schema = 'stock_dashboard' AND column_name = '_updated_at'
ORDER BY table_name;
```

**應該看到 9 張表**（除了 `backtest_strategy_defs` 以外全部）。再貼：

```sql
SELECT count(*) AS null_rows FROM stock_dashboard.silver_stock WHERE _updated_at IS NULL;
-- 應該是 0

SELECT max(processed_at) AS processed_at_max, max(_updated_at) AS updated_max
FROM stock_dashboard.silver_stock;
-- 兩個值應該一樣（舊資料補成 processed_at）。把 processed_at_max 記下來
```

離開：`.exit`

## P2-6：開 Airflow，跑一次每日 DAG

```bash
make start
```

到 http://localhost:8080 確認沒有紅色 Broken DAG，`us_market_daily_etl` 開關
是藍色（沒暫停）。然後：

```bash
make trigger dag=us_market_daily_etl
```

在網頁上等這次 run 全部變深綠色。**有紅色的**：點進去看 Logs，貼給 Claude。
最常見的錯誤是 `table ... has 8 columns but 7 values were supplied`——表示
pipeline 沒拉到新的 template，回 P2-2。

這個 DAG 會照常抓資料、更新 dashboard 並 push，跟平常每天跑的一樣。

## P2-7：確認寫入行為正確

**等 P2-6 全部綠色**再開：

```bash
make duckdb-shell
```

```sql
SELECT max(processed_at) AS processed_at_max, max(_updated_at) AS updated_max
FROM stock_dashboard.silver_stock;
```

**應該看到**：

- `updated_max` 是**今天**——今天有新交易日的資料寫進來（新列）。
- `processed_at_max` 跟 P2-5 記下的值**一樣**，或只多了今天新抓的那幾天
  的資料（新資料第一次寫入）。**不應該**整張表的 `processed_at` 都變今天。

再貼這句，看今天實際被寫的列有多少：

```sql
SELECT count(*) AS written_today
FROM stock_dashboard.silver_stock
WHERE _updated_at::DATE = current_date;
```

**應該看到**：幾十到幾百列（新交易日的 MA/RSI，加上少數因新資料而改變
的值），**不是**十萬列。十萬列 = 條件式 upsert 沒生效，回 P2-2 確認
`ON CONFLICT` 那行。

離開：`.exit`。

## P2-8：raw 的台積電改成未還原價（重抓歷史）

`raw_stock` 裡 `2330.TW` 的舊資料是 yfinance 的還原權值價，新程式改抓未還原
價（Q20）。要用 backfill 把 10 年歷史重抓一次，新舊才會是同一種價格。指數
（SPX、VIX…）不配息，重抓的值一樣，條件式 upsert 不會寫它們。

```bash
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
make shell
```

進到 container 的 bash 後：

```bash
BACKFILL_YEARS=10 python dags/scripts/backfill_stock_dashboard.py
exit
```

**應該看到**：`[us] fetching ...`、`[tw] fetching ...` 等訊息，沒有
Traceback。大約幾分鐘（要打 FRED/yfinance）。

確認只有台積電被改：

```bash
make duckdb-shell
```

```sql
SELECT market, index_id, index_name, count(*) AS rows_changed_today
FROM stock_dashboard.raw_stock
WHERE _updated_at::DATE = current_date
GROUP BY ALL
ORDER BY rows_changed_today DESC;
```

**應該看到**：`index_id = 11`（台積電）有上千列（10 年內除息日以前的價格
都變了），其他指標是 0 或只有最近幾天。離開：`.exit`。

下一次 `tw_market_daily_etl` 跑完，台積電的 MA/RSI 會用新的價格重算。
做完告訴 Claude「Part 2 跑完了」。

## Part 2 出事怎麼還原

```bash
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
make down

cd ~/duckdb-backup
docker run --rm \
  -v benny-infra-duckdb-data:/data \
  -v "$PWD":/backup \
  alpine sh -c "rm -rf /data/* && tar xzf /backup/warehouse-before-updated-at.tgz -C /data"
```

**還原 DB 之後，兩個 repo 的程式也要退回舊版**才能開 Airflow（新 template
會找不到 `_updated_at` 欄位）：到 GitHub 對兩個 PR 按 **Revert**，merge
revert PR，再 `git pull`，最後 `make start`。

---

# Part 3：啟用策略組合 DAG + 觸發服務（Step 4，2026-10-14 已執行完成）

前提：Part 2 做完；pipeline 的 `feature/layer3-combo-dag-service` 已 merge 進
`master`。這次**不改 schema、不用回補**，但 image 要重 build（加了 fastapi/
uvicorn），`.env` 要加一組密鑰。

## P3-1：拉程式碼

```bash
wsl -d Ubuntu-22.04
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
git checkout master && git pull
ls services/backtest_trigger/app.py
# 應該印出這個路徑；No such file = 沒拉到
```

## P3-2：產生密鑰，寫進 `.env`

```bash
openssl rand -hex 16
```

把印出來的那串（32 個字元）複製起來。用編輯器打開
`benny-data-pipeline/.env`，在最後加兩行（第一行貼上你的那串）：

```
BACKTEST_TRIGGER_API_TOKEN=貼上剛剛那串
BACKTEST_TRIGGER_ALLOWED_ORIGINS=https://benny0624.github.io
```

這串就是之後前端要填的 token，自己另外記在安全的地方。`.env` 不會進 git。

## P3-3：重 build image、啟動

```bash
make down
make build
make start
```

`make build` 第一次要幾分鐘（所有 service 現在共用同一個 image
`benny-data-pipeline-airflow:latest`）。`make start` 之後：

```bash
docker ps --format "{{.Names}}	{{.Status}}"
```

**應該看到**多一個 `benny-data-pipeline-backtest-trigger-1`，狀態
`Up ... (healthy)`（剛啟動時是 `health: starting`，等 30 秒）。

## P3-4：確認服務活著

```bash
curl http://localhost:8000/healthz
# {"status":"ok"}

curl -s http://localhost:8000/api/backtest/options | head -c 300
# 一串 JSON，開頭是 {"triggers":[{"trigger_type":"YIELD_INVERSION_ENTER","label":"殖利率曲線倒掛",...
```

## P3-5：確認 DAG 載入、沒暫停

http://localhost:8080 → 找 `layer3_strategy_combo_backtest`：頁面上方沒有紅色
Broken DAG、左邊開關是**藍色**（這個 DAG 設定成建立時就啟用）。

## P3-6：送出第一個組合（方案四，拿來對答案）

把 `你的token` 換成 P3-2 那串：

```bash
curl -s -X POST http://localhost:8000/api/backtest/trigger   -H "X-Api-Token: 你的token" -H "Content-Type: application/json"   -d '{"entry_trigger_types":["SPX_GOLDEN_CROSS"],"exit_mode":"TRIGGER","exit_trigger_types":["SPX_DEATH_CROSS"],"ticker":"SPY"}'
```

**應該看到**：

```json
{"status":"triggered","strategy_def_id":1,"combo_key":"SPX_GOLDEN_CROSS||TRIGGER|SPX_DEATH_CROSS|||SPY"}
```

到 Airflow 網頁看 `layer3_strategy_combo_backtest` 多了一個 run，等它變深綠色
（幾秒～一分鐘）。

常見錯誤：

| 回應 | 原因 |
|---|---|
| `401 token 不正確` | token 打錯，或 `.env` 改完沒有 `make down && make start` |
| `503 服務端沒有設定 BACKTEST_TRIGGER_API_TOKEN` | `.env` 沒加那一行 |
| `502 組合已記錄，但通知 Airflow 計算失敗` | Airflow webserver 還沒起來，等一分鐘再送一次 |
| `503 資料庫忙碌中` | 剛好有 DAG 在寫 DuckDB，等一兩分鐘再送 |

## P3-7：對答案

**等 P3-6 的 run 變綠**，然後同一個指令再送一次：

**應該看到** `"status":"exists"`，後面帶 `result`，裡面
`"total_return_pct":147.3...`、`"sharpe_ratio":0.69...`、`"num_trades":6`
——跟寫死的方案四（`spx_golden_death_cross`）一樣，代表整條路是對的。

再試一個不合法的，確認會被擋：

```bash
curl -s -X POST http://localhost:8000/api/backtest/trigger   -H "X-Api-Token: 你的token" -H "Content-Type: application/json"   -d '{"entry_trigger_types":["VIX_PANIC_STATE"],"exit_mode":"FIXED_HOLD","fixed_holding_days":60,"ticker":"SPY"}'
# {"detail":"進場條件有不在可選清單內的 trigger（STATE 類不能選）：['VIX_PANIC_STATE']"}
```

做完告訴 Claude「Part 3 跑完了」。這時候還沒有排行榜頁面（Step 6）、排名也還
不會每天更新（Step 5），但「送出組合 → 算出結果」已經通了。

## Part 3 出事怎麼辦

這次沒有動既有資料，不用還原 DB。服務有問題就看 log：

```bash
make logs service=backtest-trigger
```

想把測試送出的組合清掉（`make duckdb-shell`，確認沒有 DAG 在跑）：

```sql
DELETE FROM stock_dashboard.backtest_strategy_results;
DELETE FROM stock_dashboard.backtest_strategy_defs;
```

---

# Part 4：啟用排行榜每日重算（Step 5，**還沒執行**）

這次有一張**新表**（名次快照）、每日市場 DAG 多兩個 task、多一個新 DAG。不用
手動回補：交易標的價格會由每日 DAG 自己整段重抓。

## P4-0：兩個 PR 都 merge

| repo | 分支 | 合進 |
|---|---|---|
| `benny-data-infra` | `feature/rank-history` | `master` |
| `benny-data-pipeline` | `feature/layer3-leaderboard-daily` | `master` |

## P4-1：拉程式碼、確認

```bash
wsl -d Ubuntu-22.04
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-infra
git checkout master && git pull
grep -c "backtest_strategy_rank_history" sql/stock_dashboard/init_schema.sql
# 應該印出 4（或任何 > 0）

cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
git checkout master && git pull
ls dags/stock_dashboard_etl/layer3_strategy_leaderboard_daily.py dags/.airflowignore
# 兩個路徑都印出來 = 拉到了
```

## P4-2：停 Airflow、建新表

```bash
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
make down

cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-infra
make start
# [OK] Schema initialized at /data/warehouse.duckdb
```

這次 `pyproject.toml` 沒改，**不用** `make build`。

## P4-3：開 Airflow，確認 DAG

```bash
cd /mnt/c/Users/BennyXu/Benny_Repo/Repositories/benny-data-pipeline
make start
```

http://localhost:8080：

- 頁面上方**沒有**紅色 Broken DAG。
- 多一個 `layer3_strategy_leaderboard_daily`，開關是**藍色**，Schedule 欄顯示
  Dataset（不是時間）。
- 點進 `us_market_daily_etl` → Graph，多了 `fetch_backtest_universe_us` 跟
  `market_ready_us` 兩格。

## P4-4：手動跑一次美股每日 DAG

```bash
make trigger dag=us_market_daily_etl
```

等它全部變綠（含新的兩格）。`market_ready_us` 一綠，
`layer3_strategy_leaderboard_daily` 應該**自己**多一個 run（不用手動觸發），
等它的 4 格也變綠。

`report_failed_combos` 紅色 = 有組合算失敗，點進 Logs 看是哪個組合；
`compare_hardcoded_with_combos` 紅色 = 寫死策略跟組合引擎算出來不一樣，把 Logs
貼給 Claude。

## P4-5：確認結果

**等上面全部綠**再開：

```bash
make duckdb-shell
```

```sql
-- 交易標的價格補到最近的交易日
SELECT ticker, max(updated_at) FROM stock_dashboard.backtest_universe GROUP BY 1 ORDER BY 1;

-- 今天的名次快照
SELECT h.rank, d.combo_key, round(h.sharpe_ratio, 2) AS sharpe
FROM stock_dashboard.backtest_strategy_rank_history h
JOIN stock_dashboard.backtest_strategy_defs d USING (strategy_def_id)
WHERE h.snapshot_date = (SELECT max(snapshot_date) FROM stock_dashboard.backtest_strategy_rank_history)
ORDER BY h.rank;
```

**應該看到**：美股兩檔（SPY、SOXX）的 `max(updated_at)` 是最近一個美股交易日；
台股兩檔要等 `tw_market_daily_etl` 跑過才會更新（可以 `make trigger
dag=tw_market_daily_etl` 一起驗）。名次表有你之前送出的組合，`rank` 從 1 開始。

離開：`.exit`。做完告訴 Claude「Part 4 跑完了」。

## Part 4 出事怎麼辦

新表是空的、不影響既有資料。每日 DAG 新加的兩格失敗，只會讓排行榜不重算，
`update_dashboard` 照常 push 指標 dashboard。看 Logs 貼給 Claude 即可。
