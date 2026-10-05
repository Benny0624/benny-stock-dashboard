# Runbook：套用 Layer 3 排行榜 schema + 重跑回測（2026-10-04）

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

- `processed_at` 全部是今天。
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
