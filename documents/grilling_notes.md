# Grilling Session 筆記 — Benny Stock Dashboard（濃縮版，2026-09-16 整理）

來源文件：`user_request.txt`、`market_indicators_dashboard_architecture.pdf`
本檔記錄專案 grill-me 過程拍板的決定與待解問題。舊版逐輪討論過程過長，
已濃縮成結論；只列「決定了什麼」跟「為什麼」，來回問答細節可從 git 舊版找回。

## 專案一句話摘要

全球大總經與市場情緒轉折監測系統。Airflow 每日 ETL 抓 8 大美股/總經指標寫入
DuckDB，算衍生指標與轉折點 flag，仿照 `fantasy_daily_etl` 的模式產出靜態
dashboard html、推上 GitHub Pages。

參照 repo：

- Airflow：`benny-data-pipeline`（DAG 模式仿照 `dags/fantasy_daily_etl/fantasy_daily_etl.py`）
- DuckDB：`benny-data-infra`（新專案 schema 開在 `sql/<project>/`）
- Dashboard 產出方式：仿照 `Yahoo_fantasy_dashboard/scripts/build_dashboard.py`
  （查 DuckDB → 產靜態 html → clone dashboard repo → commit + push → GitHub Pages）

## 架構總覽（已拍板，穩定）

- **指標範圍**：8 大美股/總經指標（DGS10、DGS2、T10Y2Y、VIX、SPX、IXIC、
  SOX、DJI、RUT）+ 2 檔台股（^TWII、2330.TW）+ 3 個合成 ratio 指標
  （101=SOX/SPX、102=DJIA/Nasdaq、103=RUT/SPX，components 記在 `INDEX_MAP`）。
  DGS10/DGS2/T10Y2Y 三者都留：前兩者服務「顯示殖利率水準」、T10Y2Y 服務
  「倒掛/解倒掛 trigger」，用途不同。
- **三層架構**：Layer 1 `raw_stock`（原始資料）、Layer 2 `silver_stock` +
  `dim_triggers`（MA/RSI/ratio + 轉折點 EAV 表，`dim_triggers` 用
  `trigger_type` 區分 STATE/ENTRY/EXIT，STATE 每天寫、ENTRY/EXIT 只在跨越
  當天寫；要看寬表自己開 DuckDB `PIVOT` view，不改底層表）、Layer 3 gold
  （VectorBT 回測，`backtest_runs`/`backtest_equity_curve`/`backtest_trades`/
  `backtest_kpis` 四張表，key=`(strategy_name, ticker)`，重跑覆寫、不留
  歷史版本）。
- **DuckDB schema**：單一 schema `stock_dashboard`，`raw_stock`/
  `silver_stock` 用 `market` 欄位（'tw'/'us'）區分國家，不按國家拆 schema。
  `updated_at` 型別 `DATE`（無 intraday 意義）、`processed_at` 型別
  `TIMESTAMP`（Airflow 實際執行時間）。無 FK constraint（配合每天整表
  重算）。
- **Airflow**：兩個獨立 DAG（`us_market_daily_etl`/`tw_market_daily_etl`，
  排程時間對不上故拆開），task chain `fetch_raw → load_raw →
  [compute_ma_rsi, compute_ratios] → update_dashboard`，全掛
  `pool="duckdb_writer"`。Layer 3 是獨立的 `layer3_backtest_etl` DAG。
- **Backfill**：一次性 bulk 抓 2 年歷史（非 Airflow `catchup=True` 逐日，
  避免 rate limit），只灌 `raw_stock`/`silver_stock`，不碰 dashboard html；
  daily 跟 backfill 共用同一份 layer 2 重算 SQL。Layer 3 回補 10 年。
- **Dashboard**：靜態 html，clone `benny-stock-dashboard`（public repo，
  GitHub Pages 免費方案要求），`scripts/build_dashboard.py` 產出、git push。
  查詢視窗上限 2 年（DB 層不設 retention，永久保留給 Layer 3 用）。
- **資料源**：美股/總經用 FRED + yfinance；台股放棄 TWSE OpenAPI（只有
  當日快照、無歷史區間查詢）改用 yfinance（^TWII + 2330.TW），跟美股共用
  同一套 fetch/upsert 程式碼。無 fallback 資料源，失敗就在 dashboard 標紅
  `updated_at`。交易日曆判斷（`pandas_market_calendars`，美股查 NYSE、
  台股查 XTAI）已拍板要做，避免休市被誤報成資料異常。
- **常數集中管理**：`dags/stock_dashboard_etl/constants/index_map.py` 是
  唯一事實來源（`IndexDef` dataclass，真實指標填 `ticker`+`source`，合成
  指標填 `components`），`ratio_sql_params()` 動態產生 SQL 給
  `compute_silver_ratios.sql`，不再手寫死對照表。只有 `stock_dashboard_etl`
  會用，放自己資料夾底下，不升到頂層 `dags/config/`。

## 已完成的開發（Step 1-5，2026-08-12 ~ 2026-08-19）

1. `benny-data-infra/sql/stock_dashboard/init_schema.sql`：`raw_stock`/
   `silver_stock`/`dim_triggers` 三張表，數值欄位用 `DOUBLE`（非草稿的
   `FLOAT`，避免累積誤差），PK/unique index 欄位改 `NOT NULL`。
2. `index_map.py` 常數檔（`IndexDef` dataclass 統一真實/合成指標，
   `real_indices_for()`/`synthetic_indices_for()` helper）。
3. 兩個 DAG 骨架（task chain 見上）+ backfill script（`benny-data-pipeline/
   dags/scripts/backfill_stock_dashboard.py`，只灌 raw/silver 兩張表）。
4. Trigger SQL（`compute_triggers.sql`：殖利率倒掛、VIX 恐慌閾值 30/
   極度恐慌 35、SPX 均線金死叉、RUT-SPX 200SMA 突破、DJI-IXIC 20日動量
   ROC>+3%、SOX-SPX 252 交易日新高破底）+ 月線 MACD 獨立 `PythonOperator`
   task（`etl/macd.py`，DuckDB 無遞迴函數改用 pandas，計算步驟拆開命名
   `monthly_close → ema_12 → ema_26 → macd_line → signal_line` 求可讀性）。
5. `update_dashboard_fn` + `benny-stock-dashboard/scripts/build_dashboard.py`
   完整版（真實指標讀 `raw_stock`、合成指標讀 `silver_stock` 的 RAW 列，
   trigger 疊 markPoint，RSI/MACD 子圖依資料存在自動顯示/隱藏）。
6. CI/CD 修正：`benny-data-infra/local/init_db.py` 改掃
   `sql/*/init_schema.sql` 全部執行（原本寫死只讀一個檔，漏掉新 schema
   部署）；三份 repo 的 README 架構圖/CI-CD 描述互相對齊。

## Layer 3 回測（2026-09-05 已跑通）

方案四（SPX 金叉死叉 vs SPY）端到端跑通：10 年回補
（2016-09-06~2026-09-03），`layer3_backtest_etl` DAG、
`output/backtest_dashboard.html` 產出並 push，結果 6 次交易、總報酬
148.13%、Sharpe 0.83、勝率 83.3%。方案一/二/三/五、`feature/layer3-backtest`
merge master、舊 dashboard 重新設計，狀態記在 `layer3_backtest_proposal.md`
第 9 節。

## Layer 1/2 Dashboard 優化（2026-09-06 已完成）

美台股合併 html、multi-select 疊圖、指標定義 glossary、交易日曆判斷、
`_STATE` 背景著色、座標軸文字說明（Y 軸線性共用，波動幅度最大的指標撐開
軸範圍，非原始數值大小決定——只加文字沒做 log scale/雙軸）、ECharts
`dataZoom`（滾輪/拖拉縮放，純前端不用重查 DB）、手機版面
（viewport + media query，filter/KPI/圖表/字級都做了響應式）。手機閱讀
模式已做完（原本 backlog 項目提前做掉）。

**驗證缺口**：手機版面自動化環境 `resize_window` 測不出真實窄螢幕效果，
只驗證過 CSS class/id 對得到真實元素、語法合法，**還沒有人肉眼在手機或
裝置模擬器上確認過實際排版**——Benny 待辦。

## Layer 3 架構轉向：策略組合排行榜（2026-09-07 拍板，Phase 1 範圍已收斂）

長期願景是「APP + 社群比拚策略」，**Phase 1 只做 GitHub Pages 靜態版**：

- **不做**：整個指標/訊號 dashboard 改即時後端 API（已擱置——GitHub Pages
  免費託管運作穩定，這塊不動）。
- **要做的常駐服務**：一支很薄的觸發端點（估計一支 FastAPI/Flask 檔案、
  一個 POST route）——收「策略組合+標的」參數（不含區間，理由見下），
  伺服器端用存好的憑證呼叫 Airflow REST API 觸發 DAG，回傳查詢用 ID。
  理由：瀏覽器 JS 不能直接打
  Airflow REST API（帳密曝露前端 + CORS 問題，`basic_auth` 不是設計給
  公開前端呼叫的）。
- **對外開洞方式**：Cloudflare Tunnel——`cloudflared` container 跑在
  Lightsail 主機上，只主動連出去接 Cloudflare 邊緣網路（跟現有
  self-hosted runner「主動連出去 poll GitHub」同一種模式），主機防火牆
  inbound 規則維持全關，不推翻既有安全原則。
- **策略組合怎麼選（Phase 1）**：從現有 `dim_triggers` 約 30 種
  trigger_type 挑，**不做自訂數值運算式引擎**（那是即時對任意指標算
  `>` / `<` / 穿越的通用引擎，量級大很多，留給 APP 化階段）。進場/出場
  各自最多 2 個 trigger_type 用 AND（同一天交集）或 OR（聯集）組合，出場
  另有「固定持有 N 個交易日」選項（非 trigger，方案一既有邏輯抽出重用）。
  條件數上限 2（之後 APP 化再放寬，可能做 premium 解鎖更多堆疊）。
- **時間區段**：**不開放使用者自選**——Benny 拍板理由：挑歷史區間績效好
  等於自爽，排行榜應該每天都有人上有人下才好玩，所以固定用全部可用歷史
  區間跑，不留區間當篩選維度。
- **快取/重算**：**乾脆每次重算，不做快取**（VectorBT 單組合幾秒內跑完，
  不需要為此加快取複雜度）。開一張表記錄「指標進出場策略+標的」組合績效
  （不含區間，理由見上），重算前先掃這張表，算過的直接回吐，沒算過才真的
  觸發 Airflow 計算。
- **標的/指標擴充**：維持開發者手動加（`index_map.py`/
  `backtest_tickers.py` 加一筆 + 重跑 backfill 即可擴充，不需要架構改動），
  不開放使用者自訂任意 ticker（避免「選了才發現沒歷史資料」的等待體驗）。
  之後 APP 化可以做「敲碗投票」讓熱門標的上架。
- **觸發認證**：Phase 1 加一組簡單共用密鑰（前端輸入框填 token，後端比對
  字串一致才放行），之後社群化再換正式帳號系統。
- **結果回饋**：不做輪詢/loading 體驗，觸發後使用者自己晚點回來重整頁面
  看排行榜。
- **資料模型**：策略組合改用自增 ID 表（`backtest_strategy_defs`）存
  組合定義（進場條件+邏輯+出場條件+邏輯+標的，**不含區間**，理由見上），
  其他表用此 ID 關聯，取代原本人手命名的 `strategy_name` 字串（原本設計
  不夠用，因為使用者自組條件後不再有人取名字這件事）。
- **單策略 vs 複合策略**：**不開 flag 欄位**——`entry_trigger_types`/
  `exit_trigger_types` 本來就存成 list，`len(...)>1` 就是複合、`==1` 就是
  單一，UI 顯示標籤時查詢當下用陣列長度算即可。多存一個 flag 是冗餘資料，
  等於同一件事存兩份，將來若改組合定義卻忘記同步更新 flag 會產生矛盾狀態
  （flag 說單一、陣列卻有兩個），沒有必要冒這個風險換取一個查詢方便。
- **GitHub Pages 流量監控**：repo 的 **Settings → Insights → Traffic**
  （或 `https://github.com/Benny0624/benny-stock-dashboard/graphs/traffic`），
  看得到每日 views/unique visitors、referring sites、熱門頁面路徑。限制：
  **只保留最近 14 天**、且**只有 repo 有 push 權限的人看得到**（訪客看不到,
  這點沒有隱私疑慮）。要留長期歷史或更細的維度（例如按裝置/地區），得另外
  接 Cloudflare Web Analytics 或 GoatCounter 這類外部工具，現階段量小，
  建議先用 GitHub 內建的就好，之後真的要長期趨勢再考慮加外部工具。

---

## 已解決的誤解（存查）

- **（2026-09-17）沒有 Layer 4**：追問後 Benny 確認架構只到 Layer 3 為止——
  使用者選「策略+標的」組合、觸發計算、績效寫回表、依報酬排行，這整套就是
  Layer 3 排行榜本身，不是獨立的第四層。先前筆記把同一件事誤拆成「Layer 3
  做組合、Layer 4 做排行榜」，是電手自己想錯層級，已修正。

## 待處理

目前無待解項目。架構/規則層面全部拍板（Phase 1 策略組合排行榜設計已收斂），
但**尚未動工開發**——`backtest_strategy_defs` 表、觸發端點、Cloudflare Tunnel
都還只是設計，下次要問的是「什麼時候開始 Step 1（schema 改動）」還是先問
其他優先順序。
