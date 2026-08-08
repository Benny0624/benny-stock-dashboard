# Grilling Session 筆記 — 2026-08-06

來源文件：`user_request.txt`、`market_indicators_dashboard_architecture.pdf`
這份筆記記錄第一輪 grill-me 電過的問題、Benny 的回答、已拍板的決定，跟還沒解決、
明天要繼續電的坑。

## 專案一句話摘要

全球大總經與市場情緒轉折監測系統。Airflow 每日 ETL 抓 8 大美股/總經指標寫入
DuckDB，算衍生指標與轉折點 flag，仿照 `fantasy_daily_etl` 的模式產出靜態
dashboard html、推上 GitHub Pages。

參照 repo：
- Airflow：`benny-data-pipeline`（DAG 模式仿照 `dags/fantasy_daily_etl/fantasy_daily_etl.py`）
- DuckDB：`benny-data-infra`（新專案 schema 要開在 `sql/<project>/`）
- Dashboard 產出方式：仿照 `Yahoo_fantasy_dashboard/scripts/build_dashboard.py`
  （查 DuckDB → 產靜態 html → clone dashboard repo → commit + push → GitHub Pages）

## 已拍板的決定

1. **指標數量**：PDF 標題寫「7 大」是筆誤，實際 **8 個指標全部都要**：
   DGS10、DGS2(T10Y2Y)、VIX、SPX、IXIC、SOX、DJI、RUT。

2. **歷史回補（backfill）**：
   - **不用** Airflow `catchup=True` 逐日回補（兩年 = 600+ 次 job，且不確定
     FRED/yfinance 有沒有 rate limit，怕被封）。
   - 改成**一次性 bulk 歷史抓取**（一次 API call 拿一段時間範圍），跑一支
     獨立的回補流程，把資料一次性灌進 `raw_daily_quotes`。
   - 回補範圍：**兩年**——以最長回看窗（MA200）為基準抓的 buffer。
   - **已解決（2026-08-08）**：fetch 層共用 `fetch(start_date, end_date)`，
     backfill 傳兩年範圍、daily 傳「今天」。
     `fact_indicators`（layer 2 / silver）計算層**照抄 `fantasy_daily_etl` 的
     模式，每天用 `DuckDBSQLOperator` 對整張表重算**（不維護增量滾動窗口
     狀態）。backfill 只是「先灌兩年 layer 1 資料，跑一次 layer 2 重算 SQL」，
     daily 是「多一天 layer 1 資料，一樣跑一次同一份 layer 2 重算 SQL」——
     backfill 跟 daily 用同一份 SQL，沒有邏輯分岔的問題。

3. **架構文件裡的 Streamlit/read_only=True 併發鎖警語是死文字，該刪**：
   目前**沒有**常駐服務直連 DuckDB，只有 Airflow 這個 process 會碰它，產出的
   是靜態 html（跟 fantasy dashboard 一樣）。PDF 那段「前端 Dashboard 連線
   必須 read_only=True」的敘述不適用於目前的架構，之後如果真的要做 Flask/
   Streamlit 即時查詢才需要撿回來考慮。

4. **台股資料源：放棄 TWSE OpenAPI，改用 yfinance**：
   - 查過 TWSE OpenAPI（`openapi.twse.com.tw`）的大盤端點，回傳格式是
     `{日期, 指數, 收盤指數, 漲跌, ...}`——**只有當日快照，沒有歷史區間查詢**。
     兩年回補等於要打 500+ 次 API，不划算。
   - 改用 `yfinance` 的 `^TWII`（加權指數）跟 `2330.TW`（台積電），跟原本 8
     個美股指標走**同一套 fetch/upsert 程式碼**，不用為台股另開一套資料源
     整合、錯誤處理、rate limit 邏輯。
   - 本階段範圍：**只抓台股大盤（^TWII）+ 台積電（2330.TW）**，其他台股指標
     先不做。
   - 台股會產**第二份 html**（獨立於美股 dashboard）。
   - **已解決（2026-08-08）**：**拆成兩個獨立 DAG**——台股 13:30 收盤、美股+FRED
     資料要隔天台灣清晨才到齊，排程時間本來就對不上，硬塞同一個 DAG 只會
     讓其中一邊資料被迫延遲或用舊值湊數。兩個 DAG 各自維護自己的 html
     page。之後做 layer 3（回測）會是**第三個 DAG**，聽台美股當日排程完成後
     再做回測計算。
   - 回測（VectorBT，PDF 第四節）**這階段不做**，只先把台股大盤+台積電的
     資料抓進來，回測留到之後（layer 3，第三個 DAG，見上）。

5. **資料源失敗處理**：
   - 不做 fallback 資料源（避免「fallback 的數據維度跟 yfinance 對不上」的
     新坑）。
   - 失敗就開天窗：dashboard html 上用紅字明確標示**最新資料更新時間
     （updated_at）**，讓使用者自己看得出資料是不是舊的。
   - **未拍板、只是建議、Benny 沒明確接受**：用交易日曆判斷「今天該不該有
     新資料」，避免把「休市」跟「API 真的掛了」搞混、每個長週末都被紅字
     誤報。——下次要追問要不要做這個，還是先跑，之後噪音大了再處理。

6. **三層架構定義（2026-08-08 拍板）**：
   - Layer 1（raw）：API 抓取的歷史原始資料，backfill 也是灌這一層。
   - Layer 2（silver）：用 layer 1 算出 MA50/MA200/RSI 等 agg metrics，dashboard
     直接讀這層畫圖。**`dim_triggers`（轉折點 flag）也歸在 layer 2**，因為
     dashboard 要能在圖上標記轉折點，不能等 layer 3 才算。`dim_triggers`
     自己一張表，用「指標代號 + timestamp」當 join key 關聯回 silver 的
     指標表——**只是約定俗成的 join key，不宣告 DB 層 FOREIGN KEY constraint**
     （查過 `benny-data-infra/sql/fantasy/init_schema.sql`，Fantasy 6 張表
     互相關聯也完全沒用 FK constraint，跟 layer 2 每天整表重算的写法搭配，
     宣告 FK 只會在重算當下徒增「暫時性 FK violation」的麻煩，沒有實際收益）。
   - Layer 3（gold）：用台美股指標做績效回測（VectorBT）。**這階段不做**，
     第二階段才開工，也還沒設計長什麼樣。

7. **DuckDB schema 命名/分法——目前卡住，2026-08-09 待 Benny 回答**：
   - Benny 提案：資料夾 `sql/stock_dashboard/`，schema 依「國家 × 層級」全部
     拆開：`raw_tw_stock`、`raw_us_stock`、`silver_tw_stock`、`silver_us_stock`、
     `gold_tw_stock`、`gold_us_stock`（6 個 schema）。
   - 電手（Claude）的質疑：台美股 raw 層欄位理論上長一樣（date, ticker, OHLCV...），
     照國家拆 schema 等於同一份 DDL 要維護兩次；layer 3 要做「美股轉折點
     vs 台股表現」的跨國分析時，也要 join 兩個不同 schema，不如單一 schema
     用 `market` 欄位（'TW'/'US'）區分，兩個 DAG 各自 insert 自己那國的資料。
   - Benny 一度反駁「Fantasy 也存了 6 張表」，查證後**這個反例不成立**——
     Fantasy 是 **1 個 schema（`fantasy`）裡放 6 張表**，不是 6 個 schema，
     跟現在爭論的「要不要按國家拆 schema」是不同的軸。
   - Benny 接著提案「把 layer 3 併進 layer 2」，這砍的是「層級」那個軸，
     沒有解決「國家」這個軸的重複問題（砍完還是 4 個 schema：
     `raw_tw_stock`/`raw_us_stock`/`silver_tw_stock`(含未來 backtest 結果)/
     `silver_us_stock`），而且 layer 3 目前還沒設計，先塞進 silver 有「之後
     發現塞錯要遷移」的風險。**電手建議：layer 3 現在乾脆不開 schema，
     等真的要做的時候再決定要不要自己一個 schema。**
   - **已解決（2026-08-09）**：改成 `raw_stock`/`silver_stock` 一張表用
     `market` 欄位（'TW'/'US'）區分，不按國家拆 schema。Layer 3（gold）
     維持「現在不開」，等真的動工再決定。

8. **GitHub repo 建立——方案已提出，等 Benny 拍板 public/private，2026-08-09 回答**：
   - 現況查證：`gh auth status` 正常（Benny0624 帳號，有 `repo` 權限）。
     `Benny0624/benny-stock-dashboard` **這個 repo 在 GitHub 上還不存在**。
     本機 `benny-stock-dashboard/` 資料夾也**還不是 git repo**（只有
     pdf/txt/這份筆記，沒有 `.git`）。
   - 查過 `Yahoo_fantasy_dashboard` 先例：public repo、`main` 分支、issues
     有開啟（GitHub Pages 免費方案本來就要 public repo 才能用）。
   - 電手提出的執行方案（**還沒執行，等 Benny 明確回覆再做**）：
     1. 本機 `benny-stock-dashboard/` 跑 `git init`，commit 現有檔案。
     2. `gh repo create Benny0624/benny-stock-dashboard --public --source=. --push`。
     3. 開一個 issue，SOP 內容包含：申請 FRED API key 的步驟連結、確認
        repo/Pages 設定完成的檢查清單。
   - **已解決（2026-08-09）**：**Public**——之後會分享 dashboard 連結給其他人
     看，private 會讓其他人看不到 Pages，跟 fantasy dashboard 先例一致。
   - **已執行（2026-08-09）**：
     - `git init` + commit 現有 pdf/txt/筆記
     - `gh repo create Benny0624/benny-stock-dashboard --public --source=. --push`
       建好並推上去了：https://github.com/Benny0624/benny-stock-dashboard
     - 開了 SOP issue：https://github.com/Benny0624/benny-stock-dashboard/issues/1
       （內容：申請 FRED API key 的步驟、GitHub Pages 設定檢查清單）——
       **待 Benny 自己去完成，完成後在 issue 底下回報**。

9. **交易日曆判斷（2026-08-09 拍板）**：Benny 同意要做。設計：用
   `pandas_market_calendars` 套件，美股/FRED DAG 查 `NYSE` 日曆（SIFMA 債券
   市場日曆假日差異只有 Columbus Day/Veterans Day 這種冷門日子，不用抠這麼
   細，NYSE 夠用），台股 DAG 查 `XTAI`（台灣證交所）日曆。抓資料前先問日曆
   「今天是不是預期交易日」——不是就跳過、不產生紅字警示；是的話才去比對
   「有沒有抓到新的一列」，沒有才真的標紅。**待辦**：`pandas_market_calendars`
   要加進 `benny-data-pipeline` 的 `pyproject.toml` dependencies。

10. **Nasdaq 獨立 trigger（2026-08-09 拍板）**：不用另外設計，**只做 PDF
    裡已經列出來的 trigger events**（殖利率倒掛、VIX、S&P 500 均線、SOX
    輪動、羅素 2000、道瓊 vs 那指），Nasdaq 只在道瓊/那指比值裡當分母出現，
    不需要自己獨立的轉折條件。

## 還沒問 / 下次繼續電的方向

決定 1-10 全部拍板（見上方）。repo 已建好、SOP issue 已開（#1）。剩下：

- **卡進度的 blocker**：Benny 要去完成 issue #1 裡的兩件事——申請 FRED API
  key、確認 GitHub Pages 設定——完成前沒辦法真的動手寫 DAG/測試。
- `raw_stock`/`silver_stock` 的實際欄位設計（column schema）還沒討論，
  現在 schema 分法（用 `market` 欄位區分台美股）定案了，可以開始設計了。
- `pandas_market_calendars` 加進 `benny-data-pipeline/pyproject.toml` 的
  dependencies（決定 9 的待辦）。
- DAG 檔案結構還沒定：兩個 DAG（美股/台股）要各自開
  `dags/us_market_daily_etl/`、`dags/tw_market_daily_etl/` 資料夾（照
  `fantasy_daily_etl/` 的模式），細節（task 拆分、pool 設定避免 DuckDB
  lock 衝突）還沒設計。
- Backfill 的一次性腳本要放在哪裡執行、用什麼身份跑（本機手動跑一次？
  還是包成 Airflow 的一個一次性 DAG run？）沒討論過。
