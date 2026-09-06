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

11. **Issue #1 blocker 完成狀況（2026-08-11 查核）**：
    - Benny 在 issue #1 回覆「finished」並關閉，但電手實際查證 **GitHub Pages
      當時根本沒開**（`repos/.../pages` API 回 404），跟「finished」對不上。
    - 追問後 Benny 確認：**FRED_API_KEY 已寫進本機 `.env`**（查證有值）；
      **GitHub Pages 這次也真的設好了**（Benny 在本輪回覆「1. Done」）。
    - `GITHUB_PAT` 查證結果：`.env.example` 註解原本寫「只給
      `Yahoo_fantasy_dashboard` 這個 repo」，但**實測（用 API 查
      `permissions.push`）這把 fine-grained PAT 其實兩個 repo 都有 push
      權限**，註解過時，已修正成反映實際範圍。
    - 順手清掉 `.env` 裡重複的 `GITHUB_PAT=`（一行空、一行有值，dotenv 對
      同 key 重複行為因工具而異，怕踩雷）。
    - `.env.example` 補上 `FRED_API_KEY=` 占位符（原本漏了）。
    - `pyproject.toml` 加了 `pandas-market-calendars`（決定 9 待辦）、
      **另外主動加了 `yfinance`、`fredapi`**（DAG 要抓資料必須要有，原本
      完全不在依賴清單裡）——**待辦：Benny 要重新 `make build`**（WSL2 裡跑）。

12. **Layer 1-2 schema 草稿 review——4 個問題，2026-08-12 全部拍板**：
    - **A. `raw_stock.updated_at` 是交易日期，一天一筆（已解決）**：電手
      查證 FRED API 的 `frequency` 參數最細只到 `d`(daily)，DGS10/DGS2/
      T10Y2Y 本身就是財政部每日發布一次的序列，物理上沒有 intraday；
      yfinance 雖然支援到 `1h`，但 intraday 回溯限制很嚴（`1h` 只能拿最近
      ~730 天，`1m` 只能拿 7 天），且跟「一天跑一次 08:00 cron」的排程、
      MA50/MA200 的「50/200 個交易日」慣例語義都對不上。**最終定案**：
      `updated_at` 欄位型別從 `TIMESTAMP` 改成 `DATE`（沒有時分秒意義了，
      避免時區問題），`processed_at` 維持 `TIMESTAMP`（Airflow 實際執行
      時間，需要精確到秒）。
    - **B. 跨指標比值當「合成指標」處理（已解決，Benny 同意）**：SOX/SPX、
      DJIA/Nasdaq 這種比值給自己的 index_id（例如 9=SOX/SPX、10=DJIA/
      Nasdaq），算完跟真實指標一樣存進 `silver_stock`。10Y-2Y 利差不用算，
      直接用 FRED 現成的 `T10Y2Y` 序列。
    - **C. index_id 映射表（已解決，Benny 同意）**：寫進獨立的 constants
      檔集中管理（路徑待實際寫 code 時定，例如
      `dags/stock_dashboard_etl/constants/index_map.py`），不開 DuckDB
      `dim_index` 表。**這個 map 會用在 4 個地方**：① fetch task 依
      `source`(FRED/yfinance)、`market`(tw/us) 分流打 API；② 寫入
      `raw_stock` 時把 `index_name` denormalize 進資料列；③ layer 2 算
      合成指標（決定 B）時，需要另一個小 dict 記錄「合成 ID 由哪兩個真實
      ID 組成」，跟 ticker map 放一起管理；④ backfill script 同 ①；
      ⑤（2026-08-16 補）`ratio_sql_params(market)`——把 ③ 的合成指標定義轉成
      `compute_silver_ratios.sql` 的 Jinja context，daily DAG 跟 backfill
      script 共用，detail 見下方「Step 3/4 完成後的一個修正」。
      **不需要放的地方**：dashboard（`benny-stock-dashboard` repo）不用
      import 這份常數，因為 `index_name` 已經 denormalize 進每一列，
      dashboard 直接讀 DuckDB 欄位就有名稱，不用跨 repo 依賴 Python 常數。
    - **D. JSON schema 檔案的用途（已解決，Benny 同意）**：走方案 (1)——
      電手直接照這三份 json 手寫對應的
      `benny-data-infra/sql/stock_dashboard/init_schema.sql`，不另外寫
      script 自動產生 DDL（照 fantasy 先例，`init_schema.sql` 本來就是
      手寫 SQL）。

13. **DAG 資料夾結構（已解決，2026-08-12）**：確認 Benny 已建的單一
    `dags/stock_dashboard_etl/` package 底下會定義**兩個獨立 `DAG(...)`
    物件**（`us_market_daily_etl`、`tw_market_daily_etl`，dag_id 各自
    獨立、排程時間不同，對應決定 4/8 的「兩個獨立 DAG」），共用同一份
    `schemas/`/`templates`/`constants` 等程式碼，不是像 fantasy 那樣
    「一個資料夾一個 DAG」。

14. **Backfill 腳本的職責範圍（已解決，2026-08-12）**：backfill **只負責
    灌 `raw_stock`/`silver_stock`**，不負責 push html——跟 daily DAG 的
    `update_dashboard` task 職責分開，不重複邏輯。html 產生+推送**只由
    daily DAG 觸發**（第一次要看到成果，手動觸發一次 daily DAG 即可）。

15. **Dashboard html 資料窗口（已解決，2026-08-12）**：討論過程中電手一開始
    把「windows 模式」講成「可以避免抓全部資料」，Benny 指出這站不住腳——
    下拉選單只要包含最長的選項，html 就必須內嵌那個選項的全部資料，
    「windows vs 全取」本質是同一件事。真正的問題是「最長的選項該设多長」：
    - **DB 層（`raw_stock`/`silver_stock`）不設 retention，資料永久保留、
      無限增長**——layer 3 回測需要長歷史，不能因為 dashboard 好看就砍。
    - **Dashboard build script 查詢時設 2 年上限**（跟 backfill 的 2 年
      horizon 對齊），下拉選單 `3mo/6mo/1yr/2yr`（拿掉 `all`，`2yr` 就是
      上限，DB 資料以後超過 2 年也不會讓頁面無限變大）。
    - 需要回溯 2 年以前的冷資料，Benny 會自己進 DB 查，不需要 dashboard
      support。

## 目前 grilling 進度估計（2026-08-12，電手自估，非精確科學）

- **架構/需求對齊層面：約 95%，可以視為問完了**。schema 欄位、型別、
  合成指標處理、index 映射管理方式、DDL 產出方式、DAG 拆分、backfill
  職責邊界、dashboard 資料窗口全部拍板，沒有已知的懸而未決項目。
- **實際可動工開發的程度：仍然是 0——目前為止一行實作代码都還沒寫**，
  但**設計面已經清楚到可以直接動手**，不會再被「還沒想清楚」卡住。剩下
  唯一沒設計過的是**「8 個指標的轉折點 trigger 邏輯要怎麼翻譯成實際
  SQL」**（均線金叉死叉、RSI 閾值穿越、Ratio 52 週新高/破底、月線 MACD
  轉向）——這塊還沒討論過，且比 MA/RSI 這種「純聚合」複雜，因為「金叉/
  死叉/首度解倒掛」這類 trigger 定義的是**狀態轉換的瞬間**（今天穿越、
  昨天沒穿越），不是單純算個數值，SQL 需要用 `LAG()` 之類的視窗函數比較
  「今天 vs 昨天」的狀態，不能簡單複製 MA 的寫法。

## 下一步（2026-08-12 起，分階段動工）

Benny 同意「先做骨架、trigger 邏輯之後再細化」的分階段方式：

1. ~~寫 `benny-data-infra/sql/stock_dashboard/init_schema.sql`~~ **已完成
   （2026-08-12）**。
2. ~~寫 `dags/stock_dashboard_etl/constants/` 的 index 映射常數檔~~ **已完成
   （2026-08-13）**。
3. ~~仿 `fantasy_daily_etl` 設計兩個 DAG（`us_market_daily_etl`/
   `tw_market_daily_etl`）的 task 拆分、`pool` 設定~~ **已完成，2026-08-16
   查核 codebase 確認**：`stock_dashboard_etl.py` 兩個 DAG 都建好，
   task chain `fetch_raw → load_raw → [compute_ma_rsi, compute_ratios] →
update_dashboard`，寫入 task 都掛 `pool="duckdb_writer"`（跟
   `fantasy_daily_etl` 共用同一個 pool，`local/airflow.docker-compose.yaml`
   有建）。
4. ~~Backfill script（`benny-data-pipeline/dags/scripts/` 底下）~~
   **已完成，2026-08-16 查核 codebase 確認**：`backfill_stock_dashboard.py`
   只灌 `raw_stock`/`silver_stock`（兩年），不碰 dashboard html，符合
   決定 14。
5. Trigger SQL 邏輯（`LAG()` 視窗函數處理狀態轉換）——留到骨架跑通之後
   再細化，是目前唯一還沒設計過的大塊，**下一步**。

### Step 1 完成細節：`benny-data-infra/sql/stock_dashboard/init_schema.sql`

三張表：`stock_dashboard.raw_stock`（layer 1）、`stock_dashboard.silver_stock`
（layer 2，MA/RSI/合成指標比值）、`stock_dashboard.dim_triggers`（layer 2，
轉折點事件）。跟 Benny 原本的 json 草稿比，電手做了幾個修正（已跟 Benny
說明，未被推翻）：

- `index_value`/`agg_value`/`trigger_value`：草稿是 `FLOAT`（單精度），改成
  `DOUBLE`——均線/比值這種連續計算的欄位怕單精度浮點誤差累積。
- `updated_at`/`agg_type`/`trigger_type`：草稿是 `nullable: true`，改成
  `NOT NULL`——這三個都在 PK/unique index 裡，邏輯上不該允許 null。
- `dim_triggers` 表名採複數（草稿檔名跟內部 `tableName` 不一致，一個複數
  一個單數，統一成複數）。
- Schema 名稱定為 `stock_dashboard`（對齊資料夾名 `sql/stock_dashboard/`，
  跟 fantasy 的 `fantasy` schema 同一個慣例）。

### Step 2 完成細節：`dags/stock_dashboard_etl/constants/index_map.py`

過程中 Benny 電手兩題，都已修正：

- **DGS2 vs T10Y2Y 的取捨（2026-08-13 拍板）**：電手原本誤把兩者當互斥
  選項，只留 T10Y2Y。Benny 指出 DGS10 直覺上該對應 DGS2（兩者都是「殖利率
  水準值」，適合一起畫殖利率曲線），T10Y2Y 是 FRED 自己算好的「利差」，
  性質不同。**最終定案：DGS10、DGS2、T10Y2Y 三個都抓**——DGS2 服務「顯示
  殖利率水準」，T10Y2Y 服務「倒掛/解倒掛 trigger 判斷」，用途不同，FRED
  免費、多抓一個序列成本趨近於零。三者算同一個「殖利率/利差」指標概念
  底下的子序列，不算破壞「8 大指標」的範圍界定。
- **`RATIO_COMPONENTS`/`RATIO_NAMES` 沒有比照 `IndexDef` dataclass 化
  （2026-08-13 拍板）**：Benny 抓到這個不一致，電手承認純粹是沒想清楚，
  已修正——把合成指標也統一進同一個 `IndexDef`/`INDEX_MAP`，新增
  `ticker`/`source`/`components` 三個 optional 欄位（真實指標填
  `ticker`+`source`，合成指標填 `components`=(分子 id, 分母 id)），用
  `is_synthetic` property 區分兩種，不用再多維護 `RATIO_COMPONENTS`/
  `RATIO_NAMES` 兩個獨立字典。提供 `real_indices_for()`（fetch/backfill
  用）、`synthetic_indices_for()`（layer 2 算比值用）兩個 helper。
- **常數放哪（2026-08-13 確認，維持電手原本的放法）**：Benny 問是否該照
  Airflow 慣例放 `dags/config/`。電手說明這個 repo 的 `config/` 是
  `airflow.cfg override`（基礎設施層級），跟 DAG 業務邏輯常數性質不同；
  repo 現有慣例是 DAG 專屬程式碼放自己資料夾底下（`fantasy_daily_etl/
etl/`），只有真的跨 DAG 共用才升到頂層 `etl/`。`index_map.py` 目前只有
  `stock_dashboard_etl` 會用，**維持放在 `dags/stock_dashboard_etl/
constants/`**，不開新的頂層 `dags/config/`。

最終 `INDEX_MAP` 內容（`index_id` 1-11 為真實指標，101+ 為合成指標）：

| id  | 指標              | market | source   | ticker           |
| --- | ----------------- | ------ | -------- | ---------------- |
| 1   | 10年期美債殖利率  | us     | fred     | DGS10            |
| 2   | 2年期美債殖利率   | us     | fred     | DGS2             |
| 3   | 10Y-2Y 殖利率利差 | us     | fred     | T10Y2Y           |
| 4   | VIX 恐慌指數      | us     | yfinance | ^VIX             |
| 5   | S&P 500 指數      | us     | yfinance | ^GSPC            |
| 6   | Nasdaq 指數       | us     | yfinance | ^IXIC            |
| 7   | 費城半導體指數    | us     | yfinance | ^SOX             |
| 8   | 道瓊工業指數      | us     | yfinance | ^DJI             |
| 9   | 羅素 2000 指數    | us     | yfinance | ^RUT             |
| 10  | 台股加權指數      | tw     | yfinance | ^TWII            |
| 11  | 台積電            | tw     | yfinance | 2330.TW          |
| 101 | SOX/SPX Ratio     | us     | (合成)   | components=(7,5) |
| 102 | 道瓊/那指 Ratio   | us     | (合成)   | components=(8,6) |

## Step 3/4 完成後查核出的一個修正（2026-08-16）

電手 double check codebase 時發現：`templates/compute_silver_ratios.sql`
的 `ratio_defs`（`VALUES (101, 'SOX/SPX Ratio', 7, 5), ...`）是**手寫死在
SQL 裡**，跟 `constants/index_map.py` 的 `INDEX_MAP`（`components=(7, 5)`
等）是兩份獨立文字，中間沒有任何同步機制——SQL 檔頭註解還提醒「要跟
`RATIO_COMPONENTS` 保持同步」，但那個字典在 Step 2（決定 12-C）已經被砍掉
併進 `IndexDef.components` 了，註解也沒跟著更新。風險：以後新增合成指標
只改 `index_map.py`、忘記同步改這份 SQL，會靜默漏算，`silver_stock` 查不到
新比值也不會報錯。

**已修正**：讓 SQL 端直接吃 `index_map.py` 動態產生，不再手寫。

- `constants/index_map.py` 新增 `ratio_sql_params(market)`：包一層
  `synthetic_indices_for()`，轉成 SQL 端要的欄位形狀
  （`ratio_id`/`ratio_name`/`numerator_id`/`denominator_id`）。
- `templates/compute_silver_ratios.sql` 的 `ratio_defs` 改成
  `{% for r in params.ratios %}` 迴圈產生 `VALUES`，外層包
  `{% if params.ratios %}`——某個 market 沒有合成指標時（目前的 `tw`）
  `params.ratios` 是空 list，整段 SQL no-op，不會產生語法錯誤。
- `stock_dashboard_etl.py`（`compute_ratios` task）跟
  `backfill_stock_dashboard.py`（`_render_sql`）都改成呼叫同一個
  `ratio_sql_params()`，不各自維護一份轉換邏輯。
- 驗證：三個 Python 檔 `ast.parse` 語法檢查過；Jinja 實際 render 過
  US market（兩筆合成指標，VALUES 逗號分隔正確）跟 TW market（空 list，
  只剩註解、無殘留語法）兩種情境。

`INDEX_MAP` 現在是唯一事實來源，新增合成指標只要改一個地方
（`index_map.py`），`compute_silver_ratios.sql` 自動跟上，不用再手動同步
兩份定義。相關程式碼說明同步更新到
`benny-data-pipeline/dags/stock_dashboard_etl/stock_dashboard_etl.md`。

## `update_dashboard_fn` 順序（2026-08-16 拍板）：先做完 Step 5 再寫

`update_dashboard_fn`/`build_dashboard.py` 留到 Step 5（trigger SQL）做完、
`dim_triggers` 有真的資料之後再一次寫完整版，不先出閹割版。

## Step 5（trigger SQL）待回答問題（2026-08-16，對照 PDF 原始定義查出的坑）

`market_indicators_dashboard_architecture.pdf` 第 1-2 頁「核心轉折點與量化
條件」表格逐條核對，發現好幾條 PDF 寫的「量化觸發條件」本身就不夠精確、
無法直接轉成 SQL，需要 Benny 先回答：

1. **VIX 恐慌閾值，30 還是 35？** PDF 原文寫「VIX > 30 或 35」，兩個數字
   都出現，沒有明確選一個。
   -> 以 30 作為預設恐慌觸發門檻（市場標準），並將 35 定義為「極度恐慌（Extreme Fear）」。
2. **哪些 trigger 是「狀態轉換瞬間」、哪些是「持續性閾值」？** 金叉死叉、
   倒掛解倒掛、SMA 突破是「今天穿越、昨天沒穿越」，要 `LAG()` 比對狀態；
   但 VIX 恐慌/自滿這種，是「條件成立的每一天都算一筆」還是「只在剛進入
   該狀態的那天算一筆」？兩種寫法完全不同，PDF 沒說清楚。
   -> dim_triggers 的粒度依然維持 (date, index_id) 每日一筆（便於與行情 Join），但欄位明確拆分：
   -> State 欄位（每天算）：in_vix_panic (VIX >= 30)
   -> Transition 欄位（只有跨過那天為 TRUE）： is_vix_panic_entry = (VIX >= 30 and LAG(VIX) < 30), is_vix_panic_exit = (VIX < 30 and LAG(VIX) >= 30)
3. **SOX/SPX Ratio「創高/破底」要抓多長的滾動窗口？** PDF 寫「52 週」，
   換算成交易日是 252 天還是直接用 365 曆日？現在 `silver_stock` 完全沒有
   任何 rolling max/min 的 `agg_type`，這是全新的計算，不是照抄 MA/RSI
   的寫法能解決的。
   -> 採用美股標準交易日 252 天，並在 DuckDB 中以 Window Function 計算
   ```SQL
   MAX(ratio_value) OVER (
      PARTITION BY index_id
      ORDER BY trade_date
      ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
   ) AS high_252d
   ```
4. **月線 MACD 完全沒實作**（SOX 輪動 trigger 的第二個條件）：MACD 需要
   EMA12/EMA26/訊號線 EMA9，而且「月線」代表要先把 daily 資料 resample
   成月線收盤價再算 MACD——`compute_silver_indicators.sql` 目前完全沒有
   涵蓋這塊，EMA 的遞迴特性（今天的 EMA 依賴昨天的 EMA）也沒辦法像 MA
   一樣用簡單的 `ROWS BETWEEN N PRECEDING` window 函數算，DuckDB 要嘛用
   遞迴 CTE、要嘛換公式（例如用 SMA 近似），這塊需要另外設計。
   -> 能寫一個 TASK 拉出來用 PYTHON 算嗎
5. **Russell 2000/SPX Ratio 根本不在 `INDEX_MAP` 裡**：目前
   `constants/index_map.py` 只定義了 101（SOX/SPX）、102（DJIA/Nasdaq）
   兩個合成指標，PDF 的羅素 2000 trigger 需要的 RUT/SPX ratio 完全沒有
   `index_id`，要先在決定 12-B 的合成指標清單裡加一筆。
   -> 補上 103: RUT_SPX_RATIO (RUT / GSPC)
6. **「突破趨勢線」沒有可執行的定義**（羅素 2000 trigger 的一半條件）：
   PDF 原文「Russell 2000 / SPX Ratio 突破趨勢線或 200SMA」，「趨勢線」
   是線性迴歸通道、人工畫線、還是別的算法？這種主觀敘述沒辦法直接轉 SQL，
   要嘛 Benny 給一個具體算法，要嘛只做「或 200SMA」那一半、放棄趨勢線
   那一半。
   -> 直接砍掉「趨勢線」，只做「突破 200SMA（200日均線）」
7. **道瓊/那指 Ratio「顯著上升」沒有量化門檻**：PDF 原文「DJIA / Nasdaq
   Ratio 顯著上升」，沒給任何數字（不像 VIX 有 30/35/12，也不像倒掛有
   `<0`/`>0`）。需要 Benny 定義具體閾值，例如「N 個交易日內漲幅超過 X%」
   或改用「相對 MA50 的乖離率超過 X%」這種可執行的寫法。
   -> 採用 「20 交易日動量（20D Momentum）突破閥值」 或 「突破 MA60 乖離率」
   -> $$DJI\_IXIC\_20D\_ROC = \frac{Ratio_t - Ratio_{t-20}}{Ratio_{t-20}} > +3.0\%$$
   -> 代表在 1 個月內，道瓊跑贏那斯達克超過 3%，明確反映資金逃離高 Beta 轉入防禦型價值股
8. **合成指標（101/102，之後可能加 103）目前沒有 MA50/MA200**：
   `compute_silver_indicators.sql` 只吃 `raw_stock`（真實指標），SOX/SPX
   這種 ratio 的 MA 完全沒算過。SOX 輪動 trigger 如果要用 ratio 的均線，
   現有管道算不出來，需要另外一段 SQL 對 `silver_stock` 裡
   `agg_type='RAW'` 的合成指標序列再算一次 MA。
   -> Phase 1（基礎計算）：raw_stock ➔ 計算個股/指數的 MA、RSI，同時產出 101, 102, 103 的每日收盤 Ratio 寫入 silver_stock。
   -> Phase 2（衍生指標計算）：以 silver_stock 為來源，對 index_id IN (101, 102, 103) 補算 MA50 與 MA200，更新回 silver_stock。
9. **`dim_triggers` 的寫入頻率**：像 VIX 持續一整週都 > 30 這種情況，
   `dim_triggers` 要每天都寫一筆（dashboard 上轉折點標記會連續好幾天都有
   flag），還是只在「進入」跟「離開」該狀態的那兩天各寫一筆？取決於問題 2
   的答案，但要明確拍板才能定 SQL 的 `WHERE` 邏輯長什麼樣。
   -> 同 2. 日曆維度全量記錄 State，欄位內用 LAG() 標記 Event
   -> State 欄位（每天算）：in_vix_panic (VIX >= 30)
   -> Transition 欄位（只有跨過那天為 TRUE）： is_vix_panic_entry = (VIX >= 30 and LAG(VIX) < 30), is_vix_panic_exit = (VIX < 30 and LAG(VIX) >= 30)

## 2026-08-17 追問確認：Q2/Q9 EAV vs 寬表、Q4 Python task 可讀性

- **Q2/Q9（`dim_triggers` EAV vs 寬表）已解決：維持 EAV，不改 `init_schema.sql`**。
  Benny 確認 Q2/Q9 舉例的 `in_vix_panic`/`is_vix_panic_entry`/
  `is_vix_panic_exit` 只是講解用的欄位名，不是要改表結構——實際存法是
  `trigger_type='VIX_PANIC_STATE'`/`'VIX_PANIC_ENTRY'`/`'VIX_PANIC_EXIT'`
  三種不同的 EAV row（STATE 每天寫一筆，ENTRY/EXIT 只在跨越那天寫一筆）。
  Benny 要看寬表格式的話**自己在 DuckDB 上開一個 `PIVOT` view**，不需要
  底層表配合改成寬表。Step 1 的 `dim_triggers` DDL 維持不變。
- **Q4（月線 MACD）已解決：獨立 `PythonOperator` task，寫的時候要求可讀性
  優先**：確認用 pandas 算 EMA12/EMA26/Signal（DuckDB 沒有原生遞迴函數，
  硬湊 SQL window function 算 EMA 難讀），但 Benny 要求**計算邏輯本身要
  寫得可讀性高**——實作時避免一行鏈式塞太多 `.ewm().mean()`，拆成有名字
  的中間步驟（例如 `monthly_close` → `ema_12` → `ema_26` → `macd_line` →
  `signal_line` 各自一行、變數名說明用途），不要為了精簡犧牲看懂的速度。

## Step 5 SQL/task 實作完成 + CI/CD 修正 + `build_dashboard.py`（2026-08-19）

- **Step 5 trigger SQL/task 全部寫完並驗證**：`compute_triggers.sql`（殖利率
  倒掛/VIX 恐慌自滿/SPX 均線穿越金死叉/RUT-SPX 200SMA/DJI-IXIC 20 日 ROC/
  SOX-SPX 252 日新高破底）、`compute_silver_ratio_ma.sql`（合成指標 MA50/
  MA200，決定 8 的 Phase 2）、`etl/macd.py` + `compute_sox_macd` task（月線
  MACD 轉向）。用本地起一顆 in-memory DuckDB、灌合成資料（刻意埋轉折點）
  跑過全部 SQL + Python 驗證，殖利率/VIX/SPX/ratio 的 ENTER/EXIT/BREAKOUT/
  CROSS 都精準落在埋的轉折日期上，`market='tw'` 正確 no-op。
- **`INDEX_MAP` 新增 103（羅素2000/SPX Ratio，components=(9,5)）**，對應
  Q5 答案，`compute_silver_ratios.sql` 靠既有的動態渲染機制自動吃到，不用
  改 SQL。
- **`update_dashboard_fn` 實作完成**：clone `benny-stock-dashboard`、帶
  `MARKET=us`/`tw` 跑它的 `scripts/build_dashboard.py`、`git add output/`、
  沒變化就跳過、有變化才 commit + push，模式照抄 `fantasy_daily_etl.py`。
- **`benny-stock-dashboard/scripts/build_dashboard.py` 寫完**：查 2 年窗口
  資料，真實指標讀 `raw_stock`、合成指標讀 `silver_stock` 的 `RAW` 列，
  `dim_triggers` 依自己的 `index_id` 直接分組疊圖（不用額外對照表），事件型
  trigger 疊 markPoint、`_STATE` 不疊。前端指標下拉 + 時間範圍下拉，RSI/
  MACD 子圖依資料是否存在自動顯示/隱藏，模式照抄 `Yahoo_fantasy_dashboard`。
  端到端跑過本地驗證（含 HTML 裡的 JSON payload 正確性檢查）。
- **CI/CD 修正（Benny 指示）**：
  1. `benny-data-infra/local/init_db.py` 從「寫死讀一個 `init_schema.sql`」
     改成掃過 `sql/*/init_schema.sql` 全部執行，`duckdb.docker-compose.yaml`
     改掛整個 `sql/` 目錄——修好「`stock_dashboard` schema 沒被自動部署
     執行到」的缺口。
  2. `benny-data-infra/README.md` 補上「這條 CD 只處理新表，改動既有表的
     結構要手動進 DuckDB 改」的說明（`CREATE TABLE IF NOT EXISTS` 對已存在
     的表是 no-op）。
  3. `benny-data-infra`/`benny-data-pipeline`/`benny-stock-dashboard` 三份
     README 的架構圖跟 CI/CD 描述對齊、互相 cross-reference；
     `benny-data-pipeline/README.md` 拿掉「以 `fantasy_daily_etl` 為例」的
     單一 DAG 走法，改寫成整個 repo 的 general 文件（DAG 清單改表格、
     `.env`/`make trigger dag=` 改成通用寫法）。

## 2026-09-05 進度更新：Layer 3 方案四已端到端跑通

電手 check codebase 確認：`layer3_backtest_etl` DAG（方案四，SPX 金叉死叉
vs SPY）已實際跑過，`output/backtest_dashboard.html` 產出並 push 上
GitHub，10 年回補資料（2016-09-06~2026-09-03）確認有跑，結果 6 次交易、
總報酬 148.13%、Sharpe 0.83、勝率 83.3%。詳細狀態（哪些 repo/分支完成到
哪、方案三/一/二/五還沒做、`feature/layer3-backtest` 還沒 merge master、
舊 dashboard 重新設計還沒動工）記在 `layer3_backtest_proposal.md` 第 9 節。

**Layer 1/2 優化需求盤點**（同份文件第 10 節）：Benny 補了 3 項（美台股
合併 html、multi-select 疊圖、指標定義 glossary），加上原本已知的「交易
日曆判斷」「`_STATE` 背景著色」共 5 項，**2026-09-06 全部做完**（手機排版
列為 backlog）——`benny-stock-dashboard/scripts/build_dashboard.py` 改版，
detail 見 `layer3_backtest_proposal.md` 第 10 節。
