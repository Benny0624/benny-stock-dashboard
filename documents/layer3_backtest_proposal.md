# Layer 3 回測規劃草案 — 2026-08-29

這份文件是「把現有 `dim_triggers` 轉折訊號拿去對台美股個股/指數做買賣訊號
回測」的規劃草案，還沒拍板，格式比照 `grilling_notes.md`：列可行方案 + 待決定
問題，等 Benny 回覆再定案。範圍對應 `grilling_notes.md` 決定 6 的 Layer 3
（gold），跟 PDF 第四節「下一步：美台股回測策略規劃」是同一個東西的具體化。

## 0. 先講一個會影響所有方案解讀方式的限制：目前只有 2 年資料

`raw_stock`/`silver_stock`/`dim_triggers` 目前的歷史深度是 2 年（backfill
決定 2 的範圍），實際查過的 trigger 發生次數（2024-08-23 ~ 2026-08-21）：

| trigger_type                                  | 次數          |
| --------------------------------------------- | ------------- |
| `YIELD_INVERSION_ENTER`/`_EXIT`               | 各 3 次       |
| `VIX_PANIC_ENTER`/`_EXIT`                     | 各 3 次       |
| `VIX_EXTREME_FEAR_ENTER`/`_EXIT`              | 各 2 次       |
| `SPX_GOLDEN_CROSS`                            | 3 次          |
| `SPX_DEATH_CROSS`                             | 1 次          |
| `SPX_200SMA_BREAKOUT`/`_BREAKDOWN`            | 9 次 / 8 次   |
| `RUT_SPX_200SMA_BREAKOUT`/`_BREAKDOWN`        | 15 次 / 13 次 |
| `DJI_IXIC_ROC_ENTER`/`_EXIT`                  | 各 18 次      |
| `SOX_SPX_RATIO_NEW_HIGH_252D`/`_NEW_LOW_252D` | 55 次 / 23 次 |
| `SOX_MACD_BULLISH_CROSS`/`_BEARISH_CROSS`     | 3 次 / 1 次   |

**這代表**：像「殖利率解倒掛」「VIX 恐慌」「金叉死叉」這幾種 trigger，2 年
內只發生個位數次，回測出來的「勝率/期望值」樣本數小到接近軼事等級，不是
統計上站得住腳的結論——PDF 原文「驗證歷次大熊市避險效果」這種講法，2 年
資料量本來就驗證不了（一次完整的殖利率倒掛/解倒掛循環往往橫跨好幾年）。
`DJI_IXIC_ROC`/`SOX_SPX_RATIO` 這種頻率較高的 trigger（幾十次）比較有機會
看出初步統計特徵，但也稱不上穩健。

**兩個因應方向**（待 Benny 選）：

1. 先拿現有 2 年資料跑，**結果當「方向性參考」不當「結論」**，回測程式先寫
   出來、pipeline 打通，之後資料夠長再重新看數字。
2. **加開一次更長的歷史回補**（yfinance/FRED 都能抓 10-20 年，2 年上限是
   決定 2/15 為了 MA200 buffer 跟 dashboard 顯示設的，不是技術硬限制），
   專門給回測用的資料深度可以跟 dashboard 顯示的 2 年上限脫鉤。

## 1. 現有訊號盤點：哪些 trigger 可以直接當進出場訊號用

`dim_triggers` 的 EAV 設計已經把「狀態」「進入/離開瞬間」「當天達成的成就」
分成三種語意（見 `stock_dashboard_etl.md`），對應到回測的訊號型態：

| 語意                                   | trigger_type 範例                                                                                                                                                                                               | 回測用法                                                                                                        |
| -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| 進入/離開一對（狀態轉換）              | `VIX_PANIC_ENTER`/`_EXIT`、`YIELD_INVERSION_ENTER`/`_EXIT`、`SPX_200SMA_BREAKOUT`/`_BREAKDOWN`、`RUT_SPX_200SMA_BREAKOUT`/`_BREAKDOWN`、`DJI_IXIC_ROC_ENTER`/`_EXIT`、`SOX_MACD_BULLISH_CROSS`/`_BEARISH_CROSS` | ENTER 當進場訊號、EXIT 當出場訊號，持有期間由兩個事件之間的天數決定（regime-following）                         |
| 單一方向事件（金叉死叉）               | `SPX_GOLDEN_CROSS`/`SPX_DEATH_CROSS`                                                                                                                                                                            | 同上，GOLDEN 進場、DEATH 出場，是同一組 `SPX_MA_CROSS_STATE` 的子集                                             |
| 當天達成的成就（level 事件，不分狀態） | `SOX_SPX_RATIO_NEW_HIGH_252D`/`_NEW_LOW_252D`                                                                                                                                                                   | 沒有天然的「進場後何時出場」，可以互為進出場（HIGH 進場、LOW 出場）或搭配固定持有期（PDF 原始設計是後者）       |
| 連續狀態（每天一筆，不是事件）         | `*_STATE`                                                                                                                                                                                                       | 目前沒用在任何 trigger 型策略，但可以拿來做條件過�器，例如「只在 `VIX_PANIC_STATE=false` 的日子才進場」這種濾網 |

除了 `dim_triggers`，`silver_stock` 裡的連續數值（`RSI14`、`MA50`/`MA200`、
合成比值本身）也可以在回測當下直接讀出來做**額外過濾條件**，不需要為此再開
新的 trigger SQL——例如 PDF 原始的「VIX>35 且 SOX RSI<30」，RSI<30 這個條件
不用变成一個新的 `trigger_type`，回測查詢時直接對 `silver_stock` 的
`RSI14` 欄位加一個 `WHERE agg_value < 30` 就好。

## 2. 一個要先解決的架構問題：「訊號指標」跟「交易標的」通常不是同一個東西

`dim_triggers`/`silver_stock` 裡的 `index_id` 是**訊號來源**（VIX、SOX、
T10Y2Y 這些是總經/情緒指標，不是拿來交易的部位）。PDF 原文自己也是這樣設計
的：「運用 `dim_triggers` 轉折訊號，結合美股（SPY, QQQ）與台股（加權指數,
台積電 2330）」——訊號跟標的本來就是分開的兩件事，回測需要一張「trigger
的 index_id → 要交易哪個 ticker」的對照，這張表目前不存在，需要新增。

**交易標的的資料要從哪來**：Benny 要求「挑可以用現有資料源拿到的」——
`etl/fetch.py` 的 `_fetch_yfinance()` 本來就是通用的（吃任何 yfinance
ticker，不限於 `INDEX_MAP` 現有的 11+3 個），所以 SPY/QQQ/IWM/SOXX/
0050.TW/006208.TW 這些**不需要新的資料源整合，同一套 fetch 邏輯就能拿到**，
純粹是要不要多抓幾檔的問題。

**這幾檔交易標的的資料要放在哪，是一個待決定的架構分岔**：

- **方案 A：併入 `INDEX_MAP`/`raw_stock`**——把 SPY/QQQ/IWM/SOXX/0050.TW/
  006208.TW 當成新的 `index_id`（例如 12-17）直接加進現有的表，複用全部
  既有 fetch/backfill/schema 管線，改動量最小。缺點：`raw_stock` 原本的
  定位是「PDF 定義的 8 大總經/情緒指標」，混入「純粹用來交易的 ETF 收盤
  價」會讓這張表的語意變得模糊（`index_name` 對照表會同時出現「VIX 恐慌
  指數」跟「SPDR S&P 500 ETF」這種不對等的東西）。
- **方案 B：另開一張表**（例如 `stock_dashboard.backtest_universe` 或
  `layer3_prices`），專門存交易標的的收盤價，跟訊號指標的 `raw_stock`
  分開。概念上更乾淨（「這是訊號」vs「這是要交易的東西」分得清楚），但
  要多寫一份 schema/fetch 邏輯，即使可以複用 `_fetch_yfinance()` 這個
  函式本身。

## 3. 訊號怎麼變成「進場/出場」的機制設計（不管哪個方案都要處理）

1. 從 `dim_triggers` 查出某個 `index_id` + `trigger_type` 的事件日期序列。
2. 把事件日期對齊到**交易標的**的價格時間序列（`updated_at` 已經是交易日
   期，兩邊都是同一套交易日曆，理論上對齊沒有時區問題）。
3. ENTER/EXIT 這種成對的，用 forward-fill 的方式展開成「每天在不在場內」
   的布林序列（ENTER 那天到下一個 EXIT 前一天都是 `True`）。
4. 固定持有期的策略（PDF 的 60/120 日），不需要 EXIT 事件，訊號觸發後
   直接往後數 N 個交易日出場即可。
5. 餵進回測引擎——**沿用 PDF/決定 4 就講好的 VectorBT**
   （`vbt.Portfolio.from_signals(price, entries, exits)`），算出總報酬、
   勝率、期望值、Sharpe 這些指標，就是 PDF 要的「勝率與期望值」。

## 4. 可行方案（訊號 + 交易標的 + 進出場規則）

### 方案一：極端恐慌抄底（照抄 PDF #1，訊號現成，兩個條件都不用新開發）

- **進場**：`VIX_EXTREME_FEAR_ENTER`（VIX 首度站上 35）**且**同一天 SOX
  （`index_id=7`）的 `RSI14` < 30（`silver_stock` 現成欄位，查詢時加條件
  即可，不用新 trigger）。
- **出場**：固定持有 60 或 120 個交易日（PDF 原文兩個都要驗）。
- **交易標的**：SPY（美股廣度最大盤子的恐慌抄底，最貼近 PDF 原文語意）。
- **現況**：訊號完全現成，`VIX_EXTREME_FEAR_ENTER` 2 年內只有 2 次——
  樣本數非常小，是本文件開頭那個限制最明顯的案例。

### 方案二：倒掛解開避險（照抄 PDF #2，有一個訊號缺口需要先補）

- **進場（其實是「減碼」訊號，方向跟一般抄底策略相反）**：
  `YIELD_INVERSION_EXIT`（首度解倒掛）**且**倒掛已經維持超過 100 個交易日
  **且**同一天 SPX 價格跌破自己的 MA50。
- **缺口**：目前的 `SPX_200SMA_*`/`SPX_MA_CROSS`（MA50 vs MA200）都不是
  「價格 vs MA50」這個條件，`compute_triggers.sql` 沒有現成的
  `SPX_50SMA_BREAKDOWN`。不一定要開新的 EAV trigger——直接在回測查詢時
  比較 `raw_stock.index_value` 跟 `silver_stock` 的 `MA50` 兩個現成欄位
  即可，不需要動 pipeline。「倒掛維持超過 100 個交易日」也是查詢時對
  `YIELD_INVERSION_STATE` 連續 `True` 的天數做視窗函數即可，不用新表。
- **交易標的**：SPY。
- **現況**：`YIELD_INVERSION_EXIT` 2 年內 3 次，一樣是小樣本；而且「驗證
  歷次大熊市避險效果」這句話本身就超出 2 年資料能驗證的範圍（見第 0 節）。

### 方案三：半導體強勢輪動（照抄 PDF #3，訊號完全現成，最推薦優先做這個）

- **進場**：`SOX_SPX_RATIO_NEW_HIGH_252D`（SOX/SPX Ratio 突破 52 週新高）。
- **出場**：兩個選項——(a) 對稱用 `SOX_SPX_RATIO_NEW_LOW_252D`（現成訊號，
  regime-following，不用猜持有期）；(b) 固定持有期（PDF 原文比較像這個，
  但沒明講要多久）。**建議先做 (a)**，因為兩個訊號都現成、邏輯最乾淨。
- **交易標的**：台積電（`2330.TW`，已經在 `INDEX_MAP` 裡）+ 006208.TW
  （PDF 原文指定的台股 ETF，需要新抓，見第 2 節的架構分岔）。
- **現況**：`SOX_SPX_RATIO_NEW_HIGH_252D` 2 年內 55 次、`_NEW_LOW_252D`
  23 次，是目前樣本數最多、統計上最有機會看出東西的訊號，**建議優先做
  這一個**。

### 方案四（新提案，不在 PDF 裡，但用現有 trigger 就能做，適合當 pipeline 打通的第一個 sanity check）

- **進場**：`SPX_GOLDEN_CROSS`。
- **出場**：`SPX_DEATH_CROSS`。
- **交易標的**：SPY。
- **為什麼建議加這個**：是最單純的教科書級趨勢跟隨策略，訊號來源跟交易
  標的高度相關（同一個指數），沒有第 2 節提到的「訊號指標跟交易標的錯位」
  問題，也沒有任何資料缺口——適合當**第一個拿來驗證整條回測 pipeline
  （查詢→對齊→VectorBT→出報表）本身有沒有寫對**的案例，驗證過再做
  邏輯更複雜的方案一/二/三。

### 方案五（新提案）：SOX 月線 MACD 轉向

- **進場**：`SOX_MACD_BULLISH_CROSS`。
- **出場**：`SOX_MACD_BEARISH_CROSS`。
- **交易標的**：SOXX（iShares 半導體 ETF，比直接交易 `^SOX` 指數本身更
  貼近真實可交易標的）或台積電 `2330.TW`。
- **現況**：月線訊號，2 年內只有 3+1 次，樣本數是所有方案裡最小的（月線
  本身頻率就低），比較適合當「這個新做的 MACD trigger 到底有沒有用」的
  探索性檢查，不適合拿來說服自己有穩健的策略。

## 5. 待決定問題

1. **交易標的的資料放哪**（第 2 節方案 A vs B）？
   -> 方案 B，先把新的 SCHEMA 補上來我看
2. **要不要為回測另外做一次更長的歷史回補**（第 0 節），還是先用現有 2
   年資料把 pipeline 寫出來、之後再補長？
   -> 可以，你建議多長?五年夠不夠?
3. **回測要不要進 Airflow 排程**——決定 4 原本設想的是「第三個 DAG，聽
   台美股當日排程完成後再做」，變成每天自動重跑一次策略績效；還是先當
   一次性/手動跑的研究腳本（更適合探索期，之後真的要穩定追蹤某個策略
   表現再變成 DAG）？
   -> 先寫一次性 dag 但我希望回測圖要畫在第三個 HTML，且每種策略回測圖都要包含以下元素:
   1. 策略 vs. 基準累積淨值曲線（Equity Curve）
   2. 水下回撤圖（Underwater Chart）
   3. 月度 / 年度報酬熱力圖（Monthly Returns Heatmap）
      -> 告訴我回測圖還缺啥數據
4. **確認回測引擎維持用 VectorBT**（PDF/決定 4 原本就這樣講），還是有
   其他考量要重新選？
   -> VectorBT 就好
5. **方案二缺的「價格 vs MA50」條件**，要在回測查詢當下臨時算（不動
   pipeline），還是乾脆補一個正式的 `SPX_50SMA_BREAKOUT`/`_BREAKDOWN`
   trigger 進 `compute_triggers.sql`（好處是以後 dashboard 也能顯示這個
   trigger，不是回測專屬）？
   -> 補一個正式的 `SPX_50SMA_BREAKOUT`/`_BREAKDOWN` TRIGGER 並且更新 US_DASHBOARD
6. 上面 5 個方案裡，**優先順序**要不要照文件建議（方案四 sanity check →
   方案三 樣本數最多 → 方案一/二/五 探索性質），還是 Benny 有別的優先序？
   -> 可以 但排優先順序的意義是啥

## 6. 2026-08-30 追問回覆

### Q6 追問：排優先順序的意義是什麼

因為接下來要動的東西（新 schema、新 fetch、訊號轉部位邏輯、VectorBT 串接、
圖表 render）是**依序疊上去的**，不是一次全部做完。方案四（金叉死叉）零
資料缺口、訊號跟交易標的是同一個指數家族，是**唯一可以在不動任何 schema/
trigger 的情況下，把「查詢→對齊→VectorBT→出圖」整條 pipeline 從頭跑到尾
驗證一次的方案**。先拿它驗證，如果哪一段寫錯了（例如訊號對齊日期錯位、
VectorBT 參數用錯），問題範圍只會在 pipeline 本身，不會跟「新 trigger 定義
對不對」「新抓的 006208.TW 資料格式怪不怪」這種同時發生的變數混在一起、
互相干擾除錯。等 pipeline 驗證過，之後每加一個新方案，只需要煩惱這個方案
自己的訊號邏輯，不用重新懷疑地基有沒有問題。

**結論**：照文件建議的順序（四 → 三 → 一/二/五）。

### Q1 追問：方案 B 的新 schema 草稿

分成兩組表，職責不同：

**第一組：回測用的交易標的價格**（對應「交易標的的資料放哪」）——存
SPY/QQQ/IWM/SOXX/006208.TW 這些純粹用來交易、不是總經訊號的收盤價。刻意
**不**沿用 `INDEX_MAP` 的整數 `index_id` 編號慣例（那套編號是「PDF 定義的
指標 + 其衍生比值」的專屬命名空間），改用 ticker 字串直接當 key，語意上
更誠實（這些本來就不是「指標」，硬塞進同一套編號只是徒增混淆）：

```sql
CREATE TABLE IF NOT EXISTS stock_dashboard.backtest_universe (
    ticker          VARCHAR         NOT NULL,  -- yfinance ticker，例如 'SPY'、'2330.TW'
    ticker_name     VARCHAR,                   -- 顯示用名稱
    market          VARCHAR         NOT NULL,  -- 'tw' / 'us'
    close_price     DOUBLE,
    updated_at      DATE            NOT NULL,
    processed_at    TIMESTAMP       NOT NULL DEFAULT current_timestamp
);
CREATE UNIQUE INDEX uq_backtest_universe
    ON stock_dashboard.backtest_universe(ticker, updated_at);
```

**第二組：回測「跑出來的結果」**（這是 Q3 圖表需求會用到的資料來源，
Q1 沒直接問，但既然要畫圖就一定需要，一併提出來）：

```sql
-- 一次回測（某策略 + 某交易標的 + 某參數）的 metadata
CREATE TABLE IF NOT EXISTS stock_dashboard.backtest_runs (
    run_id            VARCHAR   NOT NULL,  -- 例如 'sox_spx_ratio_rotation__2330TW__20260830'
    strategy_name     VARCHAR   NOT NULL,
    ticker            VARCHAR   NOT NULL,
    benchmark_ticker  VARCHAR,             -- 對照基準（通常是同一檔 buy & hold）
    start_date        DATE      NOT NULL,
    end_date          DATE      NOT NULL,
    processed_at      TIMESTAMP NOT NULL DEFAULT current_timestamp
);

-- 策略 vs 基準的每日累積淨值 + 水下回撤，Equity Curve 跟 Underwater Chart 都吃這張
CREATE TABLE IF NOT EXISTS stock_dashboard.backtest_equity_curve (
    run_id          VARCHAR   NOT NULL,
    updated_at      DATE      NOT NULL,
    strategy_value  DOUBLE,   -- 策略累積淨值（正規化從 1.0 起算）
    benchmark_value DOUBLE,   -- 基準累積淨值
    drawdown_pct    DOUBLE,   -- 相對策略自身歷史高點的回撤（負值）
    processed_at    TIMESTAMP NOT NULL DEFAULT current_timestamp
);
CREATE UNIQUE INDEX uq_backtest_equity_curve
    ON stock_dashboard.backtest_equity_curve(run_id, updated_at);

-- 逐筆交易紀錄——樣本數小的方案，這張表比任何彙總指標都重要，
-- 能直接列出「這 3 次觸發，各自賺賠多少」
CREATE TABLE IF NOT EXISTS stock_dashboard.backtest_trades (
    run_id        VARCHAR   NOT NULL,
    trade_seq     INTEGER   NOT NULL,
    entry_date    DATE      NOT NULL,
    exit_date     DATE,
    entry_price   DOUBLE,
    exit_price    DOUBLE,
    return_pct    DOUBLE,
    holding_days  INTEGER,
    processed_at  TIMESTAMP NOT NULL DEFAULT current_timestamp
);
CREATE UNIQUE INDEX uq_backtest_trades
    ON stock_dashboard.backtest_trades(run_id, trade_seq);

-- 彙總 KPI，一次 run 一列
CREATE TABLE IF NOT EXISTS stock_dashboard.backtest_kpis (
    run_id             VARCHAR NOT NULL,
    total_return_pct   DOUBLE,
    cagr_pct           DOUBLE,
    sharpe_ratio       DOUBLE,
    sortino_ratio      DOUBLE,
    max_drawdown_pct   DOUBLE,
    max_drawdown_days  INTEGER,
    win_rate_pct       DOUBLE,
    profit_factor      DOUBLE,
    num_trades         INTEGER,
    calmar_ratio       DOUBLE,
    processed_at       TIMESTAMP NOT NULL DEFAULT current_timestamp
);
CREATE UNIQUE INDEX uq_backtest_kpis ON stock_dashboard.backtest_kpis(run_id);
```

沿用決定 7 的既有慣例——**還是同一個 `stock_dashboard` schema**，不因為
是 layer 3 就另開 schema（跟 raw/silver 一樣，用表本身的用途區分層級，不
用 schema 名稱分）。這份 DDL 是草稿，**還沒寫進
`benny-data-infra/sql/stock_dashboard/init_schema.sql`**，先讓你看過欄位
設計有沒有問題，OK 我再實際加進去。

### Q2 追問：backfill 抓多長，5 年夠不夠——建議 10 年，5 年不夠

5 年不夠的具體理由，分兩種訊號來看：

- **高頻訊號**（`SOX_SPX_RATIO_NEW_HIGH/LOW_252D`、`DJI_IXIC_ROC`、
  `SPX_GOLDEN/DEATH_CROSS`）：這些訊號本身出現頻率就高（2 年內已經有
  幾十次），拉長到 5 年或 10 年都只是單純線性增加樣本數，5 年也夠用。
- **低頻/週期性訊號**（`YIELD_INVERSION`、`VIX_PANIC`/`VIX_EXTREME_FEAR`）：
  這些綁的是**總經循環**，不是隨機出現的雜訊——VIX 真正大爆發的時間點是
  2018 年 2 月、2020 年 3 月（COVID）、2022 年熊市，殖利率倒掛/解倒掛的
  完整循環動輒橫跨 2-3 年。**只抓 5 年（2021-2026），只能完整涵蓋 2022
  年那一輪循環，2020 COVID 崩盤都還在邊界上、抓不抓得到看剛好切在哪一天**。
  這類訊號的樣本數問題，本質上不是「多抓幾年」線性能解決的，是要跨過
  夠多次「循環」才有意義。

**建議抓 10 年（2016-2026）**：涵蓋 2018 年、2020 年、2022 年三次主要
VIX/殖利率事件，把這幾個低頻 trigger 的樣本數從個位數拉到大概 2-3 倍
（仍然稱不上統計穩健，但比 2 年好一截）。**老實講，如果要真的讓
`YIELD_INVERSION` 這類訊號有統計意義，可能需要 20 年以上（涵蓋 2000、
2006-2007 那幾輪），但 10 年是「明顯比現在好、成本還算合理」的折衷點**，
不建議一次衝到 20 年（愈久遠的資料，跟現在的市場結構/流動性差異愈大，
「更長」不等於「更有參考價值」）。

**實際可行性要注意的限制**：不是所有 ticker 都有 10 年資料——
`SOXX`（2001 上市）、`IWM`（2000 上市）沒問題，但如果之後方案三/五要用到
比較年輕的 ETF，會被該 ticker 自己的上市日期卡住，抓不到比上市日更早的
資料，這是每個 ticker 各自的天花板，不是系統限制。

10 年這個數字你覺得可以就先定案，之後回測要不要再拉更長，看方案一/二
（VIX/殖利率）的結果穩不穩再說。

### Q3 追問：回測圖表還缺哪些數據

你列的三項（Equity Curve、Underwater Chart、月度/年度報酬熱力圖）是
標準 tearsheet 的核心，但考慮到**這個專案的訊號樣本數普遍很小**（見第 0
節），我建議至少再加這幾項，理由跟這個小樣本限制直接相關：

| 建議新增                                                                                               | 為什麼需要                                                                                                                                                                                          |
| ------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **KPI 彙總表**（總報酬、CAGR、Sharpe、Sortino、最大回撤%+天數、勝率、Profit Factor、交易次數、Calmar） | 這是三張圖背後的數字版本，`backtest_kpis` 表已經設計進去了，dashboard 上至少要有一個表格陳列，不能只有圖沒有數字                                                                                    |
| **逐筆交易列表**（`backtest_trades`：進場日/出場日/報酬%/持有天數）                                    | **這個對你的情境特別重要**——樣本數只有個位數到幾十次的策略，「Sharpe Ratio = 1.2」這種彙總指標很容易誤導，直接列出「這 3 次觸發分別是哪幾天、各自賺賠多少」，比任何彙總統計都誠實                   |
| **交易次數/樣本數的顯眼標示**                                                                          | 呼應第 0 節的核心提醒——每個策略的圖表旁邊都應該直接寫「N=3 次交易」，不能讓人只看到報酬率曲線就誤以為是穩健結論                                                                                     |
| **部位曝險時間比例**（Exposure，一段期間內實際持有部位的天數佔比）                                     | Regime-following 策略（ENTER/EXIT 成對）的持有時間長短差異很大，光看報酬率沒辦法判斷「是策略真的厲害，還是剛好長期壓在一段大多頭」                                                                  |
| **策略 vs 基準的 Alpha/Beta/相關係數**                                                                 | 光看兩條累積淨值線，沒辦法量化「這個策略到底比單純 buy & hold 多賺了多少風險調整後報酬」，這幾個數字才是真正回答 PDF「期望值」這個問題的東西                                                        |
| **訊號疊在價格走勢圖上**（跟 dashboard 那邊 `build_dashboard.py` 已經做的 markPoint 疊圖同一個概念）   | 這個回測 dashboard 的目的之一是讓你**看得懂訊號本身**，不是只看報酬數字——把進出場點直接標在價格線上，比任何統計數字都直觀，尤其樣本數小的時候，人眼掃過去比統計檢定更快抓到「這幾次到底是不是巧合」 |

**没建議加的**：報酬分布直方圖、rolling Sharpe 這類更進階的統計視覺化——
樣本數個位數到幾十次的情況下，這些圖畫出來意義不大（統計上噪音會蓋過
訊號），先不做，之後真的抓到 10-20 年資料、樣本數上來了再考慮補。

### Q5 確認：新增 `SPX_50SMA_BREAKOUT`/`_BREAKDOWN`，dashboard 不用改程式碼

`compute_triggers.sql` 會新增一組跟現有 `SPX_200SMA_*` 完全同構的 CTE
（只是把 `MA200` 換成 `MA50`），寫進 `dim_triggers`。**好消息**：
`build_dashboard.py` 的 `fetch_triggers()` 查詢是
`WHERE trigger_type NOT LIKE '%_STATE'`，**沒有寫死允許哪些 trigger_type
名單**——新的 `SPX_50SMA_BREAKOUT`/`_BREAKDOWN` 只要開始寫進
`dim_triggers`，SPX 那張圖表會自動疊出新的三角形標記，**dashboard
（`benny-stock-dashboard`）這邊不需要改任何程式碼**，你原文寫的「更新
US_DASHBOARD」這件事其實會自動發生，不用另外排一個任務。

## 7. 目前定案，等你 review 完 Q1 的 schema 草稿再動工

- 交易標的價格另開表（`backtest_universe`），回測結果另外四張表
  （`backtest_runs`/`backtest_equity_curve`/`backtest_trades`/
  `backtest_kpis`），都留在 `stock_dashboard` schema——**schema 草稿見
  上方，還沒寫進 `init_schema.sql`，等你回覆 OK**。
  -> 基本沒問題，追問一，同一策略須有不同 RUN_ID 的理由是啥?為了以後上排程每天重跑新的一次 RUN_ID 結果?

- Backfill 回測用的歷史深度抓 **10 年**（2016-2026），跟 dashboard 顯示
  用的 2 年上限脫鉤。
  -> 10年 TICK 資料會不會塞爆我的 LOCAL? 目前只有塞 INDEX 資料沒啥問題，塞10年的 DAY STOCK TICK LOCAL 撐得住嗎? DUCKDB檔案預估多大? 還是我得盡快上 AWS?

- Layer 3 先寫成**一次性腳本/DAG**（不是每天排程），輸出到上面那幾張
  gold 表，`benny-stock-dashboard` 再開第三份 html 讀這些表畫圖。
  -> 舊的圖表我覺得有幾個問題:
  1.  TRIGGER 轉捩點跟大盤、股價甚至是台股都沒辦法對照看，不知道比如說 VIX 黃金交叉時台積電股價多少? 我認為台美股的 html 應該產在同一份，而且應該要讓trigger 可以跟index or stock tick 畫的線圖同時顯示，從 filter 去改，改可以 multiple choose 要顯示的 index + trigger組合。
  2.  index 定義也幫寫一下，比如說 VIX 定義是啥?數值高地代表啥?寫一下

- 圖表除了你列的三項，加碼 KPI 彙總表、逐筆交易列表、樣本數標示、曝險
  時間比例、Alpha/Beta、訊號疊價格圖——**六項一起確認要不要全做，還是
  你想砍掉幾項先做最小可行版本**。
  -> 挑最重要的先做，我最在乎策略跟大盤績效比 KPI 是更好還更差，策略 賺錢/drawdown 發生的主要時間點在哪裡 其他跟策略績效無關的拜託 先剔除
- 回測引擎：VectorBT。
- `SPX_50SMA_BREAKOUT`/`_BREAKDOWN` 補進 `compute_triggers.sql`，
  dashboard 端不用改code。
- 執行順序：方案四 → 方案三 → 方案一/二/五。

## 8. 2026-08-30 第二輪追問回覆

### 追問一：`run_id` 存在的理由——原設計想多了，改成自然 key + 覆寫

老實說我原本的設計預設「每次執行都留一筆新歷史」（比較像 MLflow 那種
實驗追蹤的思路），但你已經拍板 Layer 3 是**一次性腳本**，不是每天排程
——「每天重跑留一筆新紀錄」這個情境現在不存在，原設計的複雜度找不到
對應的需求，是我想多了。

跟這個專案其他地方的既有慣例對照也對不上：`raw_stock`/`silver_stock`
從頭到尾都是「用自然 key（`market`/`index_id`/`updated_at`）`INSERT OR
REPLACE`，重跑就覆寫，不留歷史版本」（決定 2）。回測結果表也該比照辦理。

**修正**：拿掉「每次執行一個新 `run_id`」的設計，改成拿
**`strategy_name` + `ticker`** 當自然 key，重跑同一個策略、同一個標的
就直接覆寫舊結果——跟 layer 2 的行為完全一致，不用煩惱「哪個 run_id 是
最新的」這種額外的查詢邏輯。

唯一需要保留「可以同時存在多筆」的情境是**同一個策略、同一個標的，測不同
參數**（例如方案一要驗證持有 60 天 vs 120 天，這是兩個真的想並排比較的
結果，不是「重跑覆寫舊的」）——這種情況用 **`strategy_name` 裡帶參數**
解決（例如 `extreme_fear_dip_buy_60d` vs `extreme_fear_dip_buy_120d` 當
兩個不同的 `strategy_name` 值），不需要額外一個 `run_id` 欄位。

```sql
-- 拿掉 backtest_runs 的 run_id 這個人造 key，改用 (strategy_name, ticker) 當自然 key
CREATE TABLE IF NOT EXISTS stock_dashboard.backtest_runs (
    strategy_name     VARCHAR   NOT NULL,  -- 例如 'extreme_fear_dip_buy_60d'（參數編碼進名稱）
    ticker            VARCHAR   NOT NULL,
    benchmark_ticker  VARCHAR,
    start_date        DATE      NOT NULL,
    end_date          DATE      NOT NULL,
    processed_at      TIMESTAMP NOT NULL DEFAULT current_timestamp
);
CREATE UNIQUE INDEX uq_backtest_runs
    ON stock_dashboard.backtest_runs(strategy_name, ticker);

-- 其餘三張表把 run_id 全部換成 (strategy_name, ticker) 這組複合 key
CREATE TABLE IF NOT EXISTS stock_dashboard.backtest_equity_curve (
    strategy_name   VARCHAR   NOT NULL,
    ticker          VARCHAR   NOT NULL,
    updated_at      DATE      NOT NULL,
    strategy_value  DOUBLE,
    benchmark_value DOUBLE,
    drawdown_pct    DOUBLE,
    processed_at    TIMESTAMP NOT NULL DEFAULT current_timestamp
);
CREATE UNIQUE INDEX uq_backtest_equity_curve
    ON stock_dashboard.backtest_equity_curve(strategy_name, ticker, updated_at);

CREATE TABLE IF NOT EXISTS stock_dashboard.backtest_trades (
    strategy_name VARCHAR   NOT NULL,
    ticker        VARCHAR   NOT NULL,
    trade_seq     INTEGER   NOT NULL,
    entry_date    DATE      NOT NULL,
    exit_date     DATE,
    entry_price   DOUBLE,
    exit_price    DOUBLE,
    return_pct    DOUBLE,
    holding_days  INTEGER,
    processed_at  TIMESTAMP NOT NULL DEFAULT current_timestamp
);
CREATE UNIQUE INDEX uq_backtest_trades
    ON stock_dashboard.backtest_trades(strategy_name, ticker, trade_seq);

CREATE TABLE IF NOT EXISTS stock_dashboard.backtest_kpis (
    strategy_name      VARCHAR NOT NULL,
    ticker             VARCHAR NOT NULL,
    total_return_pct   DOUBLE,
    cagr_pct           DOUBLE,
    sharpe_ratio       DOUBLE,
    sortino_ratio      DOUBLE,
    max_drawdown_pct   DOUBLE,
    max_drawdown_days  INTEGER,
    win_rate_pct       DOUBLE,
    profit_factor      DOUBLE,
    num_trades         INTEGER,
    calmar_ratio       DOUBLE,
    alpha              DOUBLE,  -- Q3 追問補的，見下面「圖表最終定案」
    beta               DOUBLE,
    processed_at       TIMESTAMP NOT NULL DEFAULT current_timestamp
);
CREATE UNIQUE INDEX uq_backtest_kpis
    ON stock_dashboard.backtest_kpis(strategy_name, ticker);
```

之後真的要上排程、想留每天的歷史軌跡，再回頭加 `run_id`/日期版本——現在
不需要為了不存在的需求先把 schema 弄複雜。

### 追問二：10 年資料會不會塞爆本機——不會，差好幾個數量級，跟上雲與否無關

先澄清用詞：你說的「TICK 資料」如果是指真正的逐筆成交明細，這個專案從
決定 12-A 開始就明確排除 intraday/tick 等級的資料，`backtest_universe`
存的是**日線收盤價**（跟 `raw_stock` 完全一樣的粒度），不是 tick。

**實際數量級**：10 年 × 一年約 252 個交易日 ≈ 2520 列／檔，就算
`backtest_universe` 塞 8 檔（SPY/QQQ/IWM/SOXX/2330.TW/006208.TW/
0050.TW/`^TWII`），總共也才 **2 萬列左右**。DuckDB 是欄式儲存+字典壓縮，
`ticker`/`ticker_name`/`market` 這種每列重複的字串欄位壓縮率非常高，
這張表整個壓縮後大概落在**幾百 KB 到低個位數 MB**，不是幾百 MB 或 GB
的等級。

對照現有規模：你現在 `raw_stock`（2 年、11+2 個真實指標）也才 5-6 千列，
整個 `warehouse.duckdb`（含 `raw_stock`/`silver_stock`/`dim_triggers`
三張表）估計也是幾 MB 等級，不是拿不動的檔案。你可以自己驗證（等
container 開著的時候）：

```bash
docker exec benny-data-pipeline-airflow-scheduler-1 ls -la /data/warehouse.duckdb
```

**結論**：儲存空間完全不是問題，10 年資料量對本機硬碟來說是雜訊等級的
大小，跟要不要上雲一點關係都沒有。之前討論「要不要上雲」的理由是**排程
可靠性**（筆電沒開機、Docker 沒啟動，每天固定時間的 DAG 就會漏跑），
不是儲存空間或運算量——這兩件事分開想，別把「資料量變大」跟「要不要
上雲」混在一起判斷。

### 追問三：舊 dashboard 的兩個問題——這是「重新設計現有 dashboard」，跟 Layer 3 是兩件不同的事，先確認範圍再排順序

這兩個問題講的是**現有** `benny-stock-dashboard` 的 `build_dashboard.py`
（美股/總經 + 台股兩份獨立 html），不是新開的 Layer 3 回測頁面本身——雖然
是在討論回測時想到的，但這其實是「既有 dashboard 的重新設計」，範圍上要
跟 Layer 3 分開排，原因見下方「待確認」。

**問題 1：trigger 轉捩點沒辦法跟其他 index/個股價格對照看**——要做到你要的
效果，具體改動：

- **美股+台股合併成一份 html**：目前 `build_dashboard.py` 靠 `MARKET`
  環境變數分流，只查+只嵌一個市場的資料，兩個 DAG（`us_market_daily_etl`/
  `tw_market_daily_etl`）各自 push 一份獨立檔案。改成單一 `dashboard.html`
  的話，`build_dashboard.py` 要拿掉 `MARKET` 這個篩選，改成**每次都查
  us+tw 兩邊全部資料**，兩個 DAG 一樣各自觸發 `update_dashboard`，但都是
  查全部資料、產生同一份檔案、push 同一個路徑——因為每次都是查當下 DB
  的完整狀態，兩個 DAG 誰先誰後跑完都會產生一樣的結果，不會互相覆蓋壞掉
  彼此的資料，這點不用擔心。
- **filter 改 multiple-select，指標/trigger 可以疊圖**：目前的
  `index-select` 是單選 `<select>`，要改成可以複選（checkbox 或
  multi-select widget），`renderLevelChart()` 也要重寫成能同時畫 N 條
  指標線 + N 組指標各自的 trigger 標記，不是現在的「一次只有一條線」。
  這是這次功能裡工程量最大的一塊，本質上是把現有的單指標檢視器換成
  「多指標比較檢視器」。

**問題 2：指標定義的說明文字**——加一個小型 glossary（VIX 是什麼、數值
高低代表什麼意思，8+3 個指標各寫 2-3 句）。這裡有一個**跟既有決定衝突
的地方要先跟你確認**：決定 12-C 當時明確拍板「dashboard 不 import
`benny-data-pipeline` 的 `constants/index_map.py`，因為 `index_name`
已經 denormalize 進每一列，不用跨 repo 依賴 Python 常數」——兩個 repo
是分開的 git repo，沒有共用套件機制，硬要 import 等於要嘛把
`benny-data-pipeline` 包成套件發布、要嘛用檔案路徑硬幹，都不乾淨。
**建議做法**：跟 `index_name` 一樣的邏輯，指標說明文字直接寫死一份小
dict 放在 `build_dashboard.py` 自己裡面（`benny-stock-dashboard` repo
自己維護一份，不依賴 `benny-data-pipeline`），這樣才符合現有的架構決定，
不會走回頭路。

**待確認**：這兩個問題要現在就做，還是排在 Layer 3 之後？我會建議
**先做完 Layer 3 的方案四（sanity check）再回頭做這個**，原因：Layer 3
第三份 html 本來就需要「trigger 疊價格線」這個能力（第 0 輪回覆的
「訊號疊在價格走勢圖上」那項），如果現有 dashboard 先重新設計出一套
「多指標疊圖」的元件，Layer 3 那份新頁面可以直接複用同一套畫圖邏輯，
不用兩份頁面各自寫一套疊圖機制——但如果你希望儘快先解決「看不懂舊圖」
的痛點，也可以反過來先做這個。你的優先序是什麼？ -> 照你說的 做完 layer 3 再說

### 追問四：圖表最終定案——照你的過濾標準砍到 4 項

你的標準是「策略跟大盤績效比 KPI 更好還更差」+「賺錢/drawdown 的主要
時間點在哪」，跟這兩件事無關的先剔除。照這個標準過濾原本建議的六項：

| 項目 | 去留 | 理由 |
|---|---|---|
| Equity Curve（策略 vs 基準） | ✅ 保留（你原本就要） | 直接回答「更好還更差」 |
| Underwater Chart | ✅ 保留（你原本就要） | 直接回答「drawdown 主要時間點在哪」 |
| 月度/年度報酬熱力圖 | ✅ 保留（你原本就要） | 賺錢的主要時間點在哪 |
| KPI 彙總表（含 Alpha/Beta） | ✅ 保留 | 直接就是「績效比 KPI」本身，Alpha/Beta 併進這張表，不獨立成一節 |
| 逐筆交易列表 | ✅ 保留 | 精確標出賺賠的時間點，比熱力圖更細顆粒度 |
| 訊號疊在價格走勢圖 | ✅ 保留 | 直接看到「賺錢/drawdown 當下發生了什麼」，跟追問三問題 1 是同一個技術需求，可以共用元件 |
| 交易次數/樣本數標示 | ❌ 砍掉 | 跟策略績效比較無直接關係，你已經在第 0 節看過樣本數表格，不用在每張圖旁邊重複提醒 |
| 部位曝險時間比例 | ❌ 砍掉 | 跟「績效比較」「賺賠時間點」都無直接關係，是輔助解讀用的統計，你要求剔除 |

**最終定案：6 項**（Equity Curve、Underwater Chart、月度/年度熱力圖、
KPI 彙總表、逐筆交易列表、訊號疊價格圖），砍掉樣本數標示跟曝險比例。
