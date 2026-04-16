-- ============================================================
-- NSE Big Data Workshop — Hive Queries (Tasks 5-6)
-- ============================================================
-- Run these in the Hive editor in Hue
-- Replace <table_name> with your actual table name from Glue
-- Replace <STOCK1>, <STOCK2>, <STOCK3> with your top 3 tickers
-- Replace <STOCK> with your chosen single stock for MA analysis
-- ============================================================

-- ============================================================
-- Task 5: Compare execution engines
-- ============================================================

-- Switch to MapReduce (slower, but shows detailed job stats)
SET hive.execution.engine=mr;
SELECT ticker, COUNT(*) AS cnt 
FROM nsedatabase.<table_name> 
GROUP BY ticker 
ORDER BY cnt DESC 
LIMIT 10;

-- Switch to Tez (faster, default)
SET hive.execution.engine=tez;
SELECT ticker, COUNT(*) AS cnt 
FROM nsedatabase.<table_name> 
GROUP BY ticker 
ORDER BY cnt DESC 
LIMIT 10;


-- ============================================================
-- Task 6, Q1: Top 10 stocks by daily trading range
-- ============================================================
SELECT 
  ticker,
  datetrans,
  MAX(highprice) - MIN(lowprice) AS daily_range,
  AVG(closeprice) AS avg_close,
  AVG(volume) AS avg_volume,
  AVG(closeprice) * AVG(volume) AS avg_liquidity
FROM nsedatabase.<table_name>
GROUP BY ticker, datetrans
ORDER BY daily_range DESC
LIMIT 10;


-- ============================================================
-- Task 6, Q2: Trading date range for top 3 stocks
-- ============================================================
-- Replace <STOCK1>, <STOCK2>, <STOCK3> with tickers from Q1
SELECT 
  ticker,
  MIN(datetrans) AS first_trade_date,
  MAX(datetrans) AS last_trade_date
FROM nsedatabase.<table_name>
WHERE ticker IN ('<STOCK1>', '<STOCK2>', '<STOCK3>')
GROUP BY ticker;


-- ============================================================
-- Task 6, Q3: Absolute price increase (start to end)
-- ============================================================
WITH first_last AS (
  SELECT 
    ticker,
    MIN(datetrans) AS first_date,
    MAX(datetrans) AS last_date
  FROM nsedatabase.<table_name>
  WHERE ticker IN ('<STOCK1>', '<STOCK2>', '<STOCK3>')
  GROUP BY ticker
),
first_price AS (
  SELECT t.ticker, AVG(t.closeprice) AS start_price
  FROM nsedatabase.<table_name> t
  JOIN first_last fl ON t.ticker = fl.ticker AND t.datetrans = fl.first_date
  GROUP BY t.ticker
),
last_price AS (
  SELECT t.ticker, AVG(t.closeprice) AS end_price
  FROM nsedatabase.<table_name> t
  JOIN first_last fl ON t.ticker = fl.ticker AND t.datetrans = fl.last_date
  GROUP BY t.ticker
)
SELECT 
  fp.ticker,
  fp.start_price,
  lp.end_price,
  lp.end_price - fp.start_price AS absolute_increase
FROM first_price fp
JOIN last_price lp ON fp.ticker = lp.ticker
ORDER BY absolute_increase DESC;


-- ============================================================
-- Task 6, Q4: Percentage price increase
-- ============================================================
WITH first_last AS (
  SELECT ticker, MIN(datetrans) AS first_date, MAX(datetrans) AS last_date
  FROM nsedatabase.<table_name>
  WHERE ticker IN ('<STOCK1>', '<STOCK2>', '<STOCK3>')
  GROUP BY ticker
),
first_price AS (
  SELECT t.ticker, AVG(t.closeprice) AS start_price
  FROM nsedatabase.<table_name> t
  JOIN first_last fl ON t.ticker = fl.ticker AND t.datetrans = fl.first_date
  GROUP BY t.ticker
),
last_price AS (
  SELECT t.ticker, AVG(t.closeprice) AS end_price
  FROM nsedatabase.<table_name> t
  JOIN first_last fl ON t.ticker = fl.ticker AND t.datetrans = fl.last_date
  GROUP BY t.ticker
)
SELECT 
  fp.ticker,
  fp.start_price,
  lp.end_price,
  ((lp.end_price - fp.start_price) / fp.start_price) * 100 AS pct_increase
FROM first_price fp
JOIN last_price lp ON fp.ticker = lp.ticker
ORDER BY pct_increase DESC;


-- ============================================================
-- Task 6, Q5: Absolute highest daily TR for top 3 stocks
-- ============================================================
SELECT 
  ticker,
  datetrans,
  MAX(highprice) - MIN(lowprice) AS daily_tr,
  AVG(volume) AS avg_volume,
  AVG(closeprice) AS avg_close,
  AVG(closeprice) * AVG(volume) AS avg_liquidity_per_min,
  AVG(closeprice) * AVG(volume) * 10 AS ten_min_liquidity
FROM nsedatabase.<table_name>
WHERE ticker IN ('<STOCK1>', '<STOCK2>', '<STOCK3>')
GROUP BY ticker, datetrans
ORDER BY daily_tr DESC
LIMIT 10;


-- ============================================================
-- Task 6, Q5a: Highest daily TR as percentage
-- ============================================================
SELECT 
  ticker,
  datetrans,
  MAX(highprice) - MIN(lowprice) AS daily_tr,
  ((MAX(highprice) - MIN(lowprice)) / AVG(closeprice)) * 100 AS daily_tr_pct
FROM nsedatabase.<table_name>
WHERE ticker IN ('<STOCK1>', '<STOCK2>', '<STOCK3>')
GROUP BY ticker, datetrans
ORDER BY daily_tr_pct DESC
LIMIT 10;


-- ============================================================
-- Task 6, Q5b: Save top 10 TR records to HDFS
-- ============================================================
INSERT OVERWRITE DIRECTORY '/home/hadoop/hive_top10_tr'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT 
  ticker, datetrans,
  MAX(highprice) - MIN(lowprice) AS daily_tr,
  AVG(volume) AS avg_volume,
  AVG(closeprice) AS avg_close
FROM nsedatabase.<table_name>
WHERE ticker IN ('<STOCK1>', '<STOCK2>', '<STOCK3>')
GROUP BY ticker, datetrans
ORDER BY daily_tr DESC
LIMIT 10;


-- ============================================================
-- Task 6, Q6: Stocks with highest % price increase (full period)
-- ============================================================
WITH date_range_prices AS (
  SELECT ticker, MIN(datetrans) AS first_date, MAX(datetrans) AS last_date
  FROM nsedatabase.<table_name>
  WHERE datetrans >= '20141218' AND datetrans <= '20151001'
  GROUP BY ticker
),
start_prices AS (
  SELECT t.ticker, AVG(t.closeprice) AS start_price
  FROM nsedatabase.<table_name> t
  JOIN date_range_prices dr ON t.ticker = dr.ticker AND t.datetrans = dr.first_date
  GROUP BY t.ticker
),
end_prices AS (
  SELECT t.ticker, AVG(t.closeprice) AS end_price
  FROM nsedatabase.<table_name> t
  JOIN date_range_prices dr ON t.ticker = dr.ticker AND t.datetrans = dr.last_date
  GROUP BY t.ticker
)
SELECT 
  sp.ticker,
  sp.start_price,
  ep.end_price,
  ((ep.end_price - sp.start_price) / sp.start_price) * 100 AS pct_increase
FROM start_prices sp
JOIN end_prices ep ON sp.ticker = ep.ticker
WHERE sp.start_price > 0
ORDER BY pct_increase DESC
LIMIT 20;


-- ============================================================
-- Task 6, Q7: 10-Day Moving Average of closing price
-- ============================================================
-- Replace <STOCK> with your chosen stock ticker
SELECT 
  ticker,
  datetrans,
  daily_avg_close,
  daily_avg_volume,
  AVG(daily_avg_close) OVER (
    PARTITION BY ticker 
    ORDER BY datetrans 
    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
  ) AS ma_10day_close
FROM (
  SELECT 
    ticker, datetrans,
    AVG(closeprice) AS daily_avg_close,
    AVG(volume) AS daily_avg_volume
  FROM nsedatabase.<table_name>
  WHERE ticker = '<STOCK>'
  GROUP BY ticker, datetrans
) daily_data
ORDER BY datetrans
LIMIT 2000;

-- Save to HDFS
INSERT OVERWRITE DIRECTORY '/home/hadoop/hive_ma10day'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT 
  ticker, datetrans, daily_avg_close, daily_avg_volume,
  AVG(daily_avg_close) OVER (
    PARTITION BY ticker ORDER BY datetrans 
    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
  ) AS ma_10day_close
FROM (
  SELECT ticker, datetrans, AVG(closeprice) AS daily_avg_close, AVG(volume) AS daily_avg_volume
  FROM nsedatabase.<table_name>
  WHERE ticker = '<STOCK>'
  GROUP BY ticker, datetrans
) daily_data
ORDER BY datetrans
LIMIT 2000;


-- ============================================================
-- Task 6, Q8: 30-Day Moving Average
-- ============================================================
SELECT 
  ticker,
  datetrans,
  daily_avg_close,
  daily_avg_volume,
  AVG(daily_avg_close) OVER (
    PARTITION BY ticker 
    ORDER BY datetrans 
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS ma_30day_close
FROM (
  SELECT 
    ticker, datetrans,
    AVG(closeprice) AS daily_avg_close,
    AVG(volume) AS daily_avg_volume
  FROM nsedatabase.<table_name>
  WHERE ticker = '<STOCK>'
  GROUP BY ticker, datetrans
) daily_data
ORDER BY datetrans
LIMIT 2000;

-- Save to HDFS
INSERT OVERWRITE DIRECTORY '/home/hadoop/hive_ma30day'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT 
  ticker, datetrans, daily_avg_close, daily_avg_volume,
  AVG(daily_avg_close) OVER (
    PARTITION BY ticker ORDER BY datetrans 
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS ma_30day_close
FROM (
  SELECT ticker, datetrans, AVG(closeprice) AS daily_avg_close, AVG(volume) AS daily_avg_volume
  FROM nsedatabase.<table_name>
  WHERE ticker = '<STOCK>'
  GROUP BY ticker, datetrans
) daily_data
ORDER BY datetrans
LIMIT 2000;
