-- ============================================================
-- NSE Big Data Workshop — Athena Queries (Task 2)
-- ============================================================
-- Replace <table_name> with your actual Glue table name
-- (check in Glue console under nsedatabase)
-- ============================================================

-- ============================================================
-- Q1. Preview the data
-- ============================================================
SELECT * FROM nsedatabase.<table_name> LIMIT 20;


-- ============================================================
-- Q2. Trading date range for a specific stock
-- ============================================================
SELECT 
  ticker,
  MIN(datetrans) AS first_trade_date,
  MAX(datetrans) AS last_trade_date,
  COUNT(*) AS total_records
FROM nsedatabase.<table_name>
WHERE ticker = 'TCS'
GROUP BY ticker;


-- ============================================================
-- Q3. First day prices and timestamps
-- ============================================================
SELECT 
  ticker, datetrans, timetrans, 
  openprice, highprice, lowprice, closeprice, volume
FROM nsedatabase.<table_name>
WHERE ticker = 'TCS'
  AND datetrans = (
    SELECT MIN(datetrans) 
    FROM nsedatabase.<table_name> 
    WHERE ticker = 'TCS'
  )
ORDER BY timetrans;


-- ============================================================
-- Q4. Average volume per minute
-- ============================================================
SELECT 
  ticker, 
  AVG(volume) AS avg_volume_per_min,
  MIN(volume) AS min_volume,
  MAX(volume) AS max_volume
FROM nsedatabase.<table_name>
WHERE ticker = 'TCS'
GROUP BY ticker;


-- ============================================================
-- Q5a. Daily ATR (Average True Range)
-- ============================================================
SELECT 
  ticker, 
  datetrans,
  MAX(highprice) AS day_high,
  MIN(lowprice) AS day_low,
  MAX(highprice) - MIN(lowprice) AS daily_atr
FROM nsedatabase.<table_name>
WHERE ticker = 'TCS'
GROUP BY ticker, datetrans
ORDER BY datetrans;


-- ============================================================
-- Q5b. Monthly ATR
-- ============================================================
SELECT 
  ticker,
  SUBSTR(datetrans, 1, 6) AS month,
  MAX(highprice) - MIN(lowprice) AS monthly_atr,
  AVG(closeprice) AS avg_close
FROM nsedatabase.<table_name>
WHERE ticker = 'TCS' AND datetrans LIKE '201507%'
GROUP BY ticker, SUBSTR(datetrans, 1, 6);


-- ============================================================
-- Q6. 10-Minute Moving Average (price and volume)
-- ============================================================
SELECT 
  ticker, datetrans, timetrans, closeprice, volume,
  AVG(closeprice) OVER (
    PARTITION BY ticker, datetrans 
    ORDER BY timetrans 
    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
  ) AS ma_10min_price,
  AVG(volume) OVER (
    PARTITION BY ticker, datetrans 
    ORDER BY timetrans 
    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
  ) AS ma_10min_volume
FROM nsedatabase.<table_name>
WHERE ticker = 'TCS'
ORDER BY datetrans, timetrans
LIMIT 200;


-- ============================================================
-- Q8a. Entry points (MA < Actual Price = buy signal)
-- ============================================================
WITH ma_data AS (
  SELECT 
    ticker, datetrans, timetrans, closeprice, volume,
    AVG(closeprice) OVER (
      PARTITION BY ticker, datetrans 
      ORDER BY timetrans 
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS ma_10min
  FROM nsedatabase.<table_name>
  WHERE ticker = 'TCS'
)
SELECT 
  ticker, datetrans, timetrans, closeprice, ma_10min,
  closeprice - ma_10min AS price_above_ma
FROM ma_data
WHERE ma_10min < closeprice
ORDER BY datetrans, timetrans
LIMIT 50;


-- ============================================================
-- Q8b. Exit points (MA > Actual Price = sell signal)
-- ============================================================
WITH ma_data AS (
  SELECT 
    ticker, datetrans, timetrans, closeprice, volume,
    AVG(closeprice) OVER (
      PARTITION BY ticker, datetrans 
      ORDER BY timetrans 
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS ma_10min
  FROM nsedatabase.<table_name>
  WHERE ticker = 'TCS'
)
SELECT 
  ticker, datetrans, timetrans, closeprice, ma_10min,
  ma_10min - closeprice AS ma_above_price
FROM ma_data
WHERE ma_10min > closeprice
ORDER BY datetrans, timetrans
LIMIT 50;
