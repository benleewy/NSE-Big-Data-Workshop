-- ============================================================
-- NSE Big Data Workshop — Pig Latin Scripts (Task 4)
-- ============================================================
-- Run these in the Grunt shell on the EMR Master node
-- Start Pig by typing: pig
-- ============================================================

-- ============================================================
-- Load data with schema
-- ============================================================
nse_data = LOAD '/home/hadoop/NSEraw' USING PigStorage(',') 
  AS (transid:chararray, ticker:chararray, datetrans:chararray, 
      timetrans:chararray, openprice:double, highprice:double, 
      lowprice:double, closeprice:double, volume:double);


-- ============================================================
-- Q1. View 10 lines of data
-- ============================================================
sample_data = LIMIT nse_data 10;
DUMP sample_data;


-- ============================================================
-- Q2. Top 10 highest priced stocks
-- ============================================================
grouped_by_ticker = GROUP nse_data BY ticker;
max_prices = FOREACH grouped_by_ticker GENERATE 
  group AS ticker, 
  MAX(nse_data.closeprice) AS max_close_price;

-- Top 10 highest
sorted_desc = ORDER max_prices BY max_close_price DESC;
top10_highest = LIMIT sorted_desc 10;
DUMP top10_highest;

-- Bottom 10 lowest
sorted_asc = ORDER max_prices BY max_close_price ASC;
bottom10_lowest = LIMIT sorted_asc 10;
DUMP bottom10_lowest;


-- ============================================================
-- Q3. Preview 10 lines of TCS stock
-- ============================================================
tcs_data = FILTER nse_data BY ticker == 'TCS';
tcs_sample = LIMIT tcs_data 10;
DUMP tcs_sample;


-- ============================================================
-- Q4. Average volume per minute for TCS
-- ============================================================
tcs_grouped = GROUP tcs_data ALL;
tcs_avg_vol = FOREACH tcs_grouped GENERATE 
  'TCS' AS ticker,
  AVG(tcs_data.volume) AS avg_volume_per_min;
DUMP tcs_avg_vol;


-- ============================================================
-- Q5. Highest and lowest volume stocks per minute
-- ============================================================
vol_by_ticker = FOREACH grouped_by_ticker GENERATE 
  group AS ticker, 
  AVG(nse_data.volume) AS avg_volume_per_min;

-- Top 10 highest volume
vol_sorted_desc = ORDER vol_by_ticker BY avg_volume_per_min DESC;
top10_volume = LIMIT vol_sorted_desc 10;
DUMP top10_volume;

-- Bottom 10 lowest volume
vol_sorted_asc = ORDER vol_by_ticker BY avg_volume_per_min ASC;
bottom10_volume = LIMIT vol_sorted_asc 10;
DUMP bottom10_volume;


-- ============================================================
-- Q6. Average liquidity per minute (close price * avg volume)
-- ============================================================
liquidity_by_ticker = FOREACH grouped_by_ticker GENERATE 
  group AS ticker,
  AVG(nse_data.volume) AS avg_volume,
  AVG(nse_data.closeprice) AS avg_close,
  AVG(nse_data.closeprice) * AVG(nse_data.volume) AS avg_liquidity;

-- Sort by liquidity descending
liq_sorted_desc = ORDER liquidity_by_ticker BY avg_liquidity DESC;

-- Top 10 most liquid
top10_liquidity = LIMIT liq_sorted_desc 10;
DUMP top10_liquidity;

-- Bottom 10 least liquid
liq_sorted_asc = ORDER liquidity_by_ticker BY avg_liquidity ASC;
bottom10_liquidity = LIMIT liq_sorted_asc 10;
DUMP bottom10_liquidity;

-- Save full liquidity results to HDFS
STORE liq_sorted_desc INTO '/home/hadoop/pig_liquidity_results' 
  USING PigStorage(',');


-- ============================================================
-- Q7. Top 10 highest liquidity stocks (same as top10 above)
-- ============================================================
DUMP top10_liquidity;
