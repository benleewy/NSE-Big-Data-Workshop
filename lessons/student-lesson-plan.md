# Stock Market Analysis using the AWS Big Data Platform

## Student Lesson Plan

> **Disclaimer:** This course and its materials are for educational purposes only. AWS, its affiliates, and trainers delivering this course are in no way responsible for any losses, negative results, or consequences, direct or indirect, arising from the usage of the material and information within this course. This is not financial advice.

---

## Use Case Scenario

**Empowerment Analytics Consultancy** has a Big Data team doing analytics for the banking/finance sector.

You've been tasked to analyze stock market data in depth, visualize it, and find actionable insights for the brokerage team to execute trades for high net worth clients — targeting over 20% yearly profit.

The data sourcing team has pulled **per-minute data** from the **National Stock Exchange of India (NSE)** for ~10 months in 2015. The brokerage team needs feedback in **1 day** for newly acquired clients eager to invest.

**The challenge:** Per-minute stock data for a large market = several gigabytes. Traditional BI tools (Tableau, Qlikview) with RDBMS storage could crash or take too long. As a Big Data expert, you'll use AWS services to handle this at scale.

**Your mission:**
1. Preview/pre-analyze the data before importing into AWS
2. Import data into AWS safely and redundantly
3. Perform ETL for deeper analysis
4. Use EMR for deep analytics with actionable insights
5. Display results in a BI tool for client presentations

---

## Solution Architecture

Refer to the architecture diagrams for a visual overview of what you'll be building:

**End-to-end data flow:**

![Solution Architecture](../diagrams/generated-diagrams/solution-architecture.png)

**VPC and network layout:**

![Network Architecture](../diagrams/generated-diagrams/network-architecture.png)

---

## Workshop Overview

| Task | Title | What You'll Do | Duration |
|------|-------|---------------|----------|
| Intro | Architecture Design | Design a Big Data solution on AWS | ~2 hrs |
| 1 | Data Generation | Sync NSE data from S3 to EC2 to your S3 bucket | ~30 min |
| 2 | Preliminary Analysis | Query data with Athena + Glue schema | ~1 hr |
| 3 | EMR HDFS Import | Launch EMR, import data, set up Hue | ~1 hr |
| 4 | Pig Analysis | Analyze stocks with Pig Latin on EMR | ~1 hr |
| 5 | Hive Configuration | Configure Hive with Glue metastore | ~15 min |
| 6 | Deep Hive Analytics | Trading ranges, moving averages, visualization | ~2 hrs |

---

## NSE Data Schema

Your data has 9 columns per row, representing per-minute stock ticks:

| Column | Name | Type | Example |
|--------|------|------|---------|
| Col0 | `transid` | string | `1` |
| Col1 | `ticker` | string | `TCS` |
| Col2 | `datetrans` | string | `20150703` |
| Col3 | `timetrans` | string | `09:16:00` |
| Col4 | `openprice` | double | `2545.0` |
| Col5 | `highprice` | double | `2548.5` |
| Col6 | `lowprice` | double | `2540.0` |
| Col7 | `closeprice` | double | `2547.0` |
| Col8 | `volume` | double | `15234.0` |

---

## Before You Begin — Stack Outputs Reference

After deploying the CloudFormation stack, go to **CloudFormation -> Your Stack -> Outputs** tab. Note down these values — you'll use them throughout:

| Output Key | What It Is | Where You'll Use It |
|-----------|-----------|-------------------|
| `BastionHostPublicIP` | Bastion Host IP | SSH access |
| `S3BucketName` | Your data bucket | S3 commands, Glue, Athena |
| `S3DataPath` | Full S3 path to NSE data | Glue crawler, Athena queries |
| `GlueDatabaseName` | `nsedatabase` | Athena & Hive queries |
| `GlueCrawlerName` | Crawler name | Running the crawler |
| `AthenaWorkgroupName` | Athena workgroup | Running Athena queries |
| `EMRServiceRoleArn` | EMR service role | Creating EMR cluster |
| `EMREC2InstanceProfileArn` | EMR EC2 profile | Creating EMR cluster |
| `EMRMasterSecurityGroupId` | Master node SG | EMR cluster config |
| `EMRSlaveSecurityGroupId` | Core node SG | EMR cluster config |
| `KeyPairName` | SSH key pair name | SSH into instances |

### Retrieving Your SSH Private Key

The CloudFormation stack created a key pair. Retrieve the private key:

```bash
# Get the Key Pair ID from the Outputs tab, then run:
aws ssm get-parameter \
  --name /ec2/keypair/<YOUR-KEYPAIR-ID> \
  --with-decryption \
  --query Parameter.Value \
  --output text > workshop-keypair.pem

chmod 400 workshop-keypair.pem
```

---

## Phase 1 — Introductory Questions (2 hours)

### Design Challenge 1 (1.5 hours)

Think about a full-scale Big Data solution on AWS for this use case. Break into groups of 3-4 and:

1. Draw out the architecture diagram on a whiteboard
2. Label what each AWS service/phase will do
3. Consider: data ingestion, storage, ETL, analytics, visualization
4. Present your design to the class

**Guiding questions:**
- How will you ingest per-minute stock data at scale?
- Where will you store raw vs. processed data?
- What tools will you use for ETL? For deep analytics?
- How will you visualize results for non-technical clients?

### Design Challenge 2 (0.5 hours)

Come up with an **alternative** architecture that could also solve this use case. Consider different AWS services or approaches.

### Build Phase

Based on your proposed architecture, construct the Big Data ecosystem on AWS for Phase 1. The CloudFormation stack has already set up the foundation (VPC, S3, Glue, IAM). Now you'll build on top of it.

---

## Task 1 of 6 — Data Generation (EC2 to S3)

> **Goal:** Get the NSE stock data into your own S3 bucket, simulating a data generation pipeline.

> **Good news:** The CloudFormation stack has already synced the NSE data into your S3 bucket automatically! You can verify this and also practice the manual process.

### 1.1 — SSH into the Bastion Host

```bash
ssh -i workshop-keypair.pem ec2-user@<BastionHostPublicIP>
```

### 1.2 — Verify the Data is Already There

The stack's UserData script already copied the NSE data. Verify:

```bash
# Check local copy on the instance
ls -la ~/nse-data/

# Check your S3 bucket
aws s3 ls s3://<S3BucketName>/NSE/
```

You should see 5 CSV files (`nseComp.1.csv` through `nseComp.5.csv`), each ~380MB.

### 1.3 — (Optional) Practice Manual S3 Sync

If you want to practice the S3 sync workflow manually:

```bash
# Create a working directory
mkdir my_EC2_instance_copy_directory

# Sync from the source bucket to your EC2 instance
aws s3 sync s3://dbsbdclassnv/indiastockex/raw/ my_EC2_instance_copy_directory

# Sync from EC2 to your own S3 bucket
aws s3 sync my_EC2_instance_copy_directory s3://<S3BucketName>/NSE
```

### 1.4 — Preview the Raw Data

Take a quick look at the data structure:

```bash
head -20 ~/nse-data/nseComp.1.csv
```

**Q: What columns do you see? Can you identify the schema before we set it up in Glue?**

---

## Task 2 of 6 — Preliminary Analysis using Athena

> **Goal:** Use AWS Glue to catalog the data schema, then query it with Athena for quick insights.

### 2.1 — Verify the Glue Crawler Ran

The CloudFormation stack created a Glue crawler and triggered it automatically. Check its status:

1. Navigate to **AWS Glue** console
2. Click **Crawlers** in the left panel
3. Find the crawler (named `<StackName>-NSECrawler`)
4. Status should be **Ready** (if still running, wait for it to complete)
5. Click **Tables** under **Databases** — you should see a table under `nsedatabase`

### 2.2 — Set the Schema Column Names

The crawler auto-detected columns as `col0`, `col1`, etc. Let's rename them:

1. In **AWS Glue** -> **Tables** -> click on the table name under `nsedatabase`
2. Click **Edit schema** (top right)
3. Rename the columns:

| Original | New Name | Data Type |
|----------|----------|-----------|
| col0 | transid | string |
| col1 | ticker | string |
| col2 | datetrans | string |
| col3 | timetrans | string |
| col4 | openprice | double |
| col5 | highprice | double |
| col6 | lowprice | double |
| col7 | closeprice | double |
| col8 | volume | double |

4. Click **Save**

### 2.3 — Open Athena and Start Querying

1. Navigate to **Athena** console
2. Select the workgroup: `<StackName>-Workgroup` (from Outputs)
3. Select database: `nsedatabase` on the left panel
4. Click the three-dot icon next to the table name -> **Preview table**

### 2.4 — Questions to Answer with Athena

Work through these queries. See `sample-queries/athena-queries.sql` for reference solutions.

**Q1. View a sample of the data:**
```sql
SELECT * FROM nsedatabase.<your_table_name> LIMIT 20;
```

**Q2. Get the trading date range for a stock of your choice (e.g., TCS — Tata Consultancy Services):**
```sql
SELECT 
  ticker,
  MIN(datetrans) AS first_trade_date,
  MAX(datetrans) AS last_trade_date
FROM nsedatabase.<your_table_name>
WHERE ticker = 'TCS'
GROUP BY ticker;
```

**Q3. Find closing, opening, high/low prices and timestamp for the first day of your stock:**
```sql
SELECT ticker, datetrans, timetrans, openprice, highprice, lowprice, closeprice, volume
FROM nsedatabase.<your_table_name>
WHERE ticker = 'TCS'
  AND datetrans = (
    SELECT MIN(datetrans) FROM nsedatabase.<your_table_name> WHERE ticker = 'TCS'
  )
ORDER BY timetrans;
```

**Q4. Find average volume per 1-minute window for your stock:**
```sql
SELECT ticker, AVG(volume) AS avg_volume_per_min
FROM nsedatabase.<your_table_name>
WHERE ticker = 'TCS'
GROUP BY ticker;
```

**Q5. Find the ATR (Average True Range) for the first day:**

ATR = Max price of stock for that day minus the minimum price for the same day.

```sql
-- Daily ATR
SELECT 
  ticker, 
  datetrans,
  MAX(highprice) - MIN(lowprice) AS daily_atr
FROM nsedatabase.<your_table_name>
WHERE ticker = 'TCS'
GROUP BY ticker, datetrans
ORDER BY datetrans
LIMIT 10;

-- Monthly ATR (pick any month in the trading range)
SELECT 
  ticker,
  SUBSTR(datetrans, 1, 6) AS month,
  MAX(highprice) - MIN(lowprice) AS monthly_atr
FROM nsedatabase.<your_table_name>
WHERE ticker = 'TCS' AND datetrans LIKE '201507%'
GROUP BY ticker, SUBSTR(datetrans, 1, 6);
```

**Q6. 10-minute Moving Average of price and volume:**
```sql
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
FROM nsedatabase.<your_table_name>
WHERE ticker = 'TCS'
ORDER BY datetrans, timetrans
LIMIT 100;
```

**Q7. Visualize in QuickSight:**
- Save the Q6 query results
- Open **Amazon QuickSight** -> Create a new analysis
- Use the Athena results or create a manifest file pointing to your data
- Plot actual price vs. 10-min moving average over time

**Q8. Find entry & exit points for intraday trading:**

Entry point = when Moving Average < Actual Price (buy signal)
Exit point = when Moving Average > Actual Price (sell signal)

```sql
-- Entry points (MA crosses below actual price)
WITH ma_data AS (
  SELECT 
    ticker, datetrans, timetrans, closeprice, volume,
    AVG(closeprice) OVER (
      PARTITION BY ticker, datetrans 
      ORDER BY timetrans 
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS ma_10min
  FROM nsedatabase.<your_table_name>
  WHERE ticker = 'TCS'
)
SELECT * FROM ma_data
WHERE ma_10min < closeprice
ORDER BY datetrans, timetrans
LIMIT 50;
```

**Q9. Visualize entry/exit points in QuickSight** — overlay the MA line on the actual price chart and mark crossover points.

---

## Task 3 of 6 — Importing Data into EMR HDFS

> **Goal:** Launch an EMR cluster, import NSE data into HDFS, and set up Hue for interactive analysis.

### 3.1 — Think First (20-30 minutes)

Before creating the cluster, think about and write down:
- What applications/frameworks do we need installed on EMR?
- How will we use Hive metadata from Glue inside EMR?
- How will we import S3 data into HDFS?
- What visualization tool runs on EMR?

### 3.2 — Create the EMR Cluster

1. Navigate to **EMR** console -> **Create Cluster** -> **Go to advanced options**
2. **Software Configuration:**
   - Latest EMR release (default)
   - Check: **Hadoop, Hive, Pig, Hue, Tez, Sqoop, Zookeeper**
   - Under **AWS Glue Data Catalog settings**, check **"Use for Hive table metadata"**
3. Click **Next**
4. **Hardware Configuration:**
   - Network: Select your workshop VPC (from stack Outputs: `VPCId`)
   - EC2 Subnet: Select the **Public Subnet** (from Outputs: `PublicSubnetId`)
   - Root device EBS volume size: **20 GiB**
   - Leave instance types as default (m5.xlarge recommended)
5. Click **Next**
6. **General Options:**
   - Cluster name: `BigDataWorkshop-<YourInitials>`
   - Under Additional Options, check **"EMRFS consistent view"** (ensures S3 file integrity)
7. Click **Next**
8. **Security Options:**
   - EC2 key pair: Select `<StackName>-keypair` (from Outputs: `KeyPairName`)
   - Under Permissions, select **Custom**:
     - EMR role: Use the role from Outputs (`EMRServiceRoleArn`)
     - EC2 instance profile: Use the profile from Outputs (`EMREC2InstanceProfileArn`)
   - Under EC2 security groups:
     - Master: Select the EMR Master SG (from Outputs: `EMRMasterSecurityGroupId`)
     - Core & Task: Select the EMR Slave SG (from Outputs: `EMRSlaveSecurityGroupId`)
9. Click **Create cluster**
10. Wait for status to change from **Starting** to **Waiting** (~10-15 minutes)

### 3.3 — SSH into the EMR Master Node

1. In the EMR console, click on your cluster -> note the **Master public DNS**
2. From your Bastion Host (already SSH'd in):

```bash
# Transfer the key pair to the Bastion Host first (from your laptop):
scp -i workshop-keypair.pem workshop-keypair.pem ec2-user@<BastionHostPublicIP>:~/

# Then from the Bastion Host, SSH into the EMR Master:
ssh -i ~/workshop-keypair.pem hadoop@<EMR-Master-Public-DNS>
```

3. Configure AWS CLI on the Master node:
```bash
aws configure
# Enter your Access Key ID and Secret Access Key
# Default region: <your-region-code> (e.g., us-east-1)
# Default output format: (just press Enter)
```

### 3.4 — Import Data into HDFS

```bash
# Import NSE data from your S3 bucket into HDFS
s3-dist-cp --src s3://<S3BucketName>/NSE --dest /home/hadoop/NSEraw

# Verify the files are copied
hadoop fs -ls /home/hadoop/NSEraw
```

You should see all 5 `nseComp.*.csv` files in HDFS.

### 3.5 — Set Up Hue Web Interface

To access Hue (EMR's web UI), you need to set up an SSH tunnel with SOCKS proxy:

**Step 1: Create SSH tunnel** (run this from your laptop, not the Bastion):
```bash
ssh -i workshop-keypair.pem -N -D 8157 hadoop@<EMR-Master-Public-DNS>
```

**Step 2: Configure FoxyProxy in your browser:**
- Install the FoxyProxy extension for Chrome or Firefox
- Import the config from `config/foxyproxy-emr.xml` in this repo
- Or manually configure: SOCKS5 proxy -> `localhost:8157`
- Set URL patterns to route `*ec2*.amazonaws.com*` and `*ec2*.compute*` through the proxy

**Step 3: Open Hue:**
- In your browser (with FoxyProxy active), navigate to: `http://<EMR-Master-Public-DNS>:8888`
- Create a super-user account: `admin` / `Admin12345!` (or your own credentials)

### 3.6 — Verify in Hue

1. In Hue, navigate to the **File Browser** -> browse to `/home/hadoop/NSEraw`
2. Verify all 5 CSV files are visible
3. Try running an Athena query in the **Hive editor** (remember to remove double quotes around database/table names in Hive)

### 3.7 — Compare Execution Engines

Try running the same query with different execution engines:

```sql
-- Switch to MapReduce
SET hive.execution.engine=mr;
SELECT ticker, COUNT(*) FROM nsedatabase.nse GROUP BY ticker LIMIT 10;

-- Switch back to Tez (faster)
SET hive.execution.engine=tez;
SELECT ticker, COUNT(*) FROM nsedatabase.nse GROUP BY ticker LIMIT 10;
```

Check the **Resource Manager** at `http://<EMR-Master-Public-DNS>:8088` to compare:
- Number of mappers and reducers
- Memory used
- Execution time
- Where the Application Master is located

### 3.8 — Think About Liquidity

From your Athena analysis (Task 2, Q8-Q9), consider:
- Even if it's the right time to enter/exit a trade, is there enough **liquidity**?
- Can you buy/sell at your target price, or will there be **slippage**?
- Slippage = difference between actual transacted price vs. your target buy/sell price

---

## Task 4 of 6 — Pig Analysis

> **Goal:** Use Pig Latin on EMR to perform data analysis directly on HDFS data.

### 4.1 — Launch Pig

SSH into the EMR Master node and start Pig:
```bash
pig
```

You're now in the **Grunt shell**. See `sample-queries/pig-scripts.pig` for reference solutions.

### 4.2 — Questions to Answer with Pig

**Q1. View 10 lines of data — load with correct schema:**
```pig
-- Load data with schema
nse_data = LOAD '/home/hadoop/NSEraw' USING PigStorage(',') 
  AS (transid:chararray, ticker:chararray, datetrans:chararray, 
      timetrans:chararray, openprice:double, highprice:double, 
      lowprice:double, closeprice:double, volume:double);

-- Sample 10 records
sample_data = LIMIT nse_data 10;
DUMP sample_data;
```

**Q2. Top 10 highest priced stocks and bottom 10 lowest priced:**
```pig
-- Group by ticker, get max close price
grouped = GROUP nse_data BY ticker;
avg_prices = FOREACH grouped GENERATE 
  group AS ticker, 
  MAX(nse_data.closeprice) AS max_price;

-- Top 10 highest
sorted_desc = ORDER avg_prices BY max_price DESC;
top10 = LIMIT sorted_desc 10;
DUMP top10;

-- Bottom 10 lowest
sorted_asc = ORDER avg_prices BY max_price ASC;
bottom10 = LIMIT sorted_asc 10;
DUMP bottom10;
```

**Q3. Preview 10 lines of a specific stock (e.g., TCS):**
```pig
tcs_data = FILTER nse_data BY ticker == 'TCS';
tcs_sample = LIMIT tcs_data 10;
DUMP tcs_sample;
```

**Q4. Average volume per minute for your stock:**
```pig
tcs_data = FILTER nse_data BY ticker == 'TCS';
tcs_grouped = GROUP tcs_data ALL;
avg_vol = FOREACH tcs_grouped GENERATE AVG(tcs_data.volume) AS avg_volume_per_min;
DUMP avg_vol;
```

**Q5. Highest and lowest volume stocks per minute (top 10 and bottom 10):**
```pig
grouped = GROUP nse_data BY ticker;
vol_stats = FOREACH grouped GENERATE 
  group AS ticker, 
  AVG(nse_data.volume) AS avg_volume;

-- Top 10 highest volume
sorted_vol_desc = ORDER vol_stats BY avg_volume DESC;
top10_vol = LIMIT sorted_vol_desc 10;
DUMP top10_vol;

-- Bottom 10 lowest volume
sorted_vol_asc = ORDER vol_stats BY avg_volume ASC;
bottom10_vol = LIMIT sorted_vol_asc 10;
DUMP bottom10_vol;
```

**Q6. Average liquidity per minute (close price x avg volume):**

Liquidity = stock monetary value transacted per time frame.

```pig
grouped = GROUP nse_data BY ticker;
liquidity = FOREACH grouped GENERATE 
  group AS ticker,
  AVG(nse_data.volume) AS avg_volume,
  AVG(nse_data.closeprice) AS avg_close,
  AVG(nse_data.closeprice) * AVG(nse_data.volume) AS avg_liquidity;

-- Sort by liquidity
sorted_liq = ORDER liquidity BY avg_liquidity DESC;

-- Top 10
top10_liq = LIMIT sorted_liq 10;
DUMP top10_liq;

-- Bottom 10
sorted_liq_asc = ORDER liquidity BY avg_liquidity ASC;
bottom10_liq = LIMIT sorted_liq_asc 10;
DUMP bottom10_liq;

-- Save results to HDFS
STORE sorted_liq INTO '/home/hadoop/pig_liquidity_results';
```

**Q7. Top 10 highest liquidity stocks:**
```pig
DUMP top10_liq;
```

### Pig Tips

- To view job stats on Resource Manager, switch to MapReduce: `set exectype=mr;`
- For long-running jobs, switch back to Tez: `set exectype=tez;`
- View stats at `http://<EMR-Master-DNS>:8088/`
- The Grunt shell is recommended over Hue's Pig editor (less overwhelming output)

---

## Task 5 of 6 — Hive Configuration

> **Goal:** Configure Hive to use AWS Glue as a shared metastore.

### 5.1 — Key Points

Since we checked **"Use for Hive table metadata"** when creating the EMR cluster, Hive is already configured to use AWS Glue Data Catalog. This means:

- Hive, Pig, and Athena all share the **same metastore** (Glue)
- Tables created in Athena are visible in Hive and vice versa
- No additional configuration needed

### 5.2 — Verify in Hue

1. Open the **Hive editor** in Hue
2. Run: `SHOW DATABASES;` — you should see `nsedatabase`
3. Run: `USE nsedatabase; SHOW TABLES;` — you should see your NSE table
4. Run: `SELECT * FROM <table_name> LIMIT 10;`

### 5.3 — Compare MapReduce vs Tez

Run the same query with both engines and note the differences:

```sql
-- MapReduce mode
SET hive.execution.engine=mr;
SELECT ticker, COUNT(*) as cnt FROM nsedatabase.<table_name> GROUP BY ticker ORDER BY cnt DESC LIMIT 10;

-- Tez mode (faster)
SET hive.execution.engine=tez;
SELECT ticker, COUNT(*) as cnt FROM nsedatabase.<table_name> GROUP BY ticker ORDER BY cnt DESC LIMIT 10;
```

**Compare:** execution time, number of mappers/reducers, memory used, number of retries.

---

## Task 6 of 6 — Deep Hive Analytics

> **Goal:** Perform deep analytics on the NSE data using Hive — trading ranges, moving averages, entry/exit points — and visualize in QuickSight.

Use the Hive editor in Hue for all queries below. See `sample-queries/hive-queries.sql` for reference solutions.

### Q1. Top 10 Stocks by Daily Trading Range

Trading Range = Max price of a stock in a day minus the Min price in a day.

```sql
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
```

### Q2. Trading Date Range for Top 3 Stocks

Take the top 3 tickers from Q1 and find their full trading date range:

```sql
SELECT 
  ticker,
  MIN(datetrans) AS first_trade_date,
  MAX(datetrans) AS last_trade_date
FROM nsedatabase.<table_name>
WHERE ticker IN ('<STOCK1>', '<STOCK2>', '<STOCK3>')
GROUP BY ticker;
```

### Q3. Absolute Price Increase (Start to End)

```sql
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
```

**Visualize this in Hue** — use the bar chart option to compare the 3 stocks.

### Q4. Percentage Price Increase

```sql
-- Same CTEs as Q3, then:
SELECT 
  fp.ticker,
  fp.start_price,
  lp.end_price,
  ((lp.end_price - fp.start_price) / fp.start_price) * 100 AS pct_increase
FROM first_price fp
JOIN last_price lp ON fp.ticker = lp.ticker
ORDER BY pct_increase DESC;
```

**Visualize with Hue.**

### Q5. Absolute Highest Daily Trading Range for Top 3 Stocks

```sql
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
```

**Q5a. Highest daily TR as percentage:**
```sql
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
```

**Q5b. Save the top 10 records to HDFS:**
```sql
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
```

### Q6. Stocks with Highest Percentage Price Increase (Full Period)

For the trading date range 2014/12/18 - 2015/10/01:

```sql
WITH date_range_prices AS (
  SELECT 
    ticker,
    MIN(datetrans) AS first_date,
    MAX(datetrans) AS last_date
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
```

### Q7. 10-Day Moving Average of Closing Price

Pick a stock from the top 10 in Q5 (e.g., replace `<STOCK>` below):

```sql
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
    ticker,
    datetrans,
    AVG(closeprice) AS daily_avg_close,
    AVG(volume) AS daily_avg_volume
  FROM nsedatabase.<table_name>
  WHERE ticker = '<STOCK>'
  GROUP BY ticker, datetrans
) daily_data
ORDER BY datetrans
LIMIT 2000;
```

**Save to HDFS:**
```sql
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
```

### Q8. 30-Day Moving Average

```sql
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
```

### Q9. Visualize in QuickSight

1. Copy the saved HDFS files to S3 for QuickSight access:
```bash
# From EMR Master node
hadoop fs -copyToLocal /home/hadoop/hive_ma10day /tmp/ma10day
hadoop fs -copyToLocal /home/hadoop/hive_ma30day /tmp/ma30day

aws s3 cp /tmp/ma10day s3://<S3BucketName>/results/ma10day/ --recursive
aws s3 cp /tmp/ma30day s3://<S3BucketName>/results/ma30day/ --recursive
```

2. In **Amazon QuickSight**:
   - Create a new dataset from S3 (use a manifest file or direct S3 path)
   - Create a line chart with `datetrans` on X-axis
   - Plot both `daily_avg_close` and `ma_10day_close` (or `ma_30day_close`)
   - Identify **entry points** (MA crosses below actual price) and **exit points** (MA crosses above)

**Think about:**
- a. What are the entry/exit points for 30-day MA vs 10-day MA?
- b. What's the difference between the two windows?
- c. Which is more volatile and requires more transactions?

### Q10. Copy Results to S3 for Redundancy

```bash
# From EMR Master node
hadoop fs -cp /home/hadoop/hive_ma10day s3://<S3BucketName>/results/ma10day_backup/
hadoop fs -cp /home/hadoop/hive_ma30day s3://<S3BucketName>/results/ma30day_backup/
```

---

## Cleanup

**Important: Do this to avoid ongoing charges!**

1. **Terminate the EMR cluster:**
   - EMR Console -> Select your cluster -> **Terminate**

2. **Delete the CloudFormation stack:**
   ```bash
   aws cloudformation delete-stack --stack-name NSE-BigData-Workshop
   ```
   Or via Console: **CloudFormation** -> Select stack -> **Delete**

3. **Verify** all resources are removed in the AWS Console.

---

## Key Concepts Reference

| Term | Definition |
|------|-----------|
| **ATR (Average True Range)** | Max price - Min price for a stock in a given period |
| **Moving Average (MA)** | Average of stock price over a rolling window (e.g., 10 min, 10 days) |
| **Liquidity** | Stock monetary value transacted = Close Price x Volume |
| **Slippage** | Difference between actual transacted price and target buy/sell price |
| **Entry Point** | When MA < Actual Price (buy signal) |
| **Exit Point** | When MA > Actual Price (sell signal) |
| **Trading Range** | Max price - Min price for a stock in a single day |
| **EMRFS Consistent View** | Ensures S3 file integrity when accessed from EMR |
| **s3-dist-cp** | Distributed copy tool optimized for moving data between S3 and HDFS on EMR |

---

## Congratulations!

You've completed the full Big Data analytics pipeline on AWS:
- Ingested per-minute NSE stock data into S3
- Cataloged data with AWS Glue
- Ran preliminary analysis with Athena
- Set up an EMR cluster with Hadoop, Hive, Pig, and Hue
- Performed Pig Latin analysis for stock metrics
- Ran deep Hive analytics (trading ranges, moving averages)
- Visualized results in QuickSight for client presentations

**Next steps to explore on your own:**
- Try Kinesis Firehose for real-time data ingestion
- Use AWS Lambda for automated ETL triggers
- Explore Amazon SageMaker for predictive stock modeling
- Set up CloudWatch alarms for pipeline monitoring
