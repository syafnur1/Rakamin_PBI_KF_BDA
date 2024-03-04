-- Data Preparation --
-- Create new schema -- 
CREATE SCHEMA IF NOT EXISTS `canvas-bot-415513.kimia_farma`;

-- Create table from dataset --
CREATE TABLE IF NOT EXISTS `canvas-bot-415513.kimia_farma.final_transaction`
(
	transaction_id STRING PRIMARY KEY,
	date DATE,
	branch_id INT64,
	customer_name STRING,
    product_id INT64, 
    price INT64,
    discount_percentage FLOAT64,
    rating FLOAT64
);

CREATE TABLE IF NOT EXISTS `canvas-bot-415513.kimia_farma.inventory`
(
	Inventory_ID STRING PRIMARY KEY,
    branch_id INT64,
    product_id INT64,
    product_name STRING,
    opname_stock INT64
);

CREATE TABLE IF NOT EXISTS `canvas-bot-415513.kimia_farma.kantor_cabang`
(
	branch_id INT64 PRIMARY KEY,
    branch_category	STRING,
    branch_name	STRING,
    kota STRING,
    provinsi STRING,
    rating FLOAT64
);

CREATE TABLE IF NOT EXISTS `canvas-bot-415513.kimia_farma.product`
(
	product_id STRING PRIMARY KEY,
    product_name STRING,
    product_category STRING,
    price INT64
);

-- Base Tabel
CREATE TABLE `canvas-bot-415513.kimia_farma.base_table` AS (
SELECT
  ft.transaction_id,
  ft.date,
  kc.branch_id,
  kc.branch_name,
  kc.kota, 
  kc.provinsi,
  kc.rating AS branch_rate,
  ft.customer_name,
  p.product_id,
  p.product_name,
  p.price,
  ft.discount_percentage,
  CASE
    WHEN p.price >  500000 THEN 'laba 30%'
    WHEN p.price >= 300000 THEN 'laba 25%'
    WHEN p.price >=  100000 THEN 'laba 20%'
    WHEN p.price >= 50000 THEN 'laba 15%'
    ELSE 'laba 10%'
  END AS pct_gross_laba,
  (p.price - (p.price * ft.discount_percentage)) AS nett_sales,
  CASE
    WHEN p.price >  500000 THEN (p.price*(30/100))
    WHEN p.price >= 300000 THEN (p.price*(25/100))
    WHEN p.price >=  100000 THEN (p.price*(20/100))
    WHEN p.price >= 50000 THEN (p.price*(15/100))
    ELSE (p.price*(10/100))
  END AS nett_profit,
  ft.rating AS trx_rate
FROM `canvas-bot-415513.kimia_farma.final_transaction` AS ft
JOIN `canvas-bot-415513.kimia_farma.kantor_cabang` AS kc
  ON ft.branch_id = kc.branch_id
JOIN `canvas-bot-415513.kimia_farma.product` AS p
  ON ft.product_id = p.product_id
);

-- Aggregate Table 1 --
-- Yearly sales --
CREATE TABLE `canvas-bot-415513.kimia_farma.Yearly_Sales` AS (
SELECT
  EXTRACT(YEAR FROM DATE(date)) Year,
  ROUND(SUM(nett_sales),2) AS Total_Sales,
  ROUND(SUM(price),2) AS Total_Price,
  ROUND(SUM(discount_percentage),2) AS Total_Discount,
  ROUND(SUM(nett_profit),2) AS Total_Profit
FROM `canvas-bot-415513.kimia_farma.base_table`
GROUP BY 1
ORDER BY 1 ASC
);

-- Aggregate Table 2 --
-- Branch sales over years --
CREATE TABLE `canvas-bot-415513.kimia_farma.Branch_Sales` AS (
SELECT
  EXTRACT(YEAR FROM DATE(date)) Year,
  branch_name,
  ROUND(SUM(nett_sales),2) AS Total_Sales,
  ROUND(SUM(price),2) AS Total_price,
  ROUND(SUM(discount_percentage),2) AS Total_discount,
  ROUND(SUM(nett_profit),2) AS Total_nett_profit
FROM `canvas-bot-415513.kimia_farma.base_table`
GROUP BY 1,2
ORDER BY 1,2
);

-- Aggregate Table 3 --
-- Quarterly sales growth --
CREATE TABLE `canvas-bot-415513.kimia_farma.Quarterly_Sales_Growth` AS (
WITH sales_summary AS (
    SELECT
        EXTRACT(YEAR FROM date) AS Year,
        EXTRACT(QUARTER FROM date) AS Quarter,
        SUM(nett_sales) AS Total_Sales
    FROM
        `canvas-bot-415513.kimia_farma.base_table`
    GROUP BY 1, 2
    ORDER BY 1, 2
)
SELECT
    Year,
    Quarter,
    Total_Sales,
    LAG(Total_Sales) OVER (ORDER BY Year, Quarter) AS Prev_Quarter_Sales,
    Total_Sales - LAG(Total_Sales) OVER (ORDER BY Year, Quarter) AS Quarterly_Growth,
    CONCAT(CAST(ROUND(((Total_Sales - LAG(Total_Sales) OVER (ORDER BY Year, Quarter)) / LAG(Total_Sales) OVER (ORDER BY Year, Quarter)) * 100,2) AS STRING), '%') AS QoQ_Growth_Rate
FROM
    sales_summary
ORDER BY 1, 2
);


-- Aggregate Table 4 --
-- Product sales over years--
CREATE TABLE `canvas-bot-415513.kimia_farma.Product_Sales` AS (
SELECT
  EXTRACT(YEAR FROM DATE(date)) Year,
  product_name,
  ROUND(SUM(nett_sales),2) AS Total_Sales,
  ROUND(SUM(price),2) AS Total_Price,
  ROUND(SUM(discount_percentage),2) AS Total_Discount,
  ROUND(SUM(nett_profit),2) AS Total_Profit
FROM `canvas-bot-415513.kimia_farma.base_table`
GROUP BY 1,2
ORDER BY 1,2
);

-- Aggregate Table 5 --
-- Top 10 Total transaksi cabang provinsi--
CREATE TABLE `canvas-bot-415513.kimia_farma.Top10_Trx_Branch_Province` AS (
SELECT
  provinsi AS Province,
  COUNT(transaction_id) Total_Trx
FROM `canvas-bot-415513.kimia_farma.base_table`
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10
);

-- Aggregate Table 6 --
-- Top 10 Nett sales cabang provinsi --
CREATE TABLE `canvas-bot-415513.Top10_Profit_Province` AS (
SELECT
  provinsi AS Province,
  ROUND(SUM(nett_sales)) AS Total_Profit
FROM `canvas-bot-415513.kimia_farma.base_table`
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10
);

-- Aggregate Table 7 --
-- Top 5 Cabang Dengan Rating Tertinggi, namun Rating Transaksi Terendah --
CREATE TABLE `canvas-bot-415513.kimia_farma.Top_Branch_Rate_&_Low_Rate_Trx` AS (
SELECT
  branch_id AS Branch,
  branch_name AS Banch_Name,
  ROUND(AVG(branch_rate),2) AS Rating_Branch,
  -- branch_rate,
  ROUND(AVG(trx_rate),2) AS Rating_Trx
FROM `canvas-bot-415513.kimia_farma.base_table`
GROUP BY 1,2
ORDER BY 3 DESC, 4 ASC
LIMIT 5
);

-- Aggregate Table 8 --
-- Indonesia's Geo Map Untuk Total Profit Masing-masing Provinsi --
CREATE TABLE `canvas-bot-415513.kimia_farma.Geo_Profit_Map` AS (
SELECT
  provinsi AS Province,
  ROUND(SUM(nett_profit),2) AS Profit
FROM `canvas-bot-415513.kimia_farma.base_table`
GROUP BY 1
ORDER BY 2 DESC
);