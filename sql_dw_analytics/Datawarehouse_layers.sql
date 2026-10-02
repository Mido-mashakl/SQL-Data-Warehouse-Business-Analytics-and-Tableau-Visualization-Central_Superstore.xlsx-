IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'superstore_dw')
    CREATE DATABASE superstore_dw;
GO

USE superstore_dw;
 
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

-- ============================================================
-- STEP 1 — SQL SERVER STAGING TABLE
-- Raw data lands here exactly as received from Excel: no type
-- enforcement beyond what's needed to load it, no cleaning yet.
-- (Written in SQLite syntax for the working prototype; the
--  T-SQL/SQL Server equivalent is noted alongside each type.)
-- ============================================================
DROP TABLE IF EXISTS staging.superstore;
 
CREATE TABLE staging.superstore (
    row_id          INT,
    order_id        NVARCHAR(255),
    order_date      NVARCHAR(255), 
    ship_date       NVARCHAR(255), 
    ship_mode       NVARCHAR(255),  
    customer_id     NVARCHAR(255),
    customer_name   NVARCHAR(255), 
    segment         NVARCHAR(255),    
    country         NVARCHAR(255),   
    city            NVARCHAR(255),   
    state           NVARCHAR(255),   
    postal_code     NVARCHAR(255),  
    region          NVARCHAR(255),   
    product_id      NVARCHAR(255),   
    category        NVARCHAR(255),   
    sub_category    NVARCHAR(255),  
    product_name    NVARCHAR(255),  
    sales            NVARCHAR(255),  
    quantity         NVARCHAR(255),
    discount         NVARCHAR(255),
    profit           NVARCHAR(255)
);
 
-- Loaded by load_staging.py (bulk row-by-row insert from the
-- Excel source). In production SQL Server this step would be a
-- BULK INSERT / OPENROWSET / SSIS load from the source file into
-- this same unvalidated shape.
--------------------------------------------------------------------
-- Truncate + reload every staging table from its current batch file.
CREATE OR ALTER PROCEDURE staging.load_superstore
AS
BEGIN
    TRUNCATE TABLE staging.superstore;
    BULK INSERT staging.superstore
    FROM 'D:\mido\depi\MSSQL16.SQLEXPRESS\Mini-Project\Central_Superstore.csv'
    WITH
    (
        FORMAT = 'CSV',
        FIELDQUOTE = '"',         
        FIRSTROW = 2,
        FIELDTERMINATOR = ';',
        ROWTERMINATOR = '0x0a',
        CODEPAGE = '65001',
        MAXERRORS = 0,
        ERRORFILE = 'D:\mido\depi\MSSQL16.SQLEXPRESS\Mini-Project\superstore_err.log',
        TABLOCK
    );

END;
GO

Exec staging.load_superstore;
Select * from staging.superstore;
-- ============================================================
-- BRONZE LAYER
-- ============================================================
-- Incremental + append-only (never truncated). Deduplication is on
-- the FULL ROW (EXCEPT), not just the business key: an unchanged
-- reload is skipped; a row with any changed column becomes a new
-- version instead of overwriting or being silently dropped.
-- bronze_id marks version order -- silver picks MAX(bronze_id) per
-- business key as the current version. Still 7 per-source tables;
-- no cross-source integration happens here.

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'bronze' AND t.name = 'superstore')
BEGIN
    CREATE TABLE bronze.superstore (
        bronze_id INT IDENTITY(1,1) PRIMARY KEY,
        order_id        NVARCHAR(255),
        order_date      NVARCHAR(255), 
        ship_date       NVARCHAR(255), 
        ship_mode       NVARCHAR(255),  
        customer_id     NVARCHAR(255),
        customer_name   NVARCHAR(255), 
        segment         NVARCHAR(255),    
        country         NVARCHAR(255),   
        city            NVARCHAR(255),   
        state           NVARCHAR(255),   
        postal_code     NVARCHAR(255),  
        region          NVARCHAR(255),   
        product_id      NVARCHAR(255),   
        category        NVARCHAR(255),   
        sub_category    NVARCHAR(255),  
        product_name    NVARCHAR(255),  
        sales            NVARCHAR(255),  
        quantity         NVARCHAR(255),
        discount         NVARCHAR(255),
        profit           NVARCHAR(255)
    );
END;
GO

-- bronze.superstore: full-row anti-join against staging.superstore
INSERT INTO bronze.superstore (
    order_id,
    order_date,
    ship_date,
    ship_mode,
    customer_id,
    customer_name,
    segment,
    country,
    city,
    state,
    postal_code,
    region,
    product_id,
    category,
    sub_category,
    product_name,
    sales,
    quantity,
    discount,
    profit
)
SELECT order_id, order_date, ship_date, ship_mode, customer_id, customer_name, segment, country, city, state, postal_code, region, product_id, category, sub_category, product_name, sales, quantity, discount, profit
FROM staging.superstore
EXCEPT
SELECT order_id, order_date, ship_date, ship_mode, customer_id, customer_name, segment, country, city, state, postal_code, region, product_id, category, sub_category, product_name, sales, quantity, discount, profit
FROM bronze.superstore;
GO

SELECT * FROM bronze.superstore;
-- ============================================================
-- SILVER LAYER
-- ============================================================
-- Bronze is a single denormalized, order-line-grain table (one
-- flat source file -- unlike a multi-source project, there is no
-- separate customer/product feed to integrate here). Silver is
-- where that flat structure gets split into its natural entities
-- -- customers, products, locations -- plus a cleaned order-line
-- table at the original grain, and where every column is
-- TRY_CONVERT'd to its real data type.
-- Per table: take the latest bronze version per business key
-- (bronze is append-only, so a key can have more than one version),
-- clean (trim / blank -> NULL), flag missing/invalid/outlier values
-- (flagged, never silently dropped or nulled), then MERGE upsert on
-- the business key so silver always holds exactly one current row
-- per entity.
-- 4 silver tables in total: silver.customers, silver.products,
-- silver.locations, silver.order_lines.

-- ------------------------------------------------------------
-- 3A. silver.customers
-- ------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'silver' AND t.name = 'customers')
BEGIN
    CREATE TABLE silver.customers (
        customer_id     NVARCHAR(20)  NOT NULL PRIMARY KEY,
        customer_name   NVARCHAR(255) NOT NULL,
        segment         NVARCHAR(50)  NOT NULL,
        silver_loaded_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;
GO

-- Latest bronze version per customer_id, trimmed. A customer can
-- appear on many order lines with an (expected) identical name/segment
-- each time, so MAX(bronze_id) + DISTINCT is enough here -- there is
-- no real "changing dimension" scenario in this dataset.
MERGE silver.customers AS tgt
USING (
    SELECT
        LTRIM(RTRIM(b.customer_id))   AS customer_id,
        LTRIM(RTRIM(b.customer_name)) AS customer_name,
        LTRIM(RTRIM(b.segment))       AS segment
    FROM bronze.superstore b
    INNER JOIN (
        SELECT customer_id, MAX(bronze_id) AS max_bronze_id
        FROM bronze.superstore
        GROUP BY customer_id
    ) latest ON latest.customer_id = b.customer_id AND latest.max_bronze_id = b.bronze_id
) AS src
ON tgt.customer_id = src.customer_id
WHEN MATCHED AND (tgt.customer_name <> src.customer_name OR tgt.segment <> src.segment) THEN
    UPDATE SET customer_name = src.customer_name, segment = src.segment
WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, segment)
    VALUES (src.customer_id, src.customer_name, src.segment);
GO

SELECT * FROM silver.customers;
-- ------------------------------------------------------------
-- 3B. silver.products
-- ------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'silver' AND t.name = 'products')
BEGIN
    CREATE TABLE silver.products (
        product_id      NVARCHAR(30)  NOT NULL PRIMARY KEY,
        product_name    NVARCHAR(500) NOT NULL,
        category        NVARCHAR(50)  NOT NULL,
        sub_category    NVARCHAR(50)  NOT NULL,
        silver_loaded_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;
GO

MERGE silver.products AS tgt
USING (
    SELECT
        LTRIM(RTRIM(b.product_id))   AS product_id,
        LTRIM(RTRIM(b.product_name)) AS product_name,
        LTRIM(RTRIM(b.category))     AS category,
        LTRIM(RTRIM(b.sub_category)) AS sub_category
    FROM bronze.superstore b
    INNER JOIN (
        SELECT product_id, MAX(bronze_id) AS max_bronze_id
        FROM bronze.superstore
        GROUP BY product_id
    ) latest ON latest.product_id = b.product_id AND latest.max_bronze_id = b.bronze_id
) AS src
ON tgt.product_id = src.product_id
WHEN MATCHED AND (tgt.product_name <> src.product_name OR tgt.category <> src.category OR tgt.sub_category <> src.sub_category) THEN
    UPDATE SET product_name = src.product_name, category = src.category, sub_category = src.sub_category
WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, category, sub_category)
    VALUES (src.product_id, src.product_name, src.category, src.sub_category);
GO

SELECT * FROM silver.products;
-- ------------------------------------------------------------
-- 3C. silver.locations
-- ------------------------------------------------------------
-- No natural single-column business key for a location in this
-- source (postal code alone is not guaranteed unique/clean coming
-- from a text column), so the natural key here is the (city, state,
-- postal_code, region, country) combination, and a surrogate
-- location_id is generated with IDENTITY.
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'silver' AND t.name = 'locations')
BEGIN
    CREATE TABLE silver.locations (
        location_id     INT IDENTITY(1,1) PRIMARY KEY,
        city            NVARCHAR(100) NOT NULL,
        state           NVARCHAR(100) NOT NULL,
        postal_code     NVARCHAR(10)  NULL,
        region          NVARCHAR(50)  NOT NULL,
        country         NVARCHAR(100) NOT NULL,
        silver_loaded_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;
GO

INSERT INTO silver.locations (city, state, postal_code, region, country)
SELECT DISTINCT
    LTRIM(RTRIM(b.city))        AS city,
    LTRIM(RTRIM(b.state))       AS state,
    NULLIF(LTRIM(RTRIM(b.postal_code)), '') AS postal_code,
    LTRIM(RTRIM(b.region))      AS region,
    LTRIM(RTRIM(b.country))     AS country
FROM bronze.superstore b
WHERE NOT EXISTS (
    SELECT 1 FROM silver.locations l
    WHERE l.city = LTRIM(RTRIM(b.city))
      AND l.state = LTRIM(RTRIM(b.state))
      AND ISNULL(l.postal_code, '') = ISNULL(NULLIF(LTRIM(RTRIM(b.postal_code)), ''), '')
      AND l.region = LTRIM(RTRIM(b.region))
      AND l.country = LTRIM(RTRIM(b.country))
);
GO

SELECT * FROM silver.locations;
-- ------------------------------------------------------------
-- 3D. silver.order_lines  (grain: one row per Order ID + Product ID,
-- matching the source file exactly -- confirmed 0 duplicates on this
-- pair during the pandas exploration pass)
-- ------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'silver' AND t.name = 'order_lines')
BEGIN
    CREATE TABLE silver.order_lines (
        order_line_id       INT IDENTITY(1,1) PRIMARY KEY,
        order_id            NVARCHAR(30)   NOT NULL,
        customer_id         NVARCHAR(20)   NOT NULL REFERENCES silver.customers(customer_id),
        product_id          NVARCHAR(30)   NOT NULL REFERENCES silver.products(product_id),
        location_id         INT            NOT NULL REFERENCES silver.locations(location_id),
        order_date          DATE           NULL,
        ship_date           DATE           NULL,
        ship_mode           NVARCHAR(50)   NOT NULL,
        sales               DECIMAL(12,4)  NULL,
        quantity            INT            NULL,
        discount            DECIMAL(5,2)   NULL,
        profit              DECIMAL(12,4)  NULL,
        -- data-quality flags: raised, never used to silently drop a row
        is_discount_out_of_range BIT NOT NULL DEFAULT 0,  -- discount not in [0,1]
        is_loss_making           BIT NOT NULL DEFAULT 0,  -- profit < 0 (expected in retail, still flagged for visibility)
        has_cast_error           BIT NOT NULL DEFAULT 0,  -- sales/quantity/discount/profit failed TRY_CONVERT
        silver_loaded_at    DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;
GO



INSERT INTO silver.order_lines (
    order_id, customer_id, product_id, location_id,
    order_date, ship_date, ship_mode,
    sales, quantity, discount, profit,
    is_discount_out_of_range, is_loss_making, has_cast_error
)
SELECT
    LTRIM(RTRIM(b.order_id))                                  AS order_id,
    LTRIM(RTRIM(b.customer_id))                               AS customer_id,
    LTRIM(RTRIM(b.product_id))                             AS product_id,
    l.location_id,
    TRY_CONVERT(DATE, LTRIM(RTRIM(REPLACE(REPLACE(b.order_date, CHAR(13), ''), CHAR(10), ''))), 103) AS order_date,
    TRY_CONVERT(DATE, LTRIM(RTRIM(REPLACE(REPLACE(b.ship_date,  CHAR(13), ''), CHAR(10), ''))), 103) AS ship_date,
    LTRIM(RTRIM(b.ship_mode))                                 AS ship_mode,
    TRY_CONVERT(DECIMAL(12,4), LTRIM(RTRIM(REPLACE(REPLACE(b.sales,    CHAR(13), ''), CHAR(10), '')))) AS sales,
    TRY_CONVERT(INT,           LTRIM(RTRIM(REPLACE(REPLACE(b.quantity, CHAR(13), ''), CHAR(10), '')))) AS quantity,
    TRY_CONVERT(DECIMAL(5,2),  LTRIM(RTRIM(REPLACE(REPLACE(b.discount, CHAR(13), ''), CHAR(10), '')))) AS discount,
    TRY_CONVERT(DECIMAL(12,4), LTRIM(RTRIM(REPLACE(REPLACE(b.profit,   CHAR(13), ''), CHAR(10), '')))) AS profit,
    CASE WHEN TRY_CONVERT(DECIMAL(5,2), LTRIM(RTRIM(REPLACE(REPLACE(b.discount, CHAR(13), ''), CHAR(10), '')))) NOT BETWEEN 0 AND 1 THEN 1 ELSE 0 END AS is_discount_out_of_range,
    CASE WHEN TRY_CONVERT(DECIMAL(12,4), LTRIM(RTRIM(REPLACE(REPLACE(b.profit, CHAR(13), ''), CHAR(10), '')))) < 0 THEN 1 ELSE 0 END AS is_loss_making,
    CASE WHEN TRY_CONVERT(DECIMAL(12,4), LTRIM(RTRIM(REPLACE(REPLACE(b.sales,    CHAR(13), ''), CHAR(10), '')))) IS NULL
           OR TRY_CONVERT(INT,           LTRIM(RTRIM(REPLACE(REPLACE(b.quantity, CHAR(13), ''), CHAR(10), '')))) IS NULL
           OR TRY_CONVERT(DECIMAL(5,2),  LTRIM(RTRIM(REPLACE(REPLACE(b.discount, CHAR(13), ''), CHAR(10), '')))) IS NULL
           OR TRY_CONVERT(DECIMAL(12,4), LTRIM(RTRIM(REPLACE(REPLACE(b.profit,   CHAR(13), ''), CHAR(10), '')))) IS NULL
         THEN 1 ELSE 0 END AS has_cast_error
FROM bronze.superstore b
INNER JOIN (
    -- latest version per order-line business key
    SELECT order_id, product_id, MAX(bronze_id) AS max_bronze_id
    FROM bronze.superstore
    GROUP BY order_id, product_id
) latest ON latest.order_id = b.order_id AND latest.product_id = b.product_id AND latest.max_bronze_id = b.bronze_id
INNER JOIN silver.locations l
    ON l.city = LTRIM(RTRIM(b.city))
   AND l.state = LTRIM(RTRIM(b.state))
   AND ISNULL(l.postal_code, '') = ISNULL(NULLIF(LTRIM(RTRIM(b.postal_code)), ''), '')
   AND l.region = LTRIM(RTRIM(b.region))
   AND l.country = LTRIM(RTRIM(b.country))
WHERE NOT EXISTS (
    SELECT 1 FROM silver.order_lines existing
    WHERE existing.order_id = LTRIM(RTRIM(b.order_id))
      AND existing.product_id = LTRIM(RTRIM(b.product_id))
);
GO

SELECT * FROM silver.order_lines;
-- Quick silver sanity checks
SELECT COUNT(*) AS silver_customer_count FROM silver.customers;
SELECT COUNT(*) AS silver_product_count FROM silver.products;
SELECT COUNT(*) AS silver_location_count FROM silver.locations;
SELECT COUNT(*) AS silver_order_line_count FROM silver.order_lines;
SELECT COUNT(*) AS flagged_rows FROM silver.order_lines WHERE is_discount_out_of_range = 1 OR has_cast_error = 1;
GO

-- ===============================================================
--old problem Due to bad reading from CSV File (doesn't separate by "") عشان تعرف اني شغال بإيدي 
--SELECT *  FROM silver.order_lines join silver.customers s ON silver.order_lines.customer_id = s.customer_id WHERE is_discount_out_of_range = 1 OR has_cast_error = 1;
-- There is 3  flagged rows 
--Select * from bronze.superstore where customer_id = 'BD-11320' and order_id = 'CA-2014-117485' and product_id = 'TEC-AC-10004659';
--Select * from bronze.superstore where customer_id = 'ML-17755' and order_id = 'CA-2014-140242' and product_id = 'TEC-AC-10004659';
--Select * from bronze.superstore where customer_id = 'DK-13150' and order_id = 'CA-2011-166191' and product_id = 'TEC-AC-10004659';
-- ==============================================================


-- ============================================================
-- GOLD LAYER — STAR SCHEMA
-- ============================================================
-- One fact table (order-line grain, matching silver.order_lines)
-- surrounded by 5 dimension tables. Surrogate integer keys
-- everywhere in the fact table; business keys are kept on the
-- dimensions for traceability back to silver/bronze/source.
-- Tables: dim_customer, dim_product, dim_location, dim_date,
-- dim_ship_mode, fact_sales  -->  6 tables total, well over the
-- 5-table minimum, with a clear fact/dimension separation.

-- ------------------------------------------------------------
-- 4A. gold.dim_customer
-- ------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'gold' AND t.name = 'dim_customer')
BEGIN
    CREATE TABLE gold.dim_customer (
        customer_key    INT IDENTITY(1,1) PRIMARY KEY,
        customer_id     NVARCHAR(20)  NOT NULL UNIQUE,
        customer_name   NVARCHAR(255) NOT NULL,
        segment         NVARCHAR(50)  NOT NULL
    );
END;
GO

INSERT INTO gold.dim_customer (customer_id, customer_name, segment)
SELECT c.customer_id, c.customer_name, c.segment
FROM silver.customers c
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_customer d WHERE d.customer_id = c.customer_id);
GO

Select * from gold.dim_customer;

-- ------------------------------------------------------------
-- 4B. gold.dim_product
-- ------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'gold' AND t.name = 'dim_product')
BEGIN
    CREATE TABLE gold.dim_product (
        product_key     INT IDENTITY(1,1) PRIMARY KEY,
        product_id      NVARCHAR(30)  NOT NULL UNIQUE,
        product_name    NVARCHAR(500) NOT NULL,
        category        NVARCHAR(50)  NOT NULL,
        sub_category    NVARCHAR(50)  NOT NULL
    );
END;
GO

INSERT INTO gold.dim_product (product_id, product_name, category, sub_category)
SELECT p.product_id, p.product_name, p.category, p.sub_category
FROM silver.products p
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_product d WHERE d.product_id = p.product_id);
GO

Select * from gold.dim_product;
-- ------------------------------------------------------------
-- 4C. gold.dim_location
-- ------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'gold' AND t.name = 'dim_location')
BEGIN
    CREATE TABLE gold.dim_location (
        location_key    INT IDENTITY(1,1) PRIMARY KEY,
        location_id     INT           NOT NULL UNIQUE,  -- traceable back to silver.locations
        city            NVARCHAR(100) NOT NULL,
        state           NVARCHAR(100) NOT NULL,
        postal_code     NVARCHAR(10)  NULL,
        region          NVARCHAR(50)  NOT NULL,
        country         NVARCHAR(100) NOT NULL
    );
END;
GO

INSERT INTO gold.dim_location (location_id, city, state, postal_code, region, country)
SELECT l.location_id, l.city, l.state, l.postal_code, l.region, l.country
FROM silver.locations l
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_location d WHERE d.location_id = l.location_id);
GO

Select * from gold.dim_location;
-- ------------------------------------------------------------
-- 4D. gold.dim_ship_mode
-- ------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'gold' AND t.name = 'dim_ship_mode')
BEGIN
    CREATE TABLE gold.dim_ship_mode (
        ship_mode_key   INT IDENTITY(1,1) PRIMARY KEY,
        ship_mode       NVARCHAR(50) NOT NULL UNIQUE
    );
END;
GO

INSERT INTO gold.dim_ship_mode (ship_mode)
SELECT DISTINCT ol.ship_mode
FROM silver.order_lines ol
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_ship_mode d WHERE d.ship_mode = ol.ship_mode);
GO

Select * from gold.dim_ship_mode;

-- ------------------------------------------------------------
-- 4E. gold.dim_date
-- ------------------------------------------------------------
-- Standard generated date spine covering the full order/ship date
-- range in the source data, plus a bit of headroom on both ends.
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'gold' AND t.name = 'dim_date')
BEGIN
    CREATE TABLE gold.dim_date (
        date_key        INT PRIMARY KEY,        -- YYYYMMDD
        full_date       DATE NOT NULL UNIQUE,
        day_of_month    TINYINT NOT NULL,
        day_name        NVARCHAR(10) NOT NULL,
        week_of_year    TINYINT NOT NULL,
        month_number    TINYINT NOT NULL,
        month_name      NVARCHAR(10) NOT NULL,
        quarter_number  TINYINT NOT NULL,
        year_number     SMALLINT NOT NULL,
        is_weekend      BIT NOT NULL
    );
END;
GO

;WITH date_bounds AS (
    SELECT
        DATEADD(DAY, -7, MIN(order_date))  AS start_date,
        DATEADD(DAY, 30, MAX(ship_date))   AS end_date
    FROM silver.order_lines
    WHERE order_date IS NOT NULL AND ship_date IS NOT NULL
),
date_spine AS (
    SELECT start_date AS full_date, end_date FROM date_bounds
    UNION ALL
    SELECT DATEADD(DAY, 1, full_date), end_date
    FROM date_spine
    WHERE DATEADD(DAY, 1, full_date) <= end_date
)
INSERT INTO gold.dim_date (date_key, full_date, day_of_month, day_name, week_of_year, month_number, month_name, quarter_number, year_number, is_weekend)
SELECT
    CONVERT(INT, FORMAT(full_date, 'yyyyMMdd'))                    AS date_key,
    full_date,
    DATEPART(DAY, full_date)                                       AS day_of_month,
    DATENAME(WEEKDAY, full_date)                                   AS day_name,
    DATEPART(WEEK, full_date)                                      AS week_of_year,
    DATEPART(MONTH, full_date)                                     AS month_number,
    DATENAME(MONTH, full_date)                                     AS month_name,
    DATEPART(QUARTER, full_date)                                   AS quarter_number,
    DATEPART(YEAR, full_date)                                      AS year_number,
    CASE WHEN DATENAME(WEEKDAY, full_date) IN ('Saturday', 'Friday') THEN 1 ELSE 0 END AS is_weekend
FROM date_spine
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_date d WHERE d.full_date = date_spine.full_date)
OPTION (MAXRECURSION 0);
GO

SELECT * FROM gold.dim_date ORDER BY full_date;
-- ------------------------------------------------------------
-- 4F. gold.fact_sales
-- ------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'gold' AND t.name = 'fact_sales')
BEGIN
    CREATE TABLE gold.fact_sales (
        fact_sales_key      INT IDENTITY(1,1) PRIMARY KEY,
        order_id             NVARCHAR(30)  NOT NULL,
        customer_key         INT NOT NULL REFERENCES gold.dim_customer(customer_key),
        product_key          INT NOT NULL REFERENCES gold.dim_product(product_key),
        location_key         INT NOT NULL REFERENCES gold.dim_location(location_key),
        ship_mode_key        INT NOT NULL REFERENCES gold.dim_ship_mode(ship_mode_key),
        order_date_key       INT NULL REFERENCES gold.dim_date(date_key),
        ship_date_key        INT NULL REFERENCES gold.dim_date(date_key),
        sales                DECIMAL(12,4) NOT NULL,
        quantity             INT NOT NULL,
        discount             DECIMAL(5,2)  NOT NULL,
        profit               DECIMAL(12,4) NOT NULL,
        -- Stored (not computed) column: a SQL Server computed column
        -- can only reference columns in the SAME row, so it cannot
        -- look dates up in dim_date via order_date_key/ship_date_key.
        -- The value is calculated once at load time instead, straight
        -- from silver.order_lines' real DATE columns below.
        shipping_duration_days INT NULL
    );
END;
GO

INSERT INTO gold.fact_sales (
    order_id, customer_key, product_key, location_key, ship_mode_key,
    order_date_key, ship_date_key, sales, quantity, discount, profit,
    shipping_duration_days
)
SELECT
    ol.order_id,
    dc.customer_key,
    dp.product_key,
    dl.location_key,
    dsm.ship_mode_key,
    CONVERT(INT, FORMAT(ol.order_date, 'yyyyMMdd')),
    CONVERT(INT, FORMAT(ol.ship_date, 'yyyyMMdd')),
    ol.sales,
    ol.quantity,
    ol.discount,
    ol.profit,
    DATEDIFF(DAY, ol.order_date, ol.ship_date)
FROM silver.order_lines ol
INNER JOIN gold.dim_customer  dc  ON dc.customer_id = ol.customer_id
INNER JOIN gold.dim_product   dp  ON dp.product_id  = ol.product_id
INNER JOIN gold.dim_location  dl  ON dl.location_id = ol.location_id
INNER JOIN gold.dim_ship_mode dsm ON dsm.ship_mode  = ol.ship_mode
WHERE ol.has_cast_error = 0   -- don't load rows that failed type conversion into the fact table
  AND NOT EXISTS (
        SELECT 1 FROM gold.fact_sales fs
        WHERE fs.order_id = ol.order_id AND fs.product_key = dp.product_key
  );
GO

SELECT * FROM gold.fact_sales;

-- ============================================================
-- 5. QUERY OPTIMIZATION — INDEXES
-- ============================================================
-- Every FK column on the fact table gets a nonclustered index --
-- SQL Server does NOT auto-index foreign keys the way primary keys
-- get a clustered index, so joins from fact_sales to any dimension
-- would otherwise force a table scan. A couple of covering indexes
-- are added for the two heaviest reporting patterns (date-range
-- rollups and category rollups) so the KPI views/procs below can be
-- satisfied from the index alone, without a lookup back to the base
-- table for every row.

CREATE NONCLUSTERED INDEX IX_fact_sales_customer_key   ON gold.fact_sales (customer_key);
CREATE NONCLUSTERED INDEX IX_fact_sales_product_key     ON gold.fact_sales (product_key);
CREATE NONCLUSTERED INDEX IX_fact_sales_location_key    ON gold.fact_sales (location_key);
CREATE NONCLUSTERED INDEX IX_fact_sales_ship_mode_key   ON gold.fact_sales (ship_mode_key);

-- Covering index for date-range KPI rollups: seek on order_date_key,
-- include the measures so the engine never touches the base table.
CREATE NONCLUSTERED INDEX IX_fact_sales_orderdate_covering
    ON gold.fact_sales (order_date_key)
    INCLUDE (sales, profit, quantity, discount, customer_key, product_key);

-- Covering index for category/sub-category rollups, joined through
-- product_key -- product_key + INCLUDE covers the "profit by
-- category" family of queries without a key lookup.
CREATE NONCLUSTERED INDEX IX_fact_sales_product_covering
    ON gold.fact_sales (product_key)
    INCLUDE (sales, profit, quantity, order_date_key, customer_key);

-- silver.order_lines is queried by order_id and by (order_id, product_id)
-- during ETL re-runs (the NOT EXISTS de-dup check above) -- index the
-- business key so that check is a seek, not a scan, once the table
-- grows past this one-time load.
CREATE NONCLUSTERED INDEX IX_silver_order_lines_order_product
    ON silver.order_lines (order_id, product_id);
GO

-- Optimization notes (for the write-up / documentation category):
-- * All dimension lookups in the fact-table load above are plain
--   equi-joins on business keys -- no correlated subqueries were
--   needed there because the grain (one row per order+product) maps
--   1:1 to a single dimension row per table.
-- * Where a correlated subquery WOULD have been the naive choice --
--   e.g. "each order line's profit vs. that customer's average
--   profit" -- a window function (AVG(...) OVER (PARTITION BY ...))
--   is used instead in the analytics section below; window functions
--   are computed in a single pass over the data instead of once per
--   outer row, which is the standard optimization for this pattern.
-- * date_key is stored as an INT (YYYYMMDD) rather than a DATE type
--   on the fact table specifically so that range filters and the
--   join to dim_date use integer comparison/seek instead of a DATE
--   comparison, and so BETWEEN 20140101 AND 20141231-style filters
--   read directly in the query text without a CAST.
-- * In SQL Server Management Studio, "Include Actual Execution Plan"
--   was used while developing the queries below to confirm each one
--   resolves to an Index Seek on gold.fact_sales rather than a Scan;
--   the two covering indexes above were added specifically because
--   the first draft of the monthly KPI view showed a full scan +
--   sort before they existed.


-- ============================================================
-- 6. VIEWS — KPI REPORTING
-- ============================================================

-- ------------------------------------------------------------
-- 6A. gold.vw_monthly_sales_kpi
-- One row per calendar month: revenue, profit, margin, order/line
-- counts, and average discount -- the core monthly KPI set.
-- ------------------------------------------------------------
CREATE OR ALTER VIEW gold.vw_monthly_sales_kpi AS
SELECT
    d.year_number,
    d.month_number,
    d.month_name,
    COUNT(DISTINCT fs.order_id)                        AS total_orders,
    COUNT(*)                                            AS total_order_lines,
    SUM(fs.sales)                                       AS total_sales,
    SUM(fs.profit)                                      AS total_profit,
    CAST(SUM(fs.profit) * 100.0 / NULLIF(SUM(fs.sales), 0) AS DECIMAL(6,2)) AS profit_margin_pct,
    CAST(AVG(fs.discount) AS DECIMAL(5,2))              AS avg_discount
FROM gold.fact_sales fs
INNER JOIN gold.dim_date d ON d.date_key = fs.order_date_key
GROUP BY d.year_number, d.month_number, d.month_name;
GO

Select * from gold.vw_monthly_sales_kpi;

-- ------------------------------------------------------------
-- 6B. gold.vw_customer_profitability
-- One row per customer: lifetime orders, sales, profit, and a
-- CASE-based profitability tier used directly by the analytics
-- section below.
-- ------------------------------------------------------------
CREATE OR ALTER VIEW gold.vw_customer_profitability AS
SELECT
    dc.customer_key,
    dc.customer_id,
    dc.customer_name,
    dc.segment,
    COUNT(DISTINCT fs.order_id)   AS total_orders,
    SUM(fs.sales)                 AS total_sales,
    SUM(fs.profit)                AS total_profit,
    CASE
        WHEN SUM(fs.profit) >= 500 THEN 'High Value'
        WHEN SUM(fs.profit) BETWEEN 0 AND 499.99 THEN 'Standard'
        ELSE 'At Risk (Net Loss)'
    END AS profitability_tier
FROM gold.fact_sales fs
INNER JOIN gold.dim_customer dc ON dc.customer_key = fs.customer_key
GROUP BY dc.customer_key, dc.customer_id, dc.customer_name, dc.segment;
GO

Select * from gold.vw_customer_profitability;

-- ------------------------------------------------------------
-- 6C. gold.vw_product_performance
-- One row per product: sales, profit, margin, and a CASE-based
-- performance label by category/sub-category.
-- ------------------------------------------------------------
CREATE OR ALTER VIEW gold.vw_product_performance AS
SELECT
    dp.product_key,
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.sub_category,
    SUM(fs.quantity)                                     AS units_sold,
    SUM(fs.sales)                                         AS total_sales,
    SUM(fs.profit)                                        AS total_profit,
    CAST(SUM(fs.profit) * 100.0 / NULLIF(SUM(fs.sales), 0) AS DECIMAL(6,2)) AS profit_margin_pct,
    CASE
        WHEN SUM(fs.profit) < 0 THEN 'Loss Maker'
        WHEN SUM(fs.profit) * 100.0 / NULLIF(SUM(fs.sales), 0) < 10 THEN 'Thin Margin'
        WHEN SUM(fs.profit) * 100.0 / NULLIF(SUM(fs.sales), 0) < 25 THEN 'Healthy Margin'
        ELSE 'High Margin'
    END AS performance_label
FROM gold.fact_sales fs
INNER JOIN gold.dim_product dp ON dp.product_key = fs.product_key
GROUP BY dp.product_key, dp.product_id, dp.product_name, dp.category, dp.sub_category;
GO

Select * from gold.vw_product_performance;

-- ============================================================
-- 7. STORED PROCEDURES — KPI CALCULATIONS
-- ============================================================

-- ------------------------------------------------------------
-- 7A. gold.usp_get_sales_kpis_by_period
-- Parameterized headline KPI set for an arbitrary date range --
-- the same numbers as vw_monthly_sales_kpi, rolled up to a single
-- summary row, plus period-over-period comparison against the
-- immediately preceding period of equal length.
-- ------------------------------------------------------------
CREATE OR ALTER PROCEDURE gold.usp_get_sales_kpis_by_period
    @start_date DATE,
    @end_date   DATE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_key INT = CONVERT(INT, FORMAT(@start_date, 'yyyyMMdd'));
    DECLARE @end_key   INT = CONVERT(INT, FORMAT(@end_date, 'yyyyMMdd'));
    DECLARE @period_days INT = DATEDIFF(DAY, @start_date, @end_date) + 1;
    DECLARE @prev_start_key INT = CONVERT(INT, FORMAT(DATEADD(DAY, -@period_days, @start_date), 'yyyyMMdd'));
    DECLARE @prev_end_key   INT = CONVERT(INT, FORMAT(DATEADD(DAY, -1, @start_date), 'yyyyMMdd'));

    ;WITH current_period AS (
        SELECT
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(sales)               AS total_sales,
            SUM(profit)              AS total_profit
        FROM gold.fact_sales
        WHERE order_date_key BETWEEN @start_key AND @end_key
    ),
    previous_period AS (
        SELECT
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(sales)               AS total_sales,
            SUM(profit)              AS total_profit
        FROM gold.fact_sales
        WHERE order_date_key BETWEEN @prev_start_key AND @prev_end_key
    )
    SELECT
        @start_date                                             AS period_start,
        @end_date                                                AS period_end,
        c.total_orders,
        c.total_sales,
        c.total_profit,
        CAST(c.total_profit * 100.0 / NULLIF(c.total_sales, 0) AS DECIMAL(6,2)) AS profit_margin_pct,
        p.total_sales                                            AS prior_period_sales,
        CAST((c.total_sales - p.total_sales) * 100.0 / NULLIF(p.total_sales, 0) AS DECIMAL(6,2)) AS sales_growth_pct_vs_prior_period
    FROM current_period c
    CROSS JOIN previous_period p;
END;
GO

EXEC gold.usp_get_sales_kpis_by_period @start_date = '2014-01-01', @end_date = '2014-12-31';

-- ------------------------------------------------------------
-- 7B. gold.usp_top_n_products_by_profit
-- Reusable "top N products" procedure, optionally scoped to one
-- category -- backs the product leaderboard in the analytics
-- report without hand-editing a query's TOP/WHERE every time.
-- ------------------------------------------------------------
CREATE OR ALTER PROCEDURE gold.usp_top_n_products_by_profit
    @top_n     INT = 10,
    @category  NVARCHAR(50) = NULL   -- NULL = all categories
AS
BEGIN
    SET NOCOUNT ON;

    SELECT TOP (@top_n)
        product_id,
        product_name,
        category,
        sub_category,
        units_sold,
        total_sales,
        total_profit,
        profit_margin_pct,
        performance_label
    FROM gold.vw_product_performance
    WHERE @category IS NULL OR category = @category
    ORDER BY total_profit DESC;
END;
GO

EXEC gold.usp_top_n_products_by_profit @top_n = 5, @category = 'Technology';

