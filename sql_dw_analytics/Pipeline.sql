-- ============================================================
-- 10. PIPELINE ORCHESTRATION & LOGGING
-- ============================================================
-- Everything above this point is correct ETL logic, but it is not
-- yet a pipeline: staging/bronze/silver/gold are separate statement
-- blocks meant to be run by hand, in order, with no record of
-- whether a run succeeded, how long it took, or how many rows moved.
-- This section wraps each layer's load logic (previously written
-- inline for bronze/silver/gold) into its own stored procedure, adds
-- a single orchestrator that calls all of them in the correct
-- dependency order inside TRY/CATCH, and logs every step to
-- etl.load_log. That combination -- reusable steps, an entry point,
-- and a run history -- is what makes this a pipeline rather than a
-- script.
--
-- Still missing for a production deployment (deliberately out of
-- scope for a single .sql file): a SCHEDULER. In SQL Server this
-- means wrapping EXEC etl.usp_run_full_pipeline; in a SQL Server
-- Agent Job with a nightly schedule (or an Azure Data Factory
-- pipeline / Airflow DAG if this ever needs to run outside SQL
-- Server, e.g. across multiple source systems). The orchestrator
-- procedure below is exactly what that job/DAG would call.

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
    EXEC('CREATE SCHEMA etl');
GO

-- ------------------------------------------------------------
-- 10A. etl.load_log — one row per layer, per run
-- ------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'etl' AND t.name = 'load_log')
BEGIN
    CREATE TABLE etl.load_log (
        log_id          INT IDENTITY(1,1) PRIMARY KEY,
        run_id          UNIQUEIDENTIFIER NOT NULL,   -- shared by every layer in one pipeline run
        layer_name      NVARCHAR(50)  NOT NULL,      -- 'staging' / 'bronze' / 'silver' / 'gold'
        started_at      DATETIME2     NOT NULL,
        finished_at     DATETIME2     NULL,
        status          NVARCHAR(20)  NOT NULL,      -- 'RUNNING' / 'SUCCESS' / 'FAILED'
        rows_affected   INT           NULL,
        error_message   NVARCHAR(4000) NULL
    );
END;
GO

-- ------------------------------------------------------------
-- 10B. Per-layer load procedures
-- Bronze/silver/gold logic is unchanged from sections 3-4 above --
-- moved here verbatim, just wrapped so the orchestrator can call
-- each layer as a single unit and know how many rows it touched.
-- ------------------------------------------------------------

CREATE OR ALTER PROCEDURE bronze.usp_load_bronze
    @rows_affected INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO bronze.superstore (
        order_id, order_date, ship_date, ship_mode, customer_id, customer_name,
        segment, country, city, state, postal_code, region, product_id,
        category, sub_category, product_name, sales, quantity, discount, profit
    )
    SELECT order_id, order_date, ship_date, ship_mode, customer_id, customer_name, segment, country, city, state, postal_code, region, product_id, category, sub_category, product_name, sales, quantity, discount, profit
    FROM staging.superstore
    EXCEPT
    SELECT order_id, order_date, ship_date, ship_mode, customer_id, customer_name, segment, country, city, state, postal_code, region, product_id, category, sub_category, product_name, sales, quantity, discount, profit
    FROM bronze.superstore;

    SET @rows_affected = @@ROWCOUNT;
END;
GO

CREATE OR ALTER PROCEDURE silver.usp_load_silver
    @rows_affected INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @total INT = 0;

    MERGE silver.customers AS tgt
    USING (
        SELECT LTRIM(RTRIM(b.customer_id)) AS customer_id, LTRIM(RTRIM(b.customer_name)) AS customer_name, LTRIM(RTRIM(b.segment)) AS segment
        FROM bronze.superstore b
        INNER JOIN (SELECT customer_id, MAX(bronze_id) AS max_bronze_id FROM bronze.superstore GROUP BY customer_id) latest
            ON latest.customer_id = b.customer_id AND latest.max_bronze_id = b.bronze_id
    ) AS src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED AND (tgt.customer_name <> src.customer_name OR tgt.segment <> src.segment) THEN
        UPDATE SET customer_name = src.customer_name, segment = src.segment
    WHEN NOT MATCHED THEN
        INSERT (customer_id, customer_name, segment) VALUES (src.customer_id, src.customer_name, src.segment);
    SET @total += @@ROWCOUNT;

    MERGE silver.products AS tgt
    USING (
        SELECT LTRIM(RTRIM(b.product_id)) AS product_id, LTRIM(RTRIM(b.product_name)) AS product_name, LTRIM(RTRIM(b.category)) AS category, LTRIM(RTRIM(b.sub_category)) AS sub_category
        FROM bronze.superstore b
        INNER JOIN (SELECT product_id, MAX(bronze_id) AS max_bronze_id FROM bronze.superstore GROUP BY product_id) latest
            ON latest.product_id = b.product_id AND latest.max_bronze_id = b.bronze_id
    ) AS src
    ON tgt.product_id = src.product_id
    WHEN MATCHED AND (tgt.product_name <> src.product_name OR tgt.category <> src.category OR tgt.sub_category <> src.sub_category) THEN
        UPDATE SET product_name = src.product_name, category = src.category, sub_category = src.sub_category
    WHEN NOT MATCHED THEN
        INSERT (product_id, product_name, category, sub_category) VALUES (src.product_id, src.product_name, src.category, src.sub_category);
    SET @total += @@ROWCOUNT;

    INSERT INTO silver.locations (city, state, postal_code, region, country)
    SELECT DISTINCT LTRIM(RTRIM(b.city)), LTRIM(RTRIM(b.state)), NULLIF(LTRIM(RTRIM(b.postal_code)), ''), LTRIM(RTRIM(b.region)), LTRIM(RTRIM(b.country))
    FROM bronze.superstore b
    WHERE NOT EXISTS (
        SELECT 1 FROM silver.locations l
        WHERE l.city = LTRIM(RTRIM(b.city)) AND l.state = LTRIM(RTRIM(b.state))
          AND ISNULL(l.postal_code, '') = ISNULL(NULLIF(LTRIM(RTRIM(b.postal_code)), ''), '')
          AND l.region = LTRIM(RTRIM(b.region)) AND l.country = LTRIM(RTRIM(b.country))
    );
    SET @total += @@ROWCOUNT;

    INSERT INTO silver.order_lines (
        order_id, customer_id, product_id, location_id, order_date, ship_date, ship_mode,
        sales, quantity, discount, profit, is_discount_out_of_range, is_loss_making, has_cast_error
    )
    SELECT
        LTRIM(RTRIM(b.order_id)), LTRIM(RTRIM(b.customer_id)), LTRIM(RTRIM(b.product_id)), l.location_id,
        TRY_CONVERT(DATE, LTRIM(RTRIM(REPLACE(REPLACE(b.order_date, CHAR(13), ''), CHAR(10), ''))), 103),
        TRY_CONVERT(DATE, LTRIM(RTRIM(REPLACE(REPLACE(b.ship_date,  CHAR(13), ''), CHAR(10), ''))), 103),
        LTRIM(RTRIM(b.ship_mode)),
        TRY_CONVERT(DECIMAL(12,4), LTRIM(RTRIM(REPLACE(REPLACE(b.sales,    CHAR(13), ''), CHAR(10), '')))),
        TRY_CONVERT(INT,           LTRIM(RTRIM(REPLACE(REPLACE(b.quantity, CHAR(13), ''), CHAR(10), '')))),
        TRY_CONVERT(DECIMAL(5,2),  LTRIM(RTRIM(REPLACE(REPLACE(b.discount, CHAR(13), ''), CHAR(10), '')))),
        TRY_CONVERT(DECIMAL(12,4), LTRIM(RTRIM(REPLACE(REPLACE(b.profit,   CHAR(13), ''), CHAR(10), '')))),
        CASE WHEN TRY_CONVERT(DECIMAL(5,2), LTRIM(RTRIM(REPLACE(REPLACE(b.discount, CHAR(13), ''), CHAR(10), '')))) NOT BETWEEN 0 AND 1 THEN 1 ELSE 0 END,
        CASE WHEN TRY_CONVERT(DECIMAL(12,4), LTRIM(RTRIM(REPLACE(REPLACE(b.profit, CHAR(13), ''), CHAR(10), '')))) < 0 THEN 1 ELSE 0 END,
        CASE WHEN TRY_CONVERT(DECIMAL(12,4), LTRIM(RTRIM(REPLACE(REPLACE(b.sales,    CHAR(13), ''), CHAR(10), '')))) IS NULL
                  OR TRY_CONVERT(INT,           LTRIM(RTRIM(REPLACE(REPLACE(b.quantity, CHAR(13), ''), CHAR(10), '')))) IS NULL
                  OR TRY_CONVERT(DECIMAL(5,2),  LTRIM(RTRIM(REPLACE(REPLACE(b.discount, CHAR(13), ''), CHAR(10), '')))) IS NULL
                  OR TRY_CONVERT(DECIMAL(12,4), LTRIM(RTRIM(REPLACE(REPLACE(b.profit,   CHAR(13), ''), CHAR(10), '')))) IS NULL
             THEN 1 ELSE 0 END
    FROM bronze.superstore b
    INNER JOIN (SELECT order_id, product_id, MAX(bronze_id) AS max_bronze_id FROM bronze.superstore GROUP BY order_id, product_id) latest
        ON latest.order_id = b.order_id AND latest.product_id = b.product_id AND latest.max_bronze_id = b.bronze_id
    INNER JOIN silver.locations l
        ON l.city = LTRIM(RTRIM(b.city)) AND l.state = LTRIM(RTRIM(b.state))
       AND ISNULL(l.postal_code, '') = ISNULL(NULLIF(LTRIM(RTRIM(b.postal_code)), ''), '')
       AND l.region = LTRIM(RTRIM(b.region)) AND l.country = LTRIM(RTRIM(b.country))
    WHERE NOT EXISTS (SELECT 1 FROM silver.order_lines existing WHERE existing.order_id = LTRIM(RTRIM(b.order_id)) AND existing.product_id = LTRIM(RTRIM(b.product_id)));
    SET @total += @@ROWCOUNT;

    SET @rows_affected = @total;
END;
GO

CREATE OR ALTER PROCEDURE gold.usp_load_gold
    @rows_affected INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @total INT = 0;

    INSERT INTO gold.dim_customer (customer_id, customer_name, segment)
    SELECT c.customer_id, c.customer_name, c.segment FROM silver.customers c
    WHERE NOT EXISTS (SELECT 1 FROM gold.dim_customer d WHERE d.customer_id = c.customer_id);
    SET @total += @@ROWCOUNT;

    INSERT INTO gold.dim_product (product_id, product_name, category, sub_category)
    SELECT p.product_id, p.product_name, p.category, p.sub_category FROM silver.products p
    WHERE NOT EXISTS (SELECT 1 FROM gold.dim_product d WHERE d.product_id = p.product_id);
    SET @total += @@ROWCOUNT;

    INSERT INTO gold.dim_location (location_id, city, state, postal_code, region, country)
    SELECT l.location_id, l.city, l.state, l.postal_code, l.region, l.country FROM silver.locations l
    WHERE NOT EXISTS (SELECT 1 FROM gold.dim_location d WHERE d.location_id = l.location_id);
    SET @total += @@ROWCOUNT;

    INSERT INTO gold.dim_ship_mode (ship_mode)
    SELECT DISTINCT ol.ship_mode FROM silver.order_lines ol
    WHERE NOT EXISTS (SELECT 1 FROM gold.dim_ship_mode d WHERE d.ship_mode = ol.ship_mode);
    SET @total += @@ROWCOUNT;

    INSERT INTO gold.fact_sales (
        order_id, customer_key, product_key, location_key, ship_mode_key,
        order_date_key, ship_date_key, sales, quantity, discount, profit, shipping_duration_days
    )
    SELECT
        ol.order_id, dc.customer_key, dp.product_key, dl.location_key, dsm.ship_mode_key,
        CONVERT(INT, FORMAT(ol.order_date, 'yyyyMMdd')), CONVERT(INT, FORMAT(ol.ship_date, 'yyyyMMdd')),
        ol.sales, ol.quantity, ol.discount, ol.profit, DATEDIFF(DAY, ol.order_date, ol.ship_date)
    FROM silver.order_lines ol
    INNER JOIN gold.dim_customer  dc  ON dc.customer_id = ol.customer_id
    INNER JOIN gold.dim_product   dp  ON dp.product_id  = ol.product_id
    INNER JOIN gold.dim_location  dl  ON dl.location_id = ol.location_id
    INNER JOIN gold.dim_ship_mode dsm ON dsm.ship_mode  = ol.ship_mode
    WHERE ol.has_cast_error = 0
      AND NOT EXISTS (SELECT 1 FROM gold.fact_sales fs WHERE fs.order_id = ol.order_id AND fs.product_key = dp.product_key);
    SET @total += @@ROWCOUNT;

    SET @rows_affected = @total;
END;
GO
-- ------------------------------------------------------------
-- 10C. etl.usp_run_full_pipeline — the entry point
-- Calls staging -> bronze -> silver -> gold in dependency order.
-- One run_id ties all four log rows together. If any layer throws,
-- that layer's row is marked FAILED with the error message, the
-- pipeline STOPS (no point loading gold on top of a broken silver),
-- and the error is re-thrown so a caller (a SQL Agent Job, e.g.)
-- sees it as a failed run.
-- ------------------------------------------------------------
CREATE OR ALTER PROCEDURE etl.usp_run_full_pipeline
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @run_id UNIQUEIDENTIFIER = NEWID();
    DECLARE @rows INT;
    DECLARE @step_start DATETIME2;

    -- ---- staging ----
    SET @step_start = SYSUTCDATETIME();
    INSERT INTO etl.load_log (run_id, layer_name, started_at, status) VALUES (@run_id, 'staging', @step_start, 'RUNNING');
    BEGIN TRY
        EXEC staging.load_superstore;
        SELECT @rows = COUNT(*) FROM staging.superstore;
        UPDATE etl.load_log SET finished_at = SYSUTCDATETIME(), status = 'SUCCESS', rows_affected = @rows
        WHERE run_id = @run_id AND layer_name = 'staging';
    END TRY
    BEGIN CATCH
        UPDATE etl.load_log SET finished_at = SYSUTCDATETIME(), status = 'FAILED', error_message = ERROR_MESSAGE()
        WHERE run_id = @run_id AND layer_name = 'staging';
        THROW;
    END CATCH

    -- ---- bronze ----
    SET @step_start = SYSUTCDATETIME();
    INSERT INTO etl.load_log (run_id, layer_name, started_at, status) VALUES (@run_id, 'bronze', @step_start, 'RUNNING');
    BEGIN TRY
        EXEC bronze.usp_load_bronze @rows_affected = @rows OUTPUT;
        UPDATE etl.load_log SET finished_at = SYSUTCDATETIME(), status = 'SUCCESS', rows_affected = @rows
        WHERE run_id = @run_id AND layer_name = 'bronze';
    END TRY
    BEGIN CATCH
        UPDATE etl.load_log SET finished_at = SYSUTCDATETIME(), status = 'FAILED', error_message = ERROR_MESSAGE()
        WHERE run_id = @run_id AND layer_name = 'bronze';
        THROW;
    END CATCH

    -- ---- silver ----
    SET @step_start = SYSUTCDATETIME();
    INSERT INTO etl.load_log (run_id, layer_name, started_at, status) VALUES (@run_id, 'silver', @step_start, 'RUNNING');
    BEGIN TRY
        EXEC silver.usp_load_silver @rows_affected = @rows OUTPUT;
        UPDATE etl.load_log SET finished_at = SYSUTCDATETIME(), status = 'SUCCESS', rows_affected = @rows
        WHERE run_id = @run_id AND layer_name = 'silver';
    END TRY
    BEGIN CATCH
        UPDATE etl.load_log SET finished_at = SYSUTCDATETIME(), status = 'FAILED', error_message = ERROR_MESSAGE()
        WHERE run_id = @run_id AND layer_name = 'silver';
        THROW;
    END CATCH

    -- ---- gold ----
    SET @step_start = SYSUTCDATETIME();
    INSERT INTO etl.load_log (run_id, layer_name, started_at, status) VALUES (@run_id, 'gold', @step_start, 'RUNNING');
    BEGIN TRY
        EXEC gold.usp_load_gold @rows_affected = @rows OUTPUT;
        UPDATE etl.load_log SET finished_at = SYSUTCDATETIME(), status = 'SUCCESS', rows_affected = @rows
        WHERE run_id = @run_id AND layer_name = 'gold';
    END TRY
    BEGIN CATCH
        UPDATE etl.load_log SET finished_at = SYSUTCDATETIME(), status = 'FAILED', error_message = ERROR_MESSAGE()
        WHERE run_id = @run_id AND layer_name = 'gold';
        THROW;
    END CATCH

    SELECT * FROM etl.load_log WHERE run_id = @run_id ORDER BY log_id;
END;
GO

-- Run the whole pipeline in one call:
EXEC etl.usp_run_full_pipeline;

-- Check the history of every run so far:
SELECT * FROM etl.load_log ORDER BY log_id DESC;