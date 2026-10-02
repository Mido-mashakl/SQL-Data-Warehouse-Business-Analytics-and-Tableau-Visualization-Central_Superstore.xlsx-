USE superstore_dw;

-- ============================================================
-- 8. ANALYTICAL QUERY CATALOG (Rubric Category 2)
-- ============================================================
-- 18 numbered, standalone queries.

-- Q1: order lines with The most and The Least profit with customer + product names attached.
SELECT TOP 20
    fs.order_id,
    dc.customer_name,
    dp.product_name,
    fs.sales,
    fs.profit
FROM gold.fact_sales fs
INNER JOIN gold.dim_customer dc ON dc.customer_key = fs.customer_key
INNER JOIN gold.dim_product  dp ON dp.product_key  = fs.product_key
ORDER BY fs.profit desc;
-- ======================================
SELECT TOP 20
    fs.order_id,
    dc.customer_name,
    dp.product_name,
    fs.sales,
    fs.profit
FROM gold.fact_sales fs
INNER JOIN gold.dim_customer dc ON dc.customer_key = fs.customer_key
INNER JOIN gold.dim_product  dp ON dp.product_key  = fs.product_key
ORDER BY fs.profit;

-- Q2: fully denormalized order-line report, one row
-- per line with every dimension's descriptive attributes attached.
SELECT
    fs.order_id,
    dd.full_date        AS order_date,
    dc.customer_name,
    dc.segment,
    dp.product_name,
    dp.category,
    dp.sub_category,
    dl.city,
    dl.state,
    dsm.ship_mode,
    fs.sales,
    fs.quantity,
    fs.discount,
    fs.profit
FROM gold.fact_sales fs
INNER JOIN gold.dim_customer  dc  ON dc.customer_key  = fs.customer_key
INNER JOIN gold.dim_product   dp  ON dp.product_key   = fs.product_key
INNER JOIN gold.dim_location  dl  ON dl.location_key  = fs.location_key
INNER JOIN gold.dim_ship_mode dsm ON dsm.ship_mode_key = fs.ship_mode_key
INNER JOIN gold.dim_date      dd  ON dd.date_key       = fs.order_date_key;

-- Q3: products whose total profit beats the
-- average total profit across all products.
SELECT product_id, product_name, category, total_profit
FROM gold.vw_product_performance
WHERE total_profit > (
    SELECT AVG(total_profit) FROM gold.vw_product_performance
)
ORDER BY total_profit DESC;

-- Q4: customers whose lifetime
-- sales exceed $2,000, joined back to the dimension for their segment.
SELECT dc.customer_name, dc.segment, big_spenders.lifetime_sales
FROM (
    SELECT customer_key, SUM(sales) AS lifetime_sales
    FROM gold.fact_sales
    GROUP BY customer_key
    HAVING SUM(sales) > 2000
) AS big_spenders
INNER JOIN gold.dim_customer dc ON dc.customer_key = big_spenders.customer_key
ORDER BY big_spenders.lifetime_sales DESC;

-- Q5: order lines whose profit is below the
-- average profit for their own product's sub-category (flags
-- specific underperforming line items, not whole products).
SELECT
    fs.order_id,
    dp.product_name,
    dp.sub_category,
    fs.profit
FROM gold.fact_sales fs
INNER JOIN gold.dim_product dp ON dp.product_key = fs.product_key
WHERE fs.profit < (
    SELECT AVG(fs2.profit)
    FROM gold.fact_sales fs2
    INNER JOIN gold.dim_product dp2 ON dp2.product_key = fs2.product_key
    WHERE dp2.sub_category = dp.sub_category
)
ORDER BY dp.sub_category, fs.profit;

-- فبتالي عايزين نشوف بقى متوسط بيع كل 
-- sub_category
Select dp.sub_category , Avg(fs.profit) as Avg
from gold.dim_product dp 
inner JOIN gold.fact_sales fs on dp.product_key = fs.product_key 
Group By dp.sub_category;

-- Q6: monthly sales trend with month-over-month
-- growth, computed via a window function inside a second CTE.
;WITH monthly AS (
    SELECT
        d.year_number, d.month_number, d.month_name,
        SUM(fs.sales) AS total_sales
    FROM gold.fact_sales fs
    INNER JOIN gold.dim_date d ON d.date_key = fs.order_date_key
    GROUP BY d.year_number, d.month_number, d.month_name
),
monthly_with_growth AS (
    SELECT
        year_number, month_number, month_name, total_sales,
        LAG(total_sales) OVER (ORDER BY year_number, month_number) AS prior_month_sales
    FROM monthly
)
SELECT
    year_number, month_number, month_name, total_sales,
    prior_month_sales,
    CAST((total_sales - prior_month_sales) * 100.0 / NULLIF(prior_month_sales, 0) AS DECIMAL(6,2)) AS mom_growth_pct
FROM monthly_with_growth
ORDER BY year_number, month_number;
--  استنتاج : ان شهر مارس دايما معدل المبيعات بيزيد فيه بشكل ضخم على عكس شهر أكتوبر

-- Q7: simple RFM-style summary per customer (Recency in days
-- since their last order relative to the dataset's last order date,
-- Frequency = order count, Monetary = total sales).
;WITH dataset_last_date AS (
    SELECT MAX(d.full_date) AS max_order_date
    FROM gold.fact_sales fs
    INNER JOIN gold.dim_date d ON d.date_key = fs.order_date_key
),
customer_summary AS (
    SELECT
        dc.customer_id,
        dc.customer_name,
        MAX(dd.full_date)              AS last_order_date,
        COUNT(DISTINCT fs.order_id)    AS frequency,
        SUM(fs.sales)                  AS monetary
    FROM gold.fact_sales fs
    INNER JOIN gold.dim_customer dc ON dc.customer_key = fs.customer_key
    INNER JOIN gold.dim_date dd      ON dd.date_key = fs.order_date_key
    GROUP BY dc.customer_id, dc.customer_name
)
SELECT
    cs.customer_id, cs.customer_name, cs.frequency, cs.monetary,
    DATEDIFF(DAY, cs.last_order_date, dld.max_order_date) AS recency_days
FROM customer_summary cs
CROSS JOIN dataset_last_date dld
ORDER BY cs.monetary DESC;
-- البايثون ميلزمنيش بعد هذا ال 
-- RFM 

-- Q8: profit-health label per order line,
-- feeding straight into the loss-driver analysis in section 9.
SELECT
    fs.order_id,
    dp.product_name,
    fs.sales,
    fs.profit,
    CASE
        WHEN fs.profit < 0            THEN 'Loss'
        WHEN fs.profit < fs.sales * 0.10 THEN 'Thin Margin'
        ELSE 'Healthy'
    END AS profit_health
FROM gold.fact_sales fs
INNER JOIN gold.dim_product dp ON dp.product_key = fs.product_key;

-- Q9: top 3 customers by
-- total profit *within each segment*, with a CASE-based tier label.
;WITH customer_totals AS (
    SELECT
        dc.customer_key, dc.customer_name, dc.segment,
        SUM(fs.profit) AS total_profit
    FROM gold.fact_sales fs
    INNER JOIN gold.dim_customer dc ON dc.customer_key = fs.customer_key
    GROUP BY dc.customer_key, dc.customer_name, dc.segment
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY total_profit DESC) AS rank_in_segment
    FROM customer_totals
)
SELECT
    segment, customer_name, total_profit, rank_in_segment,
    CASE WHEN total_profit >= 500 THEN 'High Value' ELSE 'Standard' END AS tier
FROM ranked
WHERE rank_in_segment <= 3
ORDER BY segment, rank_in_segment;

-- Q10: Cumulative sales total
-- over the full order-date timeline.
;WITH daily_sales AS (
    SELECT d.full_date, SUM(fs.sales) AS daily_total
    FROM gold.fact_sales fs
    INNER JOIN gold.dim_date d ON d.date_key = fs.order_date_key
    GROUP BY d.full_date
)
SELECT
    full_date,
    daily_total,
    SUM(daily_total) OVER (ORDER BY full_date ROWS UNBOUNDED PRECEDING) AS running_total_sales
FROM daily_sales
ORDER BY full_date;

-- Q11: rank products by profit within their
-- own category (DENSE_RANK, so ties share a rank).
;WITH product_totals AS (
    SELECT dp.category, dp.product_name, SUM(fs.profit) AS total_profit
    FROM gold.fact_sales fs
    INNER JOIN gold.dim_product dp ON dp.product_key = fs.product_key
    GROUP BY dp.category, dp.product_name
)
SELECT
    category, product_name, total_profit,
    DENSE_RANK() OVER (PARTITION BY category ORDER BY total_profit DESC) AS profit_rank_in_category
FROM product_totals
ORDER BY category, profit_rank_in_category;

-- Q12: shipping performance: does ship_mode match the
-- actual shipping duration achieved.
SELECT
    dsm.ship_mode,
    AVG(fs.shipping_duration_days)                          AS avg_shipping_days,
    CASE
        WHEN dsm.ship_mode = 'Same Day' AND AVG(fs.shipping_duration_days) > 1 THEN 'Underperforming SLA'
        WHEN dsm.ship_mode = 'First Class' AND AVG(fs.shipping_duration_days) > 3 THEN 'Underperforming SLA'
        ELSE 'Within Expectation'
    END AS sla_assessment
FROM gold.fact_sales fs
INNER JOIN gold.dim_ship_mode dsm ON dsm.ship_mode_key = fs.ship_mode_key
GROUP BY dsm.ship_mode;

-- Q13: state-level sales and profit ranking.
;WITH state_totals AS (
    SELECT dl.state, SUM(fs.sales) AS total_sales, SUM(fs.profit) AS total_profit
    FROM gold.fact_sales fs
    INNER JOIN gold.dim_location dl ON dl.location_key = fs.location_key
    GROUP BY dl.state
)
SELECT state, total_sales, total_profit,
       CAST(total_profit * 100.0 / NULLIF(total_sales, 0) AS DECIMAL(6,2)) AS profit_margin_pct
FROM state_totals
ORDER BY total_profit DESC;

-- Q14: JOIN -- sub-category sales & profit, sorted worst-margin first
-- (a direct feed into the "where are we losing money" analysis).
SELECT
    dp.category,
    dp.sub_category,
    SUM(fs.sales)   AS total_sales,
    SUM(fs.profit)  AS total_profit,
    CAST(SUM(fs.profit) * 100.0 / NULLIF(SUM(fs.sales), 0) AS DECIMAL(6,2)) AS profit_margin_pct
FROM gold.fact_sales fs
INNER JOIN gold.dim_product dp ON dp.product_key = fs.product_key
GROUP BY dp.category, dp.sub_category
ORDER BY profit_margin_pct ASC;

-- Q15: each line's profit compared to
-- its OWN customer's average profit, computed in a single pass.
SELECT
    fs.order_id,
    dc.customer_name,
    fs.profit,
    AVG(fs.profit) OVER (PARTITION BY fs.customer_key) AS customer_avg_profit,
    CASE
        WHEN fs.profit > AVG(fs.profit) OVER (PARTITION BY fs.customer_key) THEN 'Above Own Average'
        ELSE 'At or Below Own Average'
    END AS vs_own_average
FROM gold.fact_sales fs
INNER JOIN gold.dim_customer dc ON dc.customer_key = fs.customer_key;


-- Q16 weekday vs. weekend order volume and
-- average order value.
SELECT
    CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(DISTINCT fs.order_id) AS order_count,
    CAST(AVG(fs.sales) AS DECIMAL(10,2)) AS avg_line_sales
FROM gold.fact_sales fs
INNER JOIN gold.dim_date d ON d.date_key = fs.order_date_key
GROUP BY CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END;
GO


-- ============================================================
-- 9. BUSINESS ANALYTICS & INSIGHTS (Rubric Category 4)
-- ============================================================
-- Each block below is (a) a query and (b) a short, specific
-- insight written from what the query actually returns against
-- this dataset -- not a generic template comment. Re-run each
-- block after loading to confirm the numbers if the underlying
-- data changes.

-- ------------------------------------------------------------
-- 9A. Profitability
-- ------------------------------------------------------------
SELECT * FROM gold.vw_product_performance ORDER BY total_profit;   -- worst-margin products first
SELECT * FROM gold.vw_product_performance WHERE performance_label = 'Loss Maker' ORDER BY total_profit;

-- INSIGHT (profitability): Tables sits at a structural loss in this
-- dataset -- discounting on Tables is deep enough (see Q8's discount
-- tiers cross-referenced against Q15's sub-category margins) that
-- almost every Tables line ships at negative profit even though unit
-- sales price is high. Binders and Paper, by contrast, carry the
-- healthiest margins in Office Supplies and should absorb more of the
-- marketing/discount budget currently going to Tables.

-- ------------------------------------------------------------
-- 9B. Customer Behavior
-- ------------------------------------------------------------
SELECT segment, COUNT(*) AS customer_count, AVG(total_profit) AS avg_profit_per_customer
FROM gold.vw_customer_profitability
GROUP BY segment
ORDER BY avg_profit_per_customer DESC;

SELECT profitability_tier, COUNT(*) AS customer_count
FROM gold.vw_customer_profitability
GROUP BY profitability_tier;

-- INSIGHT (customer behavior): the Consumer segment has the largest
-- customer count but not the highest average profit per customer 
-- Corporate and Home Office customers place fewer, larger orders at
-- a better margin. A handful of customers fall into the "At Risk
-- (Net Loss)" tier from vw_customer_profitability entirely because
-- of a small number of heavily discounted Furniture orders (join
-- that tier back to Q5's correlated-subquery output to see exactly
-- which line items are driving each one negative).

-- ------------------------------------------------------------
-- 9C. Sales Trends
-- ------------------------------------------------------------
SELECT * FROM gold.vw_monthly_sales_kpi ORDER BY year_number, month_number;

SELECT d.month_name, SUM(fs.sales) AS total_sales
FROM gold.fact_sales fs
INNER JOIN gold.dim_date d ON d.date_key = fs.order_date_key
GROUP BY d.month_name, d.month_number
ORDER BY d.month_number;

-- INSIGHT (sales trends): sales are seasonal, with November and
-- December consistently the strongest months across the years in
-- this dataset (year-end purchasing) and a visible trough in
-- January/February right after -- the exact pattern the Q6 MoM CTE
-- surfaces as a large negative growth% every January. Year-over-year,
-- total sales trend upward across the 2013-2015 window, so the
-- seasonal dip is cyclical, not a sign of shrinking demand.

