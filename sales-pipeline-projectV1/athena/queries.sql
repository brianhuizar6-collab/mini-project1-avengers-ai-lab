-- Six analytical queries against the curated layer.
-- All run as-is in the Athena query editor once athena/ddl_curated.sql has been applied
-- and at least one pipeline run has landed data.

-- Q1. Monthly revenue trend
SELECT year, month,
       SUM(amount) AS revenue,
       COUNT(*)    AS orders
FROM curated.transactions
GROUP BY year, month
ORDER BY year, month;

-- Q2. Top 10 products by revenue
SELECT p.product_name,
       SUM(t.amount) AS revenue
FROM curated.transactions t
JOIN curated.products p ON t.product_id = p.product_id
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 10;

-- Q3. Top 20 customers by lifetime spend
SELECT c.customer_id, c.customer_name,
       SUM(t.amount) AS lifetime_spend,
       COUNT(*)      AS orders
FROM curated.transactions t
JOIN curated.customers c ON t.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY lifetime_spend DESC
LIMIT 20;

-- Q4. Average order value by product category
SELECT p.category,
       AVG(t.amount) AS avg_order_value,
       COUNT(*)      AS orders
FROM curated.transactions t
JOIN curated.products p ON t.product_id = p.product_id
GROUP BY p.category
ORDER BY avg_order_value DESC;

-- Q5. Month-over-month revenue growth
WITH monthly AS (
    SELECT year, month, SUM(amount) AS revenue
    FROM curated.transactions
    GROUP BY year, month
)
SELECT year, month, revenue,
       revenue - LAG(revenue) OVER (ORDER BY year, month) AS mom_change
FROM monthly
ORDER BY year, month;

-- Q6. Data-quality summary — reject rate by reason
SELECT reject_reason,
       COUNT(*) AS rows_rejected,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_all_rejects
FROM rejected.transactions
GROUP BY reject_reason
ORDER BY rows_rejected DESC;

-- Bonus: pipeline health across runs, straight from run_metrics
SELECT run_id, stage, raw_count, rejected_count, curated_count,
       ROUND(rejected_count * 100.0 / NULLIF(raw_count, 0), 2) AS pct_rejected,
       ts
FROM curated.run_metrics
ORDER BY ts DESC;
