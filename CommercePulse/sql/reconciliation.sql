-- Run against equivalent Gold/Silver views in a SQL-capable environment.
-- Reconciliation target: Gold totals should equal aggregates from active Silver state.
SELECT
  CAST(transaction_date AS DATE) AS business_date,
  product_id,
  SUM(quantity * unit_price) AS expected_gross_revenue,
  COUNT(DISTINCT transaction_id) AS expected_transaction_count,
  SUM(quantity) AS expected_units_sold
FROM silver_transactions
WHERE UPPER(event_type) <> 'REVERSAL'
GROUP BY CAST(transaction_date AS DATE), product_id;
