-- Validation Queries for Delta Lake Tables (Run in Databricks SQL)

-- Row counts per layer
SELECT 'Silver Customers' as layer, COUNT(*) as row_count FROM delta.`/silver/customers`;
SELECT 'Silver Loans' as layer, COUNT(*) as row_count FROM delta.`/silver/loans`;
SELECT 'Silver Transactions' as layer, COUNT(*) as row_count FROM delta.`/silver/transactions`;
SELECT 'Gold Analytics' as layer, COUNT(*) as row_count FROM delta.`/gold/customer_analytics`;

-- Data Quality Checks
SELECT COUNT(*) as null_customers FROM delta.`/silver/customers` WHERE CustomerID IS NULL;

-- Gold Analytics Summary
SELECT 
    Province,
    COUNT(*) as customer_count,
    AVG(CreditScore) as avg_credit_score,
    SUM(TotalLoanAmount) as total_loans,
    AVG(AvgTransactionAmount) as avg_transaction,
    COUNT(CASE WHEN HasDefaulted = true THEN 1 END) as defaults
FROM delta.`/gold/customer_analytics`
GROUP BY Province
ORDER BY total_loans DESC;
