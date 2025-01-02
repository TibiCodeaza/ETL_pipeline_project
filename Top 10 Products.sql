SELECT 
    c.name AS customer_name,
    COUNT(s.transaction_id) AS purchase_count
FROM sales s
JOIN customers c ON s.customer_id = c.customer_id
WHERE s.transaction_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY c.name
HAVING COUNT(s.transaction_id) > 5;
