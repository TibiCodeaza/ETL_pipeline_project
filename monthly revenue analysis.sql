SELECT 
    DATE_TRUNC('month', s.transaction_date) AS month,
    p.category,
    SUM(s.quantity * p.price) AS total_revenue
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY month, p.category
ORDER BY month, p.category;
