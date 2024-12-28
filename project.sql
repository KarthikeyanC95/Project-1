create database retail_order;
use retail_order;

SELECT * FROM retail_order.order_details;
SELECT * FROM retail_order.product_details;

UPDATE retail_order.product_details
SET Discount = ROUND(Discount, 2), Sale_price = ROUND(Sale_price, 2), Profit = ROUND(Profit, 2);

ALTER TABLE retail_order.order_details ADD PRIMARY KEY (order_id);

ALTER TABLE retail_order.product_details ADD foreign key (order_id) REFERENCES retail_order.order_details (order_id);

# First query
SELECT p.product_id, o.category, p.Sub_Category, SUM(p.Quantity * p.sale_price) AS revenue
FROM retail_order.product_details p
join retail_order.order_details o on p.order_id = o.order_id 
GROUP BY p.product_id, o.category, p.Sub_Category
ORDER BY revenue DESC
LIMIT 10;

# second query
SELECT o.City, SUM((p.Quantity * p.sale_price) - p.Cost_price) / SUM(p.Quantity * p.sale_price) * 100 AS profit_margin
FROM retail_order.product_details p
join retail_order.order_details o on p.order_id = o.order_id 
GROUP BY o.City
ORDER BY profit_margin DESC
LIMIT 5;

#third query
SELECT o.Category, sum(p.Discount), sum(Discount_percent)
FROM retail_order.product_details p
join retail_order.order_details o on p.order_id = o.order_id 
GROUP BY o.Category
ORDER BY 1 DESC;

#fourth query
SELECT o.Category, avg(p.sale_price)
FROM retail_order.product_details p
join retail_order.order_details o on p.order_id = o.order_id 
GROUP BY o.Category
ORDER BY 1 DESC;

#fifth query
SELECT o.region, avg(p.sale_price)
FROM retail_order.product_details p
join retail_order.order_details o on p.order_id = o.order_id 
GROUP BY o.region
ORDER BY 2 DESC;

#sixth query
SELECT o.Category, sum(p.Profit)
FROM retail_order.product_details p
join retail_order.order_details o on p.order_id = o.order_id 
GROUP BY o.Category
ORDER BY 1 DESC;

#siventh query
SELECT o.segment, sum(p.Quantity)
FROM retail_order.product_details p
join retail_order.order_details o on p.order_id = o.order_id 
GROUP BY o.segment
ORDER BY 2 DESC
limit 3;

#eighth query
SELECT o.region, avg(p.Discount_Percent)
FROM retail_order.product_details p
join retail_order.order_details o on p.order_id = o.order_id 
GROUP BY o.region
ORDER BY 2 DESC;

#nineth query
SELECT o.Category, p.Profit
FROM retail_order.product_details p
join retail_order.order_details o on p.order_id = o.order_id 
#GROUP BY o.Category
ORDER BY 2 DESC;

#tenth query
SELECT 
    YEAR(o.Order_Date) AS year,
    SUM(Sale_price) AS total_revenue
FROM retail_order.product_details p
join retail_order.order_details o on p.order_id = o.order_id 
WHERE o.Order_Date BETWEEN '2022-01-01' AND '2023-12-31'
GROUP BY YEAR(o.Order_Date)
ORDER BY year;

#eleventh query
SELECT 
    p.product_id,
    SUM(p.Quantity * p.Sale_price) AS total_revenue
FROM retail_order.product_details p
GROUP BY p.product_id
ORDER BY total_revenue DESC
LIMIT 10;

#twelveth query
SELECT 
    p.product_id,
    SUM(p.Quantity * p.Sale_price) AS total_revenue
FROM retail_order.product_details p
join retail_order.order_details o on p.order_id = o.order_id 
WHERE o.Order_Date BETWEEN '2022-01-01' AND '2023-12-31'
GROUP BY p.product_id
ORDER BY total_revenue DESC
LIMIT 10;

#threenth query
SELECT
    MONTH(o.Order_Date) AS month,
    YEAR(o.Order_Date) AS year,
    SUM(p.Sale_price) AS total_sales,
    -- Calculate the sales from the previous year
    LAG(SUM(p.Sale_price)) OVER (PARTITION BY MONTH(o.Order_Date) ORDER BY YEAR(o.Order_Date)) AS last_year_sales,
    -- Calculate the YoY growth percentage
    IFNULL(ROUND(
        (SUM(p.Sale_price) - LAG(SUM(p.Sale_price)) OVER (PARTITION BY MONTH(o.Order_Date) ORDER BY YEAR(o.Order_Date))) / 
        LAG(SUM(p.Sale_price)) OVER (PARTITION BY MONTH(o.Order_Date) ORDER BY YEAR(o.Order_Date)) * 100, 2), 0) AS yoy_growth_percentage
FROM retail_order.product_details p
join retail_order.order_details o on p.order_id = o.order_id 
where o.Order_Date BETWEEN '2022-01-01' AND '2023-12-31'
GROUP BY YEAR(o.Order_Date), MONTH(o.Order_Date)
ORDER BY month, year;

#fourteennth query
SELECT
    p.product_name,
    SUM(s.sale_amount) AS total_revenue,
    SUM(s.sale_amount) - SUM(s.quantity * p.cost_price) AS total_profit,
    SUM(s.sale_amount) / SUM(s.sale_amount) - 1 AS profit_margin,  -- Example profit margin calculation
    CASE
        WHEN (SUM(s.sale_amount) / SUM(s.sale_amount) - 1) > 0.2 THEN 'High Margin'
        WHEN (SUM(s.sale_amount) / SUM(s.sale_amount) - 1) BETWEEN 0.1 AND 0.2 THEN 'Medium Margin'
        ELSE 'Low Margin'
    END AS margin_category,
    ROW_NUMBER() OVER (ORDER BY SUM(s.sale_amount) DESC) AS product_rank
FROM
    sales s
JOIN
    products p ON s.product_id = p.product_id
GROUP BY
    p.product_id
HAVING
    total_revenue > 10000  -- Only show products with revenue above 10,000
ORDER BY
    product_rank;
