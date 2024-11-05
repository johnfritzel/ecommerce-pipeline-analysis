USE ecommerce;

-- DATA STRUCTURE OVERVIEW -- 
DESCRIBE customers;
DESCRIBE geolocation;
DESCRIBE orders;
DESCRIBE items;
DESCRIBE payments;
DESCRIBE reviews;
DESCRIBE products;
DESCRIBE sellers;
DESCRIBE product_categories;

-- DATA RANGE AND DISTRIBUTION --
-- Date range in 'orders' table
SELECT 
    MIN(order_purchase_timestamp) AS earliest_purchase,
    MAX(order_purchase_timestamp) AS latest_purchase
FROM orders;

-- Price range in 'items' table
SELECT 
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    AVG(price) AS avg_price
FROM items;

-- CATEGORY AND STATE DISTRIBUTIONS --
-- Product category distribution
SELECT 
    product_category_name,
    COUNT(*) AS product_count
FROM products
GROUP BY product_category_name
ORDER BY product_count DESC;

-- Order status distribution
SELECT 
    order_status,
    COUNT(*) AS order_count
FROM orders
GROUP BY order_status;

-- JOIN ANALYSIS --
-- Customer Location
SELECT 
    c.customer_id,
    c.customer_city,
    g.geolocation_lat,
    g.geolocation_lng
FROM customers c
LEFT JOIN geolocation g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix;

-- Order Details
SELECT 
    o.order_id,
    o.order_status,
    i.price,
    p.payment_type,
    p.payment_value
FROM orders o
LEFT JOIN items i ON o.order_id = i.order_id
LEFT JOIN payments p ON o.order_id = p.order_id;

-- Review and Delivery Times
SELECT 
    o.order_id,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date) AS delivery_delay,
    r.review_score
FROM orders o
LEFT JOIN reviews r ON o.order_id = r.order_id
WHERE o.order_delivered_customer_date IS NOT NULL;

-- AGGREGATED ANALYSIS --
/**
	Aggregate data to identify patterns in pricing, shipment costs, and reviews
**/
-- Average price and freight by product
SELECT 
    product_id,
    AVG(price) AS avg_price,
    AVG(freight_value) AS avg_freight
FROM items
GROUP BY product_id;

-- Order Count and Revenue by Customer
SELECT 
    c.customer_id,
    COUNT(o.order_id) AS total_orders,
    SUM(i.price) AS total_revenue
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
LEFT JOIN items i ON o.order_id = i.order_id
GROUP BY c.customer_id;

-- INSIGHTS ON PRODUCT DIMENSIONS --
/**
	Analyze product dimensions for shipping efficiency and costs
**/
SELECT 
    product_category_name,
    AVG(product_weight_g) AS avg_weight,
    AVG(product_length_cm) AS avg_length,
    AVG(product_height_cm) AS avg_height,
    AVG(product_width_cm) AS avg_width
FROM products
GROUP BY product_category_name;

-- CUSTOMER REVIEW ANALYSIS --
/**
	Calculate average review scores and frequency of reviews per customer
**/
SELECT 
    r.review_score,
    COUNT(*) AS review_count
FROM reviews r
GROUP BY r.review_score;

SELECT 
    o.customer_id,
    AVG(r.review_score) AS avg_review_score,
    COUNT(r.review_id) AS total_reviews
FROM orders o
LEFT JOIN reviews r ON o.order_id = r.order_id
GROUP BY o.customer_id;

-- CUSTOMER ORDER FREQUENCY AND RECENCY ANALYSIS --
/**
	Analyze customer purchasing behavior in terms of frequency and recency
**/
-- Frequency: Count of orders per customer
SELECT 
    customer_id,
    COUNT(order_id) AS order_count
FROM orders
GROUP BY customer_id
ORDER BY order_count DESC;

-- Recency: Days since the last purchase for each customer
SELECT 
    customer_id,
    DATEDIFF(CURDATE(), MAX(order_purchase_timestamp)) AS days_since_last_order
FROM orders
GROUP BY customer_id
ORDER BY days_since_last_order;

-- PRODUCT POPULARITY AND REVENUE CONTRIBUTION --
/**
	Identify top-selling products by order frequency and revenue generated
**/
-- Product popularity by sales volume
SELECT 
    product_id,
    COUNT(order_item_id) AS total_sold,
    SUM(price) AS total_revenue
FROM items
GROUP BY product_id
ORDER BY total_sold DESC
LIMIT 10;

-- Product revenue contribution
SELECT 
    items.product_id,
    product_category_name,
    SUM(price * order_item_id) AS revenue_contribution
FROM items
JOIN products ON items.product_id = products.product_id
GROUP BY product_id, product_category_name
ORDER BY revenue_contribution DESC;

-- CUSTOMER RETENTION BY STATE --
/**
	Evaluate customer distribution and retention across states
**/
-- Orders by customer state
SELECT 
    c.customer_state,
    COUNT(o.order_id) AS total_orders,
    COUNT(DISTINCT c.customer_id) AS unique_customers
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_state
ORDER BY total_orders DESC;

-- Average order value by state
SELECT 
    c.customer_state,
    AVG(i.price) AS avg_order_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN items i ON o.order_id = i.order_id
GROUP BY c.customer_state
ORDER BY avg_order_value DESC;

-- SHIPPING TIMELINES AND DELAYS --
/**
	Calculate the difference between actual delivery dates and estimated delivery dates to identify trends in delays
**/
-- Delivery delays
SELECT 
    order_id,
    DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) AS delay_days
FROM orders
WHERE order_delivered_customer_date IS NOT NULL;

-- Average delay by state
SELECT 
    c.customer_state,
    AVG(DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date)) AS avg_delay_days
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_delivered_customer_date IS NOT NULL
GROUP BY c.customer_state
ORDER BY avg_delay_days DESC;

-- PAYMENT METHOD PREFERENCE AND INSTALLMENTS --
/**
	Investigate which payment types and installment counts are most popular
**/
-- Payment type distribution
SELECT 
    payment_type,
    COUNT(*) AS payment_count,
    AVG(payment_value) AS avg_payment_value
FROM payments
GROUP BY payment_type
ORDER BY payment_count DESC;

-- Installment preference by payment type
SELECT 
    payment_type,
    payment_installments,
    COUNT(*) AS count
FROM payments
GROUP BY payment_type, payment_installments
ORDER BY count DESC;

-- REVIEW SCORE ANALYSIS --
/**
	Examine the distribution of review scores and their relationship to order delays and delivery
**/
-- Distribution of review scores
SELECT 
    review_score,
    COUNT(*) AS review_count
FROM reviews
GROUP BY review_score
ORDER BY review_score DESC;

-- Average delivery delay by review score
SELECT 
    r.review_score,
    AVG(DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date)) AS avg_delivery_delay
FROM reviews r
JOIN orders o ON r.order_id = o.order_id
WHERE o.order_delivered_customer_date IS NOT NULL
GROUP BY r.review_score
ORDER BY r.review_score DESC;

-- CUSTOMER-PRODUCT INERACTION --
/**
	Analyze how often customers buy specific categories of products
**/
SELECT 
    c.customer_id,
    p.product_category_name,
    COUNT(i.order_item_id) AS total_purchases
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN items i ON o.order_id = i.order_id
JOIN products p ON i.product_id = p.product_id
GROUP BY c.customer_id, p.product_category_name
ORDER BY total_purchases DESC;

-- HIGH-VALUE CUSTOMERS --
/**
	Identify customers with the highest spending across all orders
**/
SELECT 
    c.customer_id,
    SUM(i.price) AS total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN items i ON o.order_id = i.order_id
GROUP BY c.customer_id
ORDER BY total_spent DESC
LIMIT 10;

-- PRODUCT SIZE AND WEIGHT INFLUENCE ON SHIPPING COST --
/**
	Examine how product dimensions and weight affect the freight value in shipping
**/
SELECT 
    p.product_id,
    AVG(p.product_weight_g) AS avg_weight,
    AVG(p.product_length_cm) AS avg_length,
    AVG(p.product_height_cm) AS avg_height,
    AVG(p.product_width_cm) AS avg_width,
    AVG(i.freight_value) AS avg_freight_value
FROM products p
JOIN items i ON p.product_id = i.product_id
GROUP BY p.product_id
ORDER BY avg_freight_value DESC;

-- TIME SERIES ANALYSIS OF MONTHLY SALES TRENDS --
/**
	Calculate monthly sales trends to analyze seasonality
**/
SELECT 
    DATE_FORMAT(order_purchase_timestamp, '%Y-%m') AS month,
    COUNT(i.order_id) AS total_orders,
    SUM(i.price) AS total_revenue
FROM orders o
JOIN items i ON o.order_id = i.order_id
GROUP BY month
ORDER BY month;

-- PRICE VS REVIEW SCORE ANALYSIS --
/**
	Analyze the relationship between product prices and customer review scores
**/
SELECT 
    i.price,
    AVG(r.review_score) AS avg_review_score
FROM items i
JOIN reviews r ON i.order_id = r.order_id
GROUP BY i.price
ORDER BY i.price;

-- MULTI-PRODUCT ORDERS --
/**
	Find orders with multiple items to understand bundling and potential upsell opportunities
**/
SELECT 
    order_id,
    COUNT(order_item_id) AS items_in_order,
    SUM(price) AS order_total
FROM items
GROUP BY order_id
HAVING items_in_order > 1
ORDER BY items_in_order DESC;

-- PRODUCT REVIEW TIMING --
/**
	Analyze the time between order delivery and review creation
**/
SELECT 
    r.review_id,
    r.order_id,
    DATEDIFF(r.review_creation_date, o.order_delivered_customer_date) AS days_to_review
FROM reviews r
JOIN orders o ON r.order_id = o.order_id
WHERE o.order_delivered_customer_date IS NOT NULL;
