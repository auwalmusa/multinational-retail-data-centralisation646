select * from dim_card_details;

select * from dim_date_times;

select * from dim_products;

select * from dim_store_details;

select * from dim_users;

select * from orders_table;


--- How many stores does the business have and in which country
SELECT country_code AS country, 
       COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;


---which locations currently have the most stores?

SELECT locality, 
       COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 10;

-- which months produced the largest amount of sales?
SELECT 
    dt.month AS month,
    SUM(o.product_quantity * p.product_price::NUMERIC) AS total_sales
FROM 
    orders_table o
JOIN 
    dim_date_times dt ON o.date_uuid = dt.date_uuid
JOIN 
    dim_products p ON o.product_code = p.product_code
GROUP BY 
    dt.month
ORDER BY 
    total_sales DESC;


--how many sales are coming online?

SELECT 
    COUNT(o.index) AS numbers_of_sales,
    SUM(o.product_quantity) AS product_quantity_count,
    CASE
        WHEN s.store_type = 'Web' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM 
    orders_table o
JOIN 
    dim_store_details s ON o.store_code = s.store_code
GROUP BY 
    location;



UPDATE dim_products
SET product_price = REGEXP_REPLACE(product_price, '[^0-9\.]', '', 'g')
WHERE product_price IS NOT NULL;

--- what percentage of sales come through each type of store

SELECT 
    s.store_type,
    SUM(o.product_quantity * p.product_price::NUMERIC) AS total_sales,
    ROUND(100.0 * COUNT(o.index) / SUM(COUNT(o.index)) OVER (), 2) AS sales_made_percentage
FROM 
    orders_table o
JOIN 
    dim_store_details s ON o.store_code = s.store_code
JOIN 
    dim_products p ON o.product_code = p.product_code
GROUP BY 
    s.store_type
ORDER BY 
    total_sales DESC;

--- Which month in each year produced the highest cost of sales

SELECT 
    year,
    month,
    SUM(o.product_quantity * p.product_price) AS total_sales
FROM 
    orders_table o
JOIN 
    dim_date_times d ON o.date_uuid = d.date_uuid
JOIN 
    dim_products p ON o.product_code = p.product_code
GROUP BY 
    year, month
ORDER BY 
    year, total_sales DESC;

-- what is our staff headcount?
SELECT 
    country_code,
    SUM(CAST(staff_numbers AS INTEGER)) AS total_staff_numbers
FROM 
    dim_store_details
GROUP BY 
    country_code
ORDER BY 
    total_staff_numbers DESC;

-- what is our staff headcount?
SELECT 
    country_code,
    SUM(CAST(staff_numbers AS INTEGER)) AS total_staff_numbers
FROM 
    dim_store_details
WHERE 
    staff_numbers ~ '^[0-9]+$' -- Filters rows where staff_numbers contain only digits
GROUP BY 
    country_code
ORDER BY 
    total_staff_numbers DESC;

-- which german store type is selling the most?
SELECT 
    dim_store_details.store_type, 
    dim_store_details.country_code, 
    SUM(dim_products.product_price::NUMERIC * orders_table.product_quantity) AS total_sales
FROM 
    orders_table
JOIN 
    dim_store_details 
    ON orders_table.store_code = dim_store_details.store_code
JOIN 
    dim_products 
    ON orders_table.product_code = dim_products.product_code
WHERE 
    dim_store_details.country_code = 'DE' -- Filter for Germany
GROUP BY 
    dim_store_details.store_type, dim_store_details.country_code
ORDER BY 
    total_sales DESC;

--- How quickly is the company making sales?

select * from orders_table;

select * from dim_date_times;



ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE TEXT USING date_uuid::TEXT;

--Average Time Between Sales
WITH time_differences AS (
    SELECT 
        EXTRACT(YEAR FROM timestamp) AS year,
        EXTRACT(EPOCH FROM timestamp) - LAG(EXTRACT(EPOCH FROM timestamp)) 
            OVER (PARTITION BY EXTRACT(YEAR FROM timestamp) ORDER BY timestamp) AS time_difference
    FROM dim_date_times
    WHERE timestamp IS NOT NULL
)
SELECT 
    year,
    AVG(time_difference) AS avg_time_seconds
FROM time_differences
WHERE time_difference IS NOT NULL
GROUP BY year
ORDER BY year;



---
SELECT 
    EXTRACT(YEAR FROM timestamp) AS year, 
    COUNT(*) AS count
FROM 
    dim_date_times
WHERE 
    timestamp IS NOT NULL
GROUP BY 
    EXTRACT(YEAR FROM timestamp)
ORDER BY 
    year;



SELECT timestamp, COUNT(*) 
FROM dim_date_times
GROUP BY timestamp
HAVING COUNT(*) > 1
ORDER BY timestamp;



SELECT COUNT(*) AS null_count
FROM dim_date_times
WHERE timestamp IS NULL;



