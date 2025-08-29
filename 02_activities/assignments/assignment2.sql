/* ASSIGNMENT 2 */
/* SECTION 2 */

-- COALESCE
/* 1. Our favourite manager wants a detailed long list of products, but is afraid of tables! 
We tell them, no problem! We can produce a list with all of the appropriate details. */

-- First, let's find the NULLs in the product table
SELECT 
    product_name,
    product_size,
    product_qty_type
FROM product
WHERE product_size IS NULL OR product_qty_type IS NULL;

-- Now fix the NULLs using COALESCE
SELECT 
    product_name || ', ' || COALESCE(product_size, '') || ' (' || COALESCE(product_qty_type, 'unit') || ')'
FROM product;

-- Windowed Functions
/* 1. Write a query that selects from the customer_purchases table and numbers each customer's  
visits to the farmer's market (labeling each market date with a different number). */

-- Approach 1: Using ROW_NUMBER() - shows all rows with counter
SELECT 
    customer_id,
    market_date,
    product_id,
    quantity,
    cost_to_customer_per_qty,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY market_date) as visit_number
FROM customer_purchases
ORDER BY customer_id, market_date;

-- Approach 2: Using DENSE_RANK() - unique market dates per customer only
SELECT DISTINCT
    customer_id,
    market_date,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY market_date) as visit_number
FROM customer_purchases
ORDER BY customer_id, market_date;

/* 2. Reverse the numbering of the query from a part so each customer's most recent visit is labeled 1, 
then write another query that uses this one as a subquery (or temp table) and filters the results to 
only the customer's most recent visit. */

-- Subquery with reversed numbering (most recent visit = 1)
WITH reversed_visits AS (
    SELECT 
        customer_id,
        market_date,
        product_id,
        quantity,
        cost_to_customer_per_qty,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY market_date DESC) as reverse_visit_number
    FROM customer_purchases
)
SELECT *
FROM reversed_visits
WHERE reverse_visit_number = 1
ORDER BY customer_id;

/* 3. Using a COUNT() window function, include a value along with each row of the 
customer_purchases table that indicates how many different times that customer has purchased that product_id. */

SELECT 
    customer_id,
    market_date,
    product_id,
    quantity,
    cost_to_customer_per_qty,
    COUNT(*) OVER (PARTITION BY customer_id, product_id) as times_purchased_this_product
FROM customer_purchases
ORDER BY customer_id, product_id, market_date;

-- String manipulations
/* 1. Some product names in the product table have descriptions like "Jar" or "Organic". 
These are separated from the product name with a hyphen. */

SELECT 
    product_name,
    CASE 
        WHEN INSTR(product_name, '-') > 0 
        THEN TRIM(SUBSTR(product_name, INSTR(product_name, '-') + 1))
        ELSE NULL 
    END as description
FROM product
ORDER BY product_name;

/* 2. Filter the query to show any product_size value that contain a number with REGEXP. */

SELECT 
    product_id,
    product_name,
    product_size
FROM product
WHERE product_size REGEXP '[0-9]'
ORDER BY product_name;

-- UNION
/* 1. Using a UNION, write a query that displays the market dates with the highest and lowest total sales. */

-- Step 1: CTE to find sales values grouped by dates
WITH daily_sales AS (
    SELECT 
        market_date,
        SUM(quantity * cost_to_customer_per_qty) as total_sales
    FROM customer_purchases
    GROUP BY market_date
),
-- Step 2: CTE with rank windowed function
ranked_sales AS (
    SELECT 
        market_date,
        total_sales,
        RANK() OVER (ORDER BY total_sales DESC) as best_rank,
        RANK() OVER (ORDER BY total_sales ASC) as worst_rank
    FROM daily_sales
)
-- Step 3: Query with UNION for best and worst days
SELECT 
    market_date,
    total_sales,
    'Highest Sales Day' as sales_type
FROM ranked_sales
WHERE best_rank = 1

UNION

SELECT 
    market_date,
    total_sales,
    'Lowest Sales Day' as sales_type
FROM ranked_sales
WHERE worst_rank = 1
ORDER BY total_sales DESC;

/* SECTION 3 */

-- Cross Join
/*1. Suppose every vendor in the `vendor_inventory` table had 5 of each of their products to sell to **every** 
customer on record. How much money would each vendor make per product? */

SELECT 
    v.vendor_name,
    p.product_name,
    COUNT(c.customer_id) * 5 * vi.original_price as total_revenue_per_product
FROM vendor_inventory vi
JOIN vendor v ON vi.vendor_id = v.vendor_id
JOIN product p ON vi.product_id = p.product_id
CROSS JOIN (
    SELECT DISTINCT customer_id 
    FROM customer
) c
GROUP BY v.vendor_name, p.product_name, vi.original_price
ORDER BY v.vendor_name, p.product_name;

-- INSERT
/*1. Create a new table "product_units". 
This table will contain only products where the `product_qty_type = 'unit'`. */

CREATE TABLE product_units AS
SELECT 
    product_id,
    product_name,
    product_size,
    product_category_id,
    product_qty_type,
    CURRENT_TIMESTAMP as snapshot_timestamp
FROM product
WHERE product_qty_type = 'unit';

/*2. Using `INSERT`, add a new row to the product_units table (with an updated timestamp). */

INSERT INTO product_units (product_id, product_name, product_size, product_category_id, product_qty_type, snapshot_timestamp)
VALUES (999, 'Apple Pie', 'Large', 1, 'unit', CURRENT_TIMESTAMP);

-- DELETE
/* 1. Delete the older record for the whatever product you added. */

-- First, let's see what we have for Apple Pie
SELECT * FROM product_units WHERE product_name = 'Apple Pie' ORDER BY snapshot_timestamp;

-- Delete the older record (keeping the most recent one)
DELETE FROM product_units 
WHERE product_name = 'Apple Pie' 
AND snapshot_timestamp < (
    SELECT MAX(snapshot_timestamp) 
    FROM (SELECT snapshot_timestamp FROM product_units WHERE product_name = 'Apple Pie') as temp
);

-- UPDATE
/* 1. We want to add the current_quantity to the product_units table. */

-- First, add the new column
ALTER TABLE product_units
ADD current_quantity INT;

-- Then update with the last quantity value from vendor_inventory
UPDATE product_units 
SET current_quantity = COALESCE(
    (SELECT vi.quantity 
     FROM vendor_inventory vi 
     WHERE vi.product_id = product_units.product_id 
     ORDER BY vi.market_date DESC 
     LIMIT 1), 
    0)
WHERE product_units.product_id IS NOT NULL;

-- Alternative approach using window functions for the "last" quantity
UPDATE product_units 
SET current_quantity = COALESCE(
    (SELECT latest_qty
     FROM (
         SELECT 
             product_id,
             quantity as latest_qty,
             ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY market_date DESC) as rn
         FROM vendor_inventory
     ) ranked_inventory
     WHERE ranked_inventory.product_id = product_units.product_id 
     AND rn = 1),
    0)
WHERE product_units.product_id IS NOT NULL;

-- Verification queries to check our work
SELECT * FROM product_units ORDER BY product_name;

-- Check for any NULL current_quantity values
SELECT 
    product_name,
    current_quantity,
    CASE WHEN current_quantity IS NULL THEN 'NULL VALUE FOUND' ELSE 'OK' END as status
FROM product_units
ORDER BY product_name;