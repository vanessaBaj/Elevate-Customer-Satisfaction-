-- preliminaries creating a schema
create schema tech_electro;
use tech_electro;

-- data exploration
select*
from External_factors limit 5;
select*
from Sales_data limit 5;
select*
from product_information limit 5;
select*
from External_factors limit 5;
select*
from inventory_data limit 5;

-- understanding the structure of the datasets
show columns from External_factors;
describe Sales_data ;
desc product_information;

-- data cleaning 
-- changing to the right data type for all columns 
-- external factors 
-- sales date, GPD DECIMAL(15,2), inlationrate DECIMAL(5,2), seasonalfactor DECIMAL(5,2)
-- Add a new column for the converted date

ALTER TABLE External_factors 
ADD COLUMN new_sales_date DATE;

-- Turn off safe updates to allow unrestricted update operations
SET SQL_SAFE_UPDATES = 0;

-- Convert and update the new_sales_date column with formatted dates
UPDATE External_factors 
SET new_sales_date = STR_TO_DATE(Sales_Date, '%d/%m/%Y');

-- Drop the old sales_date column
ALTER TABLE External_factors 
DROP COLUMN Sales_Date;

-- Rename the new_sales_date column to sales_date
ALTER TABLE External_factors 
CHANGE COLUMN new_sales_date Sales_Date DATE;

-- Optionally, turn safe updates back on for safety
SET SQL_SAFE_UPDATES = 1;

alter table external_factors
modify column GDP decimal(15, 2);

ALTER TABLE external_factors
MODIFY COLUMN Inflation_Rate DECIMAL(5, 2);

ALTER TABLE external_factors
MODIFY COLUMN Seasonal_Factor DECIMAL(5, 2);

select*
from external_factors limit 5;

show columns from external_factors;

-- product information
-- product_ID INT NOT NULL, product_category TEXT, promotions ENUM('YES','NO')

alter table product_information
add column new_promotions ENUM('yes', 'no');
update product_information
set new_promotions = case
when promotions = 'yes' then 'yes'
when promotions = 'no' then 'no'
else null
end;
alter table product_information
drop column Promotions;
alter table product_information
change column new_promotions promotions ENUM('yes', 'no');

describe product_information;

-- sales data
ALTER TABLE sales_data
ADD COLUMN new_sales_date DATE;
-- Convert and update the new_sales_date column with formatted dates
UPDATE sales_data
SET new_sales_date = STR_TO_DATE(Sales_Date, '%d/%m/%Y');
-- Drop the old sales_date column
ALTER TABLE sales_data
DROP COLUMN Sales_Date;
-- Rename the new_sales_date column to sales_date
ALTER TABLE sales_data
CHANGE COLUMN new_sales_date Sales_Date DATE;

desc sales_data;

SELECT*FROM external_factors;


-- checking for missing values
-- external factor 
SELECT 
    SUM(CASE WHEN Sales_Date IS NULL THEN 1 ELSE 0 END) AS missing_sales_date,
    SUM(CASE WHEN GDP IS NULL THEN 1 ELSE 0 END) AS missing_GDP,
    SUM(CASE WHEN Inflation_Rate IS NULL THEN 1 ELSE 0 END) AS missing_Inflation_Rate,
    SUM(CASE WHEN Seasonal_Factor IS NULL THEN 1 ELSE 0 END) AS missing_Seasonal_Factor
FROM external_factors;
-- product information
 SELECT 
    SUM(CASE WHEN product_ID IS NULL THEN 1 ELSE 0 END) AS missing_Product_ID,
    SUM(CASE WHEN Product_Category IS NULL THEN 1 ELSE 0 END) AS missing_Product_Category,
    SUM(CASE WHEN Promotions IS NULL THEN 1 ELSE 0 END) AS missing_Promotions
FROM product_information;
-- sales data
 SELECT 
    SUM(CASE WHEN product_ID IS NULL THEN 1 ELSE 0 END) AS missing_Product_ID,
    SUM(CASE WHEN Sales_Date IS NULL THEN 1 ELSE 0 END) AS missing_Sales_Date ,
    SUM(CASE WHEN Inventory_Quantity IS NULL THEN 1 ELSE 0 END) AS missing_Inventory_Quantity,
    SUM(CASE WHEN Poduct_Cost IS NULL THEN 1 ELSE 0 END) AS missing_Product_Cost
FROM sales_data;

-- checking for duplicate using group by and having clause and remove them if necessary
-- extrenal factor
select Sales_Date, count(*) as count
from external_factors
group by Sales_Date
having count >1;

select count(*) from (select Sales_Date, count(*) as count
from external_factors
group by Sales_Date
having count >1) as dup;

-- product information

SELECT Product_ID, Product_Category, COUNT(*) AS count
FROM product_information
GROUP BY Product_ID, Product_Category
HAVING COUNT(*) > 1;


select count(*) from (select Product_ID, Product_Category, count(*) as count
from product_information
group by Product_ID, Product_Category
having count >1) as dup;

-- sales data
SELECT Product_ID, Sales_Date, COUNT(*) AS count
FROM sales_data
GROUP BY Product_ID, Sales_Date
HAVING COUNT(*) > 1;

-- removing duplicates from external fsctors and product information
-- exteranl factor 
delete e1 from external_factors e1
inner join (
select Sales_Date,
row_number() over (partition by Sales_Date order by Sales_Date) as rn 
from external_factors 
) e2 on e1.Sales_Date = e2.Sales_Date
where e2.rn>1;

-- product information
delete p1 from product_information p1
inner join (
select Product_ID,
row_number() over (partition by Product_ID order by Product_ID) as rn 
from product_information
) p2 on p1.Product_ID = p2.Product_ID
where p2.rn  > 1;

-- data integration
-- sales and product data first
CREATE VIEW  Sales_Product_Data AS
SELECT 
s.Product_ID,
s.Sales_Date,
s.Inventory_Quantity,
s.Product_Cost,
p.Product_Category,
p.promotions
FROM sales_data s
JOIN product_information p ON s.Product_ID = p.Product_ID;

-- sales product data and external factors
CREATE VIEW  inventory_Data AS
SELECT 
sp.Product_ID,
sp.Sales_Date,
sp.Inventory_Quantity,
sp.Product_Cost,
sp.Product_Category,
sp.promotions,
e.GDP,
e.Inflation_Rate,
e.Seasonal_Factor
from Sales_Product_Data sp
left join external_factors e
on sp.Sales_Date = e.Sales_Date;

-- describtive analysis 
-- avg sales (calculate as the product of inventory quantity and product cost)
select Product_ID,
avg(Inventory_Quantity * Product_Cost)as avg_sales
from inventory_Data
group by  Product_ID
order by avg_sales desc;

-- medium stock levels (i.e, inventory quantity)
select Product_ID, avg(Inventory_Quantity) as medium_stock
from(
select Product_ID,
Inventory_Quantity,
row_number() over(partition by Product_ID order by Inventory_Quantity)as row_num_asc,
row_number() over(partition by Product_ID order by Inventory_Quantity desc) as row_num_desc
from inventory_Data
) as subquery
where row_num_asc in (row_num_desc - 1, row_num_desc + 1)
group by Product_ID;

-- calculate product performance
select Product_ID,
round(sum(Inventory_Quantity*Product_Cost)) as total_sales
from inventory_Data
group by Product_ID
order by total_sales desc;

-- identify high demand products based on average sales 
with highdemandproducts as (
select Product_ID, avg(Inventory_Quantity)as avg_sales
from inventory_Data
group by Product_ID
having avg_sales > (
select avg(Inventory_Quantity)*0.95 from sales_data
   )
)
-- calculate stockout frequency for high demand products
select s.Product_ID,
count(*) as stockout_frequency
from inventory_data s
where s.Product_ID in (select Product_ID from highdemandproducts)
group by s.Product_ID;

-- influence of external factors
-- GDP
select Product_ID,
avg(case when 'GDP' > 0 then Inventory_Quantity else null end) as avg_sales_positive_gdp,
avg(case when 'GDP' <= 0 then Inventory_Quantity else null end) as avg_sales_non_positive_gdp
from inventory_data
group by Product_ID
having avg_sales_positive_gdp is not null;

-- inflation
select Product_ID,
avg(case when Inflation_Rate > 0 then Inventory_Quantity else null end) as avg_sales_positive_Inflation,
avg(case when Inflation_Rate<= 0 then Inventory_Quantity else null end) as avg_sales_non_positive_Inflation
from inventory_data
group by Product_ID
having avg_sales_positive_Inflation is not null;

-- inventory optimization
-- Determine the optimal reorder point for each product based on historical sales data and external factors.
-- Reorder Point= Lead Time Demand + Safety Stock
-- Lead Time Demand = Rolling Average Sales X Lead Time
-- Safety Stock= Zx Lead Time^-2 XStandard Deviation of Demand
-- Z=1.645
-- A constant lead time of 7 days for all products.
-- We aim for a 95% service level.

WITH InventoryCalculations AS (
    SELECT 
        Product_ID,
        AVG(daily_sales) OVER (PARTITION BY Product_ID ORDER BY Sales_Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_sales,
        SQRT(AVG(squared_diff) OVER (PARTITION BY Product_ID ORDER BY Sales_Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS rolling_std_dev
    FROM (
        SELECT 
            Product_ID,
            Sales_Date, 
            Inventory_Quantity * Product_Cost AS daily_sales,
            POW(Inventory_Quantity * Product_Cost - 
                AVG(Inventory_Quantity * Product_Cost) OVER (PARTITION BY Product_ID ORDER BY Sales_Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS squared_diff
        FROM Inventory_data
    ) subquery
)
SELECT 
    Product_ID,
    AVG(rolling_avg_sales) * 7 AS lead_time_demand,
    1.645 * AVG(rolling_std_dev) * SQRT(7) AS safety_stock,
    (AVG(rolling_avg_sales) * 7) + (1.645 * AVG(rolling_std_dev) * SQRT(7)) AS reorder_point
FROM InventoryCalculations
GROUP BY Product_ID;

-- creating the inventory optimization table
CREATE TABLE inventory_optimization (
    Product_ID INT,
    reorder_point DOUBLE
);
-- create the store procedure to recalculate the recorder point 
DELIMITER //

CREATE PROCEDURE recalculateReorderPoint(IN in_Product_ID INT)
BEGIN
    DECLARE avg_rolling_sales DOUBLE;
    DECLARE avg_rolling_std_dev DOUBLE;
    DECLARE lead_time_demand DOUBLE;
    DECLARE safety_stock DOUBLE;
    DECLARE reorder_point DOUBLE;

    -- Calculate average rolling sales and standard deviation
    SELECT 
        AVG(rolling_avg_sales) AS avg_rolling_sales,
        SQRT(AVG(rolling_variance)) AS avg_rolling_std_dev
    INTO 
        avg_rolling_sales, 
        avg_rolling_std_dev
    FROM (
        SELECT 
            Product_ID,
            AVG(daily_sales) OVER (PARTITION BY Product_ID ORDER BY Sales_Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_sales,
            POW(
                AVG(daily_sales) OVER (PARTITION BY Product_ID ORDER BY Sales_Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) - 
                Inventory_Quantity * Product_Cost, 2
            ) AS rolling_variance
        FROM Inventory_data
        WHERE Product_ID = in_Product_ID
    ) AS subquery;

    -- Calculate lead time demand, safety stock, and reorder point
    SET lead_time_demand = avg_rolling_sales * 7;
    SET safety_stock = 1.645 * avg_rolling_std_dev * SQRT(7);
    SET reorder_point = lead_time_demand + safety_stock;

    -- Insert or update the reorder point in the inventory_optimization table
    INSERT INTO inventory_optimization (Product_ID, reorder_point)
    VALUES (in_Product_ID, reorder_point)
    ON DUPLICATE KEY UPDATE reorder_point = reorder_point;
END //
DELIMITER ;

-- make inventory data a permanet table
create table Inventory_table as select * from inventory_data;
-- create the triggers
DELIMITER //
create trigger afterinsertunifiedtable
after insert on Inventory_table
for each row
begin
call recalculateReorderPoint(new.Product_ID);
END //
DELIMITER ;

-- overstock and understock
WITH RollingSales AS (
    SELECT 
        Product_ID, 
        Sales_Date,
        AVG(Inventory_Quantity * Product_Cost) OVER (
            PARTITION BY Product_ID 
            ORDER BY Sales_Date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_sales
    FROM inventory_table
),

StockoutDays AS (
    SELECT 
        Product_ID,
        COUNT(*) AS stockout_days
    FROM inventory_table
    WHERE Inventory_Quantity = 0
    GROUP BY Product_ID
),
-- join the above ctes with the main table to get the results

InventoryAnalysis AS (
    SELECT 
        it.Product_ID,
        it.Sales_Date,
        it.Inventory_Quantity,
        it.Product_Cost,
        rs.rolling_avg_sales,
        COALESCE(sd.stockout_days, 0) AS stockout_days,
        CASE 
            WHEN it.Inventory_Quantity > rs.rolling_avg_sales THEN 'Overstocking'
            WHEN it.Inventory_Quantity < rs.rolling_avg_sales THEN 'Understocking'
            ELSE 'Optimal Stock'
        END AS stock_status
    FROM inventory_table it
    LEFT JOIN RollingSales rs ON it.Product_ID = rs.Product_ID AND it.Sales_Date = rs.Sales_Date
    LEFT JOIN StockoutDays sd ON it.Product_ID = sd.Product_ID
)

SELECT * FROM InventoryAnalysis;

-- monitor and adjust
-- monitor inventory levels
DELIMITER //
CREATE PROCEDURE MonitorInventoryLevels()
BEGIN
    SELECT 
        Product_ID, 
        AVG(Inventory_Quantity) AS AvgInventory
    FROM Inventory_table
    GROUP BY Product_ID
    ORDER BY AvgInventory DESC;
END//
DELIMITER ;
-- monitor sales trends
DELIMITER //
CREATE PROCEDURE MonitorSalesTrends()
BEGIN
    SELECT 
        Product_ID, 
        Sales_Date,
        AVG(Inventory_Quantity * Product_Cost) OVER (
            PARTITION BY Product_ID 
            ORDER BY Sales_Date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS RollingAvgSales
    FROM inventory_table
    ORDER BY Product_ID, Sales_Date;
END//

DELIMITER ;


-- feedback loop
-- Feedback Loop Establishment:
-- Feedback Portal: Develop an online platform for stakeholders to easily submit feedback on inventory performance and challenges.
-- Review Meetings: Organize periodic sessions to discuss inventory system performance and gather direct insights.
-- System Monitoring: Use established SQL procedures to track system metrics, with deviations from expectations flagged for review.
-- Refinement Based on Feedback:
-- Feedback Analysis: Regularly compile and scrutinize feedback to identify recurring themes or pressing issues.
-- Action Implementation: Prioritize and act on the feedback to adjust reorder points, safety stock levels, or overall processes.
-- Change Communication: Inform stakeholders about changes, underscoring the value of their feedback and ensuring transparency.

