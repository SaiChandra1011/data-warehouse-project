/* this script cretaes views for the gold layer
this gold layers has one fact table and two dimensions table i.e STAR SCHEMA.


The cretaed views can be used for analytical purpose
*/


-- cretaed fact table for dales details and dimensions tables
--for customer and product tables





IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_martial_status as martial_status,
CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
    ELSE COALESCE(ca.gen,  'N/A')
END AS gender,
ca.bdate as birtdate,
ci.cst_create_date as create_date
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca 
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la 
ON ci.cst_key = la.cid

GO





IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS

SELECT 
ROW_NUMBER() OVER (ORDER BY pn.prd_start_date, pn.prd_key) AS product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_date as start_date
FROM silver.crm_prd_info  AS pn
LEFT JOIN silver.erp_px_cat_g1v2 as pc 
ON pn.cat_id = pc.id
WHERE prd_end_date IS NULL -- outputs only current products

go





IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;

GO
