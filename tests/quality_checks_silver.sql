-- test scripts for silver layer.


SELECT
prd_id,
COUNT(*)
FROM  bronze.crm_prd_info GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


SELECT prd_nm FROm bronze.crm_prd_info 
WHERE prd_nm != TRIM(prd_nm)


SELECT DISTINCT cst_gndr
FROm bronze.crm_cust_info

SELECT * FROM 
bronze.crm_prd_info

SELECT prd_cost FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost  IS NULL

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info


-- checking for invalid dates
SELECT * FROM bronze.crm_prd_info
WHERE prd_end_date < prd_start_date

-- test code to fix start and end dates
SELECT
prd_id,
prd_key,
prd_nm,
prd_start_date,
prd_end_date,
LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date) -1  AS prd_end_date_test

FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

-- checking for invalid dates in sakes details
SELECT NULLIF(sls_order_dt, 0) FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_ship_dt) != 8 
OR sls_order_dt > 20500101 OR sls_order_dt < 19000101

-- for shiping date column
SELECT NULLIF(sls_ship_dt, 0) FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 
OR sls_ship_dt > 20500101 OR sls_ship_dt < 19000101


-- checking for invalid sales records
SELECT DISTINCT sls_sales AS old_sls_sales ,sls_quantity, sls_price AS old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
      THEN sls_quantity * ABS(sls_price)
      ELSE sls_sales
END sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLif(sls_quantity, 0)
        ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity is NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 or sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price



-- identify invalid or old birtday dates in erp-cust
SELECT DISTINCT 
bdate FROm bronze.erp_cust_az12
where bdate < '1924-01-01' OR bdate > GETDATE()

-- cleaning gender column
SELECT DISTINCT gen FROM bronze.erp_cust_az12

-- checking country columsn in loc_a101
SELECT DISTINCT cntry AS old_cntry,
CASE 
    WHEN UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(160), '')))) = 'DE' THEN 'Germany'
    WHEN UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(160), '')))) IN ('US', 'USA') THEN 'United States'
    WHEN cntry IS NULL OR LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(160), ''))) = '' THEN 'N/A'
    ELSE LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(160), '')))
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;



