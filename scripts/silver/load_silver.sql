-- performing cleaning to load into silver layer. we perofrm the cleaning on the data from the bronze layer then load it 
-- into the tables in the silver layer.

-- EXECUTE silver.load_silver

-- Cleaning and loading crm files 

CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();


        PRINT '*************************************';
        PRINT 'LOADING SILVER 🥈 LAYER';
        PRINT '*************************************'; 


        PRINT '=====================================';
        PRINT 'LOADING CRM TABLES';
        PRINT '=====================================';



        -- 1st table customer infomation.
        SET @start_time = GETDATE();
        print '### Truncating table - silver.crm_cust_info'
        TRUNCATE TABLE silver.crm_cust_info
        print '### Inserting into silver.crm_cust_info'
        INSERT INTO silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_martial_status,
            cst_gndr,
            cst_create_date
        )


        SELECT cst_id, 
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE WHEN UPPER(TRIM(cst_martial_status)) = 'S' THEN 'SINGLE'
            WHEN UPPER(TRIM(cst_martial_status)) = 'M' THEN 'MARRIED'
            ELSE 'N/A' 
        END cst_martial_status, -- standardizing martial status values to readable format


        CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
            ELSE 'N/A' 
        END cst_gndr, -- standardizing gender values to readable format


        cst_create_date
        FROM(SELECT *,ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        from bronze.crm_cust_info WHERE cst_id IS NOT NULL)t
        WHERE flag_last = 1  -- helsp in selecting the latest record from the customer.

        SET @end_time = GETDATE();
        PRINT 'DURATION TO LOAD :  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + '  seconds';
        PRINT '-------------------------------'


        




        -- 2nd table product information
        SET @start_time = GETDATE();
        print '### Truncating table - silver.crm_prd_info'
        TRUNCATE TABLE silver.crm_prd_info
        print '### Inserting into silver.crm_prd_info'

        INSERT INTO silver.crm_prd_info(
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_date,
            prd_end_date
        )

        SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key,1,5), '-', '_' )AS cat_id, -- the px_cat file has _ instead of -, so to join replace it to _
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- to join with sales detailes file
        prd_nm,
        ISNULL(prd_cost,0) AS prd_cost, -- put NULL values to 0
        CASE WHEN UPPER(TRIm(prd_line)) = 'M' THEN 'Moonline'
        WHEN UPPER(TRIm(prd_line)) = 'R' THEN 'Radiance'
        WHEN UPPER(TRIm(prd_line)) = 'S' THEN 'Strive'
        WHEN UPPER(TRIm(prd_line)) = 'T' THEN 'Traiblaze'
        ELSE 'N/A'
        END AS prd_line,
        CAST (prd_start_date AS DATE) AS prd_start_date,
        -- end date is one day before the next record start date
        CAST(LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date) -1 AS DATE) AS prd_end_date
        FROM bronze.crm_prd_info
        SET @end_time = GETDATE();
        PRINT 'DURATION TO LOAD :  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + '  seconds';
        PRINT '-------------------------------'








        --3rd table sales details
        SET @start_time = GETDATE();
        print '### Truncating table - crm_sales_details'
        TRUNCATE TABLE silver.crm_sales_details
        print '### Inserting into silver.crm_sales_details'

        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )


        SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- first convert int to string then date
        END AS sls_order_dt,
        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) -- first convert int to string then date
        END AS sls_ship_dt,
        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) -- first convert int to string then date
        END AS sls_due_dt,
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END sls_sales, -- updating sales if original is invalid or missing
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLif(sls_quantity, 0)
                ELSE sls_price
        END AS sls_price  -- calculating price if original price is inavlid

        FROM bronze.crm_sales_details
        SET @end_time = GETDATE();
        PRINT 'DURATION TO LOAD :  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + '  seconds';
        PRINT '-------------------------------'





   
   
        PRINT '=====================================';
        PRINT 'LOADING ERP  TABLES';
        PRINT '=====================================';




        -- cleaning and loading erp files 




        -- 1st table erp_cus 
        SET @start_time = GETDATE();
        print '### Truncating table - silver.erp_cust_az12'
        TRUNCATE TABLE silver.erp_cust_az12
        print '### Inserting into silver.erp_cust_az12'

        INSERT INTO silver.erp_cust_az12(
            cid,
            bdate,
            gen

        )

        SELECT 
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, len(cid))
        ELSE cid
        END  as cid,
        CASE WHEN bdate > GETDATE() THEN NULL
        else bdate -- removed birthdates in future
        end as bdate,
        CASE WHEN LTRIM(RTRIM(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''))) IN ('F', 'FEMALE') THEN 'Female'
            WHEN LTRIM(RTRIM(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''))) IN ('M', 'MALE') THEN 'Male'
        ELSE 'N/A'
        END AS gen
        FROM bronze.erp_cust_az12
        SET @end_time = GETDATE();
        PRINT 'DURATION TO LOAD :  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + '  seconds';
        PRINT '-------------------------------'

        



        -- 2nd table loc_a101
        SET @start_time = GETDATE();
        print '### Truncating table - silver.erp_loc_a101'
        TRUNCATE TABLE silver.erp_loc_a101
        print '### Inserting into silver.erp_loc_a101'

        INSERT INTO silver.erp_loc_a101(
            cid,
            cntry
        )

        SELECT 
        REPLACE(cid, '-', '') cid,
        CASE 
            WHEN UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(160), '')))) = 'DE' THEN 'Germany'
            WHEN UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(160), '')))) IN ('US', 'USA') THEN 'United States'
            WHEN cntry IS NULL OR LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(160), ''))) = '' THEN 'N/A'
            ELSE LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(160), '')))
        END AS cntry
        FROM bronze.erp_loc_a101
        SET @end_time = GETDATE();
        PRINT 'DURATION TO LOAD :  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + '  seconds';
        PRINT '-------------------------------'



        -- 3rd table px_catg1v2 (no cleaning needed)
        SET @start_time = GETDATE();
        print '### Truncating table - silver.erp_px_cat_g1v2'
        TRUNCATE TABLE silver.erp_px_cat_g1v2
        print '### Inserting into silver.erp_px_cat_g1v2'

        INSERT INTO silver.erp_px_cat_g1v2(
            id,
            cat,
            subcat,
            maintenance
        )

        SELECT 
        id,
        cat,
        subcat,
        maintenance
        FROM bronze.erp_px_cat_g1v2
        SET @end_time = GETDATE();
        PRINT 'DURATION TO LOAD :  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + '  seconds';
        PRINT '-------------------------------'

    END TRY

BEGIN CATCH 
    
    PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
    PRINT 'ERROR MESSAGE :' + ERROR_MESSAGE();
    PRINT 'ERROR MESSAGE :' + CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT 'ERROR MESSAGE :' + CAST(ERROR_STATE() AS NVARCHAR); 
    PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';

END CATCH

 
    
END
