/*here we load the data from the source(csv files), implemented a truncate then bulk insert mechanism, used stored procedures
to store frequently run sql code, added pritn statements, try catch block and 
added mechanism to track duration for each loading process as well for the whole bronze layer.
*/



-- EXEC bronze.load_bronze;


CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN

    DECLARE @start_time DATETIME,  @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '*************************************';
        PRINT 'LOADING BRONZE 🥉 LAYER';
        PRINT '*************************************'; 




        PRINT '=====================================';
        PRINT 'LOADING CRM TABLES';
        PRINT '=====================================';







        -- customer table
        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE : bronze.crm_cust_info';
        -- first empty the table then bulk insert so that the data is not inserted twice
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT 'INSERTING INTO TABLE : bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/data/datasets/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            -- using tablock to lock the table during loading for opti mization
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'DURATION TO LOAD :  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + '  seconds';
        PRINT '-------------------------------'





        -- product table
        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE : bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT 'INSERTING INTO TABLE : bronze.crm_prd_info';

        BULK INSERT bronze.crm_prd_info
        FROM '/data/datasets/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'DURATION TO LOAD :  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + '  seconds';
        PRINT '-------------------------------'








        -- sales details table
        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE : bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT 'INSERTING INTO TABLE : bronze.crm_sales_details';

        BULK INSERT bronze.crm_sales_details
        FROM '/data/datasets/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'DURATION TO LOAD :  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + '  seconds';
        PRINT '-------------------------------'








        PRINT '=====================================';
        PRINT 'LOADING ERP  TABLES';
        PRINT '=====================================';




        -- loc table
        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE : bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT 'INSERTING INTO TABLE : bronze.erp_loc_a101';

        BULK INSERT bronze.erp_loc_a101
        FROM '/data/datasets/source_erp/loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'DURATION TO LOAD :  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + '  seconds';
        PRINT '-------------------------------'








        -- cust_az12 table
        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE : bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT 'INSERTING INTO TABLE : bronze.erp_cust_az12';

        BULK INSERT bronze.erp_cust_az12
        FROM '/data/datasets/source_erp/cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'DURATION TO LOAD :  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + '  seconds';
        PRINT '-------------------------------'








        -- px_cat_g1v2 table
        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE : bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT 'INSERTING INTO TABLE : bronze.erp_px_cat_g1v2';

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/data/datasets/source_erp/px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        ); 
        SET @end_time = GETDATE();
        PRINT 'DURATION TO LOAD :  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + '  SECONDS';
        PRINT '-------------------------------'


        SET @batch_end_time = GETDATE();
        PRINT '######################################'
        PRINT ' FINISHED LOADING BRONZE LAYER!!! '
        PRINT '   TOTAL TIME TO LOAD BRONZE LAYER IS :  ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + '  SECONDS'
        PRINT '######################################'


        END TRY

    BEGIN CATCH 
    
    PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
    PRINT 'ERROR MESSAGE :' + ERROR_MESSAGE();
    PRINT 'ERROR MESSAGE :' + CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT 'ERROR MESSAGE :' + CAST(ERROR_STATE() AS NVARCHAR); 
    PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';

    END CATCH


END





