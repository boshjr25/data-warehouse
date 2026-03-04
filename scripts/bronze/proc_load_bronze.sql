/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data from external CSV files into the 
    'bronze' schema tables using the BULK INSERT command.
    It represents the 'Extract and Load' (EL) section of the ETL process.

Parameters:
    None

Usage:
    EXEC bronze.load_bronze;

Load Strategy:
    - Truncate: The target table is cleared before each load.
    - Bulk Insert: Data is pulled directly from flat files into the DWH.
    - Logging: The procedure prints the start time, end time, and duration
      of each load to help monitor performance.

Note:
    Ensure that the file paths in the BULK INSERT statements are accessible 
    by the SQL Server service account.
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN 
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME;
    
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Starting Bronze Layer Load';
        PRINT 'Start Time: ' + CAST(@batch_start_time AS VARCHAR);
        PRINT '================================================';

        -- ------------------------------------------------------
        -- CRM Tables
        -- ------------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT '>> Loading CRM Tables';
        PRINT '------------------------------------------------';

        -- Table: bronze.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating and Loading: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info 
        FROM 'D:\DWH Project\datasets\source_crm\cust_info.csv' 
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Success! Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- Table: bronze.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating and Loading: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info 
        FROM 'D:\DWH Project\datasets\source_crm\prd_info.csv' 
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Success! Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- Table: bronze.crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating and Loading: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details 
        FROM 'D:\DWH Project\datasets\source_crm\sales_details.csv' 
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Success! Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- Table: bronze.crm_camp_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating and Loading: bronze.crm_camp_info';
        TRUNCATE TABLE bronze.crm_camp_info;
        BULK INSERT bronze.crm_camp_info
        FROM 'D:\DWH Project\datasets\source_crm\camp_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK, CODEPAGE = '65001');
        SET @end_time = GETDATE();
        PRINT '>> Success! Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- Table: bronze.crm_supp_tkts
        SET @start_time = GETDATE();
        PRINT '>> Truncating and Loading: bronze.crm_supp_tkts';
        TRUNCATE TABLE bronze.crm_supp_tkts;
        BULK INSERT bronze.crm_supp_tkts
        FROM 'D:\DWH Project\datasets\source_crm\supp_tkts.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK, CODEPAGE = '65001');
        SET @end_time = GETDATE();
        PRINT '>> Success! Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- ------------------------------------------------------
        -- ERP Tables
        -- ------------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT '>> Loading ERP Tables';
        PRINT '------------------------------------------------';

        -- Table: bronze.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating and Loading: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12 
        FROM 'D:\DWH Project\datasets\source_erp\CUST_AZ12.csv' 
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Success! Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- Table: bronze.erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncating and Loading: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101 
        FROM 'D:\DWH Project\datasets\source_erp\LOC_A101.csv' 
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Success! Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- Table: bronze.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Truncating and Loading: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2 
        FROM 'D:\DWH Project\datasets\source_erp\PX_CAT_G1V2.csv' 
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Success! Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- Table: bronze.erp_hr_hx19
        SET @start_time = GETDATE();
        PRINT '>> Truncating and Loading: bronze.erp_hr_hx19';
        TRUNCATE TABLE bronze.erp_hr_hx19;
        BULK INSERT bronze.erp_hr_hx19
        FROM 'D:\DWH Project\datasets\source_erp\HR_HX19.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK, CODEPAGE = '65001');
        SET @end_time = GETDATE();
        PRINT '>> Success! Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- Table: bronze.erp_vnd_z90
        SET @start_time = GETDATE();
        PRINT '>> Truncating and Loading: bronze.erp_vnd_z90';
        TRUNCATE TABLE bronze.erp_vnd_z90;
        BULK INSERT bronze.erp_vnd_z90
        FROM 'D:\DWH Project\datasets\source_erp\VND_Z90.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK, CODEPAGE = '65001');
        SET @end_time = GETDATE();
        PRINT '>> Success! Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- Table: bronze.erp_inv_q4v1
        SET @start_time = GETDATE();
        PRINT '>> Truncating and Loading: bronze.erp_inv_q4v1';
        TRUNCATE TABLE bronze.erp_inv_q4v1;
        BULK INSERT bronze.erp_inv_q4v1
        FROM 'D:\DWH Project\datasets\source_erp\INV_Q4V1.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK, CODEPAGE = '65001');
        SET @end_time = GETDATE();
        PRINT '>> Success! Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- Table: bronze.erp_po_ord44
        SET @start_time = GETDATE();
        PRINT '>> Truncating and Loading: bronze.erp_po_ord44';
        TRUNCATE TABLE bronze.erp_po_ord44;
        BULK INSERT bronze.erp_po_ord44
        FROM 'D:\DWH Project\datasets\source_erp\PO_ORD44.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK, CODEPAGE = '65001');
        SET @end_time = GETDATE();
        PRINT '>> Success! Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';

        PRINT '================================================';
        PRINT 'Bronze Layer Load Completed Successfully';
        PRINT 'Total Duration: ' + CAST(DATEDIFF(second, @batch_start_time, GETDATE()) AS VARCHAR) + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING LOADING';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT '================================================';
        
        -- Throw the error so the caller knows it failed
        THROW;
    END CATCH
END;
GO

-- Execute the master procedure
EXEC bronze.load_bronze;
