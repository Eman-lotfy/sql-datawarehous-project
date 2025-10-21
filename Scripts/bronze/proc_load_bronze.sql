/*       
Stored procducer----> Load bronze layer(source to tables)
Usage Example : EXEC bronze.load_bronze

*/

Create OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time DATETIME,  @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
BEGIN TRY
	PRINT 'Loading Bronze Layer'
	set @batch_start_time = getdate()
	SET @start_time = getdate();
	TRUNCATE Table Bronze.crm_cust_info;

	BULK INSERT Bronze.crm_cust_info
	FROM 'C:\Users\EmanLotfy\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	SET @end_time = getdate();
	PRINT'>> LOAD DURATION: ' + cast(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' seconds';
	PRINT'---------------------------------------------'

	SET @start_time = getdate();
	TRUNCATE Table Bronze.crm_prd_info;

	BULK INSERT Bronze.crm_prd_info
	FROM 'C:\Users\EmanLotfy\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	SET @end_time = getdate();
	PRINT'>> LOAD DURATION: ' + cast(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' seconds';
	PRINT'---------------------------------------------'

	SET @start_time = getdate();
	TRUNCATE Table Bronze.crm_sales_details;

	BULK INSERT Bronze.crm_sales_details
	FROM 'C:\Users\EmanLotfy\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	SET @end_time = getdate();
	PRINT'>> LOAD DURATION: ' + cast(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' seconds';
	PRINT'---------------------------------------------'


	SET @start_time = getdate();
	TRUNCATE Table Bronze.erp_cust_az12;

	BULK INSERT Bronze.erp_cust_az12
	FROM 'C:\Users\EmanLotfy\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	SET @end_time = getdate();
	PRINT'>> LOAD DURATION: ' + cast(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' seconds';
	PRINT'---------------------------------------------'

	SET @start_time = getdate();
	TRUNCATE Table Bronze.erp_loc_a101;

	BULK INSERT Bronze.erp_loc_a101
	FROM 'C:\Users\EmanLotfy\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	SET @end_time = getdate();
	PRINT'>> LOAD DURATION: ' + cast(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' seconds';
	PRINT'---------------------------------------------'

	SET @start_time = getdate();
	TRUNCATE Table Bronze.erp_px_cat_g1v2;

	BULK INSERT Bronze.erp_px_cat_g1v2
	FROM 'C:\Users\EmanLotfy\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	SET @end_time = getdate();
	PRINT'>> LOAD DURATION: ' + cast(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' seconds';
	PRINT'---------------------------------------------'
	END TRY
BEGIN CATCH
PRINT '==========================================================='
PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
PRINT 'ERROR MESSAGE' + ERROR_MESSAGE(); 
PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
PRINT '==========================================================='

END CATCH
END
SET @batch_end_time = getdate();

Print '-------------------------------------------------'
PRINT'>> LOAD DURATION OF BATCH: ' + cast(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds';
