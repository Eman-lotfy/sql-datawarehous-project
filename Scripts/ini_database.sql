IF EXISTS (SELECT 1 FROM sys.database where name = 'DataWarehouse')
  BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
END;
GO

Use master;
Go
----create database
create Database DataWarehouse;
Go

use DataWarehouse;
Go

  -----create schema
create SCHEMA Bronze;
GO
create SCHEMA Silver;
GO
create SCHEMA Gold;
