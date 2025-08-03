/*

Scrip summary - created this script to create the databse named 'warehouse' and it also check if a databse named 'warehouse'already exists, also this
script creates the medallion architecture - bronze, silver, gold schemas
*/

-- Caution  - if this script runs , it will  delete the existing warehouse databse, so proceed with caution ⚠️.

USE master;
CREATE DATABASE warehouse;
 
 USE warehouse;

-- drop existing database if already created
IF EXISTS (SELECT 1 FROM sys.database WHERE name = 'warehouse')
BEGIN
        ALTER DATABASE warehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
        DROp DATABASE warehouse;
END;
GO

-- creating the bronze, silver, gold schemas 
 CREATE SCHEMA bronze;
 GO
 CREATE SCHEMA silver;
 GO 
 CREATE SCHEMA gold;
 GO
