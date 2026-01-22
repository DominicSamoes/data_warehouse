/*
**************************************************************
                                                            
         Database and Schemas for Data Warehouse                
                                                            
***************************************************************
*  This script defines the database and schemas used in the Data Warehouse  
*  environment. It creates three schemas:                     
*  - bronze                                    
*  - silver                                    
*  - gold                                      

    Warning:
      Running this script will drop the entire 'DataWarehouse' database if it exists. 
      All data in the database will be permanently deleted. Proceed with caution 
      and ensure you have proper backups before running this script.

*/

DROP DATABASE IF EXISTS "DataWarehouse" WITH (FORCE);

CREATE DATABASE "DataWarehouse";

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

