
-- 11.0.0  Challenge Lab: Loading Structured Data
--         The purpose of this lab is to give you an on-your-own experience
--         loading data into Snowflake. We’ll provide details about the expected
--         result and some hints, but you’ll do most of the work yourself.
--         If you need more than the hints provided, the solution can be found
--         at the end of this lab.
--         - Create a table.
--         - Create a file format.
--         - Load data from a file in an external stage into a table using the
--         COPY INTO command.
--         Snowbear Air has asked you to load sales data from a legacy sales
--         order system into Snowflake. Each of the eight tables in the legacy
--         system has been downloaded into one or more files. The tables are
--         region, nation, customer, orders, lineitem, part, supplier, and
--         partsupp.
--         There are eight sets of files (one set per table). Four sets are in
--         an uncompressed, pipe-delimited format. The other four sets are in a
--         Gzip compressed, comma-delimited format.
--         In Part I of this exercise, you’ll load the four tables with
--         uncompressed data. In Part II, you’ll load three of the tables with
--         compressed data. In Part III, we will load the last table, which will
--         require data transformation.

-- 11.1.0  Hints
--         The tables you will be loading are an exact match for those in the
--         SNOWBEARAIR_DB.PROMO_CATALOG_SALES schema. So, rather than write
--         create statements to create your tables, you can use the following
--         syntax to create your tables.

----   DO NOT EXECUTE - EXAMPLE SYNTAX
CREATE OR REPLACE TABLE <table name> LIKE <database>.<schema>.<table>;
----

--         You can use the LIST command to list the files in a stage.
--         You can specify a pattern when loading data to ensure you only load
--         the files you want. For example, if you have a set of files titled
--         sales_data_0_0_0.tbl, sales_data_0_0_1.tbl, etc., you can use a
--         pattern such as 'sales_data_.*' in order to load only those files
--         into a specific table. Pay close attention to the syntax!

----   DO NOT EXECUTE - EXAMPLE SYNTAX
COPY INTO <table name> 
FROM @<database>.<schema>.<stage name><path>/ pattern='.<your pattern>.*'
FILE_FORMAT = (FORMAT_NAME = <format name>);
----

--         Once you write the code to complete Part I, you can easily modify it
--         to complete Part II.
--         When validating your load, write queries to compare the row count in
--         each table to the row count in the source files to see if they match.
--         Here is one way you could do this. Be sure to update the file pattern
--         and the two references to REGION as needed.

----   DO NOT EXECUTE - EXAMPLE SYNTAX
SELECT 'REGION' AS NAME, 
    COUNT(*) AS TBL_ROW_COUNT,
    (SELECT count(1) FROM @training_db.traininglab.ed_stage/load/lab_files/
    (pattern=>'csv_uncomp_pipe_region_.*')) AS FILE_ROW_COUNT
FROM REGION;
----

--         When you get to Part III, remember that you can transform data by
--         using a SELECT statement as a sub-query of your COPY INTO command.
--         Click here for more information on transforming data during a load.
--         (https://docs.snowflake.com/en/user-guide/data-load-transform.html)

-- 11.2.0  Part I - Load Uncompressed, Pipe-delimited Data

-- 11.2.1  Set your context and make sure you have the standard lab objects.
--         This will also create a new schema that you will use for this
--         exercise.

USE ROLE TRAINING_ROLE;

CREATE WAREHOUSE IF NOT EXISTS SQUIRREL_WH;
USE WAREHOUSE SQUIRREL_WH;

CREATE DATABASE IF NOT EXISTS SQUIRREL_db;

CREATE OR REPLACE SCHEMA SQUIRREL_db.PROMO_CATALOG_SALES;
USE SCHEMA SQUIRREL_db.PROMO_CATALOG_SALES;


-- 11.2.2  Find the uncompressed files in the stage.
--         Locate the uncompressed files in the stage and path
--         (@training_db.traininglab.ed_stage/load/lab_files/). The prefix for
--         each set of files should be csv_uncomp_pipe_region_,
--         csv_uncomp_pipe_nation_, csv_uncomp_pipe_customer_, and
--         csv_uncomp_pipe_supplier_.

-- 11.2.3  Query the files in the stage to get an idea of what the data looks
--         like.
--         Create a file format with no delimiter for exploration.
--         Don’t run this statement. This method is omitting the file format and
--         defaulting to using CSV, and we have to call out which columns we
--         prefer to display. Sometimes, commas in the data can cause this to
--         render a little differently than we expected. However, this may be
--         okay when you are just exploring the data.

----   DO NOT EXECUTE - EXAMPLE SYNTAX
SELECT d.$1, d.$2, d.$3 --(so on and so forth)
FROM @<database>.<schema>.<stage name><path>/
(pattern=>'<your pattern>.*') d;
----

--         Here, we are showing you another way to explore: simply using another
--         file format with no delimiter. The code could look like this.

----   DO NOT EXECUTE - EXAMPLE SYNTAX
CREATE FILE FORMAT no_delimiter FIELD_DELIMITER = NONE;

SELECT $1
FROM @training_db.traininglab.ed_stage/load/lab_files/
   (FILE_FORMAT => 'no_delimiter', PATTERN=>'csv_uncomp_pipe_region.*');
----


-- 11.2.4  Create a file format that will handle uncompressed, pipe-delimited
--         data, and name it sales_csv_uncomp_pipe_tbl.

-- 11.2.5  Query the files using your new file format.

-- 11.2.6  Create tables in your new schema (region, nation, customer, and
--         supplier).

-- 11.2.7  Load the data in the files into your tables.
--         Don’t forget to include your file format in the COPY INTO statement.

-- 11.2.8  Write queries to validate the load.

-- 11.3.0  Part II - Load Gzip Compressed, Comma-delimited Data

-- 11.3.1  Locate the three sets of files in the stage and path
--         (@training_db.traininglab.ed_stage/load/lab_files/).
--         The prefix for each set should be csv_gzip_comma_orders_,
--         csv_gzip_comma_lineitem_, and csv_gzip_comma_partsupp_.

-- 11.3.2  Query the files in the stage to get an idea of what the data looks
--         like.
--         Remember the hint - use a file format with no delimiter for
--         exploration.
--         Let’s reuse that file format that has no delimiter from before. The
--         code could look like this.

----   DO NOT EXECUTE - EXAMPLE SYNTAX
SELECT $1
FROM @training_db.traininglab.ed_stage/load/lab_files/
   (FILE_FORMAT => 'no_delimiter', PATTERN=>'csv_gzip_comma_orders_.*');
----


-- 11.3.3  Create a file format that will handle Gzip compressed, comma-
--         delimited data, and name it sales_csv_gzip_comma_gz.

-- 11.3.4  Query the files using your new file format.

-- 11.3.5  Create your tables (orders, lineitem, and partsupp).

-- 11.3.6  Load the data in the files into your tables.
--         Don’t forget to include your file format in the COPY INTO statement.

-- 11.3.7  Write queries to validate the load.

-- 11.3.8  Run a query that joins all the tables and selects at least one column
--         from each table to check if the data looks as expected.

SELECT
    R.R_NAME,
    N.N_NAME,
    C.C_FIRSTNAME,
    C.C_LASTNAME,
    O.O_ORDERKEY,
    PS.PS_AVAILQTY,
    S.S_NAME
FROM
    REGION R
    INNER JOIN NATION N ON R.R_REGIONKEY = N.N_REGIONKEY
    INNER JOIN CUSTOMER C ON N.N_NATIONKEY = C.C_NATIONKEY
    INNER JOIN ORDERS O ON C.C_CUSTKEY = O.O_CUSTKEY
    INNER JOIN LINEITEM L ON O.O_ORDERKEY = L.L_ORDERKEY
    INNER JOIN PARTSUPP PS ON L.L_PARTKEY = PS.PS_PARTKEY
    INNER JOIN SUPPLIER S ON L.L_SUPPKEY = S.S_SUPPKEY
LIMIT 100;


-- 11.4.0  Part III - Transform Data During a Load

-- 11.4.1  Let’s examine the data as it sits in the file.
--         Let’s reuse that file format that has no delimiter from before. The
--         code could look like this.

----   DO NOT EXECUTE - EXAMPLE SYNTAX
SELECT $1
FROM @training_db.traininglab.ed_stage/load/lab_files/
   (FILE_FORMAT => 'no_delimiter', PATTERN=>'csv_gzip_comma_part_.*');
----

--         It looks like we can reuse the same file format from Part II
--         (sales_csv_gzip_comma_gz)

-- 11.4.2  Create the part table with the new specification provided by your
--         project manager.
--         Notice how column two will need to be shifted to the end and broken
--         into four new columns.

CREATE or REPLACE TABLE PART (
   P_PARTKEY NUMBER(38,0),
   P_SIZE NUMBER(38,0),
   P_RETAILPRICE NUMBER(12,2),
   P_NAME_DEPARTMENT VARCHAR(25),
   P_NAME_SIZE VARCHAR(10),
   P_NAME_COLOR VARCHAR(25),
   P_NAME_CATEGORY VARCHAR(100)
);


-- 11.4.3  Take a moment to familiarize yourself with the split_part() function.
--         Click here for the information on SPLIT_PART
--         (https://docs.snowflake.com/en/sql-
--         reference/functions/split_part.html)

SELECT DISTINCT $2,
    trim(split_part($2,'-',1))
FROM @training_db.traininglab.ed_stage/load/lab_files/ 
   (FILE_FORMAT => 'sales_csv_gzip_comma_gz', pattern=>'csv_gzip_comma_part_.*');

SELECT DISTINCT $2,
    trim(split_part($2,'-',1)),
    trim(split_part($2,'-',2)),
    trim(split_part($2,'-',3)),
    trim(split_part($2,'-',4))
FROM @training_db.traininglab.ed_stage/load/lab_files/ 
   (FILE_FORMAT => 'sales_csv_gzip_comma_gz', pattern=>'csv_gzip_comma_part_.*');


-- 11.4.4  Let’s run a select of the top 100 rows to make sure we are
--         transforming correctly.

SELECT TOP 100
    $1, 
    $3, 
    $4,
    $2,
    trim(split_part($2,'-',1)),
    trim(split_part($2,'-',2)),
    trim(split_part($2,'-',3)),
    trim(split_part($2,'-',4))
FROM @training_db.traininglab.ed_stage/load/lab_files/ 
   (FILE_FORMAT => 'sales_csv_gzip_comma_gz', pattern=>'csv_gzip_comma_part_.*');


-- 11.5.0  Now We are Ready to Load the Data
--         See if you can build the COPY INTO statement on your own.

-- 11.5.1  Write queries to validate the load.

-- 11.5.2  If you need help, the solution is on the next page.
--         Otherwise, congratulations! You have now completed this lab.

-- 11.6.0  Solution
--         If you need to, you can look at the solution below. But try not to
--         peek! You’ll learn more if you try it on your own first and rely on
--         the hints.


-------------------------------------------------------
--- PART I - Load Uncompressed, Pipe-delimited Data ---
-------------------------------------------------------

USE ROLE TRAINING_ROLE;

CREATE WAREHOUSE IF NOT EXISTS SQUIRREL_WH;
USE WAREHOUSE SQUIRREL_WH;

CREATE DATABASE IF NOT EXISTS SQUIRREL_db;

CREATE OR REPLACE SCHEMA SQUIRREL_db.PROMO_CATALOG_SALES;
USE SCHEMA SQUIRREL_db.PROMO_CATALOG_SALES;

-- Listing of required files on the stage

list @training_db.traininglab.ed_stage/load/lab_files/ pattern='csv_uncomp.*';

-- Sample query to see the data

CREATE FILE FORMAT no_delimiter FIELD_DELIMITER = NONE;
   
SELECT $1
FROM @training_db.traininglab.ed_stage/load/lab_files/
   (FILE_FORMAT => 'no_delimiter', PATTERN=>'csv_uncomp_pipe_region.*');
   
SELECT $1
FROM @training_db.traininglab.ed_stage/load/lab_files/
   (FILE_FORMAT => 'no_delimiter', PATTERN=>'csv_uncomp_pipe_nation_.*');
   
SELECT $1
FROM @training_db.traininglab.ed_stage/load/lab_files/
   (FILE_FORMAT => 'no_delimiter', PATTERN=>'csv_uncomp_pipe_customer_.*');
   
SELECT $1
FROM @training_db.traininglab.ed_stage/load/lab_files/
   (FILE_FORMAT => 'no_delimiter', PATTERN=>'csv_uncomp_pipe_supplier_.*');


-- CREATE A FILE FORMAT FOR LOADING THE DATA

CREATE OR REPLACE FILE FORMAT sales_csv_uncomp_pipe_tbl
  TYPE = CSV
  COMPRESSION = NONE
  FIELD_DELIMITER = '|'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- TEST YOUR NEW FILE FORMAT

SELECT $1, $2
FROM @training_db.traininglab.ed_stage/load/lab_files/
   (FILE_FORMAT => 'sales_csv_uncomp_pipe_tbl', PATTERN=>'csv_uncomp_pipe_region.*');
   
SELECT $1, $2, $3
FROM @training_db.traininglab.ed_stage/load/lab_files/
   (FILE_FORMAT => 'sales_csv_uncomp_pipe_tbl', PATTERN=>'csv_uncomp_pipe_nation_.*');
   
SELECT $1, $2, $3, $4, $5
FROM @training_db.traininglab.ed_stage/load/lab_files/
   (FILE_FORMAT => 'sales_csv_uncomp_pipe_tbl', PATTERN=>'csv_uncomp_pipe_customer_.*');
   
SELECT $1, $2, $3, $4
FROM @training_db.traininglab.ed_stage/load/lab_files/
   (FILE_FORMAT => 'sales_csv_uncomp_pipe_tbl', PATTERN=>'csv_uncomp_pipe_supplier_.*');

-- CREATE YOUR TABLES

CREATE OR REPLACE TABLE region LIKE snowbearair_db.promo_catalog_sales.region;
CREATE OR REPLACE TABLE nation LIKE snowbearair_db.promo_catalog_sales.nation;
CREATE OR REPLACE TABLE customer LIKE snowbearair_db.promo_catalog_sales.customer;
CREATE OR REPLACE TABLE supplier LIKE snowbearair_db.promo_catalog_sales.supplier;

-- LOAD THE DATA IN THE FILES INTO THE TABLES

COPY INTO region 
FROM @training_db.traininglab.ed_stage/load/lab_files/ pattern='csv_uncomp_pipe_region_.*'
FILE_FORMAT = (FORMAT_NAME = sales_csv_uncomp_pipe_tbl);

COPY INTO nation 
FROM @training_db.traininglab.ed_stage/load/lab_files/ pattern='csv_uncomp_pipe_nation_.*'
FILE_FORMAT = (FORMAT_NAME = sales_csv_uncomp_pipe_tbl);

COPY INTO customer 
FROM @training_db.traininglab.ed_stage/load/lab_files/ pattern='csv_uncomp_pipe_customer_.*'
FILE_FORMAT = (FORMAT_NAME = sales_csv_uncomp_pipe_tbl);

COPY INTO supplier 
FROM @training_db.traininglab.ed_stage/load/lab_files/ pattern='csv_uncomp_pipe_supplier_.*'
FILE_FORMAT = (FORMAT_NAME = sales_csv_uncomp_pipe_tbl);

--Check that all tables have the correct quantity of rows.

SELECT 'REGION' AS NAME, 
    COUNT(*) AS TBL_ROW_COUNT,
    (SELECT count(1) FROM @training_db.traininglab.ed_stage/load/lab_files/ (pattern=>'csv_uncomp_pipe_region_.*')) AS FILE_ROW_COUNT
FROM REGION;

SELECT 'NATION' AS NAME, 
    COUNT(*) AS TBL_ROW_COUNT,
    (SELECT count(1) FROM @training_db.traininglab.ed_stage/load/lab_files/ (pattern=>'csv_uncomp_pipe_nation_.*')) AS FILE_ROW_COUNT
FROM NATION;

SELECT 'CUSTOMER' AS NAME, 
    COUNT(*) AS TBL_ROW_COUNT,
    (SELECT count(1) FROM @training_db.traininglab.ed_stage/load/lab_files/ (pattern=>'csv_uncomp_pipe_customer_.*')) AS FILE_ROW_COUNT
FROM CUSTOMER;

SELECT 'SUPPLIER' AS NAME, 
    COUNT(*) AS TBL_ROW_COUNT,
    (SELECT count(1) FROM @training_db.traininglab.ed_stage/load/lab_files/ (pattern=>'csv_uncomp_pipe_supplier_.*')) AS FILE_ROW_COUNT
FROM SUPPLIER;
    

---------------------------------------------------------
--- PART II - Load Gzip Compressed, Comma-delimited Data
--------------------------------------------------------
-- Listing of required files on the stage

list @training_db.traininglab.ed_stage/load/lab_files/ pattern='csv_gzip_comma.*';

-- Sample the data by querying with the no_delimiter file format from earlier.

SELECT n.$1
FROM @training_db.traininglab.ed_stage/load/lab_files/ 
   (FILE_FORMAT => 'no_delimiter', pattern=>'csv_gzip_comma_orders_.*') n;

SELECT n.$1
FROM @training_db.traininglab.ed_stage/load/lab_files/ 
   (FILE_FORMAT => 'no_delimiter', pattern=>'csv_gzip_comma_lineitem_.*') n;

SELECT n.$1
FROM @training_db.traininglab.ed_stage/load/lab_files/ 
   (FILE_FORMAT => 'no_delimiter', pattern=>'csv_gzip_comma_partsupp_.*') n;

-- Unlike the previous pipes, this data is separated by commas.

-- CREATE A FILE FORMAT FOR LOADING THE DATA

CREATE OR REPLACE FILE FORMAT sales_csv_gzip_comma_gz
  TYPE = CSV
  COMPRESSION = GZIP
  FIELD_DELIMITER = ','
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- TEST YOUR NEW FILE FORMAT

SELECT $1, $2, $3, $4, $5, $6
FROM @training_db.traininglab.ed_stage/load/lab_files/
   (FILE_FORMAT => 'sales_csv_gzip_comma_gz', PATTERN=>'csv_gzip_comma_orders_.*');
   
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
FROM @training_db.traininglab.ed_stage/load/lab_files/
   (FILE_FORMAT => 'sales_csv_gzip_comma_gz', PATTERN=>'csv_gzip_comma_lineitem_.*');
   
SELECT $1, $2, $3, $4
FROM @training_db.traininglab.ed_stage/load/lab_files/
   (FILE_FORMAT => 'sales_csv_gzip_comma_gz', PATTERN=>'csv_gzip_comma_partsupp_.*');

-- CREATE YOUR TABLES

CREATE OR REPLACE TABLE orders LIKE snowbearair_db.promo_catalog_sales.orders;
CREATE OR REPLACE TABLE lineitem LIKE snowbearair_db.promo_catalog_sales.lineitem;
CREATE OR REPLACE TABLE partsupp LIKE snowbearair_db.promo_catalog_sales.partsupp;

-- LOAD THE DATA IN THE FILES INTO THE TABLES

COPY INTO orders 
FROM @training_db.traininglab.ed_stage/load/lab_files/ pattern='csv_gzip_comma_orders_.*'
FILE_FORMAT = (FORMAT_NAME = sales_csv_gzip_comma_gz);

COPY INTO lineitem 
FROM @training_db.traininglab.ed_stage/load/lab_files/ pattern='csv_gzip_comma_lineitem_.*'
FILE_FORMAT = (FORMAT_NAME = sales_csv_gzip_comma_gz);

COPY INTO partsupp 
FROM @training_db.traininglab.ed_stage/load/lab_files/ pattern='csv_gzip_comma_partsupp_.*'
FILE_FORMAT = (FORMAT_NAME = sales_csv_gzip_comma_gz);

--Check that all tables have the correct number of rows. There should be:

-- 1,500,000 in the ORDERS table
-- 6,001,215 in the LINEITEM table
--   800,000 in the PARTSUPP table

SELECT 'ORDERS' AS NAME, 
    COUNT(*) AS TBL_ROW_COUNT,
    (SELECT count(1) FROM @training_db.traininglab.ed_stage/load/lab_files/ (pattern=>'csv_gzip_comma_orders_.*')) AS FILE_ROW_COUNT
FROM ORDERS;

SELECT 'LINEITEM' AS NAME, 
    COUNT(*) AS TBL_ROW_COUNT,
    (SELECT count(1) FROM @training_db.traininglab.ed_stage/load/lab_files/ (pattern=>'csv_gzip_comma_lineitem_.*')) AS FILE_ROW_COUNT
FROM LINEITEM;

SELECT 'PARTSUPP' AS NAME, 
    COUNT(*) AS TBL_ROW_COUNT,
    (SELECT count(1) FROM @training_db.traininglab.ed_stage/load/lab_files/ (pattern=>'csv_gzip_comma_partsupp_.*')) AS FILE_ROW_COUNT
FROM PARTSUPP;
    
-- CHECK THAT ALL TABLES ARE JOINING PROPERLY

SELECT
    R.R_NAME,
    N.N_NAME,
    C.C_FIRSTNAME,
    C.C_LASTNAME,
    O.O_ORDERKEY,
    PS.PS_AVAILQTY,
    S.S_NAME
FROM
    REGION R
    INNER JOIN NATION N ON R.R_REGIONKEY = N.N_REGIONKEY
    INNER JOIN CUSTOMER C ON N.N_NATIONKEY = C.C_NATIONKEY
    INNER JOIN ORDERS O ON C.C_CUSTKEY = O.O_CUSTKEY
    INNER JOIN LINEITEM L ON O.O_ORDERKEY = L.L_ORDERKEY
    INNER JOIN PARTSUPP PS ON L.L_PARTKEY = PS.PS_PARTKEY
    INNER JOIN SUPPLIER S ON L.L_SUPPKEY = S.S_SUPPKEY
LIMIT 100;


----------------------------------------------
--- PART III - Transforming Data During a Load
----------------------------------------------

-- Sample the data by querying with the no_delimiter file format from earlier.

SELECT $1
FROM @training_db.traininglab.ed_stage/load/lab_files/ 
   (FILE_FORMAT => 'no_delimiter', pattern=>'csv_gzip_comma_part_.*');

-- It looks like we can reuse the same file format from part II (sales_csv_gzip_comma_gz)

-- Create the part table with the new specification provided by your project manager (notice how column 2 will need to be shifted to the end and broken into four new columns)

CREATE or REPLACE TABLE PART (
   P_PARTKEY NUMBER(38,0),
   P_SIZE NUMBER(38,0),
   P_RETAILPRICE NUMBER(12,2),
   P_NAME VARCHAR(100),
   P_NAME_DEPARTMENT VARCHAR(25),
   P_NAME_SIZE VARCHAR(10),
   P_NAME_COLOR VARCHAR(25),
   P_NAME_CATEGORY VARCHAR(100)
);

-- Let's run a select of the top 100 rows to make sure we are transforming correctly. 

SELECT TOP 100
    $1, 
    $3, 
    $4,
    $2,
    trim(split_part($2,'-',1)),
    trim(split_part($2,'-',2)),
    trim(split_part($2,'-',3)),
    trim(split_part($2,'-',4))
FROM @training_db.traininglab.ed_stage/load/lab_files/  
   (FILE_FORMAT => 'sales_csv_gzip_comma_gz', pattern=>'csv_gzip_comma_part_.*');

-- Now, we are ready to load the data.

COPY INTO PART
FROM (
    SELECT
        $1, 
        $3, 
        $4,
        $2,
        trim(split_part($2,'-',1)),
        trim(split_part($2,'-',2)),
        trim(split_part($2,'-',3)),
        trim(split_part($2,'-',4))
    FROM @training_db.traininglab.ed_stage/load/lab_files/   
        (FILE_FORMAT => 'sales_csv_gzip_comma_gz', pattern=>'csv_gzip_comma_part_.*')
);

-- Validate the data

SELECT 'PART' AS NAME, 
    COUNT(*) AS TBL_ROW_COUNT,
    (SELECT count(1) FROM @training_db.traininglab.ed_stage/load/lab_files/ (pattern=>'csv_uncomp_pipe_part_.*')) AS FILE_ROW_COUNT
FROM PART;

SELECT TOP 100 *
FROM PART;

-- Suspend your virtual warehouse

ALTER WAREHOUSE SQUIRREL_wh SUSPEND;

-- Congratulations! You have now completed this lab.

