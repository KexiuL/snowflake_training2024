
-- 7.0.0   Using Estimation Functions and Sampling Methods
--         As you know, common functions in relational database management
--         systems such as COUNT(DISTINCT) and percentage/percentile functions
--         require a scan and sort of an entire dataset to yield a result.
--         Although a cloud database like Snowflake is designed to handle
--         virtually unlimited quantities of data, executing a COUNT(DISTINCT)
--         on a very large table could take far longer than a user is willing to
--         wait. Additionally, precise counts are often unnecessary when working
--         with very large datasets, especially if your data is being updated in
--         real-time or near real-time.
--         Snowflake’s estimation functions are designed to give you approximate
--         results that should satisfy your analytical needs but in a far
--         shorter time frame. The purpose of this lab is to give you an
--         opportunity to work with a couple of these functions: HLL() or
--         HyperLogLog, which can be used instead of the standard
--         COUNT(DISTINCT) function, and APPROX_PERCENTILE, which can be used
--         instead of the standard SQL MEDIAN function.
--         When testing large datasets one can use sampling methods to return a
--         subset of rows sampled randomly from the specified table. In this
--         lab, you will also practice sampling data using the ROW and BLOCK
--         methods.
--         - Use HyperLogLog to calculate an approximate count.
--         - Use APPROX_PERCENTILE to calculate an approximate median.
--         - Use the ROW and BLOCK methods to sample data from a table.
--         You’ve just learned about some estimation functions and sampling
--         methods that may be helpful for your analysis needs. You’ve decided
--         to try these on a couple of tables you know have anywhere from
--         hundreds of millions to billions of rows to see how they perform.
--         This is your plan:
--         - Run both HyperLogLog and COUNT(DISTINCT) to see which returns a
--         result more quickly.
--         - Run both APPROX_PERCENTILE and MEDIAN() to see which returns a
--         result more quickly.
--         - Create sample datasets with the ROW and BLOCK methods to see which
--         runs more quickly.
--         HOW TO COMPLETE THIS LAB
--         Since the workbook PDF has useful diagrams and illustrations (not
--         present in the .SQL files), we recommend that you read the
--         instructions from the workbook PDF. In order to execute the code
--         presented in each step, use the SQL code file provided for this lab.
--         OPENING THE SQL FILE
--         To load the SQL file, in the left navigation bar select Projects,
--         then select Worksheets. From the Worksheets page, in the upper-right
--         corner, click the ellipsis (…) to the left of the blue plus (+)
--         button. Select Create Worksheet from SQL File from the drop-down
--         menu. Navigate to the SQL file for this lab and load it.
--         Let’s get started!

-- 7.1.0   Work with HyperLogLog

-- 7.1.1   Alter the session so it does not use cached results.
--         This will give us an accurate reading as to the longest time the
--         functions will take to run. (The use_cached_result parameter and data
--         cache will be explained further in a later section of the course.)

ALTER SESSION SET use_cached_result=false;


-- 7.1.2   Set your context.

USE ROLE training_role;

CREATE WAREHOUSE IF NOT EXISTS SQUIRREL_wh;
ALTER WAREHOUSE SQUIRREL_wh SET WAREHOUSE_SIZE=XSmall;
USE WAREHOUSE SQUIRREL_wh;

USE SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCH_SF100;


-- 7.1.3   Suspend the virtual warehouse.
--         With the statements below, you’ll suspend and resume the virtual
--         warehouse to clear any data in the data cache. Then, you’ll use the
--         query below to determine an approximate count of distinct l_orderkey
--         values with the HyperLogLog estimation function.
--         If you try to suspend the virtual warehouse and it is already
--         suspended, you may get an error. This is normal.

ALTER WAREHOUSE SQUIRREL_wh SUSPEND;
ALTER WAREHOUSE SQUIRREL_wh RESUME;

SELECT HLL(l_orderkey) FROM lineitem;

--         How long did it take to run, and how many distinct values did it
--         find? It should have taken less than 10 seconds to run and counted
--         around 145,660,677 rows.

-- 7.1.4   Suspend and resume the virtual warehouse again to clear the data
--         cache.
--         Execute the regular COUNT version of the query to compare the results
--         to those of the HyperLogLog execution.

ALTER WAREHOUSE SQUIRREL_wh SUSPEND;
ALTER WAREHOUSE SQUIRREL_wh RESUME;

SELECT COUNT(DISTINCT l_orderkey) 
FROM lineitem;

--         How long did it take to run, and how many distinct values did it
--         count? It should have taken more than 15 seconds to run and should
--         have returned a count of precisely 150,000,000 values.
--         So, the difference is approximately 4,339,323 rows, which is a
--         variance of 2.9%. Suppose a variance of 2.9% is not critical to your
--         analysis, especially when working with counts in the hundreds of
--         millions. In that case, HyperLogLog can be a better choice than
--         COUNT(DISTINCT).

-- 7.2.0   Use Percentile Estimation Functions
--         Now, let’s try out the APPROX_PERCENTILE function. This function can
--         respond more rapidly than the regular SQL MEDIAN function.
--         Rather than the LINEITEM table, we will use the ORDERS table.

-- 7.2.1   Change your virtual warehouse size to large and clear your virtual
--         warehouse cache.

ALTER WAREHOUSE SQUIRREL_wh
    SET WAREHOUSE_SIZE = 'Large';

ALTER WAREHOUSE SQUIRREL_wh SUSPEND;
ALTER WAREHOUSE SQUIRREL_wh RESUME;


-- 7.2.2   Start by using the SQL Median Function.
--         The following statement determines the median order total in each
--         year of data.

SELECT 
      YEAR(O_ORDERDATE),
      MEDIAN(O_TOTALPRICE)   
FROM
    ORDERS    
GROUP BY
    YEAR(O_ORDERDATE)    
ORDER BY  
    YEAR(O_ORDERDATE);

--         How long did it take to run, and what results did you get? It should
--         have run in 15-20 seconds, and you should see the results below:
--         - 1992 - 144310.1
--         - 1993 - 144303.67
--         - 1994 - 144285.85
--         - 1995 - 144282.92
--         - 1996 - 144322.46
--         - 1997 - 144284.45
--         - 1998 - 144318.58

-- 7.2.3   Suspend and resume the virtual warehouse again to clear the data
--         cache.

ALTER WAREHOUSE SQUIRREL_wh SUSPEND;
ALTER WAREHOUSE SQUIRREL_wh RESUME;


-- 7.2.4   Run the Percentile Estimation Function on the orders table to find
--         the approximate 50th percentile of sales for each year in the table.

SELECT 
      YEAR(O_ORDERDATE),
      APPROX_PERCENTILE(O_TOTALPRICE, 0.5)    
FROM
    ORDERS    
GROUP BY
    YEAR(O_ORDERDATE)    
ORDER BY  
    YEAR(O_ORDERDATE);  

--         How long did it take to run, and what results did you get? It should
--         have run in fewer than two seconds, and you should have gotten the
--         results that look very much like (but not exactly like) the ones
--         below:
--         - 1992 - 144315.338198642
--         - 1993 - 144302.777148666
--         - 1994 - 144284.126806681
--         - 1995 - 144278.419806512
--         - 1996 - 144325.010908467
--         - 1997 - 144289.365240779
--         - 1998 - 144323.018226321
--         As you can see, the APPROX_PERCENTILE function runs quite a bit
--         faster than the standard MEDIAN() function and produces almost the
--         same result. Your results may not look exactly like the values above
--         because the function returns an approximate value each time.
--         Regardless, for the tiny variance in the values, you get your result
--         set much more quickly.

-- 7.2.5   Change your virtual warehouse size to xsmall.

ALTER WAREHOUSE SQUIRREL_wh
    SET WAREHOUSE_SIZE = 'XSmall';

ALTER WAREHOUSE SQUIRREL_wh SUSPEND;


-- 7.3.0   Use Sampling Methods
--         Let us use the sampling methods to create sample datasets from the
--         lineitem table that resides in the TRAINING_DB.TPCH_SF100 schema.

-- 7.3.1   Set your context.

USE ROLE training_role;
USE WAREHOUSE SQUIRREL_wh;
CREATE OR REPLACE SCHEMA SQUIRREL_db.SQUIRREL_sampling;
USE SCHEMA SQUIRREL_db.SQUIRREL_sampling;


-- 7.3.2   Determine the number of rows in the lineitem table;

SELECT COUNT (*) FROM TRAINING_DB.TPCH_SF100.lineitem; 

--         Observe that this table has about 600 million rows.

-- 7.3.3   Create a sample dataset with 1% of the rows in the lineitem table
--         using the default BERNOULLI (ROW) method.

-- The value of 1 in the parantheses below represents the probability
CREATE OR REPLACE TABLE lineitem_rowsample AS SELECT * FROM TRAINING_DB.TPCH_SF100.lineitem SAMPLE (1);

--         View the Query duration in the Query Details pane, this query may
--         take about 25 seconds or so.

-- 7.3.4   Determine the number of rows in the lineitem_rowsample table;

SELECT COUNT (*) FROM lineitem_rowsample; 

--         This sample table should have about 6 million rows, or about 1% of
--         the 600 million rows from the lineitem table. Your results may vary
--         as the sampling method returns the number of rows based on the
--         probability value specified in the request.

-- 7.3.5   Create a sample dataset with 1% of the rows in the lineitem table
--         using the SYSTEM (BLOCK) method.
--         The BLOCK method is typically faster for large datasets.

-- The value of 1 in the parantheses below represents the probability
CREATE OR REPLACE TABLE lineitem_blocksample AS SELECT * FROM TRAINING_DB.TPCH_SF100.lineitem SAMPLE BLOCK (1);

--         View the Query duration in the Query Details pane, this query
--         duration should be faster than the one using the default sampling
--         method.

-- 7.3.6   Determine the number of rows in the lineitem_blocksample table;

SELECT COUNT (*) FROM lineitem_blocksample; 

--         The number of rows in the block sample table will be different from
--         the number of rows in the sample table created with the default
--         sampling method. Block sampling might be biased for small tables. For
--         very large tables, the difference between block and row sampling
--         methods should be negligible.
--         You have successfully created data samples from an existing table;

-- 7.3.7   Suspend your virtual warehouse.

ALTER WAREHOUSE SQUIRREL_wh SUSPEND;


-- 7.4.0   Key takeaways
--         - If minor variances in the results do not cause a problem for your
--         analysis, estimation functions can help you write queries that run
--         much more quickly for your business users.
--         - The BLOCK sampling method is faster than the default ROW sampling
--         method.
