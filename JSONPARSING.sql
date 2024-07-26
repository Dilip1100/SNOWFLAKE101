CREATE OR REPLACE STORAGE INTEGRATION AWS_SNOW
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::471112563078:role/DK1100'
STORAGE_ALLOWED_LOCATIONS = ('s3://snowpoc1100/');


-----------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE STAGE RETAIL.BRONZEST.BRONZESTSTG
URL = 's3://snowpoc1100/'
STORAGE_INTEGRATION = AWS_SNOW
FILE_FORMAT = RETAIL.BRONZEST.JAB1100
---------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PIPE RETAIL.BRONZEST.AWS_SNOWPIPE
auto_ingest=true as COPY INTO RETAIL.BRONZEST.AB
FROM @RETAIL.BRONZEST.BRONZESTSTG
FILE_FORMAT = RETAIL.BRONZEST.JAB1100
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
ON_ERROR = 'SKIP_FILE';

----------------------------------------------------------------------------------------------------------------
---TABLES INITIAL CREATION---

CREATE OR REPLACE TABLE RETAIL.BRONZEST.AB (
	ADDRESSLINE1 VARCHAR(16777216),
	ADDRESSLINE2 VARCHAR(16777216),
	CITY VARCHAR(16777216),
	CONTACTFIRSTNAME VARCHAR(16777216),
	CONTACTLASTNAME VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	CUSTOMERNAME VARCHAR(16777216),
	DEALSIZE VARCHAR(16777216),
	MONTH_ID VARCHAR(16777216),
	MSRP VARCHAR(16777216),
	ORDERDATE VARCHAR(16777216),
	ORDERLINENUMBER VARCHAR(16777216),
	ORDERNUMBER VARCHAR(16777216),
	PHONE VARCHAR(16777216),
	POSTALCODE VARCHAR(16777216),
	PRICEEACH VARCHAR(16777216),
	PRODUCTCODE VARCHAR(16777216),
	PRODUCTLINE VARCHAR(16777216),
	QTR_ID VARCHAR(16777216),
	QUANTITYORDERED VARCHAR(16777216),
	SALES VARCHAR(16777216),
	STATE VARCHAR(16777216),
	STATUS VARCHAR(16777216),
	TERRITORY VARCHAR(16777216),
	YEAR_ID VARCHAR(16777216),
	UPDATEDAT TIMESTAMP_LTZ(9)
);
CREATE OR REPLACE TABLE RETAIL.SILVERST.AB (
	ADDRESSLINE1 VARCHAR(16777216),
	ADDRESSLINE2 VARCHAR(16777216),
	CITY VARCHAR(16777216),
	CONTACTFIRSTNAME VARCHAR(16777216),
	CONTACTLASTNAME VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	CUSTOMERNAME VARCHAR(16777216),
	DEALSIZE VARCHAR(16777216),
	MONTH_ID VARCHAR(16777216),
	MSRP VARCHAR(16777216),
	ORDERDATE VARCHAR(16777216),
	ORDERLINENUMBER VARCHAR(16777216),
	ORDERNUMBER VARCHAR(16777216),
	PHONE VARCHAR(16777216),
	POSTALCODE VARCHAR(16777216),
	PRICEEACH VARCHAR(16777216),
	PRODUCTCODE VARCHAR(16777216),
	PRODUCTLINE VARCHAR(16777216),
	QTR_ID VARCHAR(16777216),
	QUANTITYORDERED VARCHAR(16777216),
	SALES VARCHAR(16777216),
	STATE VARCHAR(16777216),
	STATUS VARCHAR(16777216),
	TERRITORY VARCHAR(16777216),
	YEAR_ID VARCHAR(16777216),
	UPDATEDAT TIMESTAMP_LTZ(9)
);

CREATE OR REPLACE TABLE RETAIL.GOLDSTST.AB (
	CUSTOMERNAME VARCHAR(16777216),
	REVENUE FLOAT
);
------------------------------------------------------------------------------------------
CREATE OR REPLACE PIPE RETAIL.BRONZEST.BRONZESTSTG_PIPE
AUTO_INGEST = TRUE
AS 
COPY INTO RETAIL.BRONZEST.ABRAW
FROM @RETAIL.BRONZEST.BRONZESTSTG
FILE_FORMAT = (FORMAT_NAME = RETAIL.BRONZEST.JAB1100)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;
--------------------------------------------------------------------------------------------
--TASK APPROACH--
CREATE OR REPLACE TASK RETAIL.BRONZEST.TRANSFORM
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
INSERT INTO RETAIL.BRONZEST.AB (
    ADDRESSLINE1, ADDRESSLINE2, CITY, CONTACTFIRSTNAME, CONTACTLASTNAME, COUNTRY, CUSTOMERNAME, DEALSIZE, MONTH_ID, MSRP, ORDERDATE, ORDERLINENUMBER, ORDERNUMBER, PHONE, POSTALCODE, PRICEEACH, PRODUCTCODE, PRODUCTLINE, QTR_ID, QUANTITYORDERED, SALES, STATE, STATUS, TERRITORY, YEAR_ID, UPDATEDAT
)
SELECT 
    DATA:"ADDRESSLINE1"::STRING,
    DATA:"ADDRESSLINE2"::STRING,
    DATA:"CITY"::STRING,
    DATA:"CONTACTFIRSTNAME"::STRING,
    DATA:"CONTACTLASTNAME"::STRING,
    DATA:"COUNTRY"::STRING,
    DATA:"CUSTOMERNAME"::STRING,
    DATA:"DEALSIZE"::STRING,
    DATA:"MONTH_ID"::STRING,
    DATA:"MSRP"::STRING,
    DATA:"ORDERDATE"::STRING,
    DATA:"ORDERLINENUMBER"::STRING,
    DATA:"ORDERNUMBER"::STRING,
    DATA:"PHONE"::STRING,
    DATA:"POSTALCODE"::STRING,
    DATA:"PRICEEACH"::STRING,
    DATA:"PRODUCTCODE"::STRING,
    DATA:"PRODUCTLINE"::STRING,
    DATA:"QTR_ID"::STRING,
    DATA:"QUANTITYORDERED"::STRING,
    DATA:"SALES"::STRING,
    DATA:"STATE"::STRING,
    DATA:"STATUS"::STRING,
    DATA:"TERRITORY"::STRING,
    DATA:"YEAR_ID"::STRING,
    CURRENT_TIMESTAMP
FROM RETAIL.BRONZEST.ABRAW;
MERGE INTO RETAIL.SILVERST.AB AS target
        USING RETAIL.BRONZEST.AB AS source
        ON target.ORDERNUMBER = source.ORDERNUMBER
        AND target.UPDATEDAT = source.UPDATEDAT
        WHEN MATCHED THEN
            UPDATE SET 
                ADDRESSLINE1 = source.ADDRESSLINE1,
                ADDRESSLINE2 = source.ADDRESSLINE2,
                CITY = source.CITY,
                CONTACTFIRSTNAME = source.CONTACTFIRSTNAME,
                CONTACTLASTNAME = source.CONTACTLASTNAME,
                COUNTRY = source.COUNTRY,
                CUSTOMERNAME = source.CUSTOMERNAME,
                DEALSIZE = source.DEALSIZE,
                MONTH_ID = source.MONTH_ID,
                MSRP = source.MSRP,
                ORDERDATE = source.ORDERDATE,
                ORDERLINENUMBER = source.ORDERLINENUMBER,
                PHONE = source.PHONE,
                POSTALCODE = source.POSTALCODE,
                PRICEEACH = source.PRICEEACH,
                PRODUCTCODE = source.PRODUCTCODE,
                PRODUCTLINE = source.PRODUCTLINE,
                QTR_ID = source.QTR_ID,
                QUANTITYORDERED = source.QUANTITYORDERED,
                SALES = source.SALES,
                STATE = source.STATE,
                STATUS = source.STATUS,
                TERRITORY = source.TERRITORY,
                YEAR_ID = source.YEAR_ID,
                UPDATEDAT = source.UPDATEDAT
        WHEN NOT MATCHED THEN
            INSERT (
                ADDRESSLINE1,
                ADDRESSLINE2,
                CITY,
                CONTACTFIRSTNAME,
                CONTACTLASTNAME,
                COUNTRY,
                CUSTOMERNAME,
                DEALSIZE,
                MONTH_ID,
                MSRP,
                ORDERDATE,
                ORDERLINENUMBER,
                ORDERNUMBER,
                PHONE,
                POSTALCODE,
                PRICEEACH,
                PRODUCTCODE,
                PRODUCTLINE,
                QTR_ID,
                QUANTITYORDERED,
                SALES,
                STATE,
                STATUS,
                TERRITORY,
                YEAR_ID,
                UPDATEDAT
            )
            VALUES (
                source.ADDRESSLINE1, 
                source.ADDRESSLINE2, 
                source.CITY, 
                source.CONTACTFIRSTNAME, 
                source.CONTACTLASTNAME, 
                source.COUNTRY, 
                source.CUSTOMERNAME, 
                source.DEALSIZE, 
                source.MONTH_ID, 
                source.MSRP, 
                source.ORDERDATE, 
                source.ORDERLINENUMBER, 
                source.ORDERNUMBER, 
                source.PHONE, 
                source.POSTALCODE, 
                source.PRICEEACH, 
                source.PRODUCTCODE, 
                source.PRODUCTLINE, 
                source.QTR_ID, 
                source.QUANTITYORDERED, 
                source.SALES, 
                source.STATE, 
                source.STATUS, 
                source.TERRITORY, 
                source.YEAR_ID,
                source.UPDATEDAT
            );
    '';
	
---------------------------------------------------------------------------------------------------------------------------

----STORED PROCEDURE APPROACH IS AS FOLLOWS----- 

CREATE OR REPLACE PROCEDURE RETAIL.BRONZEST.INAB()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    DYNAMIC_SQL STRING;
BEGIN
    -- Insert data from staging to BRONZEST table
    DYNAMIC_SQL := ''
        INSERT INTO RETAIL.BRONZEST.AB
        SELECT
            $1:ADDRESSLINE1::STRING AS ADDRESSLINE1, 
            $1:ADDRESSLINE2::STRING AS ADDRESSLINE2, 
            $1:CITY::STRING AS CITY, 
            $1:CONTACTFIRSTNAME::STRING AS CONTACTFIRSTNAME, 
            $1:CONTACTLASTNAME::STRING AS CONTACTLASTNAME, 
            $1:COUNTRY::STRING AS COUNTRY, 
            $1:CUSTOMERNAME::STRING AS CUSTOMERNAME, 
            $1:DEALSIZE::STRING AS DEALSIZE, 
            $1:MONTH_ID::STRING AS MONTH_ID, 
            $1:MSRP::STRING AS MSRP, 
            $1:ORDERDATE::STRING AS ORDERDATE, 
            $1:ORDERLINENUMBER::STRING AS ORDERLINENUMBER, 
            $1:ORDERNUMBER::STRING AS ORDERNUMBER, 
            $1:PHONE::STRING AS PHONE, 
            $1:POSTALCODE::STRING AS POSTALCODE, 
            $1:PRICEEACH::STRING AS PRICEEACH, 
            $1:PRODUCTCODE::STRING AS PRODUCTCODE, 
            $1:PRODUCTLINE::STRING AS PRODUCTLINE, 
            $1:QTR_ID::STRING AS QTR_ID, 
            $1:QUANTITYORDERED::STRING AS QUANTITYORDERED, 
            $1:SALES::STRING AS SALES, 
            $1:STATE::STRING AS STATE, 
            $1:STATUS::STRING AS STATUS, 
            $1:TERRITORY::STRING AS TERRITORY, 
            $1:YEAR_ID::STRING AS YEAR_ID,
            CURRENT_TIMESTAMP AS UPDATEDAT
        FROM @RETAIL.BRONZEST.BRONZESTSTG (FILE_FORMAT => RETAIL.BRONZEST.JAB1100)
        WHERE METADATA$FILENAME NOT IN (SELECT FILENAME FROM RETAIL.BRONZEST.PROCESSED);
    '';
    EXECUTE IMMEDIATE :DYNAMIC_SQL;

    -- Merge data from BRONZEST to SILVERST table
    DYNAMIC_SQL := ''
        MERGE INTO RETAIL.SILVERST.AB AS target
        USING RETAIL.BRONZEST.AB AS source
        ON target.ORDERNUMBER = source.ORDERNUMBER
        AND target.UPDATEDAT = source.UPDATEDAT
        WHEN MATCHED THEN
            UPDATE SET 
                ADDRESSLINE1 = source.ADDRESSLINE1,
                ADDRESSLINE2 = source.ADDRESSLINE2,
                CITY = source.CITY,
                CONTACTFIRSTNAME = source.CONTACTFIRSTNAME,
                CONTACTLASTNAME = source.CONTACTLASTNAME,
                COUNTRY = source.COUNTRY,
                CUSTOMERNAME = source.CUSTOMERNAME,
                DEALSIZE = source.DEALSIZE,
                MONTH_ID = source.MONTH_ID,
                MSRP = source.MSRP,
                ORDERDATE = source.ORDERDATE,
                ORDERLINENUMBER = source.ORDERLINENUMBER,
                PHONE = source.PHONE,
                POSTALCODE = source.POSTALCODE,
                PRICEEACH = source.PRICEEACH,
                PRODUCTCODE = source.PRODUCTCODE,
                PRODUCTLINE = source.PRODUCTLINE,
                QTR_ID = source.QTR_ID,
                QUANTITYORDERED = source.QUANTITYORDERED,
                SALES = source.SALES,
                STATE = source.STATE,
                STATUS = source.STATUS,
                TERRITORY = source.TERRITORY,
                YEAR_ID = source.YEAR_ID,
                UPDATEDAT = source.UPDATEDAT
        WHEN NOT MATCHED THEN
            INSERT (
                ADDRESSLINE1,
                ADDRESSLINE2,
                CITY,
                CONTACTFIRSTNAME,
                CONTACTLASTNAME,
                COUNTRY,
                CUSTOMERNAME,
                DEALSIZE,
                MONTH_ID,
                MSRP,
                ORDERDATE,
                ORDERLINENUMBER,
                ORDERNUMBER,
                PHONE,
                POSTALCODE,
                PRICEEACH,
                PRODUCTCODE,
                PRODUCTLINE,
                QTR_ID,
                QUANTITYORDERED,
                SALES,
                STATE,
                STATUS,
                TERRITORY,
                YEAR_ID,
                UPDATEDAT
            )
            VALUES (
                source.ADDRESSLINE1, 
                source.ADDRESSLINE2, 
                source.CITY, 
                source.CONTACTFIRSTNAME, 
                source.CONTACTLASTNAME, 
                source.COUNTRY, 
                source.CUSTOMERNAME, 
                source.DEALSIZE, 
                source.MONTH_ID, 
                source.MSRP, 
                source.ORDERDATE, 
                source.ORDERLINENUMBER, 
                source.ORDERNUMBER, 
                source.PHONE, 
                source.POSTALCODE, 
                source.PRICEEACH, 
                source.PRODUCTCODE, 
                source.PRODUCTLINE, 
                source.QTR_ID, 
                source.QUANTITYORDERED, 
                source.SALES, 
                source.STATE, 
                source.STATUS, 
                source.TERRITORY, 
                source.YEAR_ID,
                source.UPDATEDAT
            );
    '';
    EXECUTE IMMEDIATE :DYNAMIC_SQL;

    -- Mark files as processed and truncate the BRONZEST table
    EXECUTE IMMEDIATE ''
        INSERT INTO RETAIL.BRONZEST.PROCESSED (FILENAME, PROCESSED_AT)
        SELECT DISTINCT METADATA$FILENAME, CURRENT_TIMESTAMP
        FROM @RETAIL.BRONZEST.BRONZESTSTG;
    '';
    
    EXECUTE IMMEDIATE ''
        TRUNCATE TABLE RETAIL.BRONZEST.AB;
    '';

    RETURN ''SUCCESS'';
END;
';
------------------------------------------------------------------------------------------------------------------------------
--GOLDST LAYER AGGREGATION FOR DETERMINING THE TOTAL REVENUE PER CUSTOMER --

CREATE OR REPLACE PROCEDURE RETAIL.SILVERST.REVENUE()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
BEGIN
INSERT INTO RETAIL.GOLDSTST.AB (CUSTOMERNAME, REVENUE)
        SELECT DISTINCT CUSTOMERNAME, SUM(SALES)
        FROM RETAIL.SILVERSTST.AB
        GROUP BY CUSTOMERNAME;
RETURN 'REVENUE LOAD SUCCESS';
END;