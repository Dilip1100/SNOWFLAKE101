CREATE OR REPLACE PROCEDURE RETAIL.BRONZE.INAB()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    DYNAMIC_SQL STRING;
BEGIN
    -- Insert data from staging to bronze table
    DYNAMIC_SQL := ''
        INSERT INTO RETAIL.BRONZE.AB
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
        FROM @RETAIL.BRONZE.BRONZESTG (FILE_FORMAT => RETAIL.BRONZE.TEMP)
        WHERE METADATA$FILENAME NOT IN (SELECT FILENAME FROM RETAIL.BRONZE.PROCESSED);
    '';
    EXECUTE IMMEDIATE :DYNAMIC_SQL;

    -- Merge data from bronze to silver table
    DYNAMIC_SQL := ''
        MERGE INTO RETAIL.SILVER.AB AS target
        USING RETAIL.BRONZE.AB AS source
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

    -- Mark files as processed and truncate the bronze table
    EXECUTE IMMEDIATE ''
        INSERT INTO RETAIL.BRONZE.PROCESSED (FILENAME, PROCESSED_AT)
        SELECT DISTINCT METADATA$FILENAME, CURRENT_TIMESTAMP
        FROM @RETAIL.BRONZE.BRONZESTG;
    '';
    
    EXECUTE IMMEDIATE ''
        TRUNCATE TABLE RETAIL.BRONZE.AB;
    '';

    RETURN ''SUCCESS'';
END;
';
