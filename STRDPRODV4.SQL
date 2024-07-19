CREATE OR REPLACE PROCEDURE RETAIL.BRONZE.INAB()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS $$
DECLARE
    FN STRING;
    FILE_CURSOR CURSOR FOR
        SELECT DISTINCT METADATA$FILENAME
        FROM @RETAIL.BRONZE.BRONZESTG;
BEGIN
    -- Open the cursor
    OPEN FILE_CURSOR;

    -- Loop through the filenames
    FOR FILE_RECORD IN FILE_CURSOR DO
        FN := FILE_RECORD.METADATA$FILENAME;

        -- Drop the temporary table if it exists
        DROP TABLE IF EXISTS RETAIL.BRONZE.TEMP_STAGE_DATA;

        -- Create a temporary table to load the staged data for the current file
        CREATE TEMPORARY TABLE RETAIL.BRONZE.TEMP_STAGE_DATA AS 
        SELECT 
            $1:ADDRESSLINE1::VARCHAR AS ADDRESSLINE1, 
            $1:ADDRESSLINE2::VARCHAR AS ADDRESSLINE2, 
            $1:CITY::VARCHAR AS CITY, 
            $1:CONTACTFIRSTNAME::VARCHAR AS CONTACTFIRSTNAME, 
            $1:CONTACTLASTNAME::VARCHAR AS CONTACTLASTNAME, 
            $1:COUNTRY::VARCHAR AS COUNTRY, 
            $1:CUSTOMERNAME::VARCHAR AS CUSTOMERNAME, 
            $1:DEALSIZE::VARCHAR AS DEALSIZE, 
            $1:MONTH_ID::VARCHAR AS MONTH_ID, 
            $1:MSRP::VARCHAR AS MSRP, 
            $1:ORDERDATE::VARCHAR AS ORDERDATE, 
            $1:ORDERLINENUMBER::VARCHAR AS ORDERLINENUMBER, 
            $1:ORDERNUMBER::VARCHAR AS ORDERNUMBER, 
            $1:PHONE::VARCHAR AS PHONE, 
            $1:POSTALCODE::VARCHAR AS POSTALCODE, 
            $1:PRICEEACH::VARCHAR AS PRICEEACH, 
            $1:PRODUCTCODE::VARCHAR AS PRODUCTCODE, 
            $1:PRODUCTLINE::VARCHAR AS PRODUCTLINE, 
            $1:QTR_ID::VARCHAR AS QTR_ID, 
            $1:QUANTITYORDERED::VARCHAR AS QUANTITYORDERED, 
            $1:SALES::VARCHAR AS SALES, 
            $1:STATE::VARCHAR AS STATE, 
            $1:STATUS::VARCHAR AS STATUS, 
            $1:TERRITORY::VARCHAR AS TERRITORY, 
            $1:YEAR_ID::VARCHAR AS YEAR_ID,
            CURRENT_TIMESTAMP AS UPDATEDAT
        FROM @RETAIL.BRONZE.BRONZESTG (FILE_FORMAT => 'RETAIL.BRONZE.TEMP')
        WHERE METADATA$FILENAME = :FN;

        -- Perform the update
        UPDATE RETAIL.SILVER.AB AS target
        SET 
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
        FROM RETAIL.BRONZE.TEMP_STAGE_DATA AS source
        WHERE target.ORDERNUMBER = source.ORDERNUMBER
        AND target.UPDATEDAT != source.UPDATEDAT;

        -- Perform the insert
        INSERT INTO RETAIL.SILVER.AB (
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
        SELECT 
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
        FROM RETAIL.BRONZE.TEMP_STAGE_DATA
        WHERE ORDERNUMBER NOT IN (SELECT ORDERNUMBER FROM RETAIL.SILVER.AB);

        -- Truncate the temporary table
        TRUNCATE TABLE RETAIL.BRONZE.TEMP_STAGE_DATA;
    END FOR;

    -- Close the cursor
    CLOSE FILE_CURSOR;

    RETURN 'Data load completed successfully';
END;
$$;
