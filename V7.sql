CREATE OR REPLACE PROCEDURE RETAIL.BRONZEST.DEAB()
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

    -- Execute the dynamic SQL for Step 1
    EXECUTE IMMEDIATE :DYNAMIC_SQL;

    -- Step 2: Merge data from BRONZEST.AB to SILVERST.AB
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
                ADDRESSLINE1, ADDRESSLINE2, CITY, CONTACTFIRSTNAME, CONTACTLASTNAME, 
                COUNTRY, CUSTOMERNAME, DEALSIZE, MONTH_ID, MSRP, ORDERDATE, ORDERLINENUMBER, 
                ORDERNUMBER, PHONE, POSTALCODE, PRICEEACH, PRODUCTCODE, PRODUCTLINE, QTR_ID, 
                QUANTITYORDERED, SALES, STATE, STATUS, TERRITORY, YEAR_ID, UPDATEDAT
            )
            VALUES (
                source.ADDRESSLINE1, source.ADDRESSLINE2, source.CITY, source.CONTACTFIRSTNAME, 
                source.CONTACTLASTNAME, source.COUNTRY, source.CUSTOMERNAME, source.DEALSIZE, 
                source.MONTH_ID, source.MSRP, source.ORDERDATE, source.ORDERLINENUMBER, 
                source.ORDERNUMBER, source.PHONE, source.POSTALCODE, source.PRICEEACH, 
                source.PRODUCTCODE, source.PRODUCTLINE, source.QTR_ID, source.QUANTITYORDERED, 
                source.SALES, source.STATE, source.STATUS, source.TERRITORY, source.YEAR_ID, 
                source.UPDATEDAT
            );
    '';
    
    -- Execute the dynamic SQL for Step 2
    EXECUTE IMMEDIATE :DYNAMIC_SQL;

    -- Step 3: Aggregate revenue from SILVERST.AB into GOLDST.AB
    INSERT INTO RETAIL.GOLDST.AB (CUSTOMERNAME, REVENUE)
    SELECT CUSTOMERNAME, SUM(SALES)
    FROM RETAIL.SILVERST.AB
    GROUP BY CUSTOMERNAME;

    -- Step 4: Mark files as processed and truncate the BRONZEST.AB table
    EXECUTE IMMEDIATE '' 
        INSERT INTO RETAIL.BRONZEST.PROCESSED (FILENAME, PROCESSED_AT)
        SELECT DISTINCT METADATA$FILENAME, CURRENT_TIMESTAMP
        FROM @RETAIL.BRONZEST.BRONZESTSTG;
    '';
    
    -- Truncate the BRONZEST.AB table
    EXECUTE IMMEDIATE '' 
        TRUNCATE TABLE RETAIL.BRONZEST.AB;
    '';

    -- Return success message
    RETURN ''PROCESS SUCCESS'';
END;
';
