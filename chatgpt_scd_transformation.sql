WITH OriginalValues AS (
    -- Step 1: Identify the original value for each opportunityid and fieldname
    SELECT
        opportunityid,
        fieldname,
        MIN(datetimechanged) AS startdatetime,
        NULL AS enddatetime, -- This will represent the first original record
        MIN(oldvalue) AS fieldvalue
    FROM opportunityfieldhistory
    GROUP BY opportunityid, fieldname
),
TransformedHistory AS (
    -- Step 2: Transform the old table into the desired format for historical data
    SELECT
        opportunityid,
        fieldname,
        oldvalue AS fieldvalue,
        datetimechanged AS startdatetime,
        LEAD(datetimechanged) OVER (PARTITION BY opportunityid, fieldname ORDER BY datetimechanged) AS enddatetime
    FROM opportunityfieldhistory
),
FinalTable AS (
    -- Step 3: Union the original values with the historical data
    SELECT
        opportunityid,
        fieldname,
        fieldvalue,
        startdatetime,
        enddatetime
    FROM OriginalValues
    UNION ALL
    SELECT
        opportunityid,
        fieldname,
        fieldvalue,
        startdatetime,
        enddatetime
    FROM TransformedHistory
)
-- Final output: Insert into the new table or use as a SELECT statement
SELECT
    opportunityid,
    fieldname,
    fieldvalue,
    startdatetime,
    enddatetime
FROM FinalTable
ORDER BY opportunityid, fieldname, startdatetime;
