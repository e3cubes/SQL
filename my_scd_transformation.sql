WITH newvalue AS (
    SELECT opportunityid
    ,field AS fieldname
    ,newvalue as fieldvalue
    ,createddate AS startdate
    ,RANK() OVER(PARTITION BY opportunityid,field ORDER BY createddate) AS rank

    FROM opportunityfieldhistory

    WHERE field='CloseDate'
    AND dw_delete_flag='N'
    AND isdeleted='f'
    AND datatype='DateOnly'
)

,oldvalue AS (
    SELECT opportunityid
    ,field AS fieldname
    ,oldvalue as fieldvalue
    ,createddate AS enddate
    ,(RANK() OVER(PARTITION BY opportunityid,field ORDER BY createddate)-1) AS rank

    FROM opportunityfieldhistory

    WHERE field='CloseDate'
    AND dw_delete_flag='N'
    AND isdeleted='f'
    AND datatype='DateOnly'
)

SELECT NVL(oldvalue.opportunityid,newvalue.opportunityid) AS opportunityid
,NVL(oldvalue.fieldname,newvalue.fieldname) AS  fieldname
,NVL(oldvalue.fieldvalue,newvalue.fieldvalue) AS  fieldvalue
,NVL(newvalue.startdate::DATE,'1900-01-01'::DATE) AS startdate
,NVL(oldvalue.enddate::DATE,'2099-12-31'::DATE) AS enddate
,NVL(oldvalue.rank,newvalue.rank) AS rank

FROM oldvalue
FULL OUTER JOIN newvalue ON oldvalue.opportunityid = newvalue.opportunityid
    AND oldvalue.fieldname = newvalue.fieldname
    AND oldvalue.rank = newvalue.rank /*this is the secret sauce!*/