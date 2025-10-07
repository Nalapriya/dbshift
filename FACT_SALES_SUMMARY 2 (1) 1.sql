insert into FACT_SALES_SUMMARY
SELECT 
    -- Clean and transform COMPANYCODE
    iff(psa.COMPANYCODE is null or psa.COMPANYCODE = 'NULL', 'UNKNOWN', UPPER(TRIM(REPLACE(psa.COMPANYCODE, ' ', '')))) as COMPANYCODE,
    -- Join and select COMPANYNAME with cleaning and substring
    iff(cm.COMPANYNAME is null or cm.COMPANYNAME = 'NULL', 'UNKNOWN', CONCAT(LEFT(TRIM(cm.COMPANYNAME), 3), '***')) as COMPANYNAME,
    -- Clean and transform STATECODE
    iff(psa.STATECODE is null or psa.STATECODE = 'NULL', 'UNKNOWN', LTRIM(RTRIM(psa.STATECODE))) as STATECODE,
    -- Join and select STATENAME with cleaning and lower case transformation
    iff(gm.STATENAME is null or gm.STATENAME = 'NULL', 'UNKNOWN', LOWER(TRIM(gm.STATENAME))) as STATENAME,
    -- Clean and transform SKUCODE
    iff(psa.SKUCODE is null or psa.SKUCODE = 'NULL', 'UNKNOWN', SUBSTRING(LTRIM(RTRIM(psa.SKUCODE)), 1, 10)) as SKUCODE,
    -- Join and select ITEMNAME with cleaning and replace
    iff(pm.ITEMNAME is null or pm.ITEMNAME = 'NULL', 'UNKNOWN', REPLACE(TRIM(pm.ITEMNAME), ' ', '_')) as ITEMNAME,
    -- Sum the actual volume with COALESCE and type casting
    sum(coalesce(try_cast(TRIM(psa.VOLUMEACTUALCASE) as float), 0)) as TOTAL_VOLUMEACTUALCASE,
    -- Sum the plan quantity with COALESCE and type casting
    sum(coalesce(try_cast(TRIM(psp.PLAN_QTY) as float), 0)) as TOTAL_PLAN_QTY,
    -- Calculate the average volume with cleaning and type casting
    avg(coalesce(try_cast(TRIM(psa.VOLUMEACTUALCASE) as float), 0)) as AVERAGE_PRICE,
    -- Flag if active using CASE
    case 
        when om.ACTIVE_FLAG = 1 then 'YES'
        else 'NO'
    end as ACTIVE_FLAG,
    -- Include promotion description with cleaning and concatenation
    iff(am.PROMOTIONDESCRIPTION is null or am.PROMOTIONDESCRIPTION = 'NULL', 'NO PROMOTION', CONCAT('Promo: ', LTRIM(RTRIM(am.PROMOTIONDESCRIPTION)))) as PROMOTION_DESC,

        iff(am.EFFECTIVEFROM is null or am.EFFECTIVEFROM = 'NULL', '1900-01-01', TO_DATE(am.EFFECTIVEFROM, 'yyyy-MM-dd')) as EFFECTIVE_DATE_RANGE_START,

        iff(am.EFFECTIVETO is null or am.EFFECTIVETO = 'NULL', CURRENT_DATE, TO_DATE(am.EFFECTIVETO, 'yyyy-MM-dd')) as EFFECTIVE_DATE_RANGE_END
from 
    ALCOBEVDB.ALCOBEV.PRIMARY_SALES_ACTUALS psa
left join 
    ALCOBEVDB.ALCOBEV.COMPANYMASTER cm 
    on psa.COMPANYCODE = cm.COMPANYCODE
left join 
    ALCOBEVDB.ALCOBEV.GEOGRAPHYMASTER gm 
    on psa.STATECODE = gm.STATECODE
left join 
    ALCOBEVDB.ALCOBEV.PRODUCTMASTER pm 
    on psa.SKUCODE = pm.SKUCODE
left join 
    ALCOBEVDB.ALCOBEV.PRIMARY_SALES_PLAN_AOP psp 
    on psa.STATECODE = psp.STATECODE and psa.SKUCODE = psp.SKUCODE
left join 
    ALCOBEVDB.ALCOBEV.OUTLETMASTER om 
    on psa.CUSTOMERCODE = om.OUTLET_CODE
left join 
    ALCOBEVDB.ALCOBEV.ACTIVATIONMASTER am
    on psa.SKUCODE = am.PRODUCTLEVELCODE
left join 
    -- Subquery to find the most recent promotion for each SKU
    (select 
         PRODUCTLEVELCODE, 
         max(EFFECTIVEFROM) as EFFECTIVEFROM 
     from 
         ALCOBEVDB.ALCOBEV.ACTIVATIONMASTER 
     group by 
         PRODUCTLEVELCODE
    ) amr 
    on psa.SKUCODE = amr.PRODUCTLEVELCODE 
    and am.EFFECTIVEFROM = amr.EFFECTIVEFROM
where 
    -- Only include records where the volume is greater than zero
    coalesce(try_cast(LTRIM(RTRIM(psa.VOLUMEACTUALCASE)) as float), 0) > 0
group by 
    iff(psa.COMPANYCODE is null or psa.COMPANYCODE = 'NULL', 'UNKNOWN', UPPER(TRIM(REPLACE(psa.COMPANYCODE, ' ', '')))),
    iff(cm.COMPANYNAME is null or cm.COMPANYNAME = 'NULL', 'UNKNOWN', CONCAT(LEFT(TRIM(cm.COMPANYNAME), 3), '***')),
    iff(psa.STATECODE is null or psa.STATECODE = 'NULL', 'UNKNOWN', LTRIM(RTRIM(psa.STATECODE))),
    iff(gm.STATENAME is null or gm.STATENAME = 'NULL', 'UNKNOWN', LOWER(TRIM(gm.STATENAME))),
    iff(psa.SKUCODE is null or psa.SKUCODE = 'NULL', 'UNKNOWN', SUBSTRING(LTRIM(RTRIM(psa.SKUCODE)), 1, 10)),
    iff(pm.ITEMNAME is null or pm.ITEMNAME = 'NULL', 'UNKNOWN', REPLACE(TRIM(pm.ITEMNAME), ' ', '_')),
    case 
        when om.ACTIVE_FLAG = 1 then 'YES'
        else 'NO'
    end,
    iff(am.PROMOTIONDESCRIPTION is null or am.PROMOTIONDESCRIPTION = 'NULL', 'NO PROMOTION', CONCAT('Promo: ', LTRIM(RTRIM(am.PROMOTIONDESCRIPTION)))),

        iff(am.EFFECTIVEFROM is null or am.EFFECTIVEFROM = 'NULL', '1900-01-01', TO_DATE(am.EFFECTIVEFROM, 'yyyy-MM-dd')),

        iff(am.EFFECTIVETO is null or am.EFFECTIVETO = 'NULL', CURRENT_DATE, TO_DATE(am.EFFECTIVETO, 'yyyy-MM-dd'));