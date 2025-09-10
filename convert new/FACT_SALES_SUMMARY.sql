```python
from pyspark.sql.functions import upper, trim, replace, concat, left, lit, lower, substring, ltrim, rtrim, sum, coalesce, avg, when, to_date, col, expr

# Load Delta tables
psa = spark.read.format("delta").table("PRIMARY_SALES_ACTUALS")
cm = spark.read.format("delta").table("COMPANYMASTER")
gm = spark.read.format("delta").table("GEOGRAPHYMASTER")
pm = spark.read.format("delta").table("PRODUCTMASTER")
psp = spark.read.format("delta").table("PRIMARY_SALES_PLAN_AOP")
om = spark.read.format("delta").table("OUTLETMASTER")
am = spark.read.format("delta").table("ACTIVATIONMASTER")


# Perform joins
joined_df = psa.join(cm, psa.COMPANYCODE == cm.COMPANYCODE, "left") \
    .join(gm, psa.STATECODE == gm.STATECODE, "left") \
    .join(pm, psa.SKUCODE == pm.SKUCODE, "left") \
    .join(psp, (psa.STATECODE == psp.STATECODE) & (psa.SKUCODE == psp.SKUCODE), "left") \
    .join(om, psa.CUSTOMERCODE == om.OUTLET_CODE, "left") \
    .join(am, psa.SKUCODE == am.PRODUCTLEVELCODE, "left")


# Subquery to find the most recent promotion for each SKU
amr = am.groupBy("PRODUCTLEVELCODE").agg(expr("max(EFFECTIVEFROM) as EFFECTIVEFROM"))

#Final join with subquery
final_df = joined_df.join(amr,(joined_df.SKUCODE == amr.PRODUCTLEVELCODE) & (joined_df.am.EFFECTIVEFROM == amr.EFFECTIVEFROM),"left")


# Define transformations
COMPANYCODE = when(psa.COMPANYCODE.isNull() | (psa.COMPANYCODE == 'NULL'), 'UNKNOWN').otherwise(upper(trim(replace(psa.COMPANYCODE, ' ', ''))))
COMPANYNAME = when(cm.COMPANYNAME.isNull() | (cm.COMPANYNAME == 'NULL'), 'UNKNOWN').otherwise(concat(left(trim(cm.COMPANYNAME), 3), '***'))
STATECODE = when(psa.STATECODE.isNull() | (psa.STATECODE == 'NULL'), 'UNKNOWN').otherwise(ltrim(rtrim(psa.STATECODE)))
STATENAME = when(gm.STATENAME.isNull() | (gm.STATENAME == 'NULL'), 'UNKNOWN').otherwise(lower(trim(gm.STATENAME)))
SKUCODE = when(psa.SKUCODE.isNull() | (psa.SKUCODE == 'NULL'), 'UNKNOWN').otherwise(substring(ltrim(rtrim(psa.SKUCODE)), 1, 10))
ITEMNAME = when(pm.ITEMNAME.isNull() | (pm.ITEMNAME == 'NULL'), 'UNKNOWN').otherwise(replace(trim(pm.ITEMNAME), ' ', '_'))
TOTAL_VOLUMEACTUALCASE = sum(coalesce(psa.VOLUMEACTUALCASE.cast("float"), lit(0)))
TOTAL_PLAN_QTY = sum(coalesce(psp.PLAN_QTY.cast("float"), lit(0)))
AVERAGE_PRICE = avg(coalesce(psa.VOLUMEACTUALCASE.cast("float"), lit(0)))
ACTIVE_FLAG = when(om.ACTIVE_FLAG == 1, 'YES').otherwise('NO')
PROMOTION_DESC = when(am.PROMOTIONDESCRIPTION.isNull() | (am.PROMOTIONDESCRIPTION == 'NULL'), 'NO PROMOTION').otherwise(concat(lit('Promo: '), ltrim(rtrim(am.PROMOTIONDESCRIPTION))))
EFFECTIVE_DATE_RANGE_START = when(am.EFFECTIVEFROM.isNull() | (am.EFFECTIVEFROM == 'NULL'), lit('1900-01-01')).otherwise(to_date(am.EFFECTIVEFROM, 'yyyy-MM-dd'))
EFFECTIVE_DATE_RANGE_END = when(am.EFFECTIVETO.isNull() | (am.EFFECTIVETO == 'NULL'), expr("current_date")).otherwise(to_date(am.EFFECTIVETO, 'yyyy-MM-dd'))


# Apply transformations and aggregations
transformed_df = final_df.select(
    COMPANYCODE.alias("COMPANYCODE"),
    COMPANYNAME.alias("COMPANYNAME"),
    STATECODE.alias("STATECODE"),
    STATENAME.alias("STATENAME"),
    SKUCODE.alias("SKUCODE"),
    ITEMNAME.alias("ITEMNAME"),
    TOTAL_VOLUMEACTUALCASE,
    TOTAL_PLAN_QTY,
    AVERAGE_PRICE,
    ACTIVE_FLAG,
    PROMOTION_DESC,
    EFFECTIVE_DATE_RANGE_START,
    EFFECTIVE_DATE_RANGE_END
).groupBy(
    "COMPANYCODE",
    "COMPANYNAME",
    "STATECODE",
    "STATENAME",
    "SKUCODE",
    "ITEMNAME",
    "ACTIVE_FLAG",
    "PROMOTION_DESC",
    "EFFECTIVE_DATE_RANGE_START",
    "EFFECTIVE_DATE_RANGE_END"
).agg(
    sum(col("TOTAL_VOLUMEACTUALCASE")).alias("TOTAL_VOLUMEACTUALCASE"),
    sum(col("TOTAL_PLAN_QTY")).alias("TOTAL_PLAN_QTY"),
    avg(col("AVERAGE_PRICE")).alias("AVERAGE_PRICE")
)


# Apply filter
filtered_df = transformed_df.filter(coalesce(psa.VOLUMEACTUALCASE.cast("float"), lit(0)) > 0)

# Insert into target table
target_columns = [field.name for field in spark.table("FACT_SALES_SUMMARY").schema]
final_df_insert = filtered_df.select([col(c).alias(c) for c in target_columns])

# Add missing columns with NULL values
for col_name in target_columns:
    if col_name not in final_df_insert.columns:
        final_df_insert = final_df_insert.withColumn(col_name, lit(None))
        
# Reorder Columns
final_df_insert = final_df_insert.select(target_columns)

final_df_insert.write.mode("append").insertInto("FACT_SALES_SUMMARY")

```