```python
# Step 1: Import necessary packages and functions
from pyspark.sql.functions import (
    trim,
    upper,
    lower,
    concat,
    lit,
    substring,
    ltrim,
    rtrim,
    replace,
    left,
    when,
    coalesce,
    col,
    avg,
    sum,
    to_date,
    current_date
)
from pyspark.sql.types import FloatType

# Step 2: Load Delta tables
PRIMARY_SALES_ACTUALS = spark.read.format("delta").table("PRIMARY_SALES_ACTUALS")
COMPANYMASTER = spark.read.format("delta").table("COMPANYMASTER")
GEOGRAPHYMASTER = spark.read.format("delta").table("GEOGRAPHYMASTER")
PRODUCTMASTER = spark.read.format("delta").table("PRODUCTMASTER")
PRIMARY_SALES_PLAN_AOP = spark.read.format("delta").table("PRIMARY_SALES_PLAN_AOP")
OUTLETMASTER = spark.read.format("delta").table("OUTLETMASTER")
ACTIVATIONMASTER = spark.read.format("delta").table("ACTIVATIONMASTER")


# Step 3: Perform joins and transformations

#Join all tables
joined_df = PRIMARY_SALES_ACTUALS.join(COMPANYMASTER, PRIMARY_SALES_ACTUALS.COMPANYCODE == COMPANYMASTER.COMPANYCODE, "left")\
    .join(GEOGRAPHYMASTER, PRIMARY_SALES_ACTUALS.STATECODE == GEOGRAPHYMASTER.STATECODE, "left")\
    .join(PRODUCTMASTER, PRIMARY_SALES_ACTUALS.SKUCODE == PRODUCTMASTER.SKUCODE, "left")\
    .join(PRIMARY_SALES_PLAN_AOP, (PRIMARY_SALES_ACTUALS.STATECODE == PRIMARY_SALES_PLAN_AOP.STATECODE) & (PRIMARY_SALES_ACTUALS.SKUCODE == PRIMARY_SALES_PLAN_AOP.SKUCODE), "left")\
    .join(OUTLETMASTER, PRIMARY_SALES_ACTUALS.CUSTOMERCODE == OUTLETMASTER.OUTLET_CODE, "left")\
    .join(ACTIVATIONMASTER, PRIMARY_SALES_ACTUALS.SKUCODE == ACTIVATIONMASTER.PRODUCTLEVELCODE, "left")


# Subquery to find the most recent promotion for each SKU
amr_subquery = ACTIVATIONMASTER.groupBy("PRODUCTLEVELCODE").agg(
    max("EFFECTIVEFROM").alias("EFFECTIVEFROM")
)

#Join with subquery
joined_df = joined_df.join(amr_subquery,(joined_df.SKUCODE == amr_subquery.PRODUCTLEVELCODE) & (joined_df.EFFECTIVEFROM == amr_subquery.EFFECTIVEFROM),"left")

# Create transformations
COMPANYCODE = when( (col("psa.COMPANYCODE").isNull()) | (col("psa.COMPANYCODE") == 'NULL'),lit('UNKNOWN')\
    .otherwise(upper(trim(replace(col("psa.COMPANYCODE")," ",""))))
COMPANYNAME = when((col("cm.COMPANYNAME").isNull()) | (col("cm.COMPANYNAME") == 'NULL'),lit('UNKNOWN')\
    .otherwise(concat(left(trim(col("cm.COMPANYNAME")),3),lit("***"))))
STATECODE = when((col("psa.STATECODE").isNull()) | (col("psa.STATECODE") == 'NULL'),lit('UNKNOWN')\
    .otherwise(ltrim(rtrim(col("psa.STATECODE")))))
STATENAME = when((col("gm.STATENAME").isNull()) | (col("gm.STATENAME") == 'NULL'),lit('UNKNOWN')\
    .otherwise(lower(trim(col("gm.STATENAME")))))
SKUCODE = when((col("psa.SKUCODE").isNull()) | (col("psa.SKUCODE") == 'NULL'),lit('UNKNOWN')\
    .otherwise(substring(ltrim(rtrim(col("psa.SKUCODE"))),1,10)))
ITEMNAME = when((col("pm.ITEMNAME").isNull()) | (col("pm.ITEMNAME") == 'NULL'),lit('UNKNOWN')\
    .otherwise(replace(trim(col("pm.ITEMNAME"))," ","_")))
TOTAL_VOLUMEACTUALCASE = sum(coalesce(trim(col("psa.VOLUMEACTUALCASE")).cast(FloatType()),lit(0)))
TOTAL_PLAN_QTY = sum(coalesce(trim(col("psp.PLAN_QTY")).cast(FloatType()),lit(0)))
AVERAGE_PRICE = avg(coalesce(trim(col("psa.VOLUMEACTUALCASE")).cast(FloatType()),lit(0)))
ACTIVE_FLAG = when(col("om.ACTIVE_FLAG") == 1,lit('YES')).otherwise('NO')
PROMOTION_DESC = when((col("am.PROMOTIONDESCRIPTION").isNull()) | (col("am.PROMOTIONDESCRIPTION") == 'NULL'),lit('NO PROMOTION')\
    .otherwise(concat(lit('Promo: '),ltrim(rtrim(col("am.PROMOTIONDESCRIPTION"))))))
EFFECTIVE_DATE_RANGE_START = when((col("am.EFFECTIVEFROM").isNull()) | (col("am.EFFECTIVEFROM") == 'NULL'),lit('1900-01-01')\
    .otherwise(to_date(col("am.EFFECTIVEFROM"),'yyyy-MM-dd')))
EFFECTIVE_DATE_RANGE_END = when((col("am.EFFECTIVETO").isNull()) | (col("am.EFFECTIVETO") == 'NULL'),current_date()\
    .otherwise(to_date(col("am.EFFECTIVETO"),'yyyy-MM-dd')))


#Filter 
filtered_df = joined_df.filter(coalesce(ltrim(rtrim(col("psa.VOLUMEACTUALCASE"))).cast(FloatType()),lit(0)) > 0)

# Step 4: Group and Aggregate
grouped_df = filtered_df.groupBy(
    COMPANYCODE,
    COMPANYNAME,
    STATECODE,
    STATENAME,
    SKUCODE,
    ITEMNAME,
    ACTIVE_FLAG,
    PROMOTION_DESC,
    EFFECTIVE_DATE_RANGE_START,
    EFFECTIVE_DATE_RANGE_END
).agg(
    TOTAL_VOLUMEACTUALCASE,
    TOTAL_PLAN_QTY,
    AVERAGE_PRICE
)

# Step 6: Insert into FACT_SALES_SUMMARY table
target_table = "FACT_SALES_SUMMARY"
target_columns = [field.name for field in spark.table(target_table).schema]

# Alias columns to match target table if necessary
final_df = grouped_df.select(
    col("COMPANYCODE").alias("COMPANYCODE"),
    col("COMPANYNAME").alias("COMPANYNAME"),
    col("STATECODE").alias("STATECODE"),
    col("STATENAME").alias("STATENAME"),
    col("SKUCODE").alias("SKUCODE"),
    col("ITEMNAME").alias("ITEMNAME"),
    col("TOTAL_VOLUMEACTUALCASE").alias("TOTAL_VOLUMEACTUALCASE"),
    col("TOTAL_PLAN_QTY").alias("TOTAL_PLAN_QTY"),
    col("AVERAGE_PRICE").alias("AVERAGE_PRICE"),
    col("ACTIVE_FLAG").alias("ACTIVE_FLAG"),
    col("PROMOTION_DESC").alias("PROMOTION_DESC"),
    col("EFFECTIVE_DATE_RANGE_START").alias("EFFECTIVE_DATE_RANGE_START"),
    col("EFFECTIVE_DATE_RANGE_END").alias("EFFECTIVE_DATE_RANGE_END")

)



#Add missing columns if any
for col_name in target_columns:
    if col_name not in final_df.columns:
        final_df = final_df.withColumn(col_name, lit(None))

#reorder columns
final_df = final_df.select(target_columns)

# Insert data into the target table
final_df.write.mode("append").insertInto(target_table)

```