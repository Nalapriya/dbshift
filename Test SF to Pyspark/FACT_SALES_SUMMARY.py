from pyspark.sql.functions import col, when, lit, upper, trim, substring, ltrim, rtrim, lower, concat, left, coalesce, avg, sum, to_date
from pyspark.sql.types import FloatType

# Step 2: Load Delta Tables
primary_sales_actuals = spark.read.format("delta").table("PRIMARY_SALES_ACTUALS")
companymaster = spark.read.format("delta").table("COMPANYMASTER")
geographymaster = spark.read.format("delta").table("GEOGRAPHYMASTER")
productmaster = spark.read.format("delta").table("PRODUCTMASTER")
primary_sales_plan_aop = spark.read.format("delta").table("PRIMARY_SALES_PLAN_AOP")
outletmaster = spark.read.format("delta").table("OUTLETMASTER")
activationmaster = spark.read.format("delta").table("ACTIVATIONMASTER")


# Step 3: Perform Joins
joined_df = primary_sales_actuals.join(companymaster, primary_sales_actuals.COMPANYCODE == companymaster.COMPANYCODE, "left") \
    .join(geographymaster, primary_sales_actuals.STATECODE == geographymaster.STATECODE, "left") \
    .join(productmaster, primary_sales_actuals.SKUCODE == productmaster.SKUCODE, "left") \
    .join(primary_sales_plan_aop, (primary_sales_actuals.STATECODE == primary_sales_plan_aop.STATECODE) & (primary_sales_actuals.SKUCODE == primary_sales_plan_aop.SKUCODE), "left") \
    .join(outletmaster, primary_sales_actuals.CUSTOMERCODE == outletmaster.OUTLET_CODE, "left") \
    .join(activationmaster, primary_sales_actuals.SKUCODE == activationmaster.PRODUCTLEVELCODE, "left")

# Subquery
amr_df = activationmaster.groupBy("PRODUCTLEVELCODE").agg(max("EFFECTIVEFROM").alias("EFFECTIVEFROM"))

joined_df = joined_df.join(amr_df, (joined_df.SKUCODE == amr_df.PRODUCTLEVELCODE) & (joined_df.activationmaster.EFFECTIVEFROM == amr_df.EFFECTIVEFROM), "left")


# Step 3: Perform Transformations
COMPANYCODE = when(primary_sales_actuals.COMPANYCODE.isNull() | (primary_sales_actuals.COMPANYCODE == 'NULL'), 'UNKNOWN') \
    .otherwise(upper(trim(regexp_replace(primary_sales_actuals.COMPANYCODE, ' ', ''))))
COMPANYNAME = when(companymaster.COMPANYNAME.isNull() | (companymaster.COMPANYNAME == 'NULL'), 'UNKNOWN') \
    .otherwise(concat(left(trim(companymaster.COMPANYNAME), 3), '***'))
STATECODE = when(primary_sales_actuals.STATECODE.isNull() | (primary_sales_actuals.STATECODE == 'NULL'), 'UNKNOWN') \
    .otherwise(ltrim(rtrim(primary_sales_actuals.STATECODE)))
STATENAME = when(geographymaster.STATENAME.isNull() | (geographymaster.STATENAME == 'NULL'), 'UNKNOWN') \
    .otherwise(lower(trim(geographymaster.STATENAME)))
SKUCODE = when(primary_sales_actuals.SKUCODE.isNull() | (primary_sales_actuals.SKUCODE == 'NULL'), 'UNKNOWN') \
    .otherwise(substring(ltrim(rtrim(primary_sales_actuals.SKUCODE)), 1, 10))
ITEMNAME = when(productmaster.ITEMNAME.isNull() | (productmaster.ITEMNAME == 'NULL'), 'UNKNOWN') \
    .otherwise(regexp_replace(trim(productmaster.ITEMNAME), ' ', '_'))
TOTAL_VOLUMEACTUALCASE = sum(coalesce(primary_sales_actuals.VOLUMEACTUALCASE.cast(FloatType()), lit(0)))
TOTAL_PLAN_QTY = sum(coalesce(primary_sales_plan_aop.PLAN_QTY.cast(FloatType()), lit(0)))
AVERAGE_PRICE = avg(coalesce(primary_sales_actuals.VOLUMEACTUALCASE.cast(FloatType()), lit(0)))
ACTIVE_FLAG = when(outletmaster.ACTIVE_FLAG == 1, 'YES').otherwise('NO')
PROMOTION_DESC = when(activationmaster.PROMOTIONDESCRIPTION.isNull() | (activationmaster.PROMOTIONDESCRIPTION == 'NULL'), 'NO PROMOTION') \
    .otherwise(concat('Promo: ', ltrim(rtrim(activationmaster.PROMOTIONDESCRIPTION))))
EFFECTIVE_DATE_RANGE_START = when(activationmaster.EFFECTIVEFROM.isNull() | (activationmaster.EFFECTIVEFROM == 'NULL'), lit('1900-01-01')) \
    .otherwise(to_date(activationmaster.EFFECTIVEFROM, 'yyyy-MM-dd'))
EFFECTIVE_DATE_RANGE_END = when(activationmaster.EFFECTIVETO.isNull() | (activationmaster.EFFECTIVETO == 'NULL'), to_date(lit('2024-11-20'),'yyyy-MM-dd'))\
    .otherwise(to_date(activationmaster.EFFECTIVETO, 'yyyy-MM-dd'))


# Step 4: Apply Filters
filtered_df = joined_df.filter(coalesce(ltrim(rtrim(primary_sales_actuals.VOLUMEACTUALCASE)).cast(FloatType()), lit(0)) > 0)

# Step 5: Aggregation
aggregated_df = filtered_df.groupBy(COMPANYCODE, COMPANYNAME, STATECODE, STATENAME, SKUCODE, ITEMNAME, ACTIVE_FLAG, PROMOTION_DESC, EFFECTIVE_DATE_RANGE_START, EFFECTIVE_DATE_RANGE_END) \
    .agg(TOTAL_VOLUMEACTUALCASE, TOTAL_PLAN_QTY, AVERAGE_PRICE)


#Step 6: handling insert into
from pyspark.sql.functions import col, lit

# Target table name as in the INSERT INTO statement
target_table = "FACT_SALES_SUMMARY"

# Step 1: Extract target table schema
target_columns = [field.name for field in spark.table(target_table).schema]

# Step 2: Alias columns in source DataFrame to match target table columns
aliased_columns = [
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
]

# Create a new DataFrame with aliased columns
df_selected = aggregated_df.select(*aliased_columns)

# Step 3: Add missing columns with NULL values to match target table schema
for col_name in target_columns:
    if col_name not in df_selected.columns:
        df_selected = df_selected.withColumn(col_name, lit(None))  # Add missing columns as NULL

# Step 4: Reorder DataFrame columns to match target table schema
df_selected = df_selected.select(target_columns)

# only after above preprocessing of dataframe is done insert should be done.
# Step 5: Insert the DataFrame into the target table
df_selected.write.mode("append").insertInto(target_table)