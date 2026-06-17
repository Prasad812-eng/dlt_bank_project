######## Data Transormation for customer table ########
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

## Silver customer transformed streaming table
@dp.table(
    name="Silver_customer_transformed",
    comment="transformed customer table"
)
def silver_customer_transformed():
    df=spark.readStream.table("bronze_customer_cleaned")
    df=df.withColumn("customer_age", when(col("dob").isNotNull(), floor(months_between(current_date(), col("dob")) / 12)).otherwise(lit(None)))
    df=df.withColumn("tenure_days", when(col("join_date").isNotNull(), datediff(current_date(),col('join_date'))).otherwise(lit(None)))
    df=df.withColumn("dob_out_of_range_flag", when((col("dob") < lit('1900-01-01')) | (df.dob>current_date()),lit('False')).otherwise(lit('True')))
    df=df.withColumn("transformation_date",current_timestamp())
    return df

### Applying SCD Type1
dp.create_streaming_table("Silver_customer_transformed_scd1")
dp.apply_changes(
    target="Silver_customer_transformed_scd1",
    source="Silver_customer_transformed",
    keys=["customer_id"],
    sequence_by=col("transformation_date"),
    stored_as_scd_type=1,
    except_column_list=["transformation_date"]
)
## View on silver customer transformed table
@dp.view(
    name="Silver_customer_transformed_view",
    comment="view of silver customer transformed table"
)
def silver_customer_view():
    return spark.readStream.table("Silver_customer_transformed")

### Silver account transformed streaming table   
@dp.table(
    name="silver_accounts_transformed",
    comment="transformed accounts table"
)
def silver_accounts_transformed():
    df=spark.readStream.table("bronze_accounts_cleaned")
    df=df.withColumn("channel_type",when(upper(col("txn_channel")).isin('ATM','BRANCH'),"PHYSICAL").otherwise("DIGITAL"))
    df=df.withColumn("txn_direction",when(upper(col("txn_type"))=="CREDIT","OUT").otherwise("IN"))
    df=df.withColumn("txn_year",year(col("txn_date")))
    df=df.withColumn("txn_month",month(col("txn_date")))
    df=df.withColumn("acc_transformation_date",current_timestamp())
    return df

## Applying SCD Type2 using autocdc
dp.create_streaming_table("silver_accounts_transformed_scd2")
dp.create_auto_cdc_flow(
    target="silver_accounts_transformed_scd2",
    source="silver_accounts_transformed",
    keys=["txn_id"],
    sequence_by=col("acc_transformation_date"),
    stored_as_scd_type=2,
    except_column_list=["acc_transformation_date"]
)

## View on silver accounts transformed table
@dp.view(
    name="silver_accounts_transformed_view",
    comment="view of silver accounts transformed table"
)
def silver_accounts_view():
    return spark.readStream.table("silver_accounts_transformed")
