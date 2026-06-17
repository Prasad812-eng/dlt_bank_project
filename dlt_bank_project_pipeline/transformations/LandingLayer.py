######## Customer 2023 data ingestion ########
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

customer_schema= StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("gender", StringType(), True),
    StructField("city", StringType(), True),
    StructField("join_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("preferred_channel", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("income_range", StringType(), True),
    StructField("risk_segment", StringType(), True)
])

accounts_schema=StructType([
    StructField("account_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", FloatType(), True),
    StructField("txn_id", LongType(), True),
    StructField("txn_date", DateType(), True),
    StructField("txn_type", StringType(), True),
    StructField("txn_amount", DoubleType(), True),
    StructField("txn_channel",StringType(),True)
])

transactions_schema=StructType([
])
@dp.table(
    name="landing_customers_incremental",
    comment="landing customers data"
)
def customers_incremental():
    return(
        spark.readStream.format('cloudFiles').
        option("cloudFiles.format", "csv").
        option("cloudFiles.includeExistingFiles", "true").
        option("header","True").
        schema(customer_schema).
        load("/Volumes/dlt_bank_project/dlt_bank_schema/dlt_bank_volume/customers/")
    )

@dp.table(
    name="landing_accounts_incremental",
    comment="landing accounts transaction data"
)
def accounts_incremental():
    return(
        spark.readStream.format('cloudFiles').
        option("cloudFiles.format", "csv").
        option("cloudFiles.includeExistingFiles", "true").
        option("header","True").
        schema(accounts_schema).
        load("/Volumes/dlt_bank_project/dlt_bank_schema/dlt_bank_volume/accounts/")
    )