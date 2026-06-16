######## Data Cleaning ########
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name="bronze_customer_ingestion_cleaned",
    comment="This table contains the cleaned data for the customers ingested"
)
@dlt.expect_or_fail("valid_customer_id","customer_id IS NOT NULL")
@dlt.expect_or_fail("valid_customer_name","name IS NOT NULL")
@dlt.expect_or_drop("valid_dob","dob IS NOT NULL")
@dlt.expect_or_drop("valid_city","city IS NOT NULL")
@dlt.expect_or_drop("valid_join_date","join_date IS NOT NULL")
@dlt.expect_or_drop("valid_email","email IS NOT NULL")

def customer_ingestion_cleaned():
    df=dlt.read_stream("landing_customers_incremental")
    df=(
        df.withColumn('name',upper(df.name)).
           withColumn('email',upper(df.email)).
           withColumn('occupation',upper(df.occupation)).
           withColumn('city',upper(df.city)).
           withColumn('income_range',upper(df.income_range)).
           withColumn('preferred_channel',upper(df.preferred_channel)).
           withColumn('risk_segment',upper(df.risk_segment)).
           withColumn('gender',when(upper(df.gender)=='M','MALE').when(upper(df.gender)=='F','FEMALE').otherwise(lit('UNKNOWN'))).
           withColumn('status',when( df.status.isNull() | trim(df.status).isNull(),lit('UNKNOWN')).otherwise(df.status)).
           withColumn('phone_number',trim(df.phone)).
           withColumn('phone_number',regexp_replace(col('phone_number'),r'[^0-9]','')).
           filter(col('phone_number').isNotNull()).
           filter(col('preferred_channel').isin('ONLINE','MOBILE','BRANCH','ATM')).
           filter(col('income_range').isin('HIGH','MEDIUM','LOW','VERY HIGH')).
           filter(col('risk_segment').isin('HIGH','MEDIUM','LOW','UNKNOWN'))

        )
    return df