######## Data Cleaning ########
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

txn_types={"Credit":"Credit","Crediit":"Credit","Debit":"Debit","Debiit":"Debit"}
@dp.table(
    name="bronze_customer_cleaned",
    comment="This table contains the cleaned data for the customers ingested"
)
@dp.expect_or_fail("valid_customer_id","customer_id IS NOT NULL")
@dp.expect_or_fail("valid_customer_name","name IS NOT NULL")
@dp.expect_or_drop("valid_dob","dob IS NOT NULL")
@dp.expect_or_drop("valid_city","city IS NOT NULL")
@dp.expect_or_drop("valid_join_date","join_date IS NOT NULL")
@dp.expect_or_drop("valid_email","email IS NOT NULL")

def customer_ingestion_cleaned():
    df=dp.read_stream("landing_customers_incremental")
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
@dp.table(
    name="bronze_accounts_cleaned",
    comment="This table contains the cleaned data from the accounts transactions"
)
@dp.expect_or_fail("valid_account_id","account_id IS NOT NULL")
@dp.expect_or_fail("valid_customer_id","customer_id IS NOT NULL")
@dp.expect_or_fail("valid_txn_id","balance IS NOT NULL")
@dp.expect_or_drop("valid_account_type","account_type IS NOT NULL")
@dp.expect_or_drop("valid_txn_date","txn_date IS NOT NULL")
@dp.expect_or_drop("valid_txn_channel","txn_channel IS NOT NULL")
def accounts_cleaned_incremental():
    df=dp.read_stream("landing_accounts_incremental")
    df=(
        df.withColumn('txn_type',when(df.txn_type=='Creditt','Credit').when(df.txn_type=='Debitt','Debit').otherwise(df.txn_type))
    )
    return df

