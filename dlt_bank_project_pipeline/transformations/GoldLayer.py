######## Gold aggregation table ########
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.materialized_view(
    name="gold_customer_account_transaction_mv",
    comment="Customer account transaction materialized view"
)
def gol_cust_acc_mv():
    customer=dp.read("Silver_customer_transformed")
    acc=dp.read("silver_accounts_transformed")
    joined=customer.join(acc, on="customer_id",how="inner")
    return joined

@dp.materialized_view(
    name="gold_cust_acc_trans_agg",
    comment="Gold layer aggregated mv of customer & account transaction"
)
def gold_cust_acc_trans_agg():
    df=dp.read("gold_customer_account_transaction_mv")
    df=(
        df.groupBy("customer_id","name","gender","city","status","income_range","risk_segment","customer_age","tenure_days").
        agg(countDistinct("account_id").alias("account_count"), 
            count("*").alias("txn_count"),
            sum(when(col("txn_type") == "Debit", col("txn_amount")).otherwise(lit(0))).alias("total_debit"),
            sum(when(col("txn_type") == "Credit", col("txn_amount")).otherwise(lit(0))).alias("total_credit"),
            avg(col("txn_amount")).alias("avg_txn_amount"),
            min(col("txn_date")).alias("first_txn_date"),
            max(col("txn_date")).alias("last_txn_date"),
            countDistinct(col("txn_channel")).alias("channels_used"))
    )

    return df