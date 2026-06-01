# Databricks notebook source
# Pyspark Transformations
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta")\
          .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")

display(df)

# COMMAND ----------

df = df.withColumn("amount", col("amount").cast(DoubleType()))\
       .withColumn("modifiedDate", current_timestamp())\
       .withColumn("booking_date",to_date(col("booking_date")))\
       .drop("_rescued_data")

display(df)

# COMMAND ----------
# DLT/ Lakeflow Declarative Pipelines

import dlt 
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

@dlt.table(
    name = "stage_bookings"
)
def stage_bookings():
    df = spark.readStream.format("delta")\
              .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    return df 

# COMMAND ----------

@dlt.view(
    name = "trans_bookings"
)
def trans_bookings():
    df = spark.readStream.table("stage_bookings")
    df = df.withColumn("amount", col("amount").cast(DoubleType()))\
            .withColumn("modifiedDate", current_timestamp())\
            .withColumn("booking_date",to_date(col("booking_date")))\
            .drop("_rescued_data")
    return df 

# COMMAND ----------

rules = {
    "rule1" : "booking_id IS NOT NULL",
    "rule2" : "passenger_id IS NOT NULL",
}

# COMMAND ----------

@dlt.table(
    name = "silver_bookings"
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    return df