# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.silver_passengers

# COMMAND ----------

# # Catalog name
# catalog = "workspace"

# # Key cols list
# key_cols = "['flight_id']" 
# key_cols_list = eval(key_cols)

# # CDC Column
# cdc_col = "modified_date"

# # Backdated Refresh
# backdated_refresh = ""

# # Source object
# source_object = "silver_flights"

# # Source schema
# source_schema = "silver"

# # Target schema
# target_schema = "gold"

# # Target object
# target_object = "DimFlights"

# # Surrogate key name
# surrogate_key = "DimFlightsKey"

# COMMAND ----------

# # Catalog name
# catalog = "workspace"

# # Key cols list
# key_cols = "['airport_id']" 
# key_cols_list = eval(key_cols)

# # CDC Column
# cdc_col = "modified_date"

# # Backdated Refresh
# backdated_refresh = ""

# # Source object
# source_object = "silver_airports"

# # Source schema
# source_schema = "silver"

# # Target schema
# target_schema = "gold"

# # Target object
# target_object = "DimAirports"

# # Surrogate key name
# surrogate_key = "DimAirportsKey"

# COMMAND ----------

# Catalog name
catalog = "workspace"

# Key cols list
key_cols = "['passenger_id']" 
key_cols_list = eval(key_cols)

# CDC Column
cdc_col = "modified_date"

# Backdated Refresh
backdated_refresh = ""

# Source object
source_object = "silver_passengers"

# Source schema
source_schema = "silver"

# Target schema
target_schema = "gold"

# Target object
target_object = "DimPassengers"

# Surrogate key name
surrogate_key = "DimPassengersKey"

# COMMAND ----------

# MAGIC %md
# MAGIC ### **INCREMENTAL DATA INGESTION**

# COMMAND ----------

# MAGIC %md
# MAGIC Last Load Date

# COMMAND ----------

# No back dated refresh
if len(backdated_refresh) == 0:
    
    # If tables exists in destination
    if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
        last_load = spark.sql(f"SELECT max({cdc_col}) FROM {catalog}.{target_schema}.{target_object}").collect()[0][0]

    else:
        last_load = "1900-01-01 00:00:00"

# If there is a backdated refresh date
else:
    last_load = backdated_refresh

# Test the Last load
last_load


# COMMAND ----------

df_src = spark.sql(f"SELECT * FROM {source_schema}.{source_object} WHERE {cdc_col} > '{last_load}'")

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Old vs New Records

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    # Key Columns String for Incremental load
    key_cols_string_incremental = ', '.join(key_cols_list)

    df_trg = spark.sql(f"SELECT {key_cols_string_incremental}, {surrogate_key}, create_date, update_date FROM {catalog}.{target_schema}.{target_object}")

else:
    
    # Key Columns String for Initial load
    key_cols_string_init = [f"'' AS {i}" for i in key_cols_list]
    key_cols_string_init = ', '.join(key_cols_string_init)

    df_trg = spark.sql(f"SELECT {key_cols_string_init}, CAST('0' AS INT) AS {surrogate_key}, CAST('1900-01-01 00:00:00' AS timestamp) AS create_date, CAST('1900-01-01 00:00:00' AS timestamp) AS update_date WHERE 1=0")



# COMMAND ----------

df_trg.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **JOIN CONDITION**

# COMMAND ----------

join_condition = ' AND '.join([f"src.{i} = trg.{i}" for i in key_cols_list])

#join_condition

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")

df_join = spark.sql(f"""
          SELECT src.*,
            trg.{surrogate_key},
            trg.create_date,
            trg.update_date
          FROM src
          LEFT JOIN trg
          ON {join_condition}   
          """)

# COMMAND ----------

df_join.display()

# COMMAND ----------

# Old Records
df_old = df_join.filter(col(f'{surrogate_key}').isNotNull())

# New Records
df_new = df_join.filter(col(f'{surrogate_key}').isNull())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **ENRICHED DFs**

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Preparing df_old**

# COMMAND ----------

df_old_enriched = df_old.withColumn('update_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Preparing df_new**

# COMMAND ----------

df_new.display()

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    max_surrogate_key = spark.sql(f"SELECT MAX({surrogate_key}) FROM {catalog}.{target_schema}.{target_object}").collect()[0][0]

    df_new_enriched = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                    .withColumn('create_date', current_timestamp())\
                    .withColumn('update_date', current_timestamp())


else:
    max_surrogate_key = 0
    df_new_enriched = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                    .withColumn('create_date', current_timestamp())\
                    .withColumn('update_date', current_timestamp())
    
    

# COMMAND ----------

df_new_enriched.display()

# COMMAND ----------

df_old_enriched.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Unioning OLD and NEW records**

# COMMAND ----------

df_union = df_old_enriched.unionByName(df_new_enriched)
df_union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **UPSERT**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}")\
            .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
            .whenNotMatchedInsertAll()\
            .execute()

else:

    df_union.write.format("delta")\
            .mode("append")\
            .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from workspace.gold.dimpassengers where passenger_id = 'P0049';