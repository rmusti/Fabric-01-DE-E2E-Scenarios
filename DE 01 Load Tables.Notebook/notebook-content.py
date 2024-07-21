# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "91cac876-080b-4b86-ae2d-47506199aa8b",
# META       "default_lakehouse_name": "wwilakehouse",
# META       "default_lakehouse_workspace_id": "01fc90bd-e289-4438-84bf-130c0b13882d",
# META       "known_lakehouses": [
# META         {
# META           "id": "91cac876-080b-4b86-ae2d-47506199aa8b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

print("Lets Start!")


# CELL ********************

from pyspark.sql.types import *
def loadFullDataFromSource(table_name):
    df = spark.read.format("parquet").load('Files/wwi-raw-data/full/' + table_name)
    for dt in df.dtypes:
        if dt[1] =='binary':
            df=df.drop(dt[0])
            print(f'column {dt[0]} in table {table_name} dropped')
    df.write.mode("overwrite").format("delta").save("Tables/" + table_name)

full_tables = [
    'dimension_city',
    'dimension_date',
    'dimension_employee',
    'dimension_stock_item'
    ]

for table in full_tables:
    loadFullDataFromSource(table)

# CELL ********************

from pyspark.sql.functions import col, year, month, quarter
table_name = 'fact_sale'
df = spark.read.format("parquet").load('Files/wwi-raw-data/full/fact_sale_1y_full')
df = df.withColumn('Year', year(col("InvoiceDateKey")))
df = df.withColumn('Quarter', quarter(col("InvoiceDateKey")))
df = df.withColumn('Month', month(col("InvoiceDateKey")))
df.write.mode("overwrite").format("delta").partitionBy("Year","Quarter").save("Tables/" + table_name)

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE dimension_employee
# MAGIC SET Employee = 'Jack Potter'
# MAGIC WHERE Employee = 'Lily Code'

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE HISTORY dimension_employee

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC RESTORE TABLE dimension_employee TO VERSION AS OF 0

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }
