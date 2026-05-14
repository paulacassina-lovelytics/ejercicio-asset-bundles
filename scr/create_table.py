# Databricks notebook source
dbutils.widgets.text("catalog", "")
catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "")
schema = dbutils.widgets.get("schema")

table_name = f"`{catalog}`.{schema}.ventas"

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"/Volumes/{catalog}/{schema}/inputs/")
)

display(df)

# COMMAND ----------

(
    df.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(table_name)
)
