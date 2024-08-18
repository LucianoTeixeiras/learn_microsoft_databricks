# Databricks notebook source
# MAGIC %md
# MAGIC Link Task to Execute
# MAGIC
# MAGIC <a href="https://microsoftlearning.github.io/mslearn-databricks/Instructions/Exercises/LA-04-Explore-Delta-Lake.html">Link Task to Execute</a>

# COMMAND ----------

# MAGIC  %sh
# MAGIC  rm -r /dbfs/delta_lab
# MAGIC  mkdir /dbfs/delta_lab
# MAGIC  wget -O /dbfs/delta_lab/products.csv https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/products.csv

# COMMAND ----------

df = spark.read.load('/delta_lab/products.csv', format='csv', header=True)
display(df.limit(10))

# COMMAND ----------

delta_table_path = "/delta/products-delta"
df.write.format("delta").save(delta_table_path)

# COMMAND ----------

# MAGIC  %sh
# MAGIC  ls /dbfs/delta/products-delta

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
   
# Create a deltaTable object
deltaTable = DeltaTable.forPath(spark, delta_table_path)
# Update the table (reduce price of product 771 by 10%)
deltaTable.update(
    condition = "ProductID == 771",
    set = { "ListPrice": "ListPrice * 0.9" })
# View the updated data as a dataframe
deltaTable.toDF().show(10)

# COMMAND ----------

new_df = spark.read.format("delta").load(delta_table_path)
new_df.show(10)

# COMMAND ----------

new_df = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
new_df.show(10)

# COMMAND ----------

deltaTable.history(10).show(10, False, True)

# COMMAND ----------

spark.sql("CREATE DATABASE AdventureWorks")
spark.sql("CREATE TABLE AdventureWorks.ProductsExternal USING DELTA LOCATION '{0}'".format(delta_table_path))
spark.sql("DESCRIBE EXTENDED AdventureWorks.ProductsExternal").show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE AdventureWorks;
# MAGIC SELECT * FROM ProductsExternal;

# COMMAND ----------

df.write.format("delta").saveAsTable("AdventureWorks.ProductsManaged")
spark.sql("DESCRIBE EXTENDED AdventureWorks.ProductsManaged").show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE AdventureWorks;
# MAGIC SELECT * FROM ProductsManaged;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE AdventureWorks;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC  %sh
# MAGIC  echo "External table:"
# MAGIC  ls /dbfs/delta/products-delta
# MAGIC  echo
# MAGIC  echo "Managed table:"
# MAGIC  ls /dbfs/user/hive/warehouse/adventureworks.db/productsmanaged

# COMMAND ----------

# MAGIC %sql
# MAGIC USE AdventureWorks;
# MAGIC DROP TABLE IF EXISTS ProductsExternal;
# MAGIC DROP TABLE IF EXISTS ProductsManaged;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC  %sh
# MAGIC  echo "External table:"
# MAGIC  ls /dbfs/delta/products-delta
# MAGIC  echo
# MAGIC  echo "Managed table:"
# MAGIC  ls /dbfs/user/hive/warehouse/adventureworks.db/productsmanaged

# COMMAND ----------

# MAGIC %sql
# MAGIC USE AdventureWorks;
# MAGIC CREATE TABLE Products
# MAGIC USING DELTA
# MAGIC LOCATION '/delta/products-delta';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE AdventureWorks;
# MAGIC SELECT * FROM Products;

# COMMAND ----------

spark.sql("OPTIMIZE Products")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql("VACUUM Products RETAIN 24 HOURS")

# COMMAND ----------


