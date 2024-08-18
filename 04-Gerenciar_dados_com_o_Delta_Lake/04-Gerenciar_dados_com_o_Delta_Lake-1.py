# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/products.csv"
file_type = "csv"

# COMMAND ----------

# Create a Delta table
data = spark.range(0, 5)
data.write.format("delta").save("/FileStore/tables/my_delta_table")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la /dbfs/FileStore/tables/my_delta_table

# COMMAND ----------

# Read the CSV file into a DataFrame
df = spark.read.format("csv").option("header", "true").load("/FileStore/tables/products.csv")

# Convert the DataFrame to a Delta table
df.write.format("delta").mode("overwrite").save("/FileStore/tables/products_delta")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la /dbfs/FileStore/tables/products_delta

# COMMAND ----------

# MAGIC %scala
# MAGIC val filePath = "/FileStore/tables/products.csv"
# MAGIC val fileFormat = "csv"
# MAGIC
# MAGIC val df = spark.read.format(fileFormat).option("header", "true").load(filePath)
# MAGIC
# MAGIC // val deltaTablePath = "/delta/products_delta_scala"
# MAGIC val deltaTablePath = "/FileStore/tables/products_delta_scala"
# MAGIC df.write.format("delta").mode("overwrite").save(deltaTablePath)

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la /dbfs/

# COMMAND ----------

# Append data to a Delta table using DataFrame API
new_data = spark.range(5, 10)
new_data.write.format("delta").mode("append").save("/FileStore/tables/my_delta_table")

# COMMAND ----------

# Optimize the Delta table
spark.sql("OPTIMIZE '/FileStore/tables/my_delta_table'")

# Clean up old files
spark.sql("VACUUM '/FileStore/tables/my_delta_table' RETAIN 168 HOURS")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a Delta table using Spark SQL
# MAGIC CREATE TABLE my_delta_table_schema (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     age INT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert valid data
# MAGIC INSERT INTO my_delta_table_schema (id, name, age)
# MAGIC VALUES
# MAGIC (1, 'Alice', 30),
# MAGIC (2, 'Bob', 25);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attempt to insert data with an invalid schema (missing 'age' field)
# MAGIC INSERT INTO my_delta_table_schema (id, name)
# MAGIC VALUES
# MAGIC (3, 'Charlie');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Define a temporary view with new data
# MAGIC CREATE OR REPLACE TEMP VIEW my_new_delta_table_schema AS
# MAGIC SELECT * FROM VALUES
# MAGIC (3, 'Charlie', 28),
# MAGIC (4, 'Diana', 35)
# MAGIC AS my_new_delta_table_schema(id, name, age);
# MAGIC
# MAGIC -- Use MERGE to upsert data
# MAGIC MERGE INTO my_delta_table_schema AS target
# MAGIC USING my_new_delta_table_schema AS source
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.name = source.name,
# MAGIC     target.age = source.age
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (id, name, age)
# MAGIC   VALUES (source.id, source.name, source.age);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert data with casting to match the schema
# MAGIC INSERT INTO my_delta_table_schema
# MAGIC SELECT
# MAGIC   cast(id as INT),
# MAGIC   cast(name as STRING),
# MAGIC   cast(age as INT)
# MAGIC FROM my_new_delta_table_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the Delta table
# MAGIC CREATE TABLE person_data (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     age INT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert initial data
# MAGIC INSERT INTO person_data (id, name, age)
# MAGIC VALUES
# MAGIC (1, 'Alice', 30),
# MAGIC (2, 'Bob', 25);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update age of Bob
# MAGIC UPDATE person_data
# MAGIC SET age = 26
# MAGIC WHERE name = 'Bob';
# MAGIC
# MAGIC -- Insert a new record
# MAGIC INSERT INTO person_data (id, name, age)
# MAGIC VALUES
# MAGIC (3, 'Charlie', 28);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table history
# MAGIC DESCRIBE HISTORY person_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query data as of version 0
# MAGIC SELECT * FROM person_data VERSION AS OF 0;
# MAGIC
# MAGIC -- Query data as of a specific timestamp
# MAGIC SELECT * FROM person_data TIMESTAMP AS OF '2024-08-17T10:00:00Z';

# COMMAND ----------

df = spark.read.format("delta").option("versionAsOf", 3).load("/FileStore/tables/table")

# COMMAND ----------


