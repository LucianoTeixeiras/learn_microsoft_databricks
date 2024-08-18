# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create Delta table for customer data
# MAGIC CREATE TABLE customer_data (
# MAGIC     customer_id INT,
# MAGIC     customer_name STRING,
# MAGIC     email STRING
# MAGIC );
# MAGIC
# MAGIC -- Create Delta table for transaction data
# MAGIC CREATE TABLE transaction_data (
# MAGIC     transaction_id INT,
# MAGIC     customer_id INT,
# MAGIC     transaction_date DATE,
# MAGIC     amount DOUBLE
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load customer data from CSV
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_data_view AS
# MAGIC SELECT * FROM csv.`/path/to/customer_data.csv`
# MAGIC OPTIONS (header "true", inferSchema "true");
# MAGIC
# MAGIC -- Insert data into customer Delta table
# MAGIC INSERT INTO customer_data
# MAGIC SELECT * FROM customer_data_view;
# MAGIC
# MAGIC -- Load transaction data from JSON
# MAGIC CREATE OR REPLACE TEMPORARY VIEW transaction_data_view AS
# MAGIC SELECT * FROM json.`/path/to/transaction_data.json`;
# MAGIC
# MAGIC -- Insert data into transaction Delta table
# MAGIC INSERT INTO transaction_data
# MAGIC SELECT * FROM transaction_data_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a unified view of customer transactions
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_transactions AS
# MAGIC SELECT
# MAGIC     t.transaction_id,
# MAGIC     t.customer_id,
# MAGIC     c.customer_name,
# MAGIC     c.email,
# MAGIC     t.transaction_date,
# MAGIC     t.amount
# MAGIC FROM
# MAGIC     transaction_data t
# MAGIC JOIN
# MAGIC     customer_data c
# MAGIC ON
# MAGIC     t.customer_id = c.customer_id;
# MAGIC
# MAGIC -- Create a Delta table for the integrated data
# MAGIC CREATE TABLE integrated_data USING DELTA AS
# MAGIC SELECT * FROM customer_transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the integrated data
# MAGIC SELECT
# MAGIC     customer_name,
# MAGIC     SUM(amount) AS total_spent
# MAGIC FROM
# MAGIC     integrated_data
# MAGIC GROUP BY
# MAGIC     customer_name
# MAGIC ORDER BY
# MAGIC     total_spent DESC;
