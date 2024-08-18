# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA salesdata;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE salesdata.salesorders
# MAGIC (
# MAGIC     orderid INT,
# MAGIC     orderdate DATE,
# MAGIC     customerid INT,
# MAGIC     ordertotal DECIMAL
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/data/sales/';
