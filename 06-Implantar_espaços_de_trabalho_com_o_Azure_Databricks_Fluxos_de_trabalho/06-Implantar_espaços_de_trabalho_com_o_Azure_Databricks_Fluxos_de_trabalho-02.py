# Databricks notebook source
# MAGIC %md
# MAGIC Link Task to Execute
# MAGIC
# MAGIC <a href="https://microsoftlearning.github.io/mslearn-databricks/Instructions/Exercises/LA-06-Build-workflow.html">Link Task to Execute</a>

# COMMAND ----------

from IPython.core.display import display, HTML
display(HTML("""<a href="https://microsoftlearning.github.io/mslearn-databricks/Instructions/Exercises/LA-06-Build-workflow.html">text</a>"""))

# COMMAND ----------

# MAGIC  %sh
# MAGIC  rm -r /dbfs/workflow_lab
# MAGIC  mkdir /dbfs/workflow_lab
# MAGIC  wget -O /dbfs/workflow_lab/2019.csv https://github.com/MicrosoftLearning/mslearn-databricks/raw/main/data/2019_edited.csv
# MAGIC  wget -O /dbfs/workflow_lab/2020.csv https://github.com/MicrosoftLearning/mslearn-databricks/raw/main/data/2020_edited.csv
# MAGIC  wget -O /dbfs/workflow_lab/2021.csv https://github.com/MicrosoftLearning/mslearn-databricks/raw/main/data/2021_edited.csv

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
orderSchema = StructType([
     StructField("SalesOrderNumber", StringType()),
     StructField("SalesOrderLineNumber", IntegerType()),
     StructField("OrderDate", DateType()),
     StructField("CustomerName", StringType()),
     StructField("Email", StringType()),
     StructField("Item", StringType()),
     StructField("Quantity", IntegerType()),
     StructField("UnitPrice", FloatType()),
     StructField("Tax", FloatType())
])
df = spark.read.load('/workflow_lab/*.csv', format='csv', schema=orderSchema)
display(df.limit(100))

# COMMAND ----------

from pyspark.sql.functions import col
df = df.dropDuplicates()
df = df.withColumn('Tax', col('UnitPrice') * 0.08)
df = df.withColumn('Tax', col('Tax').cast("float"))

# COMMAND ----------

yearlySales = df.select(year("OrderDate").alias("Year")).groupBy("Year").count().orderBy("Year")
# display(yearlySales)
yearlySales.show()

# COMMAND ----------

# Create a temporary view for the yearlySales DataFrame
yearlySales.createOrReplaceTempView("yearly_sales_view")

# Run SQL query on the temporary view
sqlResult = spark.sql("SELECT * FROM yearly_sales_view")

# Display the result
sqlResult.show()

# COMMAND ----------


