# Databricks notebook source
df = spark.read.load('/FileStore/tables/products.csv',
    format='csv',
    header=True
)
display(df.limit(10))

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read.format("csv").option("header", "true").load("/FileStore/tables/products.csv")
# MAGIC display(df.limit(10))

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

productSchema = StructType([
    StructField("ProductID", IntegerType()),
    StructField("ProductName", StringType()),
    StructField("Category", StringType()),
    StructField("ListPrice", FloatType())
    ])

df = spark.read.load('/FileStore/tables/products.csv',
    format='csv',
    schema=productSchema,
    header=False)
display(df.limit(10))

# COMMAND ----------

pricelist_df = df.select("ProductID", "ListPrice")
pricelist_df.show()

# COMMAND ----------

pricelist_df = df["ProductID", "ListPrice"].show()

# COMMAND ----------

bikes_df = (
    df.select("ProductName", "ListPrice").where((df["Category"]=="Mountain Bikes") | (df["Category"]=="Road Bikes"))
)
display(bikes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Para agrupar e agregar dados, você pode usar o método groupBy e as funções de agregação. Por exemplo, o seguinte código PySpark conta o número de produtos de cada categoria:

# COMMAND ----------

counts_df = df.select("ProductID", "Category").groupBy("Category").count().alias("Qtde")
display(counts_df)

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

bikes_df = spark.sql("SELECT ProductID, ProductName, ListPrice \
                      FROM products \
                      WHERE Category IN ('Mountain Bikes', 'Road Bikes')")
display(bikes_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Category,
# MAGIC   COUNT(ProductID) AS ProductCount
# MAGIC FROM
# MAGIC   products
# MAGIC GROUP BY
# MAGIC   Category
# MAGIC ORDER BY
# MAGIC   Category

# COMMAND ----------

# MAGIC %md
# MAGIC Por exemplo, você pode usar o código PySpark a seguir para agregar dados dos dados de produtos hipotéticos explorados anteriormente neste módulo e usar Matplotlib para criar um gráfico com base nos dados agregados.

# COMMAND ----------

from matplotlib import pyplot as plt

# Get the data as a Pandas dataframe
data = spark.sql("SELECT Category, COUNT(ProductID) AS ProductCount \
                  FROM products \
                  GROUP BY Category \
                  ORDER BY Category").toPandas()

# Clear the plot area
plt.clf()

# Create a Figure
fig = plt.figure(figsize=(12,8))

# Create a bar plot of product counts by category
plt.bar(x=data['Category'], height=data['ProductCount'], color='orange')

# Customize the chart
plt.title('Product Counts by Category')
plt.xlabel('Category')
plt.ylabel('Products')
plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
plt.xticks(rotation=70)

# Show the plot area
plt.show()

# COMMAND ----------


