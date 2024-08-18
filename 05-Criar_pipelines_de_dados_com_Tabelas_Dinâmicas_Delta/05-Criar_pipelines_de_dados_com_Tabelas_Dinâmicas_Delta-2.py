# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC Link Task to Execute
# MAGIC
# MAGIC <a href="https://microsoftlearning.github.io/mslearn-databricks/Instructions/Exercises/LA-05-Build-data-pipeline.html">Link Task to Execute</a>

# COMMAND ----------

# MAGIC  %sh
# MAGIC  rm -r /dbfs/delta_lab
# MAGIC  mkdir /dbfs/delta_lab
# MAGIC  wget -O /dbfs/delta_lab/covid_data.csv https://github.com/MicrosoftLearning/mslearn-databricks/raw/main/data/covid_data.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC  CREATE OR REFRESH LIVE TABLE raw_covid_data
# MAGIC  COMMENT "COVID sample dataset. This data was ingested from the COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University."
# MAGIC  AS
# MAGIC  SELECT
# MAGIC    Last_Update,
# MAGIC    Country_Region,
# MAGIC    Confirmed,
# MAGIC    Deaths,
# MAGIC    Recovered
# MAGIC  FROM read_files('dbfs:/delta_lab/covid_data.csv', format => 'csv', header => true)

# COMMAND ----------

# MAGIC %sql
# MAGIC  CREATE OR REFRESH LIVE TABLE processed_covid_data(
# MAGIC    CONSTRAINT valid_country_region EXPECT (Country_Region IS NOT NULL) ON VIOLATION FAIL UPDATE
# MAGIC  )
# MAGIC  COMMENT "Formatted and filtered data for analysis."
# MAGIC  AS
# MAGIC  SELECT
# MAGIC      DATE_FORMAT(Last_Update, 'MM/dd/yyyy') as Report_Date,
# MAGIC      Country_Region,
# MAGIC      Confirmed,
# MAGIC      Deaths,
# MAGIC      Recovered
# MAGIC  FROM live.raw_covid_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC  CREATE OR REFRESH LIVE TABLE aggregated_covid_data
# MAGIC  COMMENT "Aggregated daily data for the US with total counts."
# MAGIC  AS
# MAGIC  SELECT
# MAGIC      Report_Date,
# MAGIC      sum(Confirmed) as Total_Confirmed,
# MAGIC      sum(Deaths) as Total_Deaths,
# MAGIC      sum(Recovered) as Total_Recovered
# MAGIC  FROM live.processed_covid_data
# MAGIC  GROUP BY Report_Date;

# COMMAND ----------

# MAGIC %sh
# MAGIC echo "External table:"
# MAGIC ls /dbfs/delta

# COMMAND ----------

display(dbutils.fs.ls("/pipelines/delta_lab"))

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la dfs/

# COMMAND ----------

df = spark.read.format("delta").load('/pipelines/delta_lab/tables/aggregated_covid_data')
display(df)

# COMMAND ----------


