# Databricks notebook source
# MAGIC %md 
# MAGIC Master Workflow to run all the scripts

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - This notebook will perform the following:
# MAGIC - A database called 'raw' is created.
# MAGIC   - Then, the source tables/data are dumped in delta format in the database raw for Order, Customer and Product
# MAGIC - After that 'enriched_master_aggregate' database is created.
# MAGIC   - As instructed, the Product and Customer data is enriched/ cleaned/ nulls are dropped/ distinct is applied/Special characters are removed etc. and stored in respective tables in enriched_master_aggregate db. Only required columns are selected.
# MAGIC - The same is done for Order table which is also enriched/ cleaned etc. Profit column is rounded to 2 places. Year column is created. And Order is joined with Prodct and Customer to get respective columns.
# MAGIC - Finally the master table is created in enriched_master_aggregate db called Order_master. This has the final aggregate information over required columns.
# MAGIC - PLEASE NOTE:
# MAGIC   - TDD is performed:
# MAGIC   - Initially we have checked for columns list, no spaces in column names for all raw tables. Checked for data to be available in the source.
# MAGIC   - Then we have written test cases for the enriched tables which covers
# MAGIC     - null checks. Rows with Nulls are being dropped as an aggregation over a null value does not make sense.
# MAGIC     - ID column length checks have been done such that there is no discrepancy between the Key columns.
# MAGIC     - We are considering that ProductID and CustomerID length will be 15 and 8 characters respectively.
# MAGIC     - No special characters in columns like Custome Name , Product Category Country etc.
# MAGIC     - Making sure  no duplicates ion the final aggregation.
# MAGIC - IMPORTANT:
# MAGIC   - Not considering incremental loads.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ./Database_Config

# COMMAND ----------

# MAGIC %run ./Test_Cases

# COMMAND ----------

# MAGIC %run ./Source_Raw

# COMMAND ----------

# MAGIC %run ./Raw_Enriched

# COMMAND ----------

# MAGIC %run ./Enriched_Master

# COMMAND ----------

# MAGIC %run ./Final_Aggregation

# COMMAND ----------

