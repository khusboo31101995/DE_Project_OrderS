# Databricks notebook source
# MAGIC %md 
# MAGIC The code for all the aggregations  are shown here:

# COMMAND ----------

# DBTITLE 1,Profit by Year
# MAGIC %sql
# MAGIC --SQL query to get Profit by year
# MAGIC select Year, sum(Profit) from enriched_master_aggregate.order_master group by Year order by Year

# COMMAND ----------

# DBTITLE 1,Profit by Year and Product_Category
# MAGIC %sql
# MAGIC --SQL query to get profit by Year + ProductCategory
# MAGIC select Year, Product_Category, sum(Profit) from enriched_master_aggregate.order_master group by Year, Product_Category order by Year, Product_Category

# COMMAND ----------

# DBTITLE 1,Profit by Customer Name
# MAGIC %sql
# MAGIC --SQL query to get profit by Customer
# MAGIC select CustomerName, sum(Profit) from enriched_master_aggregate.order_master group by CustomerName order by CustomerName

# COMMAND ----------

# DBTITLE 1,Profit by Year and Customer Name
# MAGIC %sql
# MAGIC --SQL query to get profit by Customer + Year
# MAGIC select Year, CustomerName, sum(Profit) from enriched_master_aggregate.order_master group by Year, CustomerName order by Year, CustomerName

# COMMAND ----------

