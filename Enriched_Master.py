# Databricks notebook source
# MAGIC %md
# MAGIC The functions for creating master tables are defined here

# COMMAND ----------

# DBTITLE 1,Order Master
#creating final master table
order_enriched= spark.sql('select * from enriched_master_aggregate.order_en')
#aggregate on required columns and taking max of Profit
order_master = order_enriched.groupBy('Year', 'Category' , 'Sub-Category', 'CustomerName').agg(max('Profit').alias('Profit'))
#Renaming Product columns for ease in aggregating for insights
order_master = order_master.withColumnsRenamed({'Category':'Product_Category' , 'Sub-Category':'Product_Sub_Category'} )
#master_tdd to test for duplicates after aggregation
master_tdd(order_master)
#writing to final target
order_master.write.format('delta').mode('overwrite').saveAsTable('enriched_master_aggregate.order_master')