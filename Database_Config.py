# Databricks notebook source
# MAGIC %sql
# MAGIC --to create raw database/schema
# MAGIC Create schema if not exists raw
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --This will store the master and aggregate
# MAGIC create schema if not exists enriched_master_aggregate