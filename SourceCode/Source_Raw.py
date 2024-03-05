# Databricks notebook source
# MAGIC %md
# MAGIC The functions to move the data from source to raw have been defined here

# COMMAND ----------

# DBTITLE 1,Order
#Reading order.json
file_location = "/FileStore/tables/Order.json"
file_type = "json"
#reading source table
df = spark.read.format(file_type) \
    .option("multiline",True)\
      .option("inferSchema" , True)\
    .load(file_location).distinct()
#checking df has data
dfnotemptytdd(df, 'order')
#Verifying the columns names dont change
column_list_input_tdd(df, 'order')

#Replacing ' ' in columns names with ''
for col in df.columns:
  df = df.withColumnRenamed(col , col.replace(' ',''))

#veryfying no spaces in column names
spaces_input_tdd(df)

#creating raw table for the same
df.write.format("delta").mode("overwrite").saveAsTable("raw.Order")



# COMMAND ----------

# DBTITLE 1,Product
#reading Product.csv
file_location = "/FileStore/tables/Product.csv"
file_type = "csv"
#reading source table
df = spark.read.format(file_type) \
  .option("multiline","false")\
      .option("delimeter" , ',')\
          .option("header" , True)\
              .option("inferSchema","True")\
              .load(file_location).distinct()
#checking df has data
dfnotemptytdd(df, 'product')
#Verifying the columns names dont change
column_list_input_tdd(df, 'product')
#replacing space is column names
for col in df.columns:
  df = df.withColumnRenamed(col , col.replace(' ',''))
spaces_input_tdd(df)
#creating raw table for the same
df.write.format("delta").mode("overwrite").saveAsTable("raw.Product" )


# COMMAND ----------

# DBTITLE 1,Customer
#reading Customer.xlsx
file_location = "/FileStore/tables/Customer.xlsx"
file_type = "com.crealytics.spark.excel"
#reading source table
df = spark.read.format(file_type) \
.option("header", "true") \
.load(file_location)
#checking df has data
dfnotemptytdd(df, 'customer')
#Veryfying column list has not changed
column_list_input_tdd(df, 'customer')
#replacing space is column names
for col in df.columns:
  df = df.withColumnRenamed(col , col.replace(' ',''))
spaces_input_tdd(df) 
#creating raw table for the same
df.write.format("delta").mode("overwrite").saveAsTable("raw.Customer")
