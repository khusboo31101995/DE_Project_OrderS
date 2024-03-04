# Databricks notebook source
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

#TDD section
#Assuming all test cases have failed

# COMMAND ----------

#test  cases 
#testing dataframe is not empty
def dfnotemptytdd(df , name):
    if name=='order':
        assert df.count()!=0
    if name=='customer':
        assert df.count()!=0
    if name=='product':
        assert df.count()!=0
#testing for mismatch in source headers
def column_list_input_tdd(df , name):
    if name=='order':
        assert df.columns == ['Customer ID','Discount','Order Date','Order ID','Price','Product ID','Profit','Quantity','Row ID','Ship Date','Ship Mode']
    if name=='customer':
        assert df.columns == ['Customer ID','Customer Name','email','phone','address','Segment','Country','City','State','Postal Code','Region']
    if name=='product':
        assert df.columns == ['Product ID','Category','Sub-Category','Product Name','State','Price per product']
#testing for spaces in source headers
def spaces_input_tdd(df):
    for col in df.columns:
        assert ' ' not in col
#testing for edge cases while building customer enriched.
def customer_tdd(c):
    #checking that columns dont have nulls
    c_null=c.filter("CustomerName is null" or "Country is null" or "CustomerID is null")
    if len(c_null.collect())==0:
        state='Empty'
    elif len(c_null.collect())!=0:
        raise Exception("Assertion for nulls failed")
    #checking length of key column 
    for row in c.select(length('CustomerID')).collect():
        if row[0]!=8:
            raise Exception("Lenght is Not correct in key.")
#testing for edge cases while building product enriched.
def product_tdd(p):
    #checking columns dont have nulls
    p_null=p.filter("ProductID is null" or "Category is null" or "Sub-Category is null")
    if len(p_null.collect())==0:
        state='Empty'
    elif len(p_null.collect())!=0:
        raise Exception("Assertion for nulls failed")
    #checking length of key column 
    for row in p.select(length('ProductID')).collect():
        if row[0]!=15:
            raise Exception("Lenght is Not correct in key.")
#testing for edge cases while building order enriched.
def order_tdd(o , p, c):
    #checking columns dont have nulls
    o_null=o.filter("OrderID is null" or "CustomerID is null" or "ProductID is null")
    if len(o_null.collect())==0:
        state='Empty'
    elif len(o_null.collect())!=0:
        raise Exception("Assertion for nulls failed")
    #checking length of key columns as well as keys on which join is performed
    for row in o.select(length('OrderID')).collect():
        if row[0]!=14:
            raise Exception("Lenght is Not correct in key.")
    for row in c.select(length('CustomerID')).collect():
        if row[0]!=8:
            raise Exception("Lenght is Not correct in key.")
    for row in p.select(length('ProductID')).collect():
        if row[0]!=15:
            raise Exception("Lenght is Not correct in key.")
def master_tdd(df):
    #making sure no duplicates in  key of master
    oo = df.groupBy('Year' , 'Product_Category', 'CustomerName' , 'Product_Sub_Category').agg(count('*').alias("cc"))
    if oo.filter(oo.cc!=1).count()!=0:
        raise Exception('Duplicates! Error in data! Aggregation not applied!')
 

# COMMAND ----------

# MAGIC %sql
# MAGIC --to create raw database/schema
# MAGIC Create schema if not exists raw
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC READING FROM SOURCE into RAW SECTION

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC DATA ANALYSIS SECTION

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.customer where CustomerID is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.Order

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.Customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.Product where `Category` is null

# COMMAND ----------

# MAGIC %md
# MAGIC RAW to ENRICHED SECTION

# COMMAND ----------

# MAGIC %sql
# MAGIC --This will store the master and aggregate
# MAGIC create schema if not exists enriched_master_aggregate

# COMMAND ----------

#reading customer and enriching it
customer = spark.sql('select * from raw.customer').select('CustomerName' , 'Country', 'CustomerID').distinct()
#Replace special characters with '' and drop nulls
customer=customer.withColumn('CustomerName', regexp_replace('CustomerName',"[^'A-Za-z]+","")).na.drop()
#customer_tdd(customer) check for nulls in all columns and length of key column
customer_tdd(customer)
#write to enriched layer
customer.write.format('delta').mode('overwrite').saveAsTable('enriched_master_aggregate.customer_en' )

# COMMAND ----------

#reading product and enriching it
product = spark.sql('select * from raw.product').select('Category' , 'Sub-Category', 'ProductID')
#make sure null are replaced with ''
product=product.na.drop()
#product_tdd(product) check for nulls in all columns and length of key column
product_tdd(product)
#write to enriched layer
product.write.format('delta').mode('overwrite').saveAsTable('enriched_master_aggregate.product_en')


# COMMAND ----------

#creating the Order enriched table
#reading from raw
order = spark.sql('select * from raw.Order').na.drop()
#Creating Profit Column rounded to 2 places
#Extracting Year from Order Date
order=order.withColumn('Profit', round('Profit' , 2))\
                    .withColumn('Year', year(to_date('OrderDate','d/M/yyyy')))
product = spark.sql('select * from enriched_master_aggregate.product_en')
customer = spark.sql('select * from enriched_master_aggregate.customer_en')
#order_tdd(order) check for nulls in all columns and length of key column
order_tdd(order, product , customer)
#Join order with customer and product to get required columns
order_enriched = order.join(customer ,['CustomerID'] , 'left').join(product, ['ProductID'], 'left').na.drop()
#writing to enriched layer
order_enriched.write.format('delta').mode('overwrite').saveAsTable('enriched_master_aggregate.order_en')

# COMMAND ----------

# MAGIC %md
# MAGIC FINAL MASTER SECTION

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC AGGREGATION SECTION

# COMMAND ----------

# MAGIC %sql
# MAGIC --SQL query to get Profit by year
# MAGIC select Year, sum(Profit) from enriched_master_aggregate.order_master group by Year order by Year

# COMMAND ----------

# MAGIC %sql
# MAGIC --SQL query to get profit by Year + ProductCategory
# MAGIC select Year, Product_Category, sum(Profit) from enriched_master_aggregate.order_master group by Year, Product_Category order by Year, Product_Category

# COMMAND ----------

# MAGIC %sql
# MAGIC --SQL query to get profit by Customer
# MAGIC select CustomerName, sum(Profit) from enriched_master_aggregate.order_master group by CustomerName order by CustomerName

# COMMAND ----------

# MAGIC %sql
# MAGIC --SQL query to get profit by Customer + Year
# MAGIC select Year, CustomerName, sum(Profit) from enriched_master_aggregate.order_master group by Year, CustomerName order by Year, CustomerName

# COMMAND ----------

##### EOF ##############

# COMMAND ----------


