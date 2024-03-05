# Databricks notebook source
# MAGIC %md
# MAGIC The functions to move all tables from raw to enriched have been defined here

# COMMAND ----------

# DBTITLE 1,Customer
#reading customer and enriching it
customer = spark.sql('select * from raw.customer').select('CustomerName' , 'Country', 'CustomerID').distinct()
#Replace special characters with '' and drop nulls
customer=customer.withColumn('CustomerName', regexp_replace('CustomerName',"[^'A-Za-z]+","")).na.drop()
#customer_tdd(customer) check for nulls in all columns and length of key column
customer_tdd(customer)
#write to enriched layer
customer.write.format('delta').mode('overwrite').saveAsTable('enriched_master_aggregate.customer_en' )

# COMMAND ----------

# DBTITLE 1,Product
#reading product and enriching it
product = spark.sql('select * from raw.product').select('Category' , 'Sub-Category', 'ProductID')
#make sure null are replaced with ''
product=product.na.drop()
#product_tdd(product) check for nulls in all columns and length of key column
product_tdd(product)
#write to enriched layer
product.write.format('delta').mode('overwrite').saveAsTable('enriched_master_aggregate.product_en')


# COMMAND ----------

# DBTITLE 1,Order
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