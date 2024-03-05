# Databricks notebook source
# MAGIC %md
# MAGIC - All the test cases are defined here
# MAGIC - Assuming all test cases have failed

# COMMAND ----------

# DBTITLE 1,Test Cases:
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
 
