#!/usr/bin/env python
# coding: utf-8

# ## My first Spark Application

# In[14]:


print("Hola Mundo")


# In[15]:


2+4


# In[16]:


0.1+0.1


# In[17]:


0.1+0.2


# ## Spark Application and Spark Context

# In[18]:


from pyspark import SparkContext
sc= SparkContext.getOrCreate()


# In[19]:


type(sc)


# In[20]:


help(sc)


# In[21]:


dir(sc)


# ## Load data into Spark

# ## RDD

# In[22]:


data = [1,2,3,4]
distributedData = sc.parallelize(data)


# In[26]:


tesla_file = 'TSLA.csv'
tesla_rdd = sc.textFile(tesla_file)
tesla_rdd.take(5)


# In[27]:


type(tesla_rdd)


# In[28]:


## transformation example
csv_rdd = tesla_rdd.map(lambda row:row.split(","))


# In[29]:


## Action example at this point spark breaks computation into tasks to run on separate nodes
## and each node runs part of its map and local collection returning only it's answer to driver
csv_rdd.collect()


# In[30]:


google_file = 'GOOG.csv'
google_rdd = sc.textFile(google_file)
google_rdd.take(3)


# In[31]:


amazon_file ='AMZN.csv'
amazon_rdd = sc.textFile(amazon_file)
amazon_rdd.take(3)


# ## DataFrame

# In[32]:


from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


# In[35]:


## Create DataFrame
li = [('Alice', 1)]
df = sqlContext.createDataFrame(li, ['name','age'])
df.collect()


# In[83]:


tesla_file = 'TSLA.csv'
tesla_df = sqlContext.read.load(amazon_file,
                                 format='com.databricks.spark.csv',
                                header='true',
                                inferSchema='true')


# In[43]:


amazon_file = 'AMZN.csv'
amazon_df = sqlContext.read.load(amazon_file,
                                 format='com.databricks.spark.csv',
                                header='true',
                                inferSchema='true')


# In[41]:


amazon_df.take(5)


# In[44]:


amazon_df.printSchema()


# In[45]:


amazon_df.count()


# In[47]:


amazon_df.show(5)


# In[48]:


google_file = 'GOOG.csv'
google_df = sqlContext.read.load(amazon_file,
                                 format='com.databricks.spark.csv',
                                header='true',
                                inferSchema='true')


# In[49]:


google_df.take(5)


# In[50]:


google_df.count()


# In[51]:


google_df.show(5)


# ## Explore and Query Data

# ## DataFrame Operations

# In[54]:


google_df.select("Date","Close").show(5)


# In[56]:


##To extract just year
from pyspark.sql.functions import year, month

google_df.select(year("Date").alias("year"), "Close").show()


# In[58]:


##To get average closing price for google stock price and group them by year
from pyspark.sql.functions import year, month

google_df.select(year("Date").alias("year"), "Close").groupby("year").avg("Close").sort("year").show()


# In[67]:


## Average Lowest price for Amazon per month

amazon_df.select(year("Date").alias("yr"),month("date").alias("month"), "Low") .groupby("yr","month") .avg("Low").alias("Average") .sort("yr","month") .show()


# In[68]:


## explain method returns the physical plan

amazon_df.select(year("Date").alias("yr"),month("date").alias("month"), "Low") .groupby("yr","month") .avg("Low").alias("Average") .sort("yr","month") .explain()


# ## SPARK SQL

# In[70]:


## To register our dataFrames as temporary tables

amazon_df.registerTempTable("amazon_stocks")
google_df.registerTempTable("google_stocks")
tesla_df.registerTempTable("tesla_stocks")


# In[72]:


sqlContext.sql("SELECT * FROM amazon_stocks").show(5)


# In[75]:


sqlContext.sql("SELECT year(Date) as year, month(Date) as month, avg(Close) from amazon_stocks group by year , month order by year, month").show()


# In[80]:


## When did the closing price of google go up or down by more than 2 dollors in a single day

sqlContext.sql("SELECT Date, Open, Close, abs (Close- Open) as dif FROM google_stocks where abs(Close - Open) > 2 order by date").show()


# In[87]:


## Min and Max Adjusted Close price per year for Tesla

sqlContext.sql("Select year(Date) as year , max(AdjClose), min(AdjClose) from tesla_stocks group by year ").show()


# In[101]:


joinclose_df1 = sqlContext.sql("SELECT tesla_stocks.Date, tesla_stocks.Close as teslaClose, amazon_stocks.Close as amazonClose, google_stocks.Close as googleClose From tesla_stocks join google_stocks ON tesla_stocks.Date = google_stocks.Date join amazon_stocks on amazon_stocks.Date = tesla_stocks.Date order by date")


# In[102]:


joinclose_df1.show()


# # Save Spark DataFrames

# ## Parquet Files

# In[104]:


joinclose_df1.write.format("parquet").save("joinStock1.parquet")


# In[105]:


final_df=sqlContext.read.parquet("joinStock1.parquet")


# In[106]:


final_df.show()


# In[107]:


final_df.printSchema()


# In[ ]:




