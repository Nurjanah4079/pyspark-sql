#!/usr/bin/env python
# coding: utf-8

# # TLC Trips Record

# ### Practice Case 6
# Nurjanah

# In[93]:


import pyspark


# In[94]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp


# In[95]:


get_ipython().system('wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-02.parquet')

get_ipython().system('wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2021-02.parquet')


# In[96]:


spark = SparkSession.builder     .master("local[*]")     .appName('test')     .getOrCreate()


# In[97]:


df_yellow = spark.read.parquet("yellow_tripdata_2021-02.parquet")
df_fhv = spark.read.parquet("fhv_tripdata_2021-02.parquet")


# In[98]:


df_yellow.show(5)


# In[99]:


df_fhv.show(5)


# In[100]:


df_yellow.columns


# In[101]:


df_fhv.columns


# In[102]:


df_yellow = df_yellow     .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime')     .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


# ### How many taxi trips were there on February 15?

# In[103]:


df_yellow.registerTempTable('df_yellow_table')
total_trip_2021_02 = spark.sql(""" 
                                SELECT COUNT(pickup_datetime) AS total_trip_2021_02
                                FROM df_yellow_table
                                WHERE pickup_datetime >= '2021-02-15 00:00:00' AND pickup_datetime < '2021-02-16 00:00:00'
                   
                   """)
total_trip_2021_02.show()


# ### Find the longest trip for each day ?

# In[104]:


df_yellow = df_yellow.withColumn("pickup_date", df_yellow["pickup_datetime"].cast('date'))

df_yellow.registerTempTable('df_table')

longest_trip_byday = spark.sql("""
                     SELECT pickup_date, MAX(trip_distance) as longest_distance_trip_by_day
                     FROM df_table
                     WHERE pickup_date >= "2021-02-01" AND pickup_date < "2021-03-01"
                     GROUP BY 1
                     ORDER BY longest_distance_trip_by_day DESC
                     """)
longest_trip_byday.show(10)


# ### Find Top 5 Most frequent `dispatching_base_num` ?

# In[105]:


df_fhv.registerTempTable('df_fhv_table')

top5_most_dbm = spark.sql("""
                SELECT dispatching_base_num, count(dispatching_base_num) as count
                FROM df_fhv_table
                GROUP BY 1
                ORDER BY 2 DESC
                """)
top5_most_dbm .show(5)


# ### Find Top 5 Most common location pairs (PUlocationID and DOlocationID)

# In[ ]:


top5_most_PUlocationID = spark.sql(""" 
                         SELECT PUlocationID, COUNT(PUlocationID) as Count_DOlocationID
                         FROM df_yellow_table
                         GROUP BY 1
                         ORDER BY 2 DESC
                         """).limit(5)

top5_most_PUlocationID.show(5)


# In[92]:


top5_most_DOlocationID = spark.sql(""" 
                         SELECT DOlocationID, COUNT(DOlocationID) as Count_DOlocationID
                         FROM df_yellow_table
                         GROUP BY 1
                         ORDER BY 2 DESC
                         """).limit(5)

top5_most_DOlocationID.show(5)

