#!/usr/bin/env python
# coding: utf-8

# # TLC Trips Record

# ### Practice Case 6
# Nurjanah

# In[1]:


import pyspark


# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp


# In[3]:


get_ipython().system('wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-02.parquet')

get_ipython().system('wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2021-02.parquet')
    
get_ipython().system('wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-02.parquet')


# In[4]:


spark = SparkSession.builder     .master("local[*]")     .appName('test')     .getOrCreate()


# In[5]:


df_yellow = spark.read.parquet("yellow_tripdata_2021-02.parquet")
df_fhv = spark.read.parquet("fhv_tripdata_2021-02.parquet")
df_fhvhv = spark.read.parquet("fhvhv_tripdata_2021-02.parquet")


# In[6]:


df_yellow.show(5)


# In[7]:


df_fhv.show(5)


# In[8]:


df_fhvhv.show(5)


# In[9]:


df_yellow.columns


# In[10]:


df_fhv.columns


# In[11]:


df_fhvhv.columns


# ### How many taxi trips were there on February 15?

# In[12]:


df_yellow.registerTempTable('df_yellow_table')
total_trip_2021_02 = spark.sql(""" 
                                SELECT COUNT(tpep_pickup_datetime) AS total_trip_2021_02
                                FROM df_yellow_table
                                WHERE tpep_pickup_datetime >= '2021-02-15 00:00:00' AND tpep_pickup_datetime < '2021-02-16 00:00:00'
                   
                   """)
total_trip_2021_02.show()


# In[14]:


df_fhvhv.registerTempTable('df_fhvhv_table')
total_trip_2021_02 = spark.sql(""" 
                                SELECT COUNT(pickup_datetime) AS total_trip_2021_02
                                FROM df_fhvhv_table
                                WHERE pickup_datetime >= '2021-02-15 00:00:00' AND pickup_datetime < '2021-02-16 00:00:00'
                   
                   """)
total_trip_2021_02.show()


# ### Find the longest trip for each day ?

# In[16]:


df_yellow = df_yellow.withColumn("pickup_date", df_yellow["tpep_pickup_datetime"].cast('date'))

df_yellow.registerTempTable('df_table_yellow')

longest_trip_byday_yellow = spark.sql("""
                     SELECT pickup_date, MAX(trip_distance) as longest_distance_trip_by_day
                     FROM df_table_yellow
                     WHERE pickup_date >= "2021-02-01" AND pickup_date < "2021-03-01"
                     GROUP BY 1
                     ORDER BY longest_distance_trip_by_day DESC
                     """)
longest_trip_byday_yellow.show(10)


# In[18]:


df_fhvhv = df_fhvhv.withColumn("pickup_date", df_fhvhv["pickup_datetime"].cast('date'))

df_fhvhv.registerTempTable('df_fhvhv_table')

longest_trip_byday_fhvhv = spark.sql("""
                     SELECT pickup_date, MAX(trip_miles) as longest_distance_trip_by_day
                     FROM df_fhvhv_table
                     WHERE pickup_date >= "2021-02-01" AND pickup_date < "2021-03-01"
                     GROUP BY 1
                     ORDER BY longest_distance_trip_by_day DESC
                     """)
longest_trip_byday_fhvhv.show(10)


# ### Find Top 5 Most frequent `dispatching_base_num` ?

# In[19]:


df_fhv.registerTempTable('df_fhv_table')

top5_most_dbm_yellow = spark.sql("""
                SELECT dispatching_base_num, count(dispatching_base_num) as count
                FROM df_fhv_table
                GROUP BY 1
                ORDER BY 2 DESC
                """)
top5_most_dbm_yellow.show(5)


# In[20]:


top5_most_dbm_fhvhv = spark.sql("""
                SELECT dispatching_base_num, count(dispatching_base_num) as count
                FROM df_fhvhv_table
                GROUP BY 1
                ORDER BY 2 DESC
                """)
top5_most_dbm_fhvhv.show(5)


# ### Find Top 5 Most common location pairs (PUlocationID and DOlocationID)

# In[21]:


top5_most_PUlocationID_yellow = spark.sql(""" 
                         SELECT PUlocationID, COUNT(PUlocationID) as Count_DOlocationID
                         FROM df_yellow_table
                         GROUP BY 1
                         ORDER BY 2 DESC
                         """)

top5_most_PUlocationID_yellow.show(5)


# In[22]:


top5_most_PUlocationID_fhvhv = spark.sql(""" 
                         SELECT PUlocationID, COUNT(PUlocationID) as Count_DOlocationID
                         FROM df_fhvhv_table
                         GROUP BY 1
                         ORDER BY 2 DESC
                         """)

top5_most_PUlocationID_fhvhv.show(5)


# In[92]:


top5_most_DOlocationID_yellow = spark.sql(""" 
                         SELECT DOlocationID, COUNT(DOlocationID) as Count_DOlocationID
                         FROM df_yellow_table
                         GROUP BY 1
                         ORDER BY 2 DESC
                         """)

top5_most_DOlocationID_yellow.show(5)


# In[23]:


top5_most_DOlocationID_fhvhv = spark.sql(""" 
                         SELECT DOlocationID, COUNT(DOlocationID) as Count_DOlocationID
                         FROM df_fhvhv_table
                         GROUP BY 1
                         ORDER BY 2 DESC
                         """)

top5_most_DOlocationID_fhvhv.show(5)


# In[ ]:




