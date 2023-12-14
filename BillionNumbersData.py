#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
from operator import add
from pyspark.sql import SparkSession
filename = 'C:\data.txt'

spark = SparkSession\
        .builder\
        .appName("PythonNumberCount")\
        .master("local[*]")\
        .getOrCreate()

linesRdd = spark.read.text(filename).rdd.map(lambda r: r[0])
print('Number of partitions: {}'.format(linesRdd.getNumPartitions()))

counts = linesRdd.flatMap(lambda x: x.split(' '))\
        .map(lambda x: (x,1))\
        .reduceByKey(add)
output = counts.collect()
for (number, counts) in output:
    print('%s: %i' % (number, counts))


# In[ ]:




