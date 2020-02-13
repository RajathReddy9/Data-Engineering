#!/usr/bin/env python
# coding: utf-8

# In[1]:


import string
import pymongo
import json
from pymongo import MongoClient
from kafka import KafkaConsumer
from json import loads


# In[2]:


consumer = KafkaConsumer(
    'FuelStation',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')));


# In[3]:


print(consumer)


# In[4]:


client = MongoClient('localhost:27017')
collection = cl
tions


# In[5]:


for message in consumer:
    message = message.value
    collection.insert_one(message)
    print('{} added to {}'.format(message, collection))

