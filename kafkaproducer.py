#!/usr/bin/env python
# coding: utf-8

# In[1]:


import urllib.parse
from kafka import KafkaProducer
from json import dumps
from time import sleep


# In[2]:


import requests


# In[3]:


stations_heidelberg = 'https://creativecommons.tankerkoenig.de/json/list.php?lat=49.3998&lng=8.6724&rad=8&sort=dist&type=all&apikey=1db7be72-d479-dc23-4d56-7d561d8849e0'
stations_stuttgart = 'https://creativecommons.tankerkoenig.de/json/list.php?lat=48.7758&lng=9.1829&rad=25&sort=dist&type=all&apikey=1db7be72-d479-dc23-4d56-7d561d8849e0'
stations_munich = 'https://creativecommons.tankerkoenig.de/json/list.php?lat=48.13518&lng=11.5820&rad=25&sort=dist&type=all&apikey=1db7be72-d479-dc23-4d56-7d561d8849e0'
stations_berlin = 'https://creativecommons.tankerkoenig.de/json/list.php?lat=52.5200&lng=13.4050&rad=25&sort=dist&type=all&apikey=1db7be72-d479-dc23-4d56-7d561d8849e0'
stations_hamburg = 'https://creativecommons.tankerkoenig.de/json/list.php?lat=53.5511&lng=9.9937&rad=25&sort=dist&type=all&apikey=1db7be72-d479-dc23-4d56-7d561d8849e0'
stations_hanover = 'https://creativecommons.tankerkoenig.de/json/list.php?lat=52.3759&lng=9.7320&rad=25&sort=dist&type=all&apikey=1db7be72-d479-dc23-4d56-7d561d8849e0'


# In[4]:


json_data1 = requests.get(stations_heidelberg).json()
json_data2 = requests.get(stations_stuttgart).json()
json_data3 = requests.get(stations_munich).json()
json_data4 = requests.get(stations_berlin).json()
json_data5 = requests.get(stations_hamburg).json()
json_data6 = requests.get(stations_hanover).json()


# In[5]:


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
producer.send('FuelStation',value=json_data1)
producer.send('FuelStation',value=json_data2)
producer.send('FuelStation',value=json_data3)
producer.send('FuelStation',value=json_data4)
producer.send('FuelStation',value=json_data5)
producer.send('FuelStation',value=json_data6)
sleep(5)


# In[ ]:




