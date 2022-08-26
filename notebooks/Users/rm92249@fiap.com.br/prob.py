# Databricks notebook source
import requests

def useApi(access_token):
    global my_headers
    my_headers = {
        'Authorization' : f'Bearer {access_token}',
        'Content-type' : 'application/json'
    }

useApi("")

query = {'query': 'A'}
response = requests.get("https://api.themoviedb.org/3/search/movie?", headers=my_headers, params=query)
print(response.json())

# COMMAND ----------

for i in response.json()['results']:
    global image_url
    print(i['original_title'])
    print(f"nota média do filme: {i['vote_average']}")
    image_url = i['poster_path']
    print(f"{image_url}\n")

# COMMAND ----------

i=0
arr = []
while i < 100:
    movies_list = requests.get(f"https://api.themoviedb.org/3/movie/{i}", headers=my_headers)
    arr.append(movies_list.json())
    i = i + 1

# COMMAND ----------

import pandas as pd

output = pd.DataFrame()

for i in arr:
    for key, val in i.items():
        if key == "success" and val == False:
            #print("Data not found")
            break
        else:
            output = output.append(i, ignore_index=True)
            #print(f"{key} : {val}")
    #print("----------")

print(output.head())

# COMMAND ----------

print(output['genres'])
