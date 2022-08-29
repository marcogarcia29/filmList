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

for i in arr:
    print(f"{i}\n")
len(arr)

# COMMAND ----------

import pandas as pd

arr = pd.DataFrame(arr)

#for i in arr:
    #for key, val in i.items():
        #if key == "success" and val == False:
            #print("Data not found")
            #break
        #else:
            #print(f"{key} : {val}")
            #print(i)
            #output = output.append(i, ignore_index=True)
    #print("----------")

#print(arr.iloc[[1]])
update = arr
for i, x in enumerate(update['status_code']):
    if x == 34:
        update = update.drop(i)
print(update)

# COMMAND ----------

print(update['genres'])

# COMMAND ----------

df = spark.createDataFrame(update)
df.show(n=50,vertical=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

#Drop columns that only returns NAN

df = df.drop('success', 'status_code', 'status_message')
display(df)

# COMMAND ----------

display(df.sort("title"))

# COMMAND ----------

df.write.format("json").mode("overwrite").save("/teste/dadosFilmes")

# COMMAND ----------

df_success = spark.read.json('dbfs:/teste/dadosFilmes/part-00000-tid-7997115978503789053-3fd9fdce-5b1f-44bb-b7ee-673d4f272fc6-30-1-c000.json')
df.show(vertical=True)