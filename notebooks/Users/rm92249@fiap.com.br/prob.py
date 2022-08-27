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

pd.reset_option('max_columns')
print(arr['genres'])

# COMMAND ----------

df = spark.createDataFrame(arr)
df.show(n=50,vertical=True)

# COMMAND ----------

df2 = df.filter(df.status_code==34)
df2.show()

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("json").mode("overwrite").save("/teste/dadosFilmes")

# COMMAND ----------

df_success = spark.read.json('dbfs:/teste/dadosFilmes/part-00000-tid-5064522810776596884-efae8b49-69bb-455c-86c5-a84674f7490d-32-1-c000.json')
df.show(vertical=True)
