# Databricks notebook source
import requests

access_token = ""
my_headers = {
  'Authorization' : f'Bearer {access_token}',
  'Content-type' : 'application/json'
}

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

# COMMAND ----------

for i in arr:
    for key, val in i.items():
        if key == "success" and val == False:
            print("Data not found")
            break
        else:
            print(f"{key} : {val}")
    print("----------")
   
