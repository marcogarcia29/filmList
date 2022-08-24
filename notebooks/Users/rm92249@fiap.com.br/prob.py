# Databricks notebook source
import requests

access_token = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJjYWUxMTEwY2Q1NDIyOGFkZjc3NzFhZmJiNTBmYjBlMCIsInN1YiI6IjYzMDJkZTcwN2Q0MWFhMDA4MTUzYjkxYSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.tRr8KtqXlDujZjIaC5FVpLaHj11O3H4-2EvD48zLjXo"
my_headers = {
  'Authorization' : f'Bearer {access_token}',
  'Content-type' : 'application/json'
}

query = {'query': 'Nemo'}
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

