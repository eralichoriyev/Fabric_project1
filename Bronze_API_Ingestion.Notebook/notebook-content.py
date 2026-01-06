# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e9f99a26-f6b0-40b8-8267-9bfb289e66ef",
# META       "default_lakehouse_name": "UnifiedLakehouse",
# META       "default_lakehouse_workspace_id": "2cb788df-1a0e-4a24-9bba-a19496952689",
# META       "known_lakehouses": [
# META         {
# META           "id": "e9f99a26-f6b0-40b8-8267-9bfb289e66ef"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
spark

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

openaq_bronze_path = "Files/bronze/openaq"
openaq_bronze_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

headers = {
    "X-API-Key": "e80c9d4df9caed0cadeeea4e1a6ee7b42add4fd46ce06921dfa0ead18add7e85"
}

url = "https://api.openaq.org/v3/locations"

params = {
    "country": "US",
    "limit": 10
}

response = requests.get(url, params=params, headers=headers)
response.status_code


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = response.json()
data.keys()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data["results"][0]["id"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

headers = {
    "X-API-Key": "e80c9d4df9caed0cadeeea4e1a6ee7b42add4fd46ce06921dfa0ead18add7e85"
}

url = "https://api.openaq.org/v3/locations/3"

response = requests.get(url, headers=headers)
response.status_code


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

response.json().keys()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

response.json()["detail"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

url = "https://api.openaq.org/v3/locations/8118/latest"
headers = {
    "X-API-Key": "e80c9d4df9caed0cadeeea4e1a6ee7b42add4fd46ce06921dfa0ead18add7e85"
}

response = requests.get(url, headers=headers)
response.status_code


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

response.json().keys()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

response.json()["results"][0]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

bronze_path = "/lakehouse/default/Files/bronze/openaq/openaq_latest_8118.json"

raw_json = response.json()

with open(bronze_path, "w") as f:
    json.dump(raw_json, f)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

economics_bronze_path = "/lakehouse/default/Files/bronze/economics"
economics_bronze_path


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

url = "https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD"
params = {
    "format": "json",
    "per_page": 5
}

response = requests.get(url, params=params)
response.status_code


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

type(response.json()), len(response.json())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

response.json()[1][0]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

bronze_gdp_path = "/lakehouse/default/Files/bronze/economics/worldbank_gdp_usa.json"

raw_gdp_json = response.json()

with open(bronze_gdp_path, "w") as f:
    json.dump(raw_gdp_json, f)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

url = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A"
params = {
    "format": "csvdata"
}

response = requests.get(url, params=params)
response.status_code


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

response.text.splitlines()[:5]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_fx_path = "/lakehouse/default/Files/bronze/economics/ecb_usd_eur_fx.csv"

with open(bronze_fx_path, "w") as f:
    f.write(response.text)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
