# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "aa1ee62d-2f24-4c14-b409-b89563d3e7fa",
# META       "default_lakehouse_name": "M_Lakehouse_Itransition",
# META       "default_lakehouse_workspace_id": "95ebcbcf-35b4-4eda-bae3-344163c79c6c",
# META       "known_lakehouses": [
# META         {
# META           "id": "aa1ee62d-2f24-4c14-b409-b89563d3e7fa"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************




#API_KEY = "e80c9d4df9caed0cadeeea4e1a6ee7b42add4fd46ce06921dfa0ead18add7e85"

import requests

API_KEY = "e80c9d4df9caed0cadeeea4e1a6ee7b42add4fd46ce06921dfa0ead18add7e85"

url = "https://api.openaq.org/v3/locations"

headers = {
    "X-API-Key": API_KEY,
    "Accept": "application/json"
}

params = {
    "countries": "US",
    "limit": 10
}

response = requests.get(url, headers=headers, params=params)

print("Status code:", response.status_code)

data = response.json()
len(data.get("results", [])), data.get("results", [])[:2]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Take the first location from the last API response
first_location = data["results"][0]

first_location_id = first_location["id"]
first_location_name = first_location.get("name")

first_location_id, first_location_name


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Look at sensors available for this location
sensors = first_location.get("sensors", [])

len(sensors), sensors[:2]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Take the first sensor from the location
first_sensor = sensors[0]
sensor_id = first_sensor["id"]
sensor_name = first_sensor.get("name")

sensor_id, sensor_name


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

API_KEY = "e80c9d4df9caed0cadeeea4e1a6ee7b42add4fd46ce06921dfa0ead18add7e85"

url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements"

headers = {
    "X-API-Key": API_KEY,
    "Accept": "application/json"
}

params = {
    "limit": 100
}

response = requests.get(url, headers=headers, params=params)

print("Status code:", response.status_code)

measurements_data = response.json()
len(measurements_data.get("results", [])), measurements_data.get("results", [])[:2]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inspect what the API actually returned
measurements_data.keys(), measurements_data


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

API_KEY = "e80c9d4df9caed0cadeeea4e1a6ee7b42add4fd46ce06921dfa0ead18add7e85"

headers = {
    "X-API-Key": API_KEY,
    "Accept": "application/json"
}

def get_measurement_count(sensor_id):
    url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements"
    params = {"limit": 10}
    r = requests.get(url, headers=headers, params=params)
    if r.status_code != 200:
        return sensor_id, r.status_code, None
    data = r.json()
    return sensor_id, 200, data.get("meta", {}).get("found", 0)

results = []
for s in sensors[:10]:   # test first 10 sensors only
    sid = s["id"]
    results.append(get_measurement_count(sid))

results


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

API_KEY = "e80c9d4df9caed0cadeeea4e1a6ee7b42add4fd46ce06921dfa0ead18add7e85"

headers = {
    "X-API-Key": API_KEY,
    "Accept": "application/json"
}

# Get more US locations
url = "https://api.openaq.org/v3/locations"
params = {
    "countries": "US",
    "limit": 20
}

resp = requests.get(url, headers=headers, params=params)
locations_data = resp.json()["results"]

def has_data(found_value):
    # found_value can be '0', '12', '>5', None
    if found_value is None:
        return False
    if isinstance(found_value, str):
        if found_value.startswith(">"):
            return True
        return found_value.isdigit() and int(found_value) > 0
    if isinstance(found_value, int):
        return found_value > 0
    return False

def find_active_sensor(locations):
    for loc in locations:
        sensors = loc.get("sensors", [])
        for s in sensors:
            sensor_id = s["id"]
            m_url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements"
            r = requests.get(m_url, headers=headers, params={"limit": 5})
            if r.status_code == 200:
                meta = r.json().get("meta", {})
                found = meta.get("found")
                if has_data(found):
                    return loc["id"], loc.get("name"), sensor_id, s.get("name"), found
    return None

active = find_active_sensor(locations_data)
active

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd

API_KEY = "e80c9d4df9caed0cadeeea4e1a6ee7b42add4fd46ce06921dfa0ead18add7e85"

sensor_id = 13866   # active sensor we discovered

url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements"

headers = {
    "X-API-Key": API_KEY,
    "Accept": "application/json"
}

params = {
    "limit": 200
}

response = requests.get(url, headers=headers, params=params)

print("Status code:", response.status_code)

measurements_data = response.json()
len(measurements_data.get("results", [])), measurements_data.get("results", [])[:2]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

results = measurements_data["results"]
df_openaq = pd.json_normalize(results)

df_openaq.shape, df_openaq.head()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os

bronze_path = "/lakehouse/default/Files/Bronze/OpenAQ"

os.makedirs(bronze_path, exist_ok=True)

bronze_path


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_openaq.to_parquet(
    f"{bronze_path}/openaq_measurements.parquet",
    index=False
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
