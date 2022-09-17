import requests
import influxdb_client
import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

url="localhost:8086"
username="admin"
senha="123"
openWeatherMap_token = ""
openWeatherMap_lat = "33.44"
openWeatherMap_lon = "-94.04"
openWeather_url = "https://api.openweathermap.org/data/3.0/onecall"
protocol='line'
# Get time series data from OpenWeatherMap API
params = {'lat':openWeatherMap_lat, 'lon':openWeatherMap_lon, 'exclude': "minutely,daily", 'appid':openWeatherMap_token}
r = requests.get(openWeather_url, params = params).json()
print(r)
hourly = r['hourly']

# Convert data to Pandas DataFrame and convert timestamp to datetime object
df = pd.json_normalize(hourly)
df = df.drop(columns=['weather', 'pop'])
df['dt'] = pd.to_datetime(df['dt'], unit='s')
print(df.head)

# Write data to InfluxDB
with InfluxDBClient(host='localhost', port=8086, username=username, password=senha) as client:
   df = df
   client.write_points(df, 'tempo', protocol=protocol)
