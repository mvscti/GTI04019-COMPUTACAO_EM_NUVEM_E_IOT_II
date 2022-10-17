import requests
import pandas as pd
from influxdb import InfluxDBClient
import configparser
import geocoder

config = configparser.ConfigParser()
#Lê arquivo com as configurações
config.read('config.ini')
host=config['INFLUXDB']['Endereco']
port=config['INFLUXDB']['Porta']
username=config['INFLUXDB']['Username']
senha=config['INFLUXDB']['Senha']
dbname=config['INFLUXDB']['DB']
openWeatherMap_token = config['OPENWEATHER']['Key']
openWeather_url = "https://api.openweathermap.org/data/2.5/forecast"


def defineLocalizacao():
   print("Definindo sua geolocalização...")
   '''Descobre as coordenadas geográficas baseadas em seu endereço IP'''
   return geocoder.ip('me').latlng

def formataDadosProtocoloLinha(data, city_data):
   json_data = [
        {
            "measurement": "weather",
            "tags": {
                "city": city_data['name'][0],
                "location_id": city_data['id'][0]
            },
            "time": data['dt'],

            "fields":
            {
                "coord_lon": city_data['coord.lon'][0],
                "coord_lat": city_data['coord.lat'][0],
                "main_temp":data['main.temp'],
                "main_pressure":(data['main.pressure']/10),
                "main_humidity":data['main.humidity'],
                "main_temp_min":data['main.temp_min'],
                "main_temp_max":data['main.temp_max'],
                "feels_like": data['main.feels_like'],
                "visibility":data['visibility'],
                "wind_speed":data['wind.speed'],
                "wind_deg":data['wind.deg'],
                "clouds_all":data['clouds.all'],
            }
      }
   ]
   return json_data


def main():
   loc=defineLocalizacao()
   # Recebe dados de séries temporais da API do OpenWeather 
   params = {'lat':loc[0], 'lon':loc[1], 'units':'metric', 'exclude': "minutely,current,alerts", 'appid':openWeatherMap_token}
   print("Consultando a API do OpenWeather")
   r = requests.get(openWeather_url, params = params).json()

   # Convertendo os dados para um DataFrame pandas
   df = pd.json_normalize(r.get("list"))
   #informações da localidade onde a busca ocorreu
   city_df=pd.json_normalize(r.get("city")) 
   #excluí colunas informadas
   df = df.drop(columns=['weather', 'pop'])
   #converte data em formato Unix para timestamp
   df['dt'] = pd.to_datetime(df['dt'], unit='s')
   print("Cabeçalho do DataFrame pandas")
   print(df.head)


   # Grava os pontos no InfluxDB
   client = InfluxDBClient(host=host, port=port, username=username, password=senha)
   client.create_database(dbname)
   client.switch_database(dbname)
   print("Salvando os dados no InfluxDB")
   for index, row in df.iterrows():
      client.write_points(formataDadosProtocoloLinha(row, city_df))

if __name__ == '__main__':
    main()