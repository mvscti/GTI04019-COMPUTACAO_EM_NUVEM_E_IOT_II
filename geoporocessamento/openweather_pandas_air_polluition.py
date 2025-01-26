import requests
import pandas as pd
from influxdb import InfluxDBClient
import configparser

config = configparser.ConfigParser()
#Lê arquivo com as configurações
config.read('config.ini')
host=config['INFLUXDB']['Endereco']
port=config['INFLUXDB']['Porta']
username=config['INFLUXDB']['Username']
senha=config['INFLUXDB']['Senha']
dbname=config['INFLUXDB']['DB']
openWeatherMap_token = config['OPENWEATHER']['Key']
openWeather_url = "http://api.openweathermap.org/data/2.5/air_pollution/history"



def defineLocalizacao():
   lat='' #Informe a latitude desejada aqui
   lon= '' #Informe a longitude desejada aqui
   return list((lat, lon))


'''
O formato de início e fim deve ser em Unix Timestamp. Uma boa forma de conversão pode
ser obtida em https://www.epochconverter.com/
'''
def defineTemporariedade():
   inicio=''
   fim=''
   return list((inicio, fim))
   
def formataDadosProtocoloLinha(data, loc):
   json_data = [
        {
            "measurement": "air_polluition",
            "tags": {
                "lat-lon": f'[{loc[0]},{loc[1]}]'
            },
            "time": data['dt'],

            "fields":
            {
                "aqi": data['main.aqi'],
                "co": data['components.co'],
                "no":data['components.no'],
                "no2":(data['components.no2']),
                "o3":data['components.o3'],
                "so2":data['components.so2'],
                "pm2_5":data['components.pm2_5'],
                "pm10": data['components.pm10'],
                "nh3":data['components.nh3'],
            }
      }
   ]
   return json_data


def main():
   loc=defineLocalizacao()
   datas=defineTemporariedade()
   if '' in loc or '' in datas:
      quit('é necessário informar latitude, longitutde, instante de início e instante final (consultar as funções "defineLocalizacao" e "defineTemporariedade")')
   # Recebe dados de séries temporais da API do OpenWeather 
   params = {'lat':loc[0], 'lon':loc[1], 'start': datas[0], 'end': datas[1], 'appid':openWeatherMap_token}
   print("Consultando a API do OpenWeather")
   r = requests.get(openWeather_url, params = params).json()

   # Convertendo os dados para um DataFrame pandas
   df = pd.json_normalize(r.get("list"))
   print("Cabeçalho do DataFrame pandas")
   df['dt'] = pd.to_datetime(df['dt'], unit='s')
   # Grava os pontos no InfluxDB
   client = InfluxDBClient(host=host, port=port, username=username, password=senha)
   client.create_database(dbname)
   client.switch_database(dbname)
   print("Salvando os dados no InfluxDB")
   for index, row in df.iterrows():
      client.write_points(formataDadosProtocoloLinha(row, loc))

if __name__ == '__main__':
    main()