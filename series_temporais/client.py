from influxdb import InfluxDBClient

username=''
senha=''
client = InfluxDBClient(host='localhost', port=8086, username=username, password=senha)
'''Imprimindo a lista de banco de dados'''
print(client.get_list_database())

'''Cria uma base de dados'''
client.create_database('exemplo')

'''Seleciona uma base de dados'''
client.switch_database('exemplo')

''' Pontos de uma 'escova de dentes inteligente'. 
Cada ponto representa um 'evento de limpeza', com cada um deles começando por volta de 8 da manhã,
identificados com a tag da pessoa e do id da escova, bem como o tempo (em segundos) que a limpeza durou '''
json_body = [
    {
        "measurement": "brushEvents",
        "tags": {
            "user": "Carol",
            "brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
        },
        "time": "2018-03-28T8:01:00Z",
        "fields": {
            "duration": 127
        }
    },
    {
        "measurement": "brushEvents",
        "tags": {
            "user": "Carol",
            "brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
        },
        "time": "2018-03-29T8:04:00Z",
        "fields": {
            "duration": 132
        }
    },
    {
        "measurement": "brushEvents",
        "tags": {
            "user": "Carol",
            "brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
        },
        "time": "2018-03-30T8:02:00Z",
        "fields": {
            "duration": 129
        }
    }
]

''' Inserindo os pontos'''
client.write_points(json_body)

''' Selecionando o tempo da limpeza nos últimos 4 dias'''
results=client.query('SELECT "duration" FROM "exemplo"."autogen"."brushEvents" WHERE time > now() - 4d GROUP BY "user"')

''' Imprimindo o resultado'''
print(results.raw)


'''Selecionando os dados de um usuário em específico'''
points = results.get_points(tags={'user':'Carol'})
'''Iterando o resultado'''
for point in points:
    print(f"Tempo: {point['time']}, Duração: {point['duration']}")