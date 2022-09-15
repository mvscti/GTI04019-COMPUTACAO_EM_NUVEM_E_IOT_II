# -*- coding: utf-8 -*-
"""Tutorial for using pandas and the InfluxDB client."""

import argparse
import pandas as pd

from influxdb import DataFrameClient


def main(host='localhost', port=8086):
    """Instantiate the connection to the InfluxDB client."""
    user = 'admin'
    password = ''
    dbname = ''
    protocol = 'line'

    client = DataFrameClient(host, port, user, password, dbname)

    print("Create pandas DataFrame")
    df = pd.DataFrame(data=list(range(30)),
                      index=pd.date_range(start='2014-11-16',
                                          periods=30, freq='H'), columns=['0'])

    print("Create database: " + dbname)
    client.create_database(dbname)

    print("Write DataFrame")
    client.write_points(df, 'demo', protocol=protocol)

    print("Write DataFrame with Tags")
    client.write_points(df, 'demo',
                        {'k1': 'v1', 'k2': 'v2'}, protocol=protocol)

    print("Read DataFrame")
    client.query("select * from demo")

    #print("Delete database: " + dbname)
    #client.drop_database(dbname)


def parse_args():
    parser = argparse.ArgumentParser(
        description='código a rodar com InfluxDB')
    parser.add_argument('--host', type=str, required=False,
                        default='localhost',
                        help='nome do host da API http do InfluxDB')
    parser.add_argument('--porta', type=int, required=False, default=8086,
                        help='porta da API http do InfluxDB')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main()