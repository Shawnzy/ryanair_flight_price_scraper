# -*- coding: utf-8 -*-

"""
Esses são os comandos para acompanhar os resultados no banco de dados:
    Número de proxies funcionando
    Número de proxies total

# Script que fica retestando proxies antigas
%time %run retest_proxies.py
"""

# Para Observar os resultados em uma sessão do ipython
import pandas as pd
from datetime import datetime
from time import sleep
import ProxiesDB
conn = ProxiesDB._db_connect()

pd.set_option('display.width', 90)

def get_proxy_resume():
    df = pd.read_sql('SELECT * FROM proxies ORDER BY timestamp DESC', conn)
    msg = ''
    msg += str('='*45) + '\n'
    msg += str(datetime.now()) + '\n'
    msg += str(df.tail()) + '\n'
    msg += str(df.query('useable==0').tail()) + '\n'
    msg += str(df.query('useable==1').tail()) + '\n'
    msg += str(df.head()) + '\n'
    msg += str('\n') + '\n'
    msg += str(df['useable'].value_counts()) + '\n'
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    min_unuseable_ts = df.query('useable==0')['timestamp'].min()
    min_useable_ts = df.query('useable==1')['timestamp'].min()
    msg += str('Tempo maximo esperando retest:') + '\n'
    msg += str(datetime.now() - min_unuseable_ts) + '\n'
    msg += str('Tempo minimo de reutilizacao de proxy:') + '\n'
    msg += str(datetime.now() - min_useable_ts) + '\n'
    return msg

if __name__ == '__main__':
    while True:
        msg = get_proxy_resume()
        print(msg)
        sleep(2)

