# -*- coding: utf-8 -*-

"""
Script para testar algumas proxies de um arquivo e
inserir/atualizar o banco de dados com os novos resultados.
"""

import ProxiesDB

reload(ProxiesDB)
import pandas as pd
import threading
from time import time, sleep
import argparse

# Argumentos
parser = argparse.ArgumentParser()
parser.add_argument("input", help="Arquivo de input (lista de ips de proxies)")
parser.add_argument("-w", "--workers", type=int, help="Número de workers (threads)")
args = parser.parse_args()

# Lendo a lista de proxies "boas"
proxies = pd.read_csv(args.input, header=None)
proxies.columns = ["proxy"]
proxies = proxies.sample(frac=1.0)
print("# Proxies para serem processados: %s" % proxies.shape[0])

# Criando o banco de dados
# ProxiesDB.create_db()


def worker(proxy):
    print("*** Proxy: %s" % proxy)
    ProxiesDB.update_proxy_status(proxy)


threads = []
for i, (row, proxy) in enumerate(proxies.iterrows()):
    _args = [proxy["proxy"]]
    t = threading.Thread(target=worker, args=(_args))
    t.daemon = True
    threads.append(t)
    t.start()
    while threading.active_count() > args.workers:
        print("=" * 45)
        print("Threading.active_count(): %s" % threading.active_count())
        print("Processing proxy: %s/%s" % (i, proxies.shape[0]))
        sleep(1)

[t.join() for t in threads]
print("Finished!")
