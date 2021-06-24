# -*- coding: utf-8 -*-

"""
Script para restar algumas proxies e atualizar o banco de dados.
"""

import ProxiesDB
reload(ProxiesDB)
import threading
from time import time, sleep
import argparse

# Argumentos
parser = argparse.ArgumentParser()
parser.add_argument('-w', '--workers', type=int, help=u'Número de workers (threads)')
parser.add_argument('-t', '--time', type=int, help=u'Intervalo de duração do código (em segundo)',
                        default=1e8)
args = parser.parse_args()

# Retestando as proxies
def worker():
    ProxiesDB.retest_old_proxies()

# Controlando o tempo de execução do código
start_time = time()
end_time = start_time + args.time

threads = []
while time() < end_time:
    t = threading.Thread(target=worker)
    t.daemon = True
    threads.append(t)
    t.start()
    sleep(0.1)
    while threading.active_count() > args.workers:
        sleep(1)

[t.join() for t in threads]
print('Finished!')

