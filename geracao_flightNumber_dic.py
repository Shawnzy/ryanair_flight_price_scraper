# -*- coding: utf-8 -*-

import pandas as pd
import sys
import time
import glob

mes_ref = sys.argv[1]
hoje = time.strftime('%Y%m%d')
print mes_ref

aux = sorted(glob.glob('/home/ubuntu/crawlers/output/viagens/ryanair/180dias/6xdia/ryanair6xdia-01-2018-' + mes_ref + '-*'))
seisxdia = pd.read_csv(aux[0],';')#[-2]
seisxdia = seisxdia[['flightNumber','origin','originName','destination','destinationName']].drop_duplicates()

aux = sorted(glob.glob('/home/ubuntu/crawlers/output/viagens/ryanair/180dias/2xdia/ryanair2xdia-01-2018-' + mes_ref + '-*'))
doisxdia = pd.read_csv(aux[0],';')
doisxdia = doisxdia[['flightNumber','origin','originName','destination','destinationName']].drop_duplicates()

aux = sorted(glob.glob('/home/ubuntu/crawlers/output/viagens/ryanair/180dias/1xdia/ryanair1xdia-01-2018-' + mes_ref + '-*'))
umxdia = pd.read_csv(aux[0],';')
umxdia = umxdia[['flightNumber','origin','originName','destination','destinationName']].drop_duplicates()

aux = sorted(glob.glob('/home/ubuntu/crawlers/output/viagens/ryanair/180dias/1x2dias/ryanair1x2dias-01-2018-' + mes_ref + '-*'))
umx2dias = pd.read_csv(aux[0],';')
umx2dias = umx2dias[['flightNumber','origin','originName','destination','destinationName']].drop_duplicates()

total = pd.concat([seisxdia,doisxdia,umxdia,umx2dias],ignore_index=True)
total = total.drop_duplicates()
total.to_csv('/home/ubuntu/crawlers/output/viagens/ryanair/180dias/dictionaries/' + hoje + 'flightNumber_dict.csv',index=False)
