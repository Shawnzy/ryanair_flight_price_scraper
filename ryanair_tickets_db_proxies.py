# -*- coding: utf-8 -*-
"""
Coleta dados dos voos anunciados na ryanair.com

#[PRECISA DOCUMENTAR] Documentacao em: "https://basecamp.com/2229872/projects/4012037/documents/11933404".
"""

import pandas as pd
import numpy as np
import math
import json
import os
import re
import requests as req
from time import time, sleep
from random import random
from datetime import datetime
from datetime import timedelta
from unicodedata import normalize
import pdb
import sys
import csv
from Queue import Queue
from threading import Thread, Lock

from db import ProxiesDB

import log
import logging
import pprint
import ryan_results

##################
#  SETUP LOGGER  #
##################
# Logger setup
log.setupLogging.setupLogging(
        error_logfile='/home/ubuntu/crawlers/scripts/viagens/ryanair/log/log_files/ryanair_error.log',
        info_logfile='/home/ubuntu/crawlers/scripts/viagens/ryanair/log/log_files/ryanair_info.log',
        logger='ryan_logger')
logger = logging.getLogger('ryan_logger')

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-n", "--nome", type=str,
        help="Nome do crawler")
args = parser.parse_args()

def _distributeSeats(seats):
    """
    Distribui assentos segundo as categorias e seus limites da api.
    Máximos por categoria:
    > ADT:  25
    > CHD:  24
    > TEEN: 24
    """
    adt = 0
    chd = 0
    teen = 0
    if seats <= 25:
        adt = seats
    elif seats > 25 and seats <= 49:
        adt = 25
        chd = seats - adt
    elif seats > 49 and seats <= 73:
        adt = 25
        chd = 24
        teen = seats - adt - chd
    elif seats > 73:
        adt = 25
        chd = 24
        teen = 24
    else:
        adt = 0
        chd = 0
        teen = 0
    return adt, chd, teen

def _parseAvailabilityUrl(date, dest, depart, seats):
    """
    Monta o url para requisitar assentos
    """
    adt, chd, teen = _distributeSeats(seats)
    availability_url = 'https://desktopapps.ryanair.com/v4/en-ie/'+\
                        'availability?ADT='+str(adt)+\
                        '&CHD='+str(chd)+\
                        '&DateOut='+str(date)+\
                        '&Destination='+str(dest)+\
                        '&FlexDaysOut=6'+\
                        '&INF=0&Origin='+str(depart)+\
                        '&RoundTrip=false&TEEN='+str(teen)+\
                        '&ToUs=AGREED'
    return availability_url

def create_queue(inicio, parte, final, partes, pula, seats_max, from_to,
                    seats_list=[1,2,3,4,5,6,
                                8, 11, 14, 17, 20, 23,
                                28]):
    """
    Cria fila com os jobs a serem executados pelos workers.
    inicio:  Número de dias a partir de hoje para iniciar busca de voos
    parte:   No caso de crawler ser rodado em mais 'partes'
    final:   Número de máximo de dias a partir de hoje para buscar voos
    partes:  No caso de crawler ser rodado em mais 'partes'
    pula:    Quantos dias devem ser pulados nas requisições por voos
    from_to: Origens/Destinos
    """
    # Inicia fila
    queue = Queue()
    # Itera sobre os dias desde 'inicio' até 'final'
    # Pulando de 'partes' em 'partes' (ex: de 2 em 2)
    ## Sendo que cada 'parte' (indicada pelo parâmetro 'parte') fica com as
    ## requisições de sua 'partição'
    # Pulando de 'pula' em 'pula' (ex: de 3 em 3)
    ## Ex: Requisita os voos que partem dia sim, dois dias nao
    logger.info('Criando fila')
    requested_days = range(inicio+parte*pula, final)[::partes][::pula]
    #print requested_days
    # Número de assentos requisitados
    seats = 1
    intervalo = final - inicio
    intervalo = range(int(inicio / 6), int(math.ceil(final / 6.)))
    logger.debug('Intervalo = %s' % intervalo)
    #for seats in range(1, seats_max + 1):
    logger.debug('> seats_list: %s' % str(seats_list))
    for seats in seats_list:
        if seats > seats_max: continue
        logger.debug('> seatst = %s' % (seats))
        for i in intervalo:
            dt = timedelta(days = i*6)
            date = (datetime.now() + dt).strftime('%Y-%m-%d')
            logger.debug('> dt = %s\n> date = %s' % (dt, date))
            # Itera sobre as origens
            for depart in from_to:
                # Itera sobre os destinos que partem desta origem
                for dest in from_to[depart]:
                    # Adiciona 'job' na fila
                    queue.put((date, depart, dest, seats))
    logger.info('Tamanho da fila: %s' % queue.unfinished_tasks)
    # Retorna a fila completa
    return queue


def worker(queue, outf, defect_queue, nome):
    """
    Thread "worker": fica executando os jobs na fila até que ela esteja vazia.
    """
    try:
        while not queue.empty():
            tup = queue.get()
            nome = outf.split('/')[-1].split('.')[0]
            print 'nome:', nome
            logger.debug('Job: %s' % str(tup))
            resp = parseFlight(tup, outf)
            queue.task_done()
            # Se a tarefa não foi corretamente executada
            #if resp == 1:
            #    defect_queue.put(tup)
            #    logger.info(u'[TESTE] Job: %s não foi executado corretamente\t> url:\t%s' % (str(tup), _parseAvailabilityUrl(*tup)))
            # Se a tarefa não foi corretamente executada
            if resp == 0:
                queue.put(tup)
                logger.info(u'%s\tJob: %s não foi executado corretamente\t> url:\t%s' % (nome, str(tup), _parseAvailabilityUrl(*tup)))
            if resp == -2:
                queue.put(tup)
                logger.info(u'%s\tJob: %s não foi executado corretamente (dotRez Exception caught)\t> url:\t%s' % (nome, str(tup), _parseAvailabilityUrl(*tup)))
                sleep(1 + np.random.random()*2) # CHANGE
                continue
            # Se a tarefa não foi corretamente executada
            if resp == -1:
                defect_queue.put(tup)
                logger.info(u'%s\tJob: %s não foi executado corretamente (Exception caught)\t> url:\t%s' % (nome, str(tup), _parseAvailabilityUrl(*tup)))
            # Se a tarefa não foi corretamente executada
            if resp == 2:
                defect_queue.put(tup)
                logger.info(u'%s\tJob: %s não foi executado corretamente (Não parseou chave "trips")\t> url:\t%s' % (nome, str(tup), _parseAvailabilityUrl(*tup)))

            # Descansa um pouco antes de tentar outra vez
            sleep(30 + random()*70)

        logger.info(u'Thread acabou a execução normalmente.')
    except:
        logger.exception(u'Thread terminou abruptamente a execução.')

def parseAirports(response):
    """
    Parseia os aeroportos e seus destinos.
    """
    # Transforma json em dict
    airports = json.loads(response.text)

    # Dicionário com destinos
    from_to = dict()

    # Itera nos aeroportos
    airport_count = 0
    for airport in airports['airports']:
        from_to[airport['iataCode']] = [x[8:] for x in airport['routes'] if x[0]=='a'] + [x[8:] for x in airport['seasonalRoutes'] if x[0] == 'a']
        airport_count += len(from_to[airport['iataCode']])

    # Contagem de rotas
    logger.info(u'Número de aeroportos parseados: %s' % airport_count)

    return from_to

def parseFlight(tup, outf):
    """
    1.Faz o request com os seguintes parâmetros:
    > Data:        Dia de partida
    > Origem:      Aeroporto de origem
    > Destino:     Aeroporto de destino
    > N. assentos: Número de assentos desejados

    2.Pareseia informações sobre os voos
    """

    date, depart, dest, seats = tup
    availability_url = _parseAvailabilityUrl(date, dest, depart, seats)
    logger.debug('Flight URL: %s' % availability_url)
    information = ProxiesDB.request_flight_availability(availability_url)
    if not information:
        logger.info('Request did not work, returned empty dict: %s' % availability_url)
        return 0

    code = information.get('code', '')
    if "The request is invalid" in code:
        return 2
    elif ("Exception of type" in code) or ("dotRez" in code):
        return -2

    if information.get('message', '').startswith('No HTTP resource was found that'):
        logger.info('Request did not work, resopnse: %s from [%s]' % (str(information), availability_url))
        return 0

    try:
        # Itera sobre as 'trips'
        if 'trips' in information.keys():
            for trip in information['trips']:
                # Iterar sobre as datas
                for date_out in trip['dates']:
                    # Itera sobre os voos em cada data
                    for flight in date_out['flights']:
                        # Um item para cada (viagem, data, voo, assentos)
                        item = dict()
                        # Número de assentos requisitados
                        item['seatsRequired'] = seats
                        # Horário da requisição
                        item['serverTimeUTC'] = information['serverTimeUTC']
                        # Moeda dos preços
                        item['currency'] = information['currency']
                        # Viagem (Origem, Destino)
                        item['origin'] = trip['origin']
                        item['originName'] = remover_acentos(trip['originName'])
                        item['destination'] = trip['destination']
                        item['destinationName'] = remover_acentos(trip['destinationName'])
                        # Date outs (Datas de partida)
                        item['dateTakeOff'] = date_out['dateOut']
                        # Voos que partem destes destinos nesta data
                        item['flightNumber'] = flight['flightNumber']
                        item['timeTakeOff'] = flight['time'][0]
                        item['timeArrival'] = flight['time'][1]
                        item['timeUTCTakeOff'] = flight['timeUTC'][0]
                        item['timeUTCArrival'] = flight['timeUTC'][1]
                        item['flightKey'] = flight['flightKey']
                        item['duration'] = flight['duration']
                        item['faresLeft'] = flight['faresLeft']
                        item['infantsLeft'] = flight['infantsLeft']
                        # Tenta pegar informações sobre preços dos assentos (se houver)
                        ## Fare class
                        regularFare = flight.get('regularFare', None)
                        if regularFare is None: regularFare = dict()
                        item['fareClass'] = regularFare.get('fareClass', None)
                        ## Prices
                        fare_types = ['Regular', 'Leisure', 'Business']
                        for fare_type in fare_types:
                            fares = flight.get(fare_type.lower() + 'Fare', None)
                            if fares is not None:
                                fares = fares.get('fares', None)
                                if fares is not None:
                                    for fare in fares:
                                        item['price' + fare_type] = fare.get('amount', None)
                                        item['originalPrice' + fare_type] = fare.get('publishedFare', None)
                                        if fare.get('type') == 'ADT':
                                            break

                        writeToCsv(item, outf)

        else:
            logger.info(u'Não parseou chave "trips". Texto da resposta: [%s]' % response.text)
            return 2

    except KeyError:
        logger.exception('KeyError no parsing de um voo (url: %s)' % availability_url)
        pprint.pprint(information, width=1, indent=2)
        return 0

    except:
        logger.exception('Error no parsing de um voo (url: %s)' % availability_url)
        pprint.pprint(information, width=1, indent=2)
        return -1

    # Sucesso
    logger.debug('Voo parseado com sucesso: %s' % availability_url)
    return 1


def remover_acentos(txt, codif='utf-8'):
    """
    Função para remover acentos.
    """
    if txt == None or txt == []:
        return None
    else:
        try:
            return (normalize('NFKD', txt).encode('ascii', 'ignore')).replace('\n',' ')
        except:
            return (normalize('NFKD', txt[0]).encode('ascii', 'ignore')).replace('\n',' ')

def writeToCsv(item, outf, first_row=False):
    """
    Escreve o conteúdo do dicionário 'item' no arquivo de saída.
    Reindexa as colunas para as definidas em 'field_list'
    """
    #logger = logging.getLogger('writeToCsv')
    # Default field_list for output csv file
    field_list = ['currency',
                'origin',
                'originName',
                'destination',
                'destinationName',
                'flightNumber',
                'dateTakeOff',
                'timeTakeOff',
                'timeArrival',
                'flightKey',
                'duration',
                'faresLeft',
                'infantsLeft',
                'fareClass',
                'priceRegular',
                'originalPriceRegular',
                'priceLeisure',
                'originalPriceLeisure',
                'priceBusiness',
                'originalPriceBusiness',
                'timeUTCTakeOff',
                'timeUTCArrival',
                'serverTimeUTC',
                'seatsRequired']
    # Se for a primeira vez que é chamada a função, cria o arquivo e escreve o
    # header
    if first_row:
        logger.info('Criando output file: %s' % outf)
        with open(outf, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, field_list, restval='',
                                    extrasaction='raise', delimiter=';')
            writer.writeheader()
    else:
        with Lock():
            # apenda uma linha ao arquivo de saida
            with open(outf, 'a') as csvfile:
                writer = csv.DictWriter(csvfile, field_list, restval='',
                                        extrasaction='raise', delimiter=';')
                writer.writerow(item)
                logger.debug('Apendando item ao arquivo de saida:')
                pprint.pprint(item, width=1)

def getOutputLinecount(outf):
    """
    Conta as linhas do arquivo de output.
    """
    try:
        with open(outf, 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            rows = list(reader)
            row_count = len(rows)
        return row_count
    except:
        return 0


class MaxTimeExceededException(Exception):
    pass

if __name__ == "__main__":
    """
    Executa se o script for chamado como main.
    """
    # Execution flags
    done = False

    # Starting time
    start = time()
    ano, mes, dia, hora = datetime.now().strftime('%Y-%m-%d-%H').split('-')

    # Crawler parameters
    nome = 'ryanair6xdia-01' # nome do crawler
    if args.nome:
        nome = args.nome
    parte = int(nome[-2:]) - 1  # "parte" do crawler: "01"
    frequencia = nome[:-3][7:]  # frequencia do crawler: "8xdia"

    # Loggers que serão usados
    ## root logger
    logger = logging.getLogger('ryan_logger')
    ## logger que manda email
    smtp_logger = logging.getLogger('smtp_logger')

    logger.info(u'================================================')
    logger.info(u'Começando execução do programa: nome = %s' % nome)

    # Try except finally que controla o envio de emails
    try:

        ## config file
        with open('/home/ubuntu/crawlers/input/ryanair/conf.json') as f:
            config = json.load(f)
            logger.info('Configurando crawler: /home/ubuntu/crawlers/input/ryanair/conf.json')
        conf = config[nome[:-3]]        # Seleciona a configuracao por nome
        inicio = int(conf['primeiro'])  # Pirmeiro dia (a partir de datetime.now())
        final = int(conf['ultimo'])     # Último dia (a partir de datetime.now())
        pula = int(conf['pula'])        # "A cada x dias", onde x = "pula"
        partes = int(conf['particoes']) # Quantas partes tem o crawler
                                        # (para dividir o número de requisições de cada processo)
        seats_max = int(conf.get('seats_max', 1))
        logger.info(u"""Parâmetros do programa:
            > inicio: %s
            > final: %s
            > pula: %s
            > partes: %s
            > seats_max: %s""" % (inicio, final, pula, partes, seats_max))

        ## output file
        out = '/home/ubuntu/crawlers/output/viagens/ryanair/180dias/' + frequencia + '/'
        if not os.path.exists(out):    # Se não existir, cria pasta
            os.makedirs(out)
        out_file = nome + '-' + datetime.now().strftime('%Y-%m-%d-%H') + '.csv'
        outf = out + out_file
        logger.info(u'Escrevendo header do arquivo: %s' % outf)
        writeToCsv(None, outf, first_row=True)

        # Primeira requisição
        # Processa os aeroportos e seus destinos possíveis
        logger.info('GET aeroportos...')
        response = req.get('https://api.ryanair.com/aggregate/3/common?embedded=airports&market=en-gb')

        # Parseia origens e todos os seus destinos
        logger.info('Parseando aeroportos...')
        from_to = parseAirports(response)

        if nome[:-3][7:] == 'SpecificFlight':
            from_to = {'DUB': ['STN', 'BCN', 'BUD', 'CIA', 'BHX'],
                        'STN': ['DUB', 'ATH', 'LIS', 'PFO', 'MAD', 'BCN', 'SXF'],
                        'PMI': ['DUS', 'CGN', 'TXL', 'STR'],
                        'STR': ['PMI'],
                        'CIA': ['STN'],
                        'BHX': ['DUB'],
                        'DUS': ['PMI'],
                        'LGW': ['DUB'],
                        'FCO': ['CTA'],
                        'MAN': ['DUB'],
                        'FRA': ['PMI'],
                        'BCN': ['FCO'],
                        'AMS': ['DUB'],
                        'EDI': ['STN'],
                        'SXF': ['STN'],
                        'WMI': ['STN'],
                        'NAP': ['BGY'],
                        'BDS': ['BGY'],
                        'MAD': ['IBZ']
                        }
            logger.info('Criando fila...')
            queue = create_queue(inicio, parte, final, partes, pula, seats_max, from_to,
                                 seats_list=range(1,25))
        else:
            # Cria fila com as requisicoes a serem feitas
            logger.info('Criando fila...')
            queue = create_queue(inicio, parte, final, partes, pula, seats_max, from_to)

        # Cria fila com as reuisicoes que nao funcionaram
        defect_queue = Queue()

        # Cria "workers" para executarem os jobs na fila (queue)
        num_threads = 75 # CHANGE
        # Ideia, fazer uma Thread que controla as outras.
        # E verifica se o tempo limite não foi superado
        ## set timeout para agora +5 horas
        timeout = time() + 3600*24.01
        if nome[:-3] == 'ryanair2xdia':
            num_threads = 40 # CHANGE
        if nome[:-3] == 'ryanair1x2dias':
            num_threads = 85 # CHANGE
            timeout = time() + 3600*27.01
        if nome[:-3] == 'ryanair1x3dias':
            num_threads = 85 # CHANGE
            timeout = time() + 3600*27.01
        if nome[:-3] == 'ryanair45seats':
            timeout = time() + 3600*7
        if nome[:-3] == 'ryanairSpecificFlight':
            num_threads = 20
            timeout = time() + 3600*18.01

        logger.info('Criando %s threads' % num_threads)
        for i in range(num_threads):
            t1 = Thread(target=worker, args=(queue, outf, defect_queue, nome))
            # Setar todas threads como deamons para que a main-thread controle o
            # tempo de execução total do programa
            t1.setDaemon(True)
            t1.start()
            sleep(1*np.random.random())

        count = 0
        total_tasks = queue.unfinished_tasks
        recent_rate = 0
        recent_tasks = 0
        recent_start = 0
        last_done_tasks = 0
        while queue.unfinished_tasks and (time() < timeout):
            elapsed_time = time() - start
            done_tasks = (total_tasks-queue.unfinished_tasks)
            task_ratio = float(queue.unfinished_tasks) / total_tasks * 100
            defect_task_ratio = float(defect_queue.unfinished_tasks) / total_tasks * 100
            logger.info("""%s elapsed time: %s [unfinished tasks: %s/%s (%.2f%%)]
                    > unfinished tasks:\t%s/%s\t(%.2f%%)
                    > defective tasks:\t%s/%s\t(%.2f%%)
                    > mean result rate:\t%.2f [s^-1]
                    > recent result rate:\t%.2f [s^-1]
                    """ % (out_file.split('.')[0], elapsed_time,
                        queue.unfinished_tasks, total_tasks, task_ratio,
                        queue.unfinished_tasks, total_tasks, task_ratio,
                        defect_queue.unfinished_tasks, total_tasks, defect_task_ratio,
                        (done_tasks)/elapsed_time,
                        recent_rate
                        ))
            sleep(5)
            if count % 20 == 0 and count != 0: # CHANGE
                recent_elapsed_time = time() - recent_start
                recent_tasks = done_tasks - last_done_tasks
                recent_rate = recent_tasks / recent_elapsed_time
                recent_start = time()
                last_done_tasks = done_tasks
            count = count + 1

        ## Se o tempo exceder X horas
        if (time() > timeout):
            elapsed_time = time() - start
            msg = """Max time exceeded in crawler: %s
                Elapsed time: %s
                At:           %s
                """ % (nome, elapsed_time, time())
            logger.error(msg)
            raise MaxTimeExceededException(msg)
        ## Programa terminou como esperado (com a lista vazia e completa!)
        else:
            logger.info('Crawler %s terminado com sucesso!' % nome)
            done = True

    # Se der algum erro não tratado na execução
    except Exception as e:
        logger.exception('Exceiption on main handler')

    finally:
        # Tempo de execução
        exec_time = time() - start

        # Output line count
        linecount = getOutputLinecount(outf)

        # Remaining tasks
        try:
            task_ratio = float(queue.unfinished_tasks) / total_tasks * 100
        except:
            task_ratio = -1.

        # Remaining defective tasks
        try:
            defect_task_ratio = float(defect_queue.unfinished_tasks) / total_tasks * 100
        except:
            defect_task_ratio = -1.

        # Mensagem do email
        msg = dict()
        ## Se não houve problemas na execução
        if done:
            msg['subject'] = "Crawler %s executado com sucesso!" % nome
            msg['body'] = """Crawler %s executado com sucesso!
            Tempo de execucao: %s s
            Numero de resultados: %s""" % (nome, str(exec_time), str(linecount-1))
        ## Se houve problemas na execução
        else:
            msg['subject'] = "Erro no crawler %s!" % nome
            msg['body'] = """Erro no crawler %s!
            Tempo de execucao:    %s s
            Numero de resultados: %s
            Restante da fila:     %.2f%%
            Tarefas defeituosas:  %.2f%%""" % (nome, str(exec_time), str(linecount-1), task_ratio, defect_task_ratio)
        # Update email subject
        smtp_logger.handlers[0].subject = msg['subject']

        # Apendar mais informação à mesagem de email
        # Espera os logs serem escritos
        sleep(2)
        _msg = ryan_results.prepareMsg()
        msg['body_email'] = msg['body'] + '\n' + '#'*70 + '\n' + _msg

        # Send email
        if done:
            smtp_logger.info(msg['body_email'])
        else:
            smtp_logger.exception(msg['body_email'])
        # Log to file
        logger.info(msg['body'])

