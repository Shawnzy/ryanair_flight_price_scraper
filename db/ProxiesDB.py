# -*- coding: utf-8 -*-

"""
Script para criar banco de dados de gerenciamento de proxies.

O objetivo deste banco de dados (sqlite) é manter um status atualizado de funcionamento
das proxies que usamos.
"""

import pandas as pd
import sqlite3
import json
import random
import requests as req
from time import sleep, time
from datetime import datetime
from dateutil import relativedelta
from sqlalchemy import create_engine

from threading import Lock


def _db_connect():
    """
    Cria conexão com o banco de dados.
    """
    return sqlite3.connect(
        "/home/ubuntu/crawlers/scripts/viagens/ryanair/db/proxies.db"
    )


def create_db():
    """
    Função para criar o banco de dados e tabela para gerenciar proxies.
    """
    # Conectando ao banco de dados
    conn = _db_connect()
    cursor = conn.cursor()

    # Criando a tabela relevante
    drop_table = """
        DROP TABLE IF EXISTS proxies;
    """
    create_table = """
        CREATE TABLE proxies (
            proxy TEXT PRIMARY KEY,
            -- Timestamp of the last test
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            mean_response_time REAL,
            useable INTEGER
        );
    """
    resp = raw_input(
        '>>> Voce tem certeza que pretende dropar a tabela "proxies"? y/[n]\n'
    )
    if resp == "y":
        print("*** Dropando a tabela proxies")
        cursor.execute(drop_table)
        cursor.execute(create_table)

    # Fechando a conexão
    conn.commit()
    conn.close()


def update_proxy_status(proxy, mean_response_time=None, useable=None):
    """
    Função para fazer update/inserir proxy no banco de dados e fazer update
    do seu status de funcionamento.
    """
    # Testando a query
    if (mean_response_time is None) or (useable is None):
        mean_response_time, useable = test_proxy(proxy)
    # Abrindo a conexão
    with Lock():
        conn = _db_connect()
        query_proxy = """
            SELECT * FROM proxies WHERE proxy = '{}';
        """.format(
            proxy
        )
        df = pd.read_sql(query_proxy, conn)
        conn.close()
    # Se já existe uma entrada para esta proxy no banco de dados
    if df.shape[0] > 0:
        # Fazendo o update da entrada da proxy no BD
        _update_proxy_status(proxy, mean_response_time, useable)
    else:
        # Se ainda não existe entrada para esta proxy no banco de dados
        _insert_proxy_db(proxy, mean_response_time, useable)


def _update_proxy_status(proxy, mean_response_time, useable):
    """Altera o status da proxy BD"""
    # Abrindo a conexão
    with Lock():
        conn = _db_connect()
        # Atualizando o banco
        update_proxy = """
            UPDATE proxies
            SET timestamp = CURRENT_TIMESTAMP,
                mean_response_time = {mean_response_time},
                useable = {useable}
            WHERE proxy = '{proxy}';
        """.format(
            proxy=proxy, mean_response_time=mean_response_time, useable=useable
        )
        print(update_proxy)
        cursor = conn.cursor()
        cursor.execute(update_proxy)
        conn.commit()
        # Fechando a conexão
        conn.close()


def _insert_proxy_db(proxy, mean_response_time, useable):
    """Insere proxy no banco de dados"""
    # Abrindo a conexão
    conn = _db_connect()
    # Inserindo no banco
    insert_proxy = """
        INSERT INTO proxies
            (proxy, mean_response_time, useable)
        VALUES
            ('{proxy}', {mean_response_time}, {useable});
    """.format(
        proxy=proxy, mean_response_time=mean_response_time, useable=useable
    )
    print(insert_proxy)
    cursor = conn.cursor()
    cursor.execute(insert_proxy)
    conn.commit()
    # Fechando a conexão
    conn.close()


def test_proxy(proxy):
    """Testa uma proxy"""
    # Pega cookie e headers para fazer a requisição
    cookie = _get_cookie()
    headers = _get_headers(cookie=cookie)
    url = _get_url()
    print("*** Testando  proxy: [%s]" % proxy)
    print("*** com URL: [%s]" % url)
    # Faz a requisição e mede o tempo de resposta
    response, response_time, success = _request_flight_availability(url, proxy, headers)
    return response_time, success


def _get_url():
    """Retorna a URL de uma requisição comum de dados de voo"""
    availability_url = (
        "https://desktopapps.ryanair.com/v4/en-ie/"
        + "availability?ADT="
        + random.choice(["1", "2", "3"])
        + "&CHD="
        + random.choice(["1", "2", "3"])
        + "&DateOut="
        + (datetime.today() + relativedelta.relativedelta(days=5)).strftime("%Y-%m-%d")
        + "&Destination="
        + random.choice(["BUD", "DUB"])
        + "&FlexDaysOut=6"
        + "&INF=0&Origin="
        + random.choice(["FRA", "STN"])
        + "&RoundTrip=false&TEEN="
        + random.choice(["1", "2", "3"])
        + "&ToUs=AGREED"
    )
    return availability_url


def _get_cookie():
    """Faz uma requisição simples para o site da ryanair para obter um cookie novo."""
    try:
        resp = req.get("https://www.ryanair.com/gb/en/")
        return "RYANSESSION=%s" % dict(resp.cookies)["RYANSESSION"]
    except:
        return "RYANSESSION=Wp2Z3QobAmEAAFL0crUAAAAQ"


def _get_headers(cookie=""):
    """Retorna headers para a requisição."""
    headers = {
        "Host": "desktopapps.ryanair.com",
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
        "Accept": "application/json, text/plain, */*",  #'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "pt-BR,pt;q=0.8,en-US;q=0.6,en;q=0.4,de;q=0.2,fr;q=0.2",
        "Referer": "https://www.ryanair.com/gb/en/booking/home",
        "Origin": "https://www.ryanair.com",
    }
    # Escolhe um user_agent aleatoriamente
    user_agent = random.choice(open("user-agents.dat").read().split("\n"))
    headers["User-Agent"] = user_agent
    # Se for passado um cookie para o header
    if cookie:
        headers["Cookie"] = cookie
    return headers


def _request_flight_availability(
    url, proxy, headers, max_retries=3, request_timeout=45, sleep_time=1
):
    """
    Faz uma request para o url passado, usando o proxy e o cookie definidos
    na chamada.
    Retorna o resultado da requisição, bem como o tempo gasto,
    o sucesso da requisição e o horário da requisição.
    """
    proxies = {"https": "https://%s" % proxy}
    # Inicializando os parâmetros
    success = 0
    response_time = 999
    start_time = time()
    parsed_response = {}
    for retries_count in xrange(max_retries):
        print("*** Trying for the %s time" % (retries_count + 1))
        msg = """
            > url: %s
            > headers: %s
            > proxies: %s
            > cookies: %s
            """ % (
            url,
            str(headers),
            str(proxies),
            headers["Cookie"],
        )
        print(msg)
        # Tenta fazer a requisição
        try:
            response = req.get(
                url, headers=headers, proxies=proxies, timeout=request_timeout
            )
            end_time = time()
            # print('*** Response:\n%s' % response.content)
            parsed_response = json.loads(response.content)
            if parsed_response.get("message", "").startswith(
                "No HTTP resource was found that"
            ):
                break
            success = 1 if ("trips" in parsed_response.keys()) else 0
            print("*** Success: %s" % (success))
            # Tempo de resposta da requisição
            response_time = end_time - start_time
            if success:
                break
        except Exception as e:
            print("[EXCEPTION CAUGHT]: %s" % e)
            print("*** Sleeping for %s second(s) before retrying" % sleep_time)
            sleep(sleep_time)
            continue

    return parsed_response, response_time, success


def _get_proxy(useable=True):
    """Retorna a proxy usável (useable) ou não que a mais tempo não está sendo usada"""
    with Lock():
        # Abrindo a conexão
        conn = _db_connect()
        # Ordena por timestamp do último uso e filtra flag useable
        query_proxy = """
            SELECT *
            FROM proxies
            WHERE {not_useable} useable
            ORDER BY timestamp
            LIMIT 1
        """.format(
            not_useable="" if useable else "NOT"
        )
        df = pd.read_sql(query_proxy, conn)
        if df.shape[0] > 0:
            proxy = df["proxy"].values[0]
        else:
            proxy = "177.107.195.182:3128"
        conn.close()
        # "Trava" a proxy por enquanto, até que o resultado da requisição corrija
        # seu status
        _update_proxy_status(proxy, 999, 0)
    return proxy


def retest_old_proxies():
    """
    Testa proxies "não" usáveis que já não são usadas faz tempo.
    Escolhe o ip que está mais tempo sem ser usado, e faz um request de teste.
    Desta maneira o ip volta ao topo da lista com seu status atualizado.
    """
    proxy = _get_proxy(useable=False)
    update_proxy_status(proxy)


def request_flight_availability(url):
    """Faz o request para o site da ryanair, e verifica preços de passagens"""
    # Pega cookie, headers e proxy para fazer a requisição
    cookie = _get_cookie()
    headers = _get_headers(cookie=cookie)
    proxy = _get_proxy(useable=True)
    # Faz a requisição e mede o tempo de resposta
    response, response_time, success = _request_flight_availability(url, proxy, headers)
    # Atualiza o banco de dados com estes resultados
    update_proxy_status(proxy, mean_response_time=response_time, useable=success)
    return response
