# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np


def analyze():
    # Lendo o arquivo de log
    log_file = (
        "/home/ubuntu/crawlers/scripts/viagens/ryanair/log/log_files/"
        + "ryanair_info.log"
    )
    log_files = [log_file + ((".%s" % x) if x else "") for x in range(5)]
    _log = pd.DataFrame()
    for f in log_files:
        print(f)
        skpr = 0
        for skpr in range(500):
            try:
                log = pd.read_csv(f, sep="\t", header=None, skiprows=skpr)
                continue
            except Exception as e:
                pass
        info = log.iloc[:, 0:6]
        info.columns = ["level", "datetime", "thread", "logger", "module", "line_msg"]
        # Concatenando as colunas de mensagem
        log = log.iloc[:, 6:].astype(str).apply(lambda x: x.sum(), axis=1)
        log.name = "msg"

        log = pd.concat([info, log], axis=1)
        l = log.pop("line_msg").str.split(
            ": (ryanair.+dias*)-(\d{2})-(\d{4}-\d{2}-\d{2}-\d{2})", 1, expand=True
        )
        log[["line", "crawler", "part", "crawlerdate", "msg_"]] = l

        log["msg"] = (
            log.pop("msg_").fillna("").astype(str)
            + " "
            + log.pop("msg").fillna("").astype(str)
        )

        log["datetime"] = pd.to_datetime(log["datetime"], errors="coerce")

        log["unfinished_tasks"] = np.nan
        if log.loc[log["msg"].str.contains("unf"), "msg"].shape[0] > 0:
            log["unfinished_tasks"] = (
                log.loc[log["msg"].str.contains("unf"), "msg"]
                .str.split("unfinished tasks: (\d+)", expand=True)
                .iloc[:, 1]
                .astype(float)
            )

        _log = _log.append(log)

    log = _log

    dotRez = (
        log.loc[(log["msg"].str.contains("dotRez"))]
        .groupby(["crawler", "crawlerdate"])
        .agg({"datetime": [np.max, np.min], "msg": [np.count_nonzero]})
    )
    freqRes = (
        log.loc[(log["msg"].str.contains("unf"))]
        .groupby(["crawler", "crawlerdate"])
        .agg({"datetime": [np.max, np.min], "unfinished_tasks": [np.max, np.min]})
    )

    # Calcula a frequencia de dotRez por segundo
    try:
        dotRez["freq"] = (
            dotRez[("msg", "count_nonzero")]
            / (dotRez[("datetime", "amax")] - dotRez[("datetime", "amin")]).dt.seconds
        )
    except:
        dotRez["freq"] = np.nan
    freqRes["freq"] = (
        freqRes[("unfinished_tasks", "amax")] - freqRes[("unfinished_tasks", "amin")]
    ) / (freqRes[("datetime", "amax")] - freqRes[("datetime", "amin")]).dt.seconds
    freqRes["estimated_total_hours"] = (
        (freqRes[("datetime", "amax")] - freqRes[("datetime", "amin")]).dt.seconds
        / (
            1
            - (
                freqRes[("unfinished_tasks", "amin")]
                / freqRes[("unfinished_tasks", "amax")]
            )
        )
        / 3600
    )

    if dotRez["freq"].notnull().sum() > 0:
        print(dotRez.join(freqRes, how="outer", lsuffix="_dotRez").T)
    else:
        print(freqRes)


if __name__ == "__main__":
    from time import sleep
    from datetime import datetime

    while True:
        analyze()
        print(datetime.now())
        sleep(60)
