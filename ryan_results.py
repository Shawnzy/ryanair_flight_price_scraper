# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import subprocess
import datetime
import logging
import glob
import time
import log
import os
import sh
import re
from db import proxy_db_monitor

output_file = '/home/ubuntu/crawlers/output/viagens/ryanair/180dias/'

def getSystemDiskSpace(msg):
    """
    Retorna informacoes sobre o disco.
    """
    disk = os.statvfs("/var/")
    msg += "\n~~~~~~~~~~calculation of disk usage:~~~~~~~~~~\n"
    totalBytes = float(disk.f_bsize*disk.f_blocks)
    msg += "total sace:                 %.2f MBytes =\t%.2f GBytes\n" % (totalBytes/1024/1024, totalBytes/1024/1024/1024)
    totalUsedSpace = float(disk.f_bsize*(disk.f_blocks-disk.f_bfree))
    msg += "used space:                 %.2f MBytes =\t%.2f GBytes\n" % (totalUsedSpace/1024/1024, totalUsedSpace/1024/1024/1024)
    totalAvailSpace = float(disk.f_bsize*disk.f_bfree)
    msg += "available space:            %.2f MBytes =\t%.2f GBytes\n" % (totalAvailSpace/1024/1024, totalAvailSpace/1024/1024/1024)
    totalAvailSpaceNonRoot = float(disk.f_bsize*disk.f_bavail)
    msg += "available space (non-sudo): %.2f MBytes =\t%.2f GBytes\n" % (totalAvailSpaceNonRoot/1024/1024, totalAvailSpaceNonRoot/1024/1024/1024)
    return msg

def getSystemMemoryUsage(msg):
    """
    Retorna informações sobre o uso de memória do PC.
    """
    msg += "\n~~~~~~~~~calculation of memory usage:~~~~~~~~~\n"
    msg += subprocess.check_output(['free', '-h'])
    return msg

def getRyanInfo(msg, logfile='info', n=10):
    """
    Retorna log de info ou erro do crawler.
    """
    msg += '\n' + '-'*70
    msg += '\nRyanair %s log:\n' % logfile

    txt = ''
    for line in sh.tail("-n", n, "/home/ubuntu/crawlers/scripts/viagens/ryanair/log/log_files/ryanair_%s.log" % logfile):
       txt += line
    msg += txt
    msg += '-'*70 + '\n'
    return msg

def getFlightDataStats(f, msg, print_df=False):
    """
    Lê o dataframe e retorna algumas informações.
    """
    try:
        df = pd.read_csv(f, sep=';')
        ddf = df.fillna(0).groupby(['dateTakeOff', 'seatsRequired']).agg({'flightNumber': pd.Series.nunique,
                                                                          'priceRegular': np.count_nonzero})
        if print_df:
            msg += str(ddf) + '\n'
        msg += str(ddf.describe()) + '\n'
        return msg
    except:
        return msg

def prepareMsg(print_df=False, log_info=True):
    # Mensagem
    msg = ''
    files = ['6xdia/', '2xdia/', '1xdia/', '1x2dias/', '1x3dias/']
    for fil in files:
        fil = output_file + fil
        msg += '\n' + '='*70 + '\n'
        f = glob.glob(fil + '*.csv')
        if f:
            f.sort(key=os.path.getmtime)
            f.reverse()
            newest_file = f.pop(0)
            creation_date = re.findall(r'\d{4}-\d{2}-\d{2}-\d{2}', newest_file)[0]
            creation_date = datetime.datetime.strptime(creation_date, '%Y-%m-%d-%H')
            creation_date = creation_date.strftime('%A %B %d %H:%M:%S %Y')
            msg += 'Newest file in folder: %s' % fil + '\n'
            msg += '> File name:          %s' % newest_file.split('/')[-1] + '\n'
            msg += '> File creation date: %s' % creation_date + '\n'
            msg += '> File last uptadet:  %s' % time.ctime(os.path.getmtime(newest_file)) + '\n'
            msg += '> File size:          %.2f Mb' % (os.path.getsize(newest_file) / 1024. / 1024.) + '\n'
            msg += '-'*70 + '\n'
            msg += 'Last files:' + '\n'
            if f:
                for ff in f[:5]:
                    msg += '-> %.2f Mb' % (os.path.getsize(ff) / 1024. / 1024.) + ff.split('/')[-1] + '\n'

            msg += '-'*70 + '\n'
            msg += 'Data Stats' + '\n'
            msg = getFlightDataStats(newest_file, msg, print_df=print_df)

    msg += '\n' + proxy_db_monitor.get_proxy_resume() + '\n'
    msg = getSystemDiskSpace(msg)
    msg = getSystemMemoryUsage(msg)

    if log_info:
        msg = getRyanInfo(msg, logfile='error', n=20)
        msg = getRyanInfo(msg, logfile='info', n=20)

    return msg

if __name__=='__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--email",
            help="Enviar email com o log", action='store_true')
    parser.add_argument("-s", "--sleep",
            help="Tempo em segundos de espera", default=120)
    parser.add_argument("-r", "--repeat",
            help="Repetir a cada [sleep] segundos", action='store_true')
    args = parser.parse_args()

    msg = prepareMsg(print_df=False, log_info=False)
    print msg

    if args.email:
        # Logger setup
        log.setupLogging.setupLogging(
                error_logfile='/home/ubuntu/crawlers/scripts/viagens/ryanair/log/log_files/ryanair_error.log',
                info_logfile='/home/ubuntu/crawlers/scripts/viagens/ryanair/log/log_files/ryanair_info.log',
                logger='')
        smtp_logger = logging.getLogger('smtp_logger')

        print 'Sending Email...'
        today = datetime.datetime.today().strftime('%a, %d/%b/%y, %T')
        smtp_logger.handlers[0].subject = '[Crawler/Sherlock] Daily - %s' % today
        smtp_logger.info(msg)
        print 'Sent!'
    elif args.repeat:
        # Repete a mensagem a cada x segundos
        while True:
            msg = prepareMsg(print_df=False, log_info=False)
            print msg
            time.sleep(args.sleep)

