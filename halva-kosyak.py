# Восстановление status_code, callcenter_status_code, visit_status_code из sovcombank_products.csv

import csv
from mysql.connector import MySQLConnection

from lib import read_config

def count_lines(filename, chunk_size=1<<13):
    with open(filename) as file:
        return sum(chunk.count('\n')
                   for chunk in iter(lambda: file.read(chunk_size), ''))

def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
    # Print New Line on Complete
    if iteration == total:
        print()

dbconfig = read_config(filename='halva.ini', section='SaturnFIN')
dbconn = MySQLConnection(**dbconfig)
cursor = dbconn.cursor()
updates_tek = []
lines_on_file = count_lines('sovcombank_products.csv')
printProgressBar(0, lines_on_file, prefix='Прогресс:', suffix='Обновлено в БД', length=50)
with open('sovcombank_products.csv', 'r', encoding='utf-8') as input_file:
    dict_reader = csv.DictReader(input_file, delimiter='\t')
    for i, line in enumerate(dict_reader):
        updates_tek.append((line['status_code'], line['callcenter_status_code'], line['visit_status_code'], line['id']))
        if not i % 100 and i > 0:
            cursor.executemany('UPDATE saturn_fin.sovcombank_products SET status_code = %s, '
                               'callcenter_status_code = %s, visit_status_code = %s WHERE id = %s', updates_tek)
            dbconn.commit()
            updates_tek = []
            printProgressBar(i, lines_on_file, prefix='Прогресс:', suffix='Обновлено в БД', length=50)
cursor.executemany('UPDATE saturn_fin.sovcombank_products SET status_code = %s, '
                   'callcenter_status_code = %s, visit_status_code = %s WHERE id = %s', updates_tek)
dbconn.commit()
printProgressBar(lines_on_file, lines_on_file, prefix='Прогресс:', suffix='Обновлено в БД', length=50)
dbconn.close()



