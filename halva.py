# -*- coding: utf-8 -*-
# Робот, отмечающий загруженные


import sys
from _datetime import datetime, timedelta
from time import sleep
from dateutil.relativedelta import relativedelta
import time
import os
import zipfile
import csv
from mysql.connector import MySQLConnection, Error

from lib import read_config, lenl, s_minus, s, l, filter_rus_sp, filter_rus_minus

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

# Статистика с этой даты
DATE_START_COUNT = datetime(2018,8,1)


all_files = os.listdir(path=".")

                          # Распаковываем все zip в директории
for i, all_file in enumerate(all_files):
    if all_file.endswith(".zip"):
        try:
            zip = zipfile.ZipFile(all_file)
            zip.extractall(path='.')
        except zipfile.BadZipfile as e:
            print("Плохой ZIP-файл: ", all_file)
        try:
            os.remove(all_file)
        except OSError as e:          # errno.ENOENT = no such file or directory
            if e.errno != OSError.errno.ENOENT:
                raise                 # re-raise exception if a different error occured

has_files = False
all_files = os.listdir(path=".")
for all_file in all_files:
    if all_file.endswith(".csv") and all_file.find('cards_95_') > -1:
        has_files = True
if not has_files:
    sys.exit()

dbconfig = read_config(filename='halva.ini', section='SaturnFIN')
dbconn = MySQLConnection(**dbconfig)

# считаем количество одобреных заявок в базе
statistics_before = {}
sql = 'SELECT count(*) FROM saturn_fin.sovcombank_products WHERE inserted_date > %s AND status_code = 2'
cursor = dbconn.cursor()
cursor.execute(sql, (DATE_START_COUNT,))
rows = cursor.fetchall()
statistics_before['Одобренные'] = rows[0][0]
# считаем количество активированых карт в базе
sql = 'SELECT count(*) FROM saturn_fin.sovcombank_products WHERE inserted_date > %s AND status_code = 6'
cursor = dbconn.cursor()
cursor.execute(sql, (DATE_START_COUNT,))
rows = cursor.fetchall()
statistics_before['Активированные'] = rows[0][0]
# считаем количество дебетовых карт в базе
sql = 'SELECT count(*) FROM saturn_fin.sovcombank_products WHERE inserted_date > %s AND status_code = 7'
cursor = dbconn.cursor()
cursor.execute(sql, (DATE_START_COUNT,))
rows = cursor.fetchall()
statistics_before['Дебетовые'] = rows[0][0]
# считаем количество скрытых заявок в базе
cursor = dbconn.cursor()
cursor.execute('SELECT count(*) FROM saturn_fin.sovcombank_products WHERE inserted_date > %s '
               'AND status_hidden = 1', (DATE_START_COUNT,))
rows = cursor.fetchall()
statistics_before['Скрытые'] = rows[0][0]
# заявки, без статусов: одобрено, активировано(!!! отрицательный результат и отказ может стать одобреным !!!)
cursor = dbconn.cursor()
cursor.execute('SELECT count(*) FROM saturn_fin.sovcombank_products WHERE inserted_date > %s '
               'AND status_code != 2 AND status_code != 6', (DATE_START_COUNT,))
rows = cursor.fetchall()
statistics_before['НЕ одобренные и НЕ активированные'] = rows[0][0]

print('Одобренные\t', 'Активированные\t', 'Скрытые\t', 'Дебетовые\t', 'НЕ одобренные и НЕ активированные')
print(statistics_before['Одобренные'], '\t\t', statistics_before['Активированные'], '\t\t\t',
      statistics_before['Скрытые'], '\t\t', statistics_before['Дебетовые'], '\t',
      statistics_before['НЕ одобренные и НЕ активированные'])

all_files.sort()
aiib = []
cursor = dbconn.cursor()
cursor.execute('SELECT remote_id FROM saturn_fin.sovcombank_products')
rows = cursor.fetchall()
for row in rows:
    aiib.append(row[0])
all_id_in_bd = tuple(aiib)

for all_file in all_files:
    statistics_in_csv = {'Одобренные': 0, 'Активированные': 0, 'Скрытые': 0, 'Дебетовые': 0,
                         'НЕ одобренные и НЕ активированные': 0}
    if all_file.endswith(".csv") and all_file.find('cards_95_') > -1:
        print(datetime.now().strftime("%H:%M:%S"),'загружаем', all_file) # загружаем csv
        updates = []
        bids_in_xls = {}
        lines_on_file = count_lines(all_file)
        printProgressBar(0, lines_on_file, prefix='Прогресс:', suffix='Сделано', length=50)
        with open(all_file, 'r', encoding='utf-8') as input_file:
            dict_reader = csv.DictReader(input_file, delimiter='\t')
            for line in dict_reader:
                # Ищем в БД id из CAMPAIGN_CONTENT
                q = str(line['CAMPAIGN_CONTENT']).strip()
                remote_id = q[0:8] + '-' + q[8:12] + '-' + q[12:16] + '-' + q[16:20] + '-' + q[20:32]
                if remote_id not in all_id_in_bd:
                    # Если нет - ищем в БД id из CAMPAIGN_TERM
                    q = str(line['CAMPAIGN_TERM']).strip()
                    remote_id = q[0:8] + '-' + q[8:12] + '-' + q[12:16] + '-' + q[16:20] + '-' + q[20:32]
                    if remote_id not in all_id_in_bd:
                        # Если не нашли ни того ни другого id в БД - переходим на следующую строчку отчета
                        continue
                printProgressBar(dict_reader.line_num, lines_on_file, prefix='Прогресс:', suffix='Сделано', length=50)
                if str(line['applied']).strip() != '' and str(line['applied']).strip() != 'NULL':
                    # aplied => gone 0,1,2
                    gone = int(str(line['applied']).strip()) + 1
                else:
                    gone = 0
                if str(line['issued']).strip() != ''and str(line['issued']).strip() != 'NULL':
                    # issued => accepted 0,1,2
                    accepted = int(str(line['issued']).strip()) + 1
                else:
                    accepted = 0
                if str(line['contacted']).strip() != '' and str(line['contacted']).strip() != 'NULL':
                    # contacted => phoned 0,1,2
                    phoned = int(str(line['contacted']).strip()) + 1
                else:
                    phoned = 0
                if str(line['LOAN_AMOUNT']).strip() != '' and str(line['LOAN_AMOUNT']).strip() != 'NULL':
                    # LOAN_AMOUNT => loaned 0,1,2
                    if float(str(line['LOAN_AMOUNT']).strip()) > 0:
                        loaned = 2
                    else:
                        loaned = 1
                else:
                    loaned = 0
                if line.get('debit_card_issued', None):
                    # issued => accepted 0,1,2
                    if str(line['debit_card_issued']).strip() != '' and str(line['debit_card_issued']).strip() != 'NULL':
                        debit_card_issued = int(str(line['debit_card_issued']).strip()) + 1
                else:
                    debit_card_issued = 0
                if str(line['ACTIVATED']).strip() != '' and str(line['ACTIVATED']).strip() != 'NULL':
                    # issued => accepted 0,1,2
                    activated = int(str(line['ACTIVATED']).strip()) + 1
                else:
                    activated = 0
                if phoned == 2:
                    callcenter_status_code = 3
                elif phoned == 1:
                    callcenter_status_code = 2
                else:
                    callcenter_status_code = 0
                if gone == 2:
                    visit_status_code = 1
                    callcenter_status_code = 3
                elif gone == 1:
                    visit_status_code = 2
                else:
                    visit_status_code = 3
                if accepted == 1 and gone == 2 and loaned == 1:
                    status = 3
                    statistics_in_csv['НЕ одобренные и НЕ активированные'] +=1
                elif accepted == 2:
                    status = 2
                    statistics_in_csv['Одобренные'] +=1
                else:
                    status = 1
                    statistics_in_csv['НЕ одобренные и НЕ активированные'] +=1
                if debit_card_issued == 2:
                    status = 7
                    statistics_in_csv['Дебетовые'] +=1
                if activated == 2:
                    status = 6
                    statistics_in_csv['Активированные'] +=1
                bids_in_xls[remote_id] = {'remote_id' : remote_id,
                                          'status': status,
                                          'callcenter_status_code': callcenter_status_code,
                                          'visit_status_code': visit_status_code,}
                updates.append((status, callcenter_status_code, visit_status_code, remote_id))
        input_file.close()
        print(statistics_in_csv['Одобренные'], '\t\t', statistics_in_csv['Активированные'], '\t\t\t',
              statistics_in_csv['Скрытые'], '\t\t', statistics_in_csv['Дебетовые'], '\t',
              statistics_in_csv['НЕ одобренные и НЕ активированные'])

        #        has_doubles = []
#        for i, up_i in enumerate(updates):                # проверка на дубли
#            for j, up_j in enumerate(updates):
#                if i == j:
#                    continue
#                if updates[i] == updates[j]:
#                    has_doubles.append(updates[i])
#        if len(has_doubles) > 0:                                 # если были дубли - загрузка невозможна
#            print(len(has_doubles), 'дублей в файле', all_file, '- загрузка невозможна' )
#            continue


        dbconn = MySQLConnection(**dbconfig)
        cursor = dbconn.cursor()
        cursor.executemany('UPDATE saturn_fin.sovcombank_products SET status_code = %s, callcenter_status_code = %s, '
                           'visit_status_code = %s WHERE remote_id = %s', updates)
        dbconn.commit()

        try:
            os.rename(all_file, 'loaded/' + all_file)
        except OSError as e:  # errno.ENOENT = no such file or directory
            if e.errno != OSError.errno.ENOENT:
                print('Ошибка при переименовании файла', e)

# считаем количество одобреных заявок в базе
statistics_after = {}
sql = 'SELECT count(*) FROM saturn_fin.sovcombank_products WHERE inserted_date > %s AND status_code = 2'
cursor = dbconn.cursor()
cursor.execute(sql, (DATE_START_COUNT,))
rows = cursor.fetchall()
statistics_after['Одобренные'] = rows[0][0]
# считаем количество активированых карт в базе
sql = 'SELECT count(*) FROM saturn_fin.sovcombank_products WHERE inserted_date > %s AND status_code = 6'
cursor = dbconn.cursor()
cursor.execute(sql, (DATE_START_COUNT,))
rows = cursor.fetchall()
statistics_after['Активированные'] = rows[0][0]
# считаем количество скрытых заявок в базе
cursor = dbconn.cursor()
cursor.execute('SELECT count(*) FROM saturn_fin.sovcombank_products WHERE inserted_date > %s '
               'AND status_hidden = 1', (DATE_START_COUNT,))
rows = cursor.fetchall()
statistics_after['Скрытые'] = rows[0][0]
# считаем количество дебетовых карт в базе
sql = 'SELECT count(*) FROM saturn_fin.sovcombank_products WHERE inserted_date > %s AND status_code = 7'
cursor = dbconn.cursor()
cursor.execute(sql, (DATE_START_COUNT,))
rows = cursor.fetchall()
statistics_after['Дебетовые'] = rows[0][0]

# заявки, без статусов: одобрено, активировано(!!! отрицательный результат и отказ может стать одобреным !!!)
cursor = dbconn.cursor()
cursor.execute('SELECT count(*) FROM saturn_fin.sovcombank_products WHERE inserted_date > %s '
               'AND status_code != 2 AND status_code != 6', (DATE_START_COUNT,))
rows = cursor.fetchall()
statistics_after['НЕ одобренные и НЕ активированные'] = rows[0][0]

print('Стало:')
print(statistics_after['Одобренные'], '\t\t', statistics_after['Активированные'], '\t\t\t',
      statistics_after['Скрытые'], '\t\t', statistics_after['Дебетовые'], '\t',
      statistics_after['НЕ одобренные и НЕ активированные'])
print('ИЗМЕНЕНИЯ:')
print(statistics_after['Одобренные'] - statistics_before['Одобренные'], '\t\t',
      statistics_after['Активированные'] - statistics_before['Активированные'], '\t\t\t',
      statistics_after['Скрытые'] - statistics_before['Скрытые'], '\t\t',
      statistics_after['Дебетовые'] - statistics_before['Дебетовые'], '\t',
      statistics_after['НЕ одобренные и НЕ активированные'] - statistics_before['НЕ одобренные и НЕ активированные'])

# До какой даты ставить статус "Отрицательный результат"
# 15 сентября проставляем статусы на июньские и июльские заявки, 15 октября добавляется август...
date_end_otkaz = datetime.now() - timedelta(days=15) - relativedelta(months=1) + \
                 relativedelta(day=1, hour=0, minute=0, second=0, microsecond=0)
cursor = dbconn.cursor()
cursor.execute('UPDATE saturn_fin.sovcombank_products SET status_code = 5 WHERE status_code != 2 AND status_code != 3 '
               'AND status_code != 101 AND status_code != 100 AND status_code != 5 AND inserted_date < %s',
               (date_end_otkaz,) )
dbconn.commit()
dbconn.close()



