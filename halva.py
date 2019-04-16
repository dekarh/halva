# -*- coding: utf-8 -*-
# Робот, отмечающий загруженные


import sys
from _datetime import datetime
import time
import os
import zipfile
import csv
from mysql.connector import MySQLConnection, Error

from lib import read_config, lenl, s_minus, s, l, filter_rus_sp, filter_rus_minus

# Статистика с этой даты
DATE_START_COUNT = datetime(2018,8,1)
# До какой даты ставить статус "Отрицательный результат"
DATE_END_OTKAZ = '2018-03-31'

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

print('Одобренные\t', 'Активированные\t', 'Скрытые\t', 'НЕ одобренные и НЕ активированные')
print(statistics_before['Одобренные'], '\t\t', statistics_before['Активированные'], '\t\t\t\t',
      statistics_before['Скрытые'], '\t\t', statistics_before['НЕ одобренные и НЕ активированные'])

all_files.sort()
for all_file in all_files:
    statistics_in_csv = {}
    statistics_in_csv['Скрытые'] = 0
    if all_file.endswith(".csv") and all_file.find('cards_95_') > -1:

        print(datetime.now().strftime("%H:%M:%S"),'загружаем', all_file) # загружаем csv
        updates = []
        bids_in_xls = {}
        with open(all_file, 'r', encoding='utf-8') as input_file:
            dict_reader = csv.DictReader(input_file, delimiter='\t')
            for line in dict_reader:
                q = str(line['CAMPAIGN_CONTENT']).strip()
                remote_id = q[0:8] + '-' + q[8:12] + '-' + q[12:16] + '-' + q[16:20] + '-' + q[20:32]
                if str(line['applied']).strip() != '' and str(line['applied']).strip() != 'NULL':
                    gone = int(str(line['applied']).strip()) + 1
                else:
                    gone = 0
                if str(line['issued']).strip() != ''and str(line['issued']).strip() != 'NULL':
                    accepted = int(str(line['issued']).strip()) + 1
                else:
                    accepted = 0
                if str(line['contacted']).strip() != '' and str(line['contacted']).strip() != 'NULL':
                    phoned = int(str(line['contacted']).strip()) + 1
                else:
                    phoned = 0
                if str(line['LOAN_AMOUNT']).strip() != '' and str(line['LOAN_AMOUNT']).strip() != 'NULL':
                    if float(str(line['LOAN_AMOUNT']).strip()) > 0:
                        loaned = 2
                    else:
                        loaned = 1
                else:
                    loaned = 0
                if str(line['ACTIVATED']).strip() != '' and str(line['ACTIVATED']).strip() != 'NULL':
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
                    if statistics_in_csv.get('НЕ одобренные и НЕ активированные'):
                        statistics_in_csv['НЕ одобренные и НЕ активированные'] +=1
                    else:
                        statistics_in_csv['НЕ одобренные и НЕ активированные'] = 1
                elif accepted == 2:
                    status = 2
                    if statistics_in_csv.get('Одобренные'):
                        statistics_in_csv['Одобренные'] +=1
                    else:
                        statistics_in_csv['Одобренные'] = 1
                else:
                    status = 1
                    if statistics_in_csv.get('НЕ одобренные и НЕ активированные'):
                        statistics_in_csv['НЕ одобренные и НЕ активированные'] +=1
                    else:
                        statistics_in_csv['НЕ одобренные и НЕ активированные'] = 1
                if activated == 2:
                    status = 6
                    if statistics_in_csv.get('Активированные'):
                        statistics_in_csv['Активированные'] +=1
                    else:
                        statistics_in_csv['Активированные'] = 1
                bids_in_xls[remote_id] = {'remote_id' : remote_id,
                                          'status': status,
                                          'callcenter_status_code': callcenter_status_code,
                                          'visit_status_code': visit_status_code,}
                updates.append((status, callcenter_status_code, visit_status_code, remote_id))
        input_file.close()
        print(statistics_in_csv['Одобренные'], '\t\t', statistics_in_csv['Активированные'], '\t\t\t\t',
              statistics_in_csv['Скрытые'], '\t\t', statistics_in_csv['НЕ одобренные и НЕ активированные'])

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
# заявки, без статусов: одобрено, активировано(!!! отрицательный результат и отказ может стать одобреным !!!)
cursor = dbconn.cursor()
cursor.execute('SELECT count(*) FROM saturn_fin.sovcombank_products WHERE inserted_date > %s '
               'AND status_code != 2 AND status_code != 6', (DATE_START_COUNT,))
rows = cursor.fetchall()
statistics_after['НЕ одобренные и НЕ активированные'] = rows[0][0]

print('Стало:')
print(statistics_after['Одобренные'], '\t\t', statistics_after['Активированные'], '\t\t\t\t',
      statistics_after['Скрытые'], '\t\t', statistics_after['НЕ одобренные и НЕ активированные'])
print('ИЗМЕНЕНИЯ:')
print(statistics_after['Одобренные'] - statistics_before['Одобренные'], '\t\t',
      statistics_after['Активированные'] - statistics_before['Активированные'], '\t\t\t\t',
      statistics_after['Скрытые'] - statistics_before['Скрытые'], '\t\t',
      statistics_after['НЕ одобренные и НЕ активированные'] - statistics_before['НЕ одобренные и НЕ активированные'])


cursor = dbconn.cursor()
cursor.execute('UPDATE saturn_fin.sovcombank_products SET status_code = 5 WHERE status_code != 2 AND status_code != 3 '
               'AND status_code != 101 AND status_code != 100 AND status_code != 5 AND inserted_date < %s',
               (DATE_END_OTKAZ,) )
dbconn.commit()

dbconn.close()



