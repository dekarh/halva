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

dbconfig = read_config(filename='halva.ini', section='SaturnFIN')
dbconn = MySQLConnection(**dbconfig)

all_files = os.listdir(path=".")
all_files.sort()
for i, all_file in enumerate(all_files):
    if all_file.endswith(".csv") and all_file.find('cards_95_') > -1:
        print(datetime.now().strftime("%H:%M:%S"),'загружаем', all_file)
        updates = []
        statuses = []
        with open(all_file, 'r', encoding='utf-8') as input_file:
            dict_reader = csv.DictReader(input_file, delimiter='\t')
            for line in dict_reader:
                q = str(line['CAMPAIGN_CONTENT']).strip()
                remote_id = q[0:8] + '-' + q[8:12] + '-' + q[12:16] + '-' + q[16:20] + '-' + q[20:32]
                if str(line['applied']).strip() != '':
                    gone = int(str(line['applied']).strip()) + 1
                else:
                    gone = 0
                if str(line['issued']).strip() != '':
                    accepted = int(str(line['issued']).strip()) + 1
                else:
                    accepted = 0
                if str(line['contacted']).strip() != '':
                    phoned = int(str(line['contacted']).strip()) + 1
                else:
                    phoned = 0
                if str(line['LOAN_AMOUNT']).strip() != '':
                    if float(str(line['LOAN_AMOUNT']).strip()) > 0:
                        loaned = 2
                    else:
                        loaned = 1
                else:
                    loaned = 0
                updates.append(remote_id)
#                updates.append([remote_id, gone, accepted, phoned, str(line['PARTNER_EXTERNAL_ID']),
#                                str(line['ID_POTENTIAL_CUSTOMER']), str(line['DT_APPLICATION_START'])])
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
                elif accepted == 2:
                    status = 2
                else:
                    status = 1
                statuses.append((status, callcenter_status_code, visit_status_code, remote_id))
        input_file.close()

        has_doubles = []
        for i, up_i in enumerate(updates):                # проверка на дубли
            for j, up_j in enumerate(updates):
                if i == j:
                    continue
                if updates[i] == updates[j]:
                    has_doubles.append(updates[i])
        #        if updates[i][0] == updates[j][0]:
        #            print(updates[i][1], updates[i][2], updates[i][3], ' == ', updates[j][1], updates[j][2], updates[j][3])
        #            if updates[i][4] != updates[j][4] or updates[i][5] != updates[j][5] or updates[i][6] != updates[j][6]:
        #                print(updates[i][4], updates[i][5], updates[i][6], ' == ',updates[j][4], updates[j][5], updates[j][6])

        try:
            os.rename(all_file, 'loaded/' + all_file)
        except OSError as e:          # errno.ENOENT = no such file or directory
            if e.errno != OSError.errno.ENOENT:
                print('Ошибка при переименовании файла', e)

        if len(has_doubles) > 2:                                 # если были дубли - загрузка невозможна
            print(len(has_doubles), 'дублей в файле', all_file, '- загрузка невозможна' )
            continue

        cursor = dbconn.cursor()
        cursor.executemany('UPDATE sovcombank_products SET status_code = %s, callcenter_status_code = %s, '
                           'visit_status_code = %s WHERE remote_id = %s', statuses)
        dbconn.commit()

dbconn.close()



