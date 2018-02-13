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

# Партнеры, по которым не надо обрезать !!! MIN - 1 шт !!!
OUR_PARTNERS = [45,191,234]
# Коэффициент обрезки
K_HIDDEN = 0.1
# Дата начала обрезки
DATE_HIDE = '2018-01-27'
# До какой даты ставить статус "Отрицательный результат"
DATE_END_OTKAZ = '2017-12-31'


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

# агенты, которые не участвут в срезе
sql = 'SELECT code from saturn_fin.offices_staff WHERE partner_code = %s'
partners = (OUR_PARTNERS[0],)
our_agents = []
for i, partner in enumerate(OUR_PARTNERS):
    if i == 0:
        continue
    sql += ' OR partner_code = %s'
    partners += (OUR_PARTNERS[i],)
dbconn = MySQLConnection(**dbconfig)
cursor = dbconn.cursor()
cursor.execute(sql, partners)
rows = cursor.fetchall()
for row in rows:
    our_agents.append(row[0])

all_files.sort()
for all_file in all_files:
    if all_file.endswith(".csv") and all_file.find('cards_95_') > -1:
        dbconn = MySQLConnection(**dbconfig)
        # считаем количество одобреных заявок в базе, кроме договоров агентов, которые не участвут в срезе
        sql = 'SELECT count(*) FROM saturn_fin.sovcombank_products WHERE inserted_date > %s AND status_code = 2' \
              ' AND (inserted_code NOT IN (SELECT code from saturn_fin.offices_staff WHERE partner_code = %s'
        partners = (DATE_HIDE, OUR_PARTNERS[0])
        for i, partner in enumerate(OUR_PARTNERS):
            if i == 0:
                continue
            sql += ' OR partner_code = %s '
            partners += (OUR_PARTNERS[i],)
        sql += '))'
        cursor = dbconn.cursor()
        cursor.execute(sql, partners)
        rows = cursor.fetchall()
        odobr_in_db = rows[0][0]

        # считаем количество скрытых заявок в базе
        cursor = dbconn.cursor()
        cursor.execute('SELECT count(*) FROM saturn_fin.sovcombank_products WHERE status_hidden = 1')
        rows = cursor.fetchall()
        hidden_in_db = rows[0][0]

        # заявки, без статусов: одобрено, отказ, отрицательный результат
        cursor = dbconn.cursor()
        cursor.execute(
            'SELECT remote_id, inserted_code from saturn_fin.sovcombank_products WHERE status_code != 5 '
            'AND status_code != 3 AND status_code != 2')
        bids_in_db = cursor.fetchall()
        dbconn.close()

        print(datetime.now().strftime("%H:%M:%S"),'загружаем', all_file) # загружаем csv
        updates = []
        bids_in_xls = {}
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
                bids_in_xls[remote_id] = {'remote_id' : remote_id, 'status': status, 'callcenter_status_code': callcenter_status_code,
                                       'visit_status_code': visit_status_code}
        input_file.close()

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

        bid_in_xls = {}                                     # Сколько есть БД из одобренных
        bids_in_xls_db = []
        bids_in_db_agents = []
        odobr_in_xls = 0
        for i, bid_in_db in enumerate(bids_in_db):
            try:
#                print(i, bid_in_db[0])
                bid_in_xls = bids_in_xls[bid_in_db[0]]
                bids_in_xls_db.append(bid_in_xls)
                bids_in_db_agents.append(bid_in_db[1])
            except KeyError:
                continue
            if bid_in_db[1] in our_agents:
                q = 0
            else:
                if bid_in_xls['status'] == 2 :
                    odobr_in_xls += 1

        hidden_in_xls = round((odobr_in_db + odobr_in_xls) * K_HIDDEN - hidden_in_db)
        if hidden_in_xls > odobr_in_xls:
            hidden_in_xls = odobr_in_xls
        print('В файле', all_file, 'из', odobr_in_db + odobr_in_xls, 'одобренных будет скрыто', hidden_in_xls)

        statuses = []
        for i, bid_in_xls in enumerate(bids_in_xls_db):
            if bids_in_db_agents[i] in our_agents:
                statuses.append((bid_in_xls['status'], bid_in_xls['callcenter_status_code'],
                                 bid_in_xls['visit_status_code'], 0, bid_in_xls['remote_id']))
            else:
                if bid_in_xls['status'] == 2 and hidden_in_xls > 0:
                    hidden_in_xls -= 1
                    statuses.append((bid_in_xls['status'], bid_in_xls['callcenter_status_code'],
                                     bid_in_xls['visit_status_code'], 1, bid_in_xls['remote_id']))
                else:
                    statuses.append((bid_in_xls['status'], bid_in_xls['callcenter_status_code'],
                                     bid_in_xls['visit_status_code'], 0, bid_in_xls['remote_id']))

        gs =  0
        h_i = []
        for i, st in enumerate(statuses):
            if st[3] == 1:
                gs +=1
                h_i.append(i)

        dbconn = MySQLConnection(**dbconfig)
        cursor = dbconn.cursor()
#        cursor.execute('UPDATE saturn_fin.sovcombank_products SET status_code = %s, callcenter_status_code = %s, '
#                           'visit_status_code = %s, status_hidden = %s WHERE remote_id = %s', statuses[h_i[0]])
        cursor.executemany('UPDATE saturn_fin.sovcombank_products SET status_code = %s, callcenter_status_code = %s, '
                           'visit_status_code = %s, status_hidden = %s WHERE remote_id = %s', statuses)
        dbconn.commit()

        try:
            os.rename(all_file, 'loaded/' + all_file)
        except OSError as e:  # errno.ENOENT = no such file or directory
            if e.errno != OSError.errno.ENOENT:
                print('Ошибка при переименовании файла', e)

cursor = dbconn.cursor()
cursor.execute('UPDATE saturn_fin.sovcombank_products SET status_code = 5 WHERE status_code != 2 AND status_code != 3 '
               'AND status_code != 101 AND status_code != 100 AND status_code != 5 AND inserted_date < %s',
               (DATE_END_OTKAZ,) )
dbconn.commit()

dbconn.close()



