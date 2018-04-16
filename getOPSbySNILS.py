# -*- coding: utf-8 -*-
# Робот выгружающий из СатурнОПС

import sys
import datetime
import time
import csv
from mysql.connector import MySQLConnection, Error

from lib import read_config, lenl, s_minus, s, l, filter_rus_sp, filter_rus_minus

HALVA_REGIONS = ["АЛТАЙСКИЙ КРАЙ","АМУРСКАЯ ОБЛАСТЬ","АРХАНГЕЛЬСКАЯ ОБЛАСТЬ","АСТРАХАНСКАЯ ОБЛАСТЬ","БЕЛГОРОДСКАЯ ОБЛАСТЬ",
           "БРЯНСКАЯ ОБЛАСТЬ","ВЛАДИМИРСКАЯ ОБЛАСТЬ","ВОЛГОГРАДСКАЯ ОБЛАСТЬ","ВОЛОГОДСКАЯ ОБЛАСТЬ",
           "ВОРОНЕЖСКАЯ ОБЛАСТЬ","ЕВРЕЙСКАЯ АВТОНОМНАЯ ОБЛАСТЬ","ЗАБАЙКАЛЬСКИЙ КРАЙ","ИВАНОВСКАЯ ОБЛАСТЬ",
           "ИРКУТСКАЯ ОБЛАСТЬ","КАЛИНИНГРАДСКАЯ ОБЛАСТЬ","КАЛУЖСКАЯ ОБЛАСТЬ","КАМЧАТСКИЙ КРАЙ",
           "КАРАЧАЕВО-ЧЕРКЕССКАЯ РЕСПУБЛИКА","КЕМЕРОВСКАЯ ОБЛАСТЬ","КИРОВСКАЯ ОБЛАСТЬ","КОСТРОМСКАЯ ОБЛАСТЬ",
           "КРАСНОДАРСКИЙ КРАЙ","КРАСНОЯРСКИЙ КРАЙ","КУРСКАЯ ОБЛАСТЬ","ЛИПЕЦКАЯ ОБЛАСТЬ","МАГАДАНСКАЯ ОБЛАСТЬ",
           "МОСКВА И МОСКОВСКАЯ ОБЛАСТЬ","МУРМАНСКАЯ ОБЛАСТЬ","НЕНЕЦКИЙ АВТОНОМНЫЙ ОКРУГ","НИЖЕГОРОДСКАЯ ОБЛАСТЬ",
           "НОВГОРОДСКАЯ ОБЛАСТЬ","НОВОСИБИРСКАЯ ОБЛАСТЬ","ОМСКАЯ ОБЛАСТЬ","ОРЕНБУРГСКАЯ ОБЛАСТЬ","ОРЛОВСКАЯ ОБЛАСТЬ",
           "ПЕНЗЕНСКАЯ ОБЛАСТЬ","ПЕРМСКИЙ КРАЙ","ПРИМОРСКИЙ КРАЙ","ПСКОВСКАЯ ОБЛАСТЬ","РЕСПУБЛИКА АДЫГЕЯ",
           "РЕСПУБЛИКА АЛТАЙ","РЕСПУБЛИКА БАШКОРТОСТАН","РЕСПУБЛИКА БУРЯТИЯ","РЕСПУБЛИКА КАЛМЫКИЯ","РЕСПУБЛИКА КАРЕЛИЯ",
           "РЕСПУБЛИКА КОМИ","РЕСПУБЛИКА МАРИЙ ЭЛ","РЕСПУБЛИКА МОРДОВИЯ","РЕСПУБЛИКА САХА (ЯКУТИЯ)",
           "РЕСПУБЛИКА ТАТАРСТАН","РЕСПУБЛИКА ХАКАСИЯ","РОСТОВСКАЯ ОБЛАСТЬ","РЯЗАНСКАЯ ОБЛАСТЬ","САМАРСКАЯ ОБЛАСТЬ",
           "САНКТ-ПЕТЕРБУРГ И ЛЕНИНГРАДСКАЯ ОБЛАСТЬ","САРАТОВСКАЯ ОБЛАСТЬ","САХАЛИНСКАЯ ОБЛАСТЬ","СВЕРДЛОВСКАЯ ОБЛАСТЬ",
           "СМОЛЕНСКАЯ ОБЛАСТЬ","СТАВРОПОЛЬСКИЙ КРАЙ","ТАМБОВСКАЯ ОБЛАСТЬ","ТВЕРСКАЯ ОБЛАСТЬ","ТОМСКАЯ ОБЛАСТЬ",
           "ТУЛЬСКАЯ ОБЛАСТЬ","ТЮМЕНСКАЯ ОБЛАСТЬ","УДМУРТСКАЯ РЕСПУБЛИКА","УЛЬЯНОВСКАЯ ОБЛАСТЬ","ХАБАРОВСКИЙ КРАЙ",
           "ХАНТЫ-МАНСИЙСКИЙ АВТОНОМНЫЙ ОКРУГ - ЮГРА","ЧЕЛЯБИНСКАЯ ОБЛАСТЬ","ЧУВАШСКАЯ РЕСПУБЛИКА",
           "ЧУКОТСКИЙ АВТОНОМНЫЙ ОКРУГ","ЯМАЛО-НЕНЕЦКИЙ АВТОНОМНЫЙ ОКРУГ","ЯРОСЛАВСКАЯ ОБЛАСТЬ","КУРГАНСКАЯ ОБЛАСТЬ"]

HALVA_AGENT_ID = 3090

def chuvak(is_chuvak):
    if s(is_chuvak).split(' ')[0] == 'ЧУВАШСКАЯ':
        return 'ЧУВАШСКАЯ'
    elif s(is_chuvak).split(' ')[0] == 'САХА':
        return 'САХА'
    else:
        return is_chuvak

dbconfig_ops = read_config(filename='halva.ini', section='SaturnOPS')
dbconn_ops = MySQLConnection(**dbconfig_ops)

dbconfig_fin = read_config(filename='halva.ini', section='SaturnFIN')
dbconn_fin = MySQLConnection(**dbconfig_fin)


cursor = dbconn_ops.cursor()
sql_ops = 'SELECT cl.client_id, cl.p_surname, cl.p_name, cl.p_lastname, cl.email, ca.client_phone, cl.b_date, ' \
          'cl.p_region, cl.d_region, cl.p_district, cl.p_place, cl.p_subplace, cl.d_district, cl.d_place, ' \
          'cl.d_subplace, cl.`number`, cl.phone_personal_mobile, cl.phone_relative_mobile, cl.phone_home ' \
          'FROM saturn_crm.clients AS cl LEFT JOIN saturn_crm.contracts AS co ' \
          'ON cl.client_id = co.client_id LEFT JOIN saturn_crm.callcenter AS ca ON ca.contract_id = co.id ' \
          'WHERE cl.number IN (' \
          '16735705187,03689021066,13931050336,12301277600,00622642912,00619116011,12902591753,04391906160,13210526798,14261639753,09923617203,09748945950,14796966442,12686058681,14521844751,02446600728,13122438305,02233610494,02309192723,13539475077,01682135333,12282601830,19747446229,11329116518,07334400636,02547570555,00942721840,17505810160,01203785301,05413714431,11283684251,05161138016,11770858373,11028686634,10981097166,01621066299,02852131127,11227002890,03748986916,11972073563,00737413228,09807343799,05774846611,12009780728,12328636143,13364083342,13575152259,05220328100,02446878369,01719090640,14288476293,10703364919,13156230723,13239102623,11067184629,14026515722,12895307287,13446556061,01336970437,02109581215,07551829485,15704608562,13578624188,14884825617,07915400671,01958745696,14947518000,01415004176,07605362258,10498989815,11012689200,13081963752,12068856564,10489693187,04185783271,16188576811,07416760873,07668015995,13122977436,00622610899,08138923682,14336221934,08138923783,14350096232,06514437855,07228920568,14452132025,14490816675,15250314722,12890824888,05422968968,07974320704,05656164273,05330215706,12802043614,05138700133,04212639012,07273473776,15576034375' \
          ') ORDER BY co.client_id, ca.updated_date DESC'

#          'WHERE cl.number IN (11439730145, 13864400363, 15238151546)' \
# 'AND ca.client_phone = 79241609997 ' \
# 'AND co.external_status_code = 3 ' \

    # cl.p_surname = "КУДРЯШОВ" AND
cursor.execute(sql_ops)
rows = cursor.fetchall()
last_id = ''
tuples_fin = []
tuples_fins = []
tuples_fin_upd = []
tuples_fins_upd = []
tuples_ops = []
tuples_opses = []
tuples_ops_err = []
good_zayavka = 0
bad_zayavka = 0

for i, row in enumerate(rows):
    if last_id == row[0]:
        continue
    phone = row[5]
    if l(phone) == 0:
        if l(row[16]) != 0:
            phone = row[16]
        elif l(row[17]) != 0:
            phone = row[17]
        elif l(row[18]) != 0:
            phone = row[18]
    kladr_ok = True
    last_id = row[0]
    region_ch = 'd'
    region = chuvak(row[8])
    if not region:
        region_ch = 'd'
        kladr_ok = False
        region = row[13]
    if not region:
        region = chuvak(row[7])
        region_ch = 'p'
    if not region:
        region = row[10]
        kladr_ok = False
        region_ch = 'p'
    if not region:
        region = 'РЕГИОН НЕ УКАЗАН'
    region_id = -1
    for j, halva_region in enumerate(HALVA_REGIONS):
        if halva_region.find(region) > -1:
            region_id = j
            break
    if region_id == -1:
        bad_zayavka += 1
        if region == 'РЕГИОН НЕ УКАЗАН':
            print(row[15], '"' + row[1], row[2], row[3] + '"', phone, '""', '"- Регион не указан"')
        elif not kladr_ok:
            print(row[15], '"' + row[1], row[2], row[3] + '"', phone, '"' + region + '"', '"- Пересохраните КЛАДР"')
        else:
            print(row[15], '"' + row[1], row[2], row[3] + '"', phone, '"' + region + '"', '"- Регион не участвует в программе"')
        tuples_ops_err.append((row[0],))
        continue

    if region_ch == 'd':
        town = s(row[12]).strip() + ' ' + s(row[13]).strip() + ' ' + s(row[14]).strip()
    else:
        town = s(row[9]).strip() + ' ' + s(row[10]).strip() + ' ' + s(row[11]).strip()
    town = town.replace('  ',' ').replace('  ',' ').replace('  ',' ')

    if town.strip() == '':
        bad_zayavka += 1
        print(row[15], '"' + row[1], row[2], row[3] +'"', phone, '"' + region +'"', '"- Город не указан, пересохраните КЛАДР"')
        tuples_ops_err.append((row[0],))
        continue

    cursor_chk = dbconn_fin.cursor()
    cursor_chk.execute('SELECT remote_id, phone FROM sovcombank_products WHERE phone = %s', (phone,))
    rows_chk = cursor_chk.fetchall()
    if len(rows_chk) > 0:
        print(row[15], '"' + row[1], row[2], row[3] + '"', phone, '"' + region + '"', '"- Такой телефон уже есть в БД"')
        continue

    has_in_db = False
    cursor_chk = dbconn_fin.cursor()
    cursor_chk.execute('SELECT remote_id, phone FROM sovcombank_products WHERE remote_id = %s', (row[0],))
    rows_chk = cursor_chk.fetchall()
    if len(rows_chk) > 0:
        has_in_db = True
        print(row[15], '"' + row[1], row[2], row[3] + '"', phone, '"' + region + '"', '"- Такой ID уже есть в БД"')

    if has_in_db:
        tuples_fin_upd.append((phone, row[0]))
    else:
        tuples_fin.append((row[0], row[1], row[2], row[3], row[4], phone, row[6], HALVA_REGIONS[region_id],
             town, datetime.datetime.now(), HALVA_AGENT_ID, 0))

    tuples_ops.append((row[0],))
    good_zayavka += 1
    if len(tuples_fin) > 999:
        tuples_fins.append(tuples_fin)
        tuples_fin = []
    if len(tuples_fin_upd) > 999:
        tuples_fins_upd.append(tuples_fin_upd)
        tuples_fin_upd = []
    if len(tuples_ops) > 999:
        tuples_opses.append(tuples_ops)
        tuples_ops = []
tuples_fins.append(tuples_fin)
tuples_fins_upd.append(tuples_fin_upd)
tuples_opses.append(tuples_ops)

print('\nОбработано: ', bad_zayavka + good_zayavka,'   загружено: ', good_zayavka, '   ошибки: ', bad_zayavka)

if len(tuples_fin) > 0:
    for i, t_fin in enumerate(tuples_fins):
        cursor_fin = dbconn_fin.cursor()
        sql_fin = 'INSERT INTO saturn_fin.sovcombank_products(remote_id, first_name, last_name, middle_name,' \
                  ' e_mail, phone, birth_date, fact_region_name, fact_city_name, inserted_date, inserted_code,' \
                  ' status_code) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        cursor_fin.executemany(sql_fin, t_fin)
#        cursor_ops = dbconn_ops.cursor()                        # Статус "Загружено" (Бумага принята)
#        sql_ops = 'UPDATE saturn_crm.contracts SET exchanged = 1' \
#                  ' WHERE client_id = %s'
##                  ', status_secure_code = 0, status_code = 1, status_callcenter_code = 1'
#        cursor_ops.executemany(sql_ops, tuples_opses[i])
        dbconn_fin.commit()
#        dbconn_ops.commit()
#if len(tuples_ops_err) > 0:
#    cursor_ops = dbconn_ops.cursor()                        # Статус "Ошибка"
#    sql_ops = 'UPDATE saturn_crm.contracts SET exchanged = 0 WHERE client_id = %s'
#    cursor_ops.executemany(sql_ops, tuples_ops_err)
#    dbconn_ops.commit()

if len(tuples_fins_upd) > 0:
    for i, t_fin_upd in enumerate(tuples_fins_upd):
        cursor_fin = dbconn_fin.cursor()
        sql_fin = 'UPDATE saturn_fin.sovcombank_products SET phone = %s WHERE remote_id = %s'
        cursor_fin.executemany(sql_fin, t_fin_upd)
        dbconn_fin.commit()


dbconn_fin.close()
dbconn_ops.close()



