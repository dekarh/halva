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
          'cl.d_subplace, cl.`number` FROM saturn_crm.clients AS cl LEFT JOIN saturn_crm.contracts AS co ' \
          'ON cl.client_id = co.client_id LEFT JOIN saturn_crm.callcenter AS ca ON ca.contract_id = co.id ' \
          'WHERE cl.subdomain_id = 2 AND co.status_secure_code = 0 AND co.inserted_code = 9375 ' \
          'AND co.status_code = 1 AND co.status_callcenter_code = 1 AND co.exchanged = 0 AND cl.client_id IS NOT NULL '\
          'ORDER BY co.client_id, ca.updated_date DESC'

# 'AND ca.client_phone = 79241609997 ' \
# 'AND co.external_status_code = 3 ' \

    # cl.p_surname = "КУДРЯШОВ" AND
cursor.execute(sql_ops)
rows = cursor.fetchall()
last_id = ''
tuples_fin = []
tuples_fins = []
tuples_ops = []
tuples_opses = []
tuples_ops_err = []
good_zayavka = 0
bad_zayavka = 0

for i, row in enumerate(rows):
    if last_id == row[0]:
        continue
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
            print(row[15], '"' + row[1], row[2], row[3] + '"', row[5], '""', '"- Регион не указан"')
        elif not kladr_ok:
            print(row[15], '"' + row[1], row[2], row[3] + '"', row[5], '"' + region + '"', '"- Пересохраните КЛАДР"')
        else:
            print(row[15], '"' + row[1], row[2], row[3] + '"', row[5], '"' + region + '"', '"- Регион не участвует в программе"')
        tuples_ops_err.append((row[0],))
        continue

    if region_ch == 'd':
        town = s(row[12]).strip() + ' ' + s(row[13]).strip() + ' ' + s(row[14]).strip()
    else:
        town = s(row[9]).strip() + ' ' + s(row[10]).strip() + ' ' + s(row[11]).strip()
    town = town.replace('  ',' ').replace('  ',' ').replace('  ',' ')

    if town.strip() == '':
        bad_zayavka += 1
        print(row[15], '"' + row[1], row[2], row[3] +'"', row[5], '"' + region +'"', '"- Город не указан, пересохраните КЛАДР"')
        tuples_ops_err.append((row[0],))
        continue

    tuples_fin.append((row[0], row[1], row[2], row[3], row[4], row[5], row[6], HALVA_REGIONS[region_id],
                 town, datetime.datetime.now(), 3090, 0))
    tuples_ops.append((row[0],))
    good_zayavka += 1
    if len(tuples_fin) > 999:
        tuples_fins.append(tuples_fin)
        tuples_fin = []
    if len(tuples_ops) > 999:
        tuples_opses.append(tuples_ops)
        tuples_ops = []
tuples_fins.append(tuples_fin)
tuples_opses.append(tuples_ops)

print('\nОбработано: ', bad_zayavka + good_zayavka,'   загружено: ', good_zayavka, '   ошибки: ', bad_zayavka)

dbconn_fin.close()
dbconn_ops.close()



