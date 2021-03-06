# -*- coding: utf-8 -*-
# Робот выгружающий из СатурнОПС

import sys
import datetime
import time
import csv
from mysql.connector import MySQLConnection, Error

from lib import read_config, s, l, fine_snils

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

HALVA_AGENT_ID = 2630

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
          'WHERE cl.client_id IN (' \
          '"cb53bd81-64d3-11e8-8416-5254004b76e6","c86e16cd-6760-11e8-8416-5254004b76e6","6efebf82-67f3-11e8-8416-5254004b76e6","6d44cd7b-6801-11e8-8416-5254004b76e6","8ccd546b-68e8-11e8-8416-5254004b76e6","4470e5c7-68f5-11e8-8416-5254004b76e6","901eca83-69b7-11e8-8416-5254004b76e6","63d6b80e-69b8-11e8-8416-5254004b76e6","e561f96f-6a1b-11e8-8416-5254004b76e6","d4467a45-6a45-11e8-8416-5254004b76e6","90c350ab-6a48-11e8-8416-5254004b76e6","e092286f-6a4a-11e8-8416-5254004b76e6","daee6710-6a8e-11e8-8416-5254004b76e6","16478926-6b3d-11e8-8416-5254004b76e6","98e484a6-6d32-11e8-8416-5254004b76e6","61bf00f7-6d7c-11e8-8416-5254004b76e6","ddb41f57-6d7d-11e8-8416-5254004b76e6","0141050d-6d87-11e8-8416-5254004b76e6","a90d232c-6e3b-11e8-8416-5254004b76e6","deb6c5f2-6e3c-11e8-8416-5254004b76e6","cd342983-6e45-11e8-8416-5254004b76e6","b42f268e-78ad-11e8-828b-5254004b76e6","e79a2c9c-7972-11e8-828b-5254004b76e6","10fb9e7f-7a08-11e8-828b-5254004b76e6"' \
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
            print('"' + fine_snils(row[15]) + '" "' + row[1], row[2], row[3] + '"', phone, '""', '"- Регион не указан"')
        elif not kladr_ok:
            print('"' + fine_snils(row[15]) + '" "' + row[1], row[2], row[3] + '"', phone, '"' + region + '"', '"- Пересохраните КЛАДР"')
        else:
            print('"' + fine_snils(row[15]) + '" "' + row[1], row[2], row[3] + '"', phone, '"' + region + '"', '"- Регион не участвует в программе"')
        tuples_ops_err.append((row[0],))
        continue

    if region_ch == 'd':
        town = s(row[12]).strip() + ' ' + s(row[13]).strip() + ' ' + s(row[14]).strip()
    else:
        town = s(row[9]).strip() + ' ' + s(row[10]).strip() + ' ' + s(row[11]).strip()
    town = town.replace('  ',' ').replace('  ',' ').replace('  ',' ')

    if town.strip() == '':
        bad_zayavka += 1
        print('"' + fine_snils(row[15]) + '" "' + row[1], row[2], row[3] +'"', phone, '"' + region +'"', '"- Город не указан, пересохраните КЛАДР"')
        tuples_ops_err.append((row[0],))
        continue

    cursor_chk = dbconn_fin.cursor()
    cursor_chk.execute('SELECT remote_id, phone FROM sovcombank_products WHERE phone = %s', (phone,))
    rows_chk = cursor_chk.fetchall()
    if len(rows_chk) > 0:
        print('"' + fine_snils(row[15]) + '" "' + row[1], row[2], row[3] + '"', phone, '"' + region + '"', '"- Такой телефон уже есть в БД"')
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
             town, datetime.datetime.now(), HALVA_AGENT_ID, 0, 1))

    tuples_ops.append((row[0],))
    good_zayavka += 1
    if len(tuples_fin) > 99:
        tuples_fins.append(tuples_fin)
        tuples_fin = []
    if len(tuples_fin_upd) > 99:
        tuples_fins_upd.append(tuples_fin_upd)
        tuples_fin_upd = []
    if len(tuples_ops) > 99:
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
                  ' status_code, enable_exchange) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
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



