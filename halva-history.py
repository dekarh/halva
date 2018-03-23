# -*- coding: utf-8 -*-
# Поиск мухлежа в истории статусов в Халве

import sys
from _datetime import datetime
import time
import os
import zipfile
import csv
from openpyxl import Workbook

from lib import read_config, lenl, s_minus, s, l, filter_rus_sp, filter_rus_minus

# !!!! Обязательно первым значением "Имя файла"
CONTROLLEDS = [
'Имя файла',
'4db004afec5211e7897e5254004b76e6',
'4d5c62f2ec4f11e7897e5254004b76e6'
]

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

all_files.sort()
wb = Workbook(write_only=True)
ws = wb.create_sheet('Лист1')
ws.append(CONTROLLEDS)  # добавляем первую строку xlsx

for all_file in all_files:
    if all_file.endswith(".csv") and all_file.find('cards_95_') > -1:
        print(datetime.now().strftime("%H:%M:%S"),'Проверяем', all_file) # загружаем csv
        statuses = {}
        with open(all_file, 'r', encoding='utf-8') as input_file:
            dict_reader = csv.DictReader(input_file, delimiter='\t')
            for line in dict_reader:
                cc_id = str(line['CAMPAIGN_CONTENT']).strip()
#                cc_id = q[0:8] + '-' + q[8:12] + '-' + q[12:16] + '-' + q[16:20] + '-' + q[20:32]
                for i, controlled in enumerate(CONTROLLEDS):
                    if i == 0:
                        continue
                    if cc_id == controlled:
                        if str(line['applied']).strip() != '':
                            gone = int(str(line['applied']).strip()) + 1
                        else:
                            gone = 0
                        if str(line['issued']).strip() != '':
                            accepted = int(str(line['issued']).strip()) + 1
                        else:
                            accepted = 0
                        if str(line['LOAN_AMOUNT']).strip() != '':
                            if float(str(line['LOAN_AMOUNT']).strip()) > 0:
                                loaned = 2
                            else:
                                loaned = 1
                        else:
                            loaned = 0
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
                        statuses[controlled] = status
        input_file.close()
        excel_line = []
        for i,cd in enumerate(CONTROLLEDS):
            if i == 0:
                excel_line.append(all_file)
            else:
                try:
                    excel_line.append(statuses[CONTROLLEDS[i]])
                except KeyError:
                    excel_line.append('-')
        ws.append(excel_line)

wb.save('halva-muhleg.xlsx')



