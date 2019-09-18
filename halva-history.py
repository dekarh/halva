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
'1532f2b3bf6911e9be76005056b95c35', '1ba1ab56c7ee11e9b258005056b95c35'
]

has_files = False
all_files = os.listdir(path='loaded')
for all_file in all_files:
    if all_file.endswith('.csv') and all_file.find('cards_95_') > -1:
        has_files = True
if not has_files:
    sys.exit()

all_files.sort()
wb = Workbook(write_only=True)
ws = wb.create_sheet('История получения файлов по продукту Совкомбанк-Халва')
ws.append(CONTROLLEDS)  # добавляем первую строку xlsx

for all_file in all_files:
    if all_file.endswith(".csv") and all_file.find('cards_95_') > -1:
        print(datetime.now().strftime("%H:%M:%S"),'Проверяем', all_file) # загружаем csv
        statuses = {}
        with open('loaded/' + all_file, 'r', encoding='utf-8') as input_file:
            dict_reader = csv.DictReader(input_file, delimiter='\t')
            for line in dict_reader:
                cc_id = str(line['CAMPAIGN_CONTENT']).strip()
                ct_id = str(line.get('CAMPAIGN_TERM','')).strip()
#                cc_id = q[0:8] + '-' + q[8:12] + '-' + q[12:16] + '-' + q[16:20] + '-' + q[20:32]
                for i, controlled in enumerate(CONTROLLEDS):
                    if i == 0:
                        continue
                    if cc_id == controlled or ct_id == controlled:
                        if str(line['applied']).strip() != '' and str(line['applied']).strip() != 'NULL':
                            # aplied => gone 0,1,2
                            gone = int(str(line['applied']).strip()) + 1
                        else:
                            gone = 0
                        if str(line['issued']).strip() != '' and str(line['issued']).strip() != 'NULL':
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
                            if str(line['debit_card_issued']).strip() != '' and str(
                                    line['debit_card_issued']).strip() != 'NULL':
                                debit_card_issued = int(str(line['debit_card_issued']).strip()) + 1
                        else:
                            debit_card_issued = 0
                        if str(line['ACTIVATED']).strip() != '' and str(line['ACTIVATED']).strip() != 'NULL':
                            # issued => accepted 0,1,2
                            activated = int(str(line['ACTIVATED']).strip()) + 1
                        else:
                            activated = 0
                        if phoned == 2:
                            callcenter_status_code = 'Дозвонились'
                        elif phoned == 1:
                            callcenter_status_code = 'Не дозвонились'
                        else:
                            callcenter_status_code = 'нетИнфОЗвонке'
                        if gone == 2:
                            visit_status_code = 'Приходил в Банк'
                            callcenter_status_code = 'нетИнфОЗвонке'
                        elif gone == 1:
                            visit_status_code = 'нетИнфОВизите'
                        else:
                            visit_status_code = 'нетИнфОВизите'
                        if accepted == 1 and gone == 2 and loaned == 1:
                            status = 'Отказ Банка'
                        elif accepted == 2:
                            status = 'Получена'
                        else:
                            status = 'В обработке'
                        if debit_card_issued == 2:
                            status = 'Дебетовая карта'
                        if activated == 2:
                            status = 'Активирована'
                        statuses[controlled] = status + '=' + callcenter_status_code + '=' + visit_status_code
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

wb.save('halva-history.xlsx')



