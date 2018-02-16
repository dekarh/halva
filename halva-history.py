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

CONTROLLEDS = [
'Имя файла',
'6e9e6978fc4811e7896b5254004b76e6',
'0e633402fcbb11e7896b5254004b76e6',
'57349a36003a11e8896b5254004b76e6',
'374ac8df003f11e8896b5254004b76e6',
'ce465a6d005011e8896b5254004b76e6',
'db85314d00d911e8896b5254004b76e6',
'a8a87b5000e511e8896b5254004b76e6',
'8d4893c200e811e8896b5254004b76e6',
'8a45529b026d11e88e335254004b76e6',
'6fc45d0f032a11e88e335254004b76e6',
'ca7eed8f033811e88e335254004b76e6',
'bdd75558035611e88e335254004b76e6',
'059e08e7048911e88e335254004b76e6',
'880ce05804be11e88e335254004b76e6',
'6e05a36004c711e88e335254004b76e6',
'895a519804de11e88e335254004b76e6',
'24cc72dc04e111e88e335254004b76e6',
'e350ebdd04e211e88e335254004b76e6',
'a330609e057811e88e335254004b76e6',
'307acd5e058011e88e335254004b76e6',
'e3a8f962058d11e88e335254004b76e6',
'8b72a932059111e88e335254004b76e6',
'ae07d53a059611e88e335254004b76e6',
'ddec74fa059811e88e335254004b76e6',
'182e5b0605a511e88e335254004b76e6',
'cf9ea79d05a611e88e335254004b76e6',
'ce87705a05aa11e88e335254004b76e6',
'2899bfc005b511e88e335254004b76e6',
'76b1e65305be11e88e335254004b76e6',
'37e30f5305d711e88e335254004b76e6',
'78bf6f7f05dc11e88e335254004b76e6',
'da269d98064111e88e335254004b76e6',
'bc58807f064511e88e335254004b76e6',
'a5afcb4a064f11e88e335254004b76e6',
'82e6ccaa065111e88e335254004b76e6',
'fe007e27065611e88e335254004b76e6',
'ba27a522065711e88e335254004b76e6',
'6fcbc36b065911e88e335254004b76e6',
'309894c0065c11e88e335254004b76e6',
'3adacb72065e11e88e335254004b76e6',
'1277579d066511e88e335254004b76e6',
'b9f4993d066911e88e335254004b76e6',
'78b8ebfa066a11e88e335254004b76e6',
'15eccd60067611e88e335254004b76e6',
'd485dcf4067711e88e335254004b76e6',
'31f000b1067911e88e335254004b76e6',
'261b3426068711e88e335254004b76e6',
'38eda816068811e88e335254004b76e6',
'7714de2606a111e88e335254004b76e6',
'c392ad5606a211e88e335254004b76e6'
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



