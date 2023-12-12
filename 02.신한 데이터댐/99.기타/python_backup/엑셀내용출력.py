import glob
import openpyxl
import csv
import xlrd
import pandas as pd

list = 0

path_dir = 'C:/신한 데이터댐/99.자료정리/자료/*'

file_list = glob.glob(path_dir)

for i in file_list:
    print(file_list[list])
    str = file_list[list]
    l_str = str[-3:]
    if file_list[list].endswith('xlsx'):
        load_wb = openpyxl.load_workbook(file_list[list])
        load_ws = load_wb.active
        all_values = []
        for row in load_ws.rows:
            row_value = []
            for cell in row:
                row_value.append(cell.value)
                print(cell.value)
            all_values.append(row_value)
        print(all_values)

#        for row_data in load_ws.iter_rows(min_row=1):
#            for cell in row_data:
#                print('[', cell.value, ']')
#        load_wb.close()
        list += 1
    elif l_str == 'csv':
        csvopen = open(file_list[list],'r')
        csvreader = csv.reader(csvopen)
        for line in csvreader:
            print(line)
        csvopen.close()
        list += 1
    else:
        list += 1

