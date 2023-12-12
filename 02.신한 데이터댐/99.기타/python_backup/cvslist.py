import csv
import glob

clist = 0

path_csv = 'C:/신한 데이터댐/99.자료정리/CSV/*'

csv_list = glob.glob(path_csv)

for i in csv_list:
    csvopen = open(csv_list[clist], 'r')
    csvreader = csv.reader(csvopen)
    print(csv_list[clist])
    for line in csvreader:
        print(line)
    clist += 1
csvopen.close()