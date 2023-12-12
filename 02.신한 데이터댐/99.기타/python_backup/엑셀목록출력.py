
import glob

list = 0

path_dir = 'C:\신한 데이터댐\99.자료정리\자료\*'
 
file_list = glob.glob(path_dir)

for i in file_list:
    str = file_list[list]
    l_str = str[-3:]
    if l_str == 'xls':
        print(file_list[list][-3:])
        list += 1
    else:
        print('fail')
        list += 1
# print(file_list)
