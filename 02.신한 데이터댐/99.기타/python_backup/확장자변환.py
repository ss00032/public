import os.path
import glob

path_file = 'C:/신한 데이터댐/99.자료정리/자료/*.xls'
files = glob.glob(path_file)

for x in files:
    if not os.path.isdir(x):
        filename = os.path.splitext(x)
        try:
            os.rename(x,filename[0] + '.')
        except:
            pass
