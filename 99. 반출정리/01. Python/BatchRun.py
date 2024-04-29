import psycopg2
import os
import datetime
import configparser
import sys
import logging
import decrypto
import importlib

#logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(' ')
logger.setLevel(level=logging.INFO)
# base_dt = arg[2] # 2번째 파라미터로 날짜 받아오기
# 공통 변수 셋팅
totlog = ""
step_num = 0
juje = sys.argv[1][0:3]

# 프로그램 시작
start_time = datetime.datetime.now()  # 시작 시간 저장
logger.warning("=======================================================")
logger.warning("■ 프로그램 시작     : {}".format(start_time.strftime("%Y%m%d%H%M%S")))
totlog += "=======================================================\n■ 프로그램 시작     : {}".format(start_time.strftime("%Y%m%d%H%M%S")) + "\n"

# Parameter 셋팅
if(len(sys.argv) < 3):
    logger.warning("■ ERROR.")
    logger.warning("■ 입력된 Parameter가 정확하지 않습니다.")
    logger.warning("■ USAGE) python BatchRun.py Jobname DATE")
    logger.warning("■    EX) python BatchRun.py UCD_WCDIFS01TM_TG 20230426")
    logger.warning("=======================================================")
    exit()

# 입력 날짜 셋팅
if(len(sys.argv[2]) == 8):
    base_dt = sys.argv[2]
elif(len(sys.argv[2]) == 6):
    base_dt = sys.argv[2] + '01'
else:
    logger.warning("■ ERROR.")
    logger.warning("■ 입력된 날짜형식이 정확하지 않습니다.")
    logger.warning("=======================================================")
    exit()

# 경로 셋팅
#head_file_path = r'C:\Users\Owner\Desktop\신세계백화점 차세대 시스템 구축\03. 개발\00. 작업폴더\Example\파이썬파일경로\head.sql'
body_file_path = r'/home/etladm/BATCH/ORG//' + sys.argv[1] + '.sql' # EC2 서버 실행시 1번째 파라미터
#tail_file_path = r'C:\Users\Owner\Desktop\신세계백화점 차세대 시스템 구축\03. 개발\00. 작업폴더\Example\파이썬파일경로/tail.sql'
gen_file_path = r'/home/etladm/BATCH/GEN/' + sys.argv[1] + '_' + sys.argv[2] + '.sql'


# env.ini 파일에서 정보 가져오기
config = configparser.ConfigParser()
config.read(r'/home/etladm/BATCH/COM/ENV/ADWenv.ini')
host = config['REDSHIFT']['host']
port = config['REDSHIFT']['port']
dbname = config['REDSHIFT']['dbname']
user = config['REDSHIFT']['user']
password = config['REDSHIFT']['password']

# db password 복호화
depassword = decrypto.decrypt_aes(password)

# # EC2 서버에서 파일 내용 가져오기
# with open(head_file_path, 'r') as f:
#     head_sql = f.read()
with open(body_file_path, 'r') as f:
    body_sql = f.read() #format(BASE_DT=base_dt)
# with open(tail_file_path, 'r') as f:
#     tail_sql = f.read()
#
# # 합쳐진 SQL 파일 생성
# body_sql = body_sql.replace('{h_dong_cd}', h_dong_cd)
#

with open(gen_file_path, 'w') as f:
    f.write(body_sql)

# Redshift에 연결
conn = psycopg2.connect(
    host=host,
    port=port,
    dbname=dbname,
    user=user,
    password=depassword
)

"""
@ body 파일에서 변수형태로 sql을 불러와서 실행하는 형태
# cur = conn.cursor()

# module_name = sys.argv[1]
# program_id = importlib.import_module(module_name)
# 
# # Working Table Truncate
# step_num += 1
# truncate_query = f
#     select * from {program_id.work_schema}.{program_id.work_table}
# 
# cur.execute(truncate_query)
# truncatecnt = cur.rowcount
# logger.warning("■ STEP [%d] Working Table Truncate : %d 건", step_num, truncatecnt)
# 
# # Working Table Insert
# step_num += 1
# #work_insert_sql = " ".join(program_id.insert_work_sql)
# sub_step = 1
# for work_insert_sql in program_id.insert_work_sql:
#     cur.execute(work_insert_sql)
#     workinsertcnt = cur.rowcount
#     logger.warning("■ STEP [%d_%d] Working Table Insert : %d 건", step_num,sub_step, workinsertcnt)
#     sub_step += 1
#workinsertcnt = cur.rowcount
#logger.warning("■ STEP [%d] Working Table Insert : %d 건", step_num, workinsertcnt)
"""
h_dong_cd = '4413132000'
sql_file = open(gen_file_path, 'r')
sql_array = sql_file.read().format(base_dt=base_dt
                                 , h_dong_cd=h_dong_cd).split('@@@') # body 파일내 쿼리들을 순차적으로 배열에 저장, .format 으로 변수설정
sql_file.close()

#total_count = 0 # 누적 처리 건수 초기화
with conn.cursor() as cur:
 for sql in sql_array:
     comment_line = sql.split('\n')[0] # sql 첫번째 문장은 comment_line
     if (sql.strip() != ""):
         try:
             cur.execute(sql)
             count = cur.rowcount # 처리 건수
             #total_count += count # 누적 처리 건수 업데이트
             logger.warning(("■ " + comment_line.replace('-- ','')).ljust(19, ' ') + f" : {count} rows affected")
             totlog += "■ " + comment_line.replace('-- ','').ljust(19, ' ') + f" : {count} rows affected"
             conn.commit()
         except Exception as e:
             if "can't execute an empty query" in str(e): # 해당 에러발생시 작업 재수행(주석걸린 쿼리 수행시 발생하는 에러)
                 continue
             else:
                 logger.warning(f"Error occurred: {e}")
                 logger.warning(f"SQL statement: {sql}")
                 totlog += f"Error occurred: {e}"
                 totlog += f"SQL statement: {sql}"
                 conn.rollback()
                 exit(1)

# 총 처리 건수 출력
#logger.warning(f"\nTotal {total_count} rows affected")

# 실행 결과 출력
#rows = cur.fetchall()
#for row in rows:
#    print(row)

# 변경 내용 저장
conn.commit()

# 커넥션 닫기
cur.close()
conn.close()

end_time = datetime.datetime.now()  # 종료 시간 저장
logger.warning("■ 프로그램 종료     : {}".format(end_time.strftime("%Y%m%d%H%M%S")))
totlog += "■ 프로그램 종료     : {}".format(end_time.strftime("%Y%m%d%H%M%S")) + "\n"

# 실행 시간 계산
duration = end_time - start_time
logger.warning("■ 프로그램 실행시간 : {}".format(duration))
logger.warning("=======================================================")
totlog += "■ 프로그램 실행시간 : {}".format(duration) + "\n======================================================="

# 로그 생성
log_file_name = r'/home/etladm/BATCH/LOG/log_' + sys.argv[1] + '_' + sys.argv[2] + '.dat'
with open(log_file_name, 'w') as f:
    f.write(totlog)
