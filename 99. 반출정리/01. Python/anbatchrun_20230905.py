import psycopg2
import os
import datetime
import configparser
import sys
import logging
import decrypto
import importlib
import subprocess
import time

#logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(' ')
logger.setLevel(level=logging.INFO)

# 공통 변수 셋팅
totlog = ""
step_num = 0
juje = sys.argv[1][4:7] #주제영역셋팅
comment_line = ""
pkresult = 0
table_name = sys.argv[1][4:-3].lower()
mart_chk = table_name[:1]
schema = ""
rowcnt = ""
dt_from = ""
dt_to = ""
# 프로그램 시작
start_time = datetime.datetime.now()  # 시작 시간 저장
begin_time = start_time.strftime("%Y%m%d%H%M%S") + "{:06d}".format(start_time.microsecond)
begin_time = begin_time[:17]
formatted_time = start_time.strftime("%Y%m%d%H%M%S") + "{:06d}".format(start_time.microsecond)
formatted_time = formatted_time[:17]

if(len(sys.argv) == 3):
    dt_from = sys.argv[2]
    dt_to = sys.argv[2]
elif(len(sys.argv) == 4):
    dt_from = sys.argv[2]
    dt_to = sys.argv[3]

logger.warning("=======================================================")
logger.warning("■ 프로그램 시작     : {}".format(start_time.strftime("%Y%m%d%H%M%S")))
logger.warning("■ 프로그램 PGM_ID   : " + sys.argv[1])
logger.warning("■ PARAMETER         : " + dt_from + " ~ " + dt_to)
totlog += "=======================================================\n■ 프로그램 시작     : {}".format(start_time.strftime("%Y%m%d%H%M%S")) + "\n■ 프로그램 PGM_ID   : " + sys.argv[1] + "\n■ PARAMETER         : " + dt_from + " ~ " + dt_to+ "\n"

# Parameter 셋팅
if(len(sys.argv) < 3):
    logger.warning("■ ERROR.")
    logger.warning("■ 입력된 Parameter가 정확하지 않습니다.")
    logger.warning("■ USAGE) python anbatchrun.py Jobname DATE")
    logger.warning("■    EX) python anbatchrun.py LCD_SMT_SHOP_MD_M_TG 20230426 20230429")
    logger.warning("=======================================================")
    exit()

# 입력 날짜 셋팅
if(len(sys.argv) == 3 and len(sys.argv[2]) == 8):
    base_dt_from = sys.argv[2]
    base_dt_to = sys.argv[2]
    base_ym_from = sys.argv[2][:6]
    base_ym_to = sys.argv[2][:6]
elif(len(sys.argv) == 4):
    if(len(sys.argv[2]) == 8 and len(sys.argv[3]) == 8):
      base_dt_from = sys.argv[2]
      base_dt_to = sys.argv[3]
      base_ym_from = sys.argv[2][:6]
      base_ym_to = sys.argv[3][:6]
#elif(len(sys.argv) == 3 and len(sys.argv[2]) == 6):
#    base_dt_from = sys.argv[2]
#    base_dt_to = sys.argv[2]
#elif(len(sys.argv) == 4):
#    if(len(sys.argv[2]) == 6 and len(sys.argv[3]) == 6):
#      base_dt_from = sys.argv[2]
#      base_dt_to = sys.argv[3]
else:
    logger.warning("■ ERROR.")
    logger.warning("■ 입력된 날짜형식이 정확하지 않습니다.")
    logger.warning("=======================================================")
    exit()

if(mart_chk == "s"):
    schema = "ansor"
elif(mart_chk == "a"):
    schema = "anana"
elif(mart_chk == "r"):
    schema = "anrep"

# 경로 셋팅
body_file_path = r'/home/etladm/BATCH/ORG/' + juje + '/' + sys.argv[1] + '.sql'  # EC2 서버 실행시 1번째 파라미터
gen_file_path = r'/home/etladm/BATCH/GEN/' + juje + '/' + sys.argv[1] + '_' + sys.argv[2] + '.sql'
log_file_name = r'/home/etladm/BATCH/LOG/' + juje + '/log_' + sys.argv[1] + '_' + sys.argv[2] + '.dat'

try:
    # 이전 gen, log 파일 삭제
    processgenrm = subprocess.Popen(f'rm -rf {gen_file_path}', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    processlogrm = subprocess.Popen(f'rm -rf {log_file_name}', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # 서버 내 실행중인 job 확인
    processprcchk = subprocess.Popen(f'ps -ef | grep anbatchrun | grep {sys.argv[1]} | wc -l', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # 실행 결과 변수셋팅
    output = processprcchk.stdout.read()
    output = output.decode()

    # 실행 결과 받기
    #output, error = processprcchk.communicate()

    def execute_sp_log_mrt(conn, pgm_nm, base_dt_to, num, begin_time, formatted_time, count, runyn,err, comment_line, endyn, sql):
        call = 'CALL public.sp_log_mrt(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        params = (sys.argv[1], f"{pgm_nm} 적재 프로그램", base_dt_to, num, begin_time, formatted_time, count, runyn, err,
                  comment_line, f"{sys.argv[1]} {base_dt_from} {base_dt_to}", endyn)
        try:
            cur = conn.cursor()
            cur.execute(call, params)
            conn.commit()
        except Exception as e:
            if "1023" in str(e):
                conn.rollback()
                retry_attempt = 0
                while retry_attempt < 1000:  # 최대 1000번까지 재시도
                    time.sleep(1)  # 3초 대기
                    try:
                        cur.execute(call, params)
                        conn.commit()

                        break  # 성공적으로 실행되었을 경우 재시도 루프 종료
                    except Exception as e:
                        conn.rollback()
                        retry_attempt += 1
                        if retry_attempt == 1000:
                            # 재시도 횟수를 초과한 경우 예외 처리
                            # ...
                            break

    # output > 2 이상일 경우엔 로그테이블을 읽어 실행중 상태 확인하는 로직
    if output is not None:
        cnt = int(output)
        if cnt > 2:
            # env.ini 파일에서 정보 가져오기
            config = configparser.ConfigParser()
            config.read(r'/home/etladm/BATCH/COM/ENV/anenv.ini')
            host = config['REDSHIFT']['host']
            port = config['REDSHIFT']['port']
            dbname = config['REDSHIFT']['dbname']
            user = config['REDSHIFT']['user']
            password = config['REDSHIFT']['password']

            # db password 복호화
            depassword = decrypto.decrypt_aes(password)

            # Redshift에 연결
            conn_log = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=depassword
            )
            # 커서 생성
            cursor = conn_log.cursor()
            # 쿼리 실행 (실행중인 작업 체크를 위해 log table에서 'R' 인 작업목록이 1개 이상일 경우 중복 작업으로 분류)
            # 로그 테이블 생성 후 쿼리 변경
            logquery = f"select count(1) from ansor.scm_bat_wrk_log_m where bat_wrk_id = '{sys.argv[1]}' and bat_wrk_rlt_cd = 'R';"
            cursor.execute(logquery)
            # 결과 가져오기
            results = cursor.fetchall()
            results = results[0][0]

            if results > 0:
                logger.warning("=======================================================")
                logger.warning("                     <프로그램 오류>")
                logger.warning("■ 이미 실행중인 작업 입니다.")
                logger.warning("=======================================================")
                totlog += "=======================================================\n"
                totlog += "                     <프로그램 오류>\n"
                totlog += "■ 이미 실행중인 작업 입니다.\n"
                totlog += "=======================================================\n"
                exit(1)

    # env.ini 파일에서 정보 가져오기
    config = configparser.ConfigParser()
    config.read(r'/home/etladm/BATCH/COM/ENV/anenv.ini')
    host = config['REDSHIFT']['host']
    port = config['REDSHIFT']['port']
    dbname = config['REDSHIFT']['dbname']
    user = config['REDSHIFT']['user']
    password = config['REDSHIFT']['password']

    # db password 복호화
    depassword = decrypto.decrypt_aes(password)

    # Redshift에 연결
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=depassword
    )

    pgmnmcur = conn.cursor()

    pgmnmquery = f"select COALESCE(obj_description('{schema}.{table_name}'::regclass), '없음')"
    pgmnmcur.execute(pgmnmquery)

    # 결과 가져오기
    pgm_nm = pgmnmcur.fetchall()
    pgm_nm = pgm_nm[0][0]

    num = '001'

    execute_sp_log_mrt(conn, pgm_nm, base_dt_to, num, begin_time, formatted_time, 0, '0', '', '프로그램 시작','N', '')
    #logcur.execute('CALL public.sp_log_mrt(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    #               , (sys.argv[1], f"{pgm_nm} 적재 프로그램", base_dt_from, num, begin_time, formatted_time, 0, '0', '', '적재 프로그램 시작'
    #                  , f"{sys.argv[1]} {base_dt_from} {base_dt_to}", 'N'))
    conn.commit()
    # body 파일 읽으며 변수치환

    with open(body_file_path, 'r') as f:
        body_sql = f.read().format(BASE_DT_FROM=base_dt_from,BASE_DT_TO=base_dt_to,BASE_YM_FROM=base_ym_from,BASE_YM_TO=base_ym_to)

    # gen 파일 생성
    with open(gen_file_path, 'w') as f:
        f.write(body_sql)

    sql_file = open(gen_file_path, 'r')
    sql_array = sql_file.read().split('####SQL')  # body 파일내 쿼리들을 순차적으로 배열에 저장, .format 으로 변수설정
    sql_file.close()
    # total_count = 0 # 누적 처리 건수 초기화
    with conn.cursor() as cur:
        for sql in sql_array:
            # comment_line = sql.split('\n')[0] # sql 첫번째 문장은 comment_line
            for line in sql.split('\n'):
                if "P_BAT_STG_NM" in line:
                    start_index = sql.find("P_BAT_STG_NM :") + 15
                    end_index = sql.find("  */", start_index)
                    comment_line = sql[start_index:end_index].strip()
            if (sql.strip() != ""):
                try:
                    num_int = int(num)  # 문자열을 정수로 변환
                    num_int += 1  # 정수를 증가시킴
                    num = str(num_int).zfill(len(num))

                    formatted_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S") + "{:06d}".format(
                        datetime.datetime.now().microsecond)
                    formatted_time = formatted_time[:17]

                    cur.execute(sql)
                    count = cur.rowcount  # 처리 건수
                    # total_count += count # 누적 처리 건수 업데이트
                    rowcnt = count

                    conn.commit()

                    execute_sp_log_mrt(conn, pgm_nm, base_dt_to, num, begin_time, formatted_time, count, '2', '', comment_line, 'Y', sql)
                    #cur.execute('CALL public.sp_log_mrt(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                    #            , (sys.argv[1], f"{pgm_nm} 적재 프로그램", base_dt_from, num, begin_time,
                    #               formatted_time, count, '2', '', comment_line
                    #               , f"{sys.argv[1]} {base_dt_from} {base_dt_to}", 'Y'))

                    if (comment_line.replace('-- ', '') == "SET"):
                        logger.warning("■ SETTING".ljust(19, ' ') + " : TRUE")
                        totlog += "■ SETTING".ljust(17, ' ') + " : TRUE\n"
                    else:
                        logger.warning(("■ " + comment_line.replace('-- ', '')).ljust(19, ' ') + f" : {count} rows affected")
                        totlog += "■ " + comment_line.replace('-- ', '').ljust(17, ' ') + f" : {count} rows affected\n"

                except Exception as e:
                    if "can't execute an empty query" in str(e):  # 해당 에러발생시 작업 재수행(주석걸린 쿼리 수행시 발생하는 에러)
                        continue
                    else:
                        # logger.warning(f"Error occurred: {e}")
                        # logger.warning(f"SQL statement: {sql}")
                        conn.rollback()
                        errorlog = str(e)
                        end_time = datetime.datetime.now()
                        duration = end_time - start_time
                        logger.warning("=======================================================")
                        logger.warning("                     <프로그램 오류>")
                        #logger.warning(f"Error occurred: {errorlog[:100]}\n")
                        totlog += "=======================================================\n"
                        totlog += "                     <프로그램 오류>\n"
                        totlog += f"Error occurred: {e}\n"
                        totlog += f"SQL statement: {sql}\n"
                        totlog += "=======================================================\n"
                        execute_sp_log_mrt(conn, pgm_nm, base_dt_to, num, begin_time, formatted_time, 0, '1', 'ERR', f'Error occurred: {errorlog[:100]}', 'Y', '')
                        conn.commit()
                        #cur.execute('CALL public.sp_log_mrt(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        #            , (sys.argv[1], f"{pgm_nm} 적재 프로그램", base_dt_from, num, begin_time,
                        #               formatted_time, 0, '1', 'ERR', f"Error occurred: {e}"
                        #               , f"{sys.argv[1]} {base_dt_from} {base_dt_to}", 'Y'))
                        with open(log_file_name, 'w') as f:
                            f.write(totlog)
                        exit(1)

    # 변경 내용 저장
    conn.commit()

    pkcur = conn.cursor()

    # 프로시저 실행
    pkcur.execute('CALL public.sp_table_key_chk(%s, %s, %s)', (table_name, base_dt_from, base_dt_to))

    # 결과 가져오기 (필요한 경우)
    pkresult = pkcur.fetchall()
    pkresult = pkresult[0][0]
    if (pkresult == 0):
        logger.warning("■ PK_Consistency    : TRUE")
        totlog += "■ PK_Consistency    : TRUE\n"
    else:
        conn.rollback()
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        logger.warning("■ PK_Consistency    : FALSE")
        logger.warning("=======================================================")
        logger.warning("                     <프로그램 오류>")
        logger.warning("■ PK 중복 Error")
        logger.warning("=======================================================")
        totlog += "■ PK_Consistency    : FALSE\n"
        totlog += "=======================================================\n"
        totlog += "                     <프로그램 오류>\n"
        totlog += "■ PK 중복 Error\n"
        totlog += "=======================================================\n"
        execute_sp_log_mrt(conn, pgm_nm, base_dt_to, num, begin_time, formatted_time, 0, '1', 'ERR', "PK 중복 Error",
                           'Y', '')
        # logcur.execute('CALL public.sp_log_mrt(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        #            , (sys.argv[1], f"{pgm_nm} 적재 프로그램", base_dt_from, num, begin_time,
        #               formatted_time, 0, '1', 'ERR', "PK 중복 Error"
        #               , f"{sys.argv[1]} {base_dt_from} {base_dt_to}", 'Y'))
        with open(log_file_name, 'w') as f:
            f.write(totlog)
        exit(1)

    num_int = int(num)  # 문자열을 정수로 변환
    num_int += 1  # 정수를 증가시킴
    num = str(num_int).zfill(len(num))

    execute_sp_log_mrt(conn, pgm_nm, base_dt_to, num, begin_time, formatted_time, rowcnt, '0', '', "정상 종료", 'Y', '')
    #logcur.execute('CALL public.sp_log_mrt(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    #            , (sys.argv[1], f"{pgm_nm} 적재 프로그램", base_dt_from, num, begin_time,
    #               formatted_time, 0, '0', '정상', ''
    #               , f"{sys.argv[1]} {base_dt_from} {base_dt_to}", 'Y'))
    # 커밋
    conn.commit()
except Exception as e:
    if "invalid name syntax" in str(e):  # 해당 에러발생시 작업 재수행(주석걸린 쿼리 수행시 발생하는 에러)
        conn.rollback()
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        logger.warning("=======================================================")
        logger.warning("                     <프로그램 오류>")
        logger.warning(f"Error occurred: TABLE NOT FOUND\n")
        logger.warning("=======================================================")
        totlog += "=======================================================\n"
        totlog += "                     <프로그램 오류>\n"
        totlog += f"Error occurred: TABLE NOT FOUND\n"
        totlog += "=======================================================\n"
        execute_sp_log_mrt(conn, pgm_nm, base_dt_to, num, begin_time, formatted_time, 0, '1', 'ERR',
                           f"Error occurred: TABLE NOT FOUND", 'Y', '')
        # logcur.execute('CALL public.sp_log_mrt(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        #            , (sys.argv[1], f"{pgm_nm} 적재 프로그램", base_dt_from, num, begin_time,
        #               formatted_time, 0, '1', 'ERR', f"Error occurred: {e}"
        #               , f"{sys.argv[1]} {base_dt_from} {base_dt_to}", 'Y'))
        with open(log_file_name, 'w') as f:
            f.write(totlog)
        exit(1)
    else:
        conn.rollback()
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        logger.warning("=======================================================")
        logger.warning("                     <프로그램 오류>")
        logger.warning(f"Error occurred: {e}\n")
        logger.warning("=======================================================")
        totlog += "=======================================================\n"
        totlog += "                     <프로그램 오류>\n"
        totlog += f"Error occurred: {e}\n"
        totlog += "=======================================================\n"
        execute_sp_log_mrt(conn, pgm_nm, base_dt_to, num, begin_time, formatted_time, 0, '1', 'ERR',
                           f"Error occurred: {e}", 'Y', '')
        # logcur.execute('CALL public.sp_log_mrt(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        #            , (sys.argv[1], f"{pgm_nm} 적재 프로그램", base_dt_from, num, begin_time,
        #               formatted_time, 0, '1', 'ERR', f"Error occurred: {e}"
        #               , f"{sys.argv[1]} {base_dt_from} {base_dt_to}", 'Y'))
        with open(log_file_name, 'w') as f:
            f.write(totlog)
        exit(1)
finally:

    end_time = datetime.datetime.now()  # 종료 시간 저장
    logger.warning("■ 프로그램 종료     : {}".format(end_time.strftime("%Y%m%d%H%M%S")))
    totlog += "■ 프로그램 종료     : {}".format(end_time.strftime("%Y%m%d%H%M%S")) + "\n"

    # 실행 시간 계산
    duration = end_time - start_time
    logger.warning("■ 프로그램 실행시간 : {}".format(duration))
    logger.warning("■ LOG_FILE_PATH     : " + log_file_name)
    logger.warning("=======================================================")
    totlog += "■ 프로그램 실행시간 : {}".format(duration) + "\n=======================================================\n"

    # 커넥션 닫기
    cur.close()
    #pkcur.close()
    conn.close()
    pgmnmcur.close()

    # 로그 생성
    with open(log_file_name, 'w') as f:
        f.write(totlog)
