# -*- coding: utf-8 -*-

from datetime import datetime
import pendulum

from airflow import DAG
from sgd import config
from sgd.operators.l0_load_operator import L0LoadOperator
from sgd.operators.w0_unload_operator import W0UnloadOperator
from sgd.operators.stg_to_bck_operator import StgToBckOperator
from sgd.log_util import *
from sgd.utils import *


__author__     = "노재홍"
__copyright__  = "Copyright 2021, Shinhan Datadam"
__credits__    = ["노재홍"]
__version__    = "1.0"
__maintainer__ = "노재홍"
__email__      = ""
__status__     = "Production"


"""
S3 파일 데이터를 Redshift에 적재하는 DAG 템플릿

기능 별로 분리 되었던 오퍼레이터를 하나로 통합해 내부적으로
아래 단계를 수행하면서 파일 데이터를 Redshift에 적재 한다.

  // APPEND 적재 프로세스
  1. W0 Working 테이블 delete
  2. W0 Working 테이블 insert (S3 -> Redshift COPY) 
  3. W0 Working 테이블 -> L0 테이블 insert
  
  // MERGE 적재 프로세스
  1. W0 Working 테이블 delete
  2. W0 Working 테이블 insert (S3 -> Redshift COPY)
  3. L0 변경 데이터 delete
  4. W0 Working 테이블 -> L0 테이블 insert
  
  // OVERWRITE 적재 프로세스
  1. W0 Working 테이블 delete
  2. W0 Working 테이블 insert (S3 -> Redshift COPY)
  3. L0 테이블 truncate
  3. W0 Working 테이블 -> L0 테이블 insert

하나의 파일을 받아 통합데이터, 동의고객데이터 클러스터에 적재하는 테이블은 UNLOAD 로직이 추가된다. 

[ 적용 방법 ]

제공된 ETL 개발 템플릿에서
아래 수정 대상 '(@)' 부분만 변경해서 바로 실행 가능

(@) 변경 대상 :
  - 프로그램 ID
  - 테이블 적재유형 (append, merge, overwrite)
  - 수집 파일명 prefix
  - INSERT용 Working 테이블 조회 쿼리 (선택적)
  - UNLOAD용 Working 테이블 조회 쿼리 (선택적)
  - L0 변경데이터 삭제 SQL (적재유형이 merge 인 경우만 해당)

"""

################################################################################
### Start of Target schema, working table, target table

""" 
(@) 프로그램 ID
"""
pgm_id = 'ALBD_DWX_WEB_APP_LOG_TG'

""" 
(@) 테이블 적재 구분  
a: append, o: overwrite, m: merge
"""
table_load_type = 'o'

""" 
(@) EXECUTION DATE
Airflow Console 에서 DAG CREATE 시 입력하는 execution date

일배치: execution_kst='{{ dag.timezone.convert(execution_date).strftime("%Y%m%d") }}'
월배치: execution_kst='{{ dag.timezone.convert(execution_date).strftime("%Y%m") }}'
"""
execution_kst = '{{ dag.timezone.convert(execution_date).strftime("%Y%m%d") }}'

""" 
(@) 수집 파일명 prefix

은행/카드/라이프 예시: 
s3_file_prefix = 'ibd_dwa_job_date_/ibd_dwa_job_date_'
s3_file_prefix = f'jd_append_table_/jd_append_table_{execution_kst}'
금투 예시: 
s3_file_prefix = 'iid_aaa001m00_'
s3_file_prefix = f'iid_aaa001m00_{execution_kst}'
"""
s3_file_prefix = f'abd_dwx_web_app_log_/abd_dwx_web_app_log_{execution_kst}'

# 적재 Layer
target_layer = 'l0'

# pgm_id 파싱하여 변수 세팅
# 사용목적코드, 프로그램적재구분, 그룹사코드, 적재시점코드, 테이블명, TG, DAG TAGS
(up_cd, pt_cd, cp_cd, tm_cd, target_table, tg_cd, tags) = parse_pgm_id(pgm_id)

use_purpose = tags[0]
company_code = tags[2]

# 적재 스키마명
target_schema = f'{target_layer}_{company_code}'

# 적재 Working 스키마
working_schema = f"{config.sgd_env['wk_layer']}_{company_code}"

""" 
(@) L0 변경데이터 삭제 SQL (적재유형이 merge인 경우만 해당)
PK를 이용한 Join Query 
"""
delete_sql_for_merge = f"""
        delete from l0_shb.dwx_web_app_log
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwx_web_app_log
                (
                  aws_ls_dt                               -- aws적재일시
                , log_dt                                  -- 로그생성일
                , log_dttm                                -- 로그생성시각
                , ci_no                                   -- ci번호
                , cusno                                   -- 고객번호
                , page_id                                 -- 페이지id
                , menu_cd                                 -- 메뉴코드
                , spark_dttm                              -- spark 처리시각
                , dw_dttm                                 -- 적재시각
                , event_nm                                -- 이벤트명
                , launcher                                -- launcher
                , login_tp                                -- 로그인 타입
                , next_page_id                            -- 다음 페이지id
                , action_exp                              -- 액션명
                , action_id                               -- 액션id
                , app_ver_nm                              -- 앱버전명
                , telco                                   -- 통신사
                , platform                                -- 플랫폼
                , device                                  -- 기기명
                , os_ver                                  -- os 버전
                , loc_lat                                 -- 위도
                , loc_long                                -- 경도
                , remote_ip                               -- 원천 ip
                , ms_from_start                           -- 진입 후 경과시간
                , session_id                              -- 세션id
                , error_yn                                -- 에러여부
                , prdt_cd                                 -- 상품코드
                , svc_id                                  -- 서비스id
                , pcid                                    -- pcid
                , domain                                  -- 도메인코드
                , error_msg                               -- 에러내용
                , process_type                            -- 적재구분
                , ad_id                                   -- ad_id
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , log_dt                                  -- 로그생성일
          , log_dttm                                -- 로그생성시각
          , ci_no                                   -- ci번호
          , cusno                                   -- 고객번호
          , page_id                                 -- 페이지id
          , menu_cd                                 -- 메뉴코드
          , spark_dttm                              -- spark 처리시각
          , dw_dttm                                 -- 적재시각
          , event_nm                                -- 이벤트명
          , launcher                                -- launcher
          , login_tp                                -- 로그인 타입
          , next_page_id                            -- 다음 페이지id
          , action_exp                              -- 액션명
          , action_id                               -- 액션id
          , app_ver_nm                              -- 앱버전명
          , telco                                   -- 통신사
          , platform                                -- 플랫폼
          , device                                  -- 기기명
          , os_ver                                  -- os 버전
          , loc_lat                                 -- 위도
          , loc_long                                -- 경도
          , remote_ip                               -- 원천 ip
          , ms_from_start                           -- 진입 후 경과시간
          , session_id                              -- 세션id
          , error_yn                                -- 에러여부
          , prdt_cd                                 -- 상품코드
          , svc_id                                  -- 서비스id
          , pcid                                    -- pcid
          , domain                                  -- 도메인코드
          , error_msg                               -- 에러내용
          , process_type                            -- 적재구분
          , ad_id                                   -- ad_id
      from w0_shb.dwx_web_app_log
"""

""" 
(@) UNLOAD용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_unload = f"""
"""

### End of Target schema, working table, target table
################################################################################

""" DAG 공통 파라미터 """
args = {
    'owner': config.sgd_env['dag_owner'],
    'retries': config.sgd_env['retries'],
    'retry_delay': config.sgd_env['retry_delay'],
    'provide_context': True,
    'on_failure_callback': handle_etl_error,
    'company_code' : company_code,
    'use_purpose' : use_purpose,
    'execution_kst' : execution_kst,
    's3_key' : s3_file_prefix
}

# DAG ID 는 프로그램명과 동일
dag_id = pgm_id

with DAG(
    dag_id=dag_id,
    description=f'{dag_id} DAG',
    start_date=config.sgd_env['start_date'],
    schedule_interval=None,
    default_args=args,
    tags=tags,
    catchup=False) as dag :

    l0_load_task = L0LoadOperator(
        task_id='l0_load_task',
        target_schema=target_schema,
        target_table=target_table,
        table_load_type=table_load_type,
        delete_sql_for_merge=delete_sql_for_merge,
        select_sql_for_insert=select_sql_for_insert,
    )


#    w0_unload_task = W0UnloadOperator(
#        task_id='w0_unload_task',
#        schema=target_schema,
#        table=target_table,
#        select_query=select_sql_for_unload,
#    )
#
#    stg_to_bck_operator = StgToBckOperator(
#        task_id='stg_to_bck_task',
#    )

#    l0_load_task >> w0_unload_task >> stg_to_bck_operator
    l0_load_task
