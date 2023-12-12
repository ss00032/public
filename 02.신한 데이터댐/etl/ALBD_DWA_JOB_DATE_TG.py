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

__author__ = "윤혁준"
__copyright__ = "Copyright 2021, Shinhan Datadam"
__credits__ = ["윤혁준"]
__version__ = "1.0"
__maintainer__ = "윤혁준"
__email__ = "Lee1122334@xgm.co.kr"
__status__ = "Production"

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
pgm_id = 'ALBD_DWA_JOB_DATE_TG'

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

s3_file_prefix = f'abd_dwa_job_date_/abd_dwa_job_date_{execution_kst}'

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
* 기준 : table_load_type = 'm'
"""
delete_sql_for_merge = f"""
        delete from l0_shb.dwa_job_date
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
* 기준 : table_load_type in ('a','m','o')
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwa_job_date
                (
                  aws_ls_dt                               -- aws적재일시
                , dw_bas_ddt                              -- dw기준d일자
                , dt_g                                    -- 날짜구분
                , biz_dt                                  -- 영업일자
                , biz_dayw_g                              -- 영업요일구분
                , biz_dayw_nm                             -- 영업요일명
                , next_biz_dt                             -- 익영업일자
                , next_next_biz_dt                        -- 익익영업일자
                , bf1_biz_dt                              -- 전1영업일자
                , bf2_biz_dt                              -- 전2영업일자
                , bf3_biz_dt                              -- 전3영업일자
                , bf4_biz_dt                              -- 전4영업일자
                , bf5_biz_dt                              -- 전5영업일자
                , thwk_st1_biz_dt                         -- 금주초영업일
                , thwk_lst_biz_dt                         -- 금주말영업일
                , bfwk_st1_biz_dt                         -- 전주초영업일
                , bfwk_lst_biz_dt                         -- 전주말영업일
                , nxwk_st1_biz_dt                         -- 다음주초영업일
                , nxwk_lst_biz_dt                         -- 다음주말영업일
                , bf1mm_start_dt                          -- 전월첫일
                , bf1mm_st1_biz_dt                        -- 전월첫영업일자
                , bf1mm_lst_biz_dt                        -- 전월말영업일자
                , bf1mm_end_dt                            -- 전월말일
                , nx1mm_start_dt                          -- 익월첫일
                , nx1mm_st1_biz_dt                        -- 익월첫영업일자
                , nx1mm_lst_biz_dt                        -- 익월말영업일자
                , nx1mm_end_dt                            -- 익월말일
                , mmstart_dt                              -- 당월첫일
                , st1_biz_dt                              -- 당월첫영업일자
                , lst_biz_dt                              -- 당월말영업일자
                , mmend_dt                                -- 당월말일
                , mm_bas_yymm                             -- 당월기준년월
                , bf1mm_bas_yymm                          -- 전1월기준년월
                , bf2mm_bas_yymm                          -- 전2월기준년월
                , bf3mm_bas_yymm                          -- 전3월기준년월
                , bf2mm_lst_biz_dt                        -- 전전월말영업일자
                , yy_st1_biz_dt                           -- 금년첫영업일자
                , bf1yy_st1_biz_dt                        -- 전년첫영업일자
                , bf1yy_same_dt                           -- 전년동일
                , bf1mm_same_dt                           -- 전1월동일
                , bf2mm_same_dt                           -- 전2월동일
                , bf3mm_same_dt                           -- 전3월동일
                , m_dcnt                                  -- 당월일수
                , yr_dcnt                                 -- 당해일수
                , nx1mm_m_dcnt                            -- 익월일수
                , bf1mm_m_dcnt                            -- 전1월일수
                , bf2mm_m_dcnt                            -- 전2월일수
                , bf3mm_m_dcnt                            -- 전3월일수
                , dur3mm_dcnt                             -- 최근3개월일수
                , dur6mm_dcnt                             -- 최근6개월일수
                , dur12mm_dcnt                            -- 최근1년일수
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , dw_bas_ddt                              -- dw기준d일자
          , dt_g                                    -- 날짜구분
          , biz_dt                                  -- 영업일자
          , biz_dayw_g                              -- 영업요일구분
          , biz_dayw_nm                             -- 영업요일명
          , next_biz_dt                             -- 익영업일자
          , next_next_biz_dt                        -- 익익영업일자
          , bf1_biz_dt                              -- 전1영업일자
          , bf2_biz_dt                              -- 전2영업일자
          , bf3_biz_dt                              -- 전3영업일자
          , bf4_biz_dt                              -- 전4영업일자
          , bf5_biz_dt                              -- 전5영업일자
          , thwk_st1_biz_dt                         -- 금주초영업일
          , thwk_lst_biz_dt                         -- 금주말영업일
          , bfwk_st1_biz_dt                         -- 전주초영업일
          , bfwk_lst_biz_dt                         -- 전주말영업일
          , nxwk_st1_biz_dt                         -- 다음주초영업일
          , nxwk_lst_biz_dt                         -- 다음주말영업일
          , bf1mm_start_dt                          -- 전월첫일
          , bf1mm_st1_biz_dt                        -- 전월첫영업일자
          , bf1mm_lst_biz_dt                        -- 전월말영업일자
          , bf1mm_end_dt                            -- 전월말일
          , nx1mm_start_dt                          -- 익월첫일
          , nx1mm_st1_biz_dt                        -- 익월첫영업일자
          , nx1mm_lst_biz_dt                        -- 익월말영업일자
          , nx1mm_end_dt                            -- 익월말일
          , mmstart_dt                              -- 당월첫일
          , st1_biz_dt                              -- 당월첫영업일자
          , lst_biz_dt                              -- 당월말영업일자
          , mmend_dt                                -- 당월말일
          , mm_bas_yymm                             -- 당월기준년월
          , bf1mm_bas_yymm                          -- 전1월기준년월
          , bf2mm_bas_yymm                          -- 전2월기준년월
          , bf3mm_bas_yymm                          -- 전3월기준년월
          , bf2mm_lst_biz_dt                        -- 전전월말영업일자
          , yy_st1_biz_dt                           -- 금년첫영업일자
          , bf1yy_st1_biz_dt                        -- 전년첫영업일자
          , bf1yy_same_dt                           -- 전년동일
          , bf1mm_same_dt                           -- 전1월동일
          , bf2mm_same_dt                           -- 전2월동일
          , bf3mm_same_dt                           -- 전3월동일
          , m_dcnt                                  -- 당월일수
          , yr_dcnt                                 -- 당해일수
          , nx1mm_m_dcnt                            -- 익월일수
          , bf1mm_m_dcnt                            -- 전1월일수
          , bf2mm_m_dcnt                            -- 전2월일수
          , bf3mm_m_dcnt                            -- 전3월일수
          , dur3mm_dcnt                             -- 최근3개월일수
          , dur6mm_dcnt                             -- 최근6개월일수
          , dur12mm_dcnt                            -- 최근1년일수
      from w0_shb.dwa_job_date
"""

""" 
(@) UNLOAD용 Working 테이블 조회 쿼리 (선택적)
* 기준 : table_load_type in ('a','m','o')
"""
select_sql_for_unload = ""

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
        s3_key=s3_file_prefix,
        delete_sql_for_merge=delete_sql_for_merge,
        select_sql_for_insert=select_sql_for_insert,
    )

#    w0_unload_task = W0UnloadOperator(
#        task_id='w0_unload_task',
#        schema=target_schema,
#        table=target_table,
#        select_query=select_sql_for_unload,
#        s3_key=s3_file_prefix,
#    )

#    stg_to_bck_operator = StgToBckOperator(
#        task_id='stg_to_bck_task',
#        s3_file_prefix=s3_file_prefix,
#        source_version_id=None,
#    )

    #l0_load_task >> w0_unload_task >> stg_to_bck_operator
    l0_load_task