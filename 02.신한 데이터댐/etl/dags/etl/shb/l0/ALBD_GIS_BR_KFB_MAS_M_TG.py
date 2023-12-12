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
pgm_id = 'ALBD_GIS_BR_KFB_MAS_M_TG'

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
s3_file_prefix = f'abd_gis_br_kfb_mas_m_/abd_gis_br_kfb_mas_m_{execution_kst}'

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
        delete from l0_shb.gis_br_kfb_mas_m
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.gis_br_kfb_mas_m
                (
                  aws_ls_dt                               -- aws적재일시
                , kfb_sqno                                -- 은행연합회_일련번호
                , bas_ym                                  -- 기준년월
                , bank_cd                                 -- 은행코드
                , girono                                  -- 지로번호
                , br_nm                                   -- 지점명
                , kfb_br_grp_gb                           -- 은행연합회점그룹구분
                , br_sbr_gb                               -- 점포형태구분
                , adr                                     -- 주소
                , chg_dt                                  -- 변동일자
                , open_dt                                 -- 개점일
                , tot_emp_cnt                             -- 총인원
                , reg_emp_cnt                             -- 정규직원수
                , tmp_emp_cnt                             -- 임시직원수
                , use_flr                                 -- 사용층
                , br_tot_area                             -- 영업점총면적
                , br_ezsq_area                            -- 영업점전용면적
                , br_cuse_area                            -- 영업점공용면적
                , kfb_ren_gb                              -- 은행연합회임차구분
                , obtn_amt                                -- 취득금액
                , ren_bojn_amt                            -- 임차보증금
                , ren_mm_amt                              -- 월세
                , ren_yy_amt                              -- 년세
                , tot_dep_jan                             -- 총수신잔액
                , bnk_tot_dep_jan                         -- 총수신잔액_은행계정
                , ta_tot_dep_jan                          -- 총수신잔액_신탁
                , tot_dep_avjn                            -- 총수신평잔
                , bnk_tot_dep_avjn                        -- 총수신평잔_은행계정
                , ta_tot_dep_avjn                         -- 총수신평잔_신탁
                , tot_dc_jan                              -- 총대출잔액
                , bnk_psn_dc_jan                          -- 개인대출잔액_은행계정
                , bnk_co_dc_jan                           -- 기업대출잔액_은행계정
                , bnk_tot_dc_jan                          -- 총대출잔액_은행계정
                , ta_tot_dc_jan                           -- 총대출잔액_신탁
                , tot_dc_avjn                             -- 총대출평잔
                , bnk_psn_dc_avjn                         -- 개인대출평잔_은행계정
                , bnk_co_dc_avjn                          -- 기업대출평잔_은행계정
                , bnk_tot_dc_avjn                         -- 총대출평잔_은행계정
                , ta_tot_dc_avjn                          -- 총대출평잔_신탁
                , fx_expt_amt                             -- 외환_수출금액
                , fx_impt_amt                             -- 외환_수입금액
                , fx_fnc_amt                              -- 외환_무역외금액
                , pl_amt                                  -- 손익금액
                , hdong_id                                -- 행정동id
                , x                                       -- x좌표
                , y                                       -- y좌표
                , dat_bas_dt                              -- 데이터기준일자
                , exchg_amt                               -- exchg_amt
                , remit_amt                               -- remit_amt
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , kfb_sqno                                -- 은행연합회_일련번호
          , bas_ym                                  -- 기준년월
          , bank_cd                                 -- 은행코드
          , girono                                  -- 지로번호
          , br_nm                                   -- 지점명
          , kfb_br_grp_gb                           -- 은행연합회점그룹구분
          , br_sbr_gb                               -- 점포형태구분
          , adr                                     -- 주소
          , chg_dt                                  -- 변동일자
          , open_dt                                 -- 개점일
          , tot_emp_cnt                             -- 총인원
          , reg_emp_cnt                             -- 정규직원수
          , tmp_emp_cnt                             -- 임시직원수
          , use_flr                                 -- 사용층
          , br_tot_area                             -- 영업점총면적
          , br_ezsq_area                            -- 영업점전용면적
          , br_cuse_area                            -- 영업점공용면적
          , kfb_ren_gb                              -- 은행연합회임차구분
          , obtn_amt                                -- 취득금액
          , ren_bojn_amt                            -- 임차보증금
          , ren_mm_amt                              -- 월세
          , ren_yy_amt                              -- 년세
          , tot_dep_jan                             -- 총수신잔액
          , bnk_tot_dep_jan                         -- 총수신잔액_은행계정
          , ta_tot_dep_jan                          -- 총수신잔액_신탁
          , tot_dep_avjn                            -- 총수신평잔
          , bnk_tot_dep_avjn                        -- 총수신평잔_은행계정
          , ta_tot_dep_avjn                         -- 총수신평잔_신탁
          , tot_dc_jan                              -- 총대출잔액
          , bnk_psn_dc_jan                          -- 개인대출잔액_은행계정
          , bnk_co_dc_jan                           -- 기업대출잔액_은행계정
          , bnk_tot_dc_jan                          -- 총대출잔액_은행계정
          , ta_tot_dc_jan                           -- 총대출잔액_신탁
          , tot_dc_avjn                             -- 총대출평잔
          , bnk_psn_dc_avjn                         -- 개인대출평잔_은행계정
          , bnk_co_dc_avjn                          -- 기업대출평잔_은행계정
          , bnk_tot_dc_avjn                         -- 총대출평잔_은행계정
          , ta_tot_dc_avjn                          -- 총대출평잔_신탁
          , fx_expt_amt                             -- 외환_수출금액
          , fx_impt_amt                             -- 외환_수입금액
          , fx_fnc_amt                              -- 외환_무역외금액
          , pl_amt                                  -- 손익금액
          , hdong_id                                -- 행정동id
          , x                                       -- x좌표
          , y                                       -- y좌표
          , dat_bas_dt                              -- 데이터기준일자
          , exchg_amt                               -- exchg_amt
          , remit_amt                               -- remit_amt
      from w0_shb.gis_br_kfb_mas_m
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
