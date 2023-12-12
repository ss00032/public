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


__author__     = "이일주"
__copyright__  = "Copyright 2021, Shinhan Datadam"
__credits__    = ["이일주"]
__version__    = "1.0"
__maintainer__ = "이일주"
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
pgm_id = 'ALLD_DA_AGMMLN_TG'

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
s3_file_prefix = f'ald_da_agmmln_/ald_da_agmmln_{execution_kst}'

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
        delete from l0_shl.da_agmmln
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shl.da_agmmln
                (
                  aws_ls_dt                               -- aws적재일시
                , clos_ym                                 -- 마감년월
                , lnco_no                                 -- 대출계약번호
                , link_info_no                            -- 연계정보번호
                , ha_n64                                  -- 해시_64
                , lon_ymd                                 -- 대출일자
                , lon_stat_cd                             -- 대출상태코드
                , lon_good_cd                             -- 대출상품코드
                , lnli_am                                 -- 대출한도금액
                , lon_am                                  -- 대출금액
                , lrem_am                                 -- 대출잔액
                , adix_lon_am                             -- 추가대출금액
                , frst_eprt_ymd                           -- 최초만기일자
                , eprt_ymd                                -- 만기일자
                , lon_peri_mon_cn                         -- 대출기간개월수
                , drpe_mon_cn                             -- 거치기간개월수
                , pypa_ordr                               -- 분납회차
                , cllt_kd_cd                              -- 담보종류코드
                , cllt_attr_cd                            -- 담보속성코드
                , lon_ryme_cd                             -- 대출상환방법코드
                , ptrp_sc_cd                              -- 분할상환구분코드
                , lon_itca_md_cd                          -- 대출이자계산방법코드
                , int_pam_cycl_cd                         -- 이자납입주기코드
                , prnc_pam_cycl_cd                        -- 원금납입주기코드
                , mtpa_dor_am                             -- 월납입금액
                , int_end_ymd                             -- 이자종료일자
                , pypa_end_ymd                            -- 분납종료일자
                , lspa_ymd                                -- 최종납입일자
                , inm_rt_apl_md_cd                        -- 금리적용방법코드
                , inm_rt_fxng_sc_cd                       -- 금리고정구분코드
                , tota_adm_rt_apl_ymd                     -- 총가산금리적용일자
                , adm_rt                                  -- 가산금리
                , tota_cnt                                -- 총횟수
                , pam_cnt                                 -- 납입횟수
                , lon_peri_day_tc                         -- 대출기간일수
                , lon_anac_cd                             -- 대출계정코드
                , fund_cd                                 -- 펀드코드
                , new_lon_am                              -- 신규대출금액
                , int_am                                  -- 이자금액
                , rep_am                                  -- 상환금액
                , fxte_rep_am                             -- 정기상환금액
                , vlnt_rep_am                             -- 임의상환금액
                , rdce_am                                 -- 감면금액
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , clos_ym                                 -- 마감년월
          , lnco_no                                 -- 대출계약번호
          , link_info_no                            -- 연계정보번호
          , ha_n64                                  -- 해시_64
          , lon_ymd                                 -- 대출일자
          , lon_stat_cd                             -- 대출상태코드
          , lon_good_cd                             -- 대출상품코드
          , lnli_am                                 -- 대출한도금액
          , lon_am                                  -- 대출금액
          , lrem_am                                 -- 대출잔액
          , adix_lon_am                             -- 추가대출금액
          , frst_eprt_ymd                           -- 최초만기일자
          , eprt_ymd                                -- 만기일자
          , lon_peri_mon_cn                         -- 대출기간개월수
          , drpe_mon_cn                             -- 거치기간개월수
          , pypa_ordr                               -- 분납회차
          , cllt_kd_cd                              -- 담보종류코드
          , cllt_attr_cd                            -- 담보속성코드
          , lon_ryme_cd                             -- 대출상환방법코드
          , ptrp_sc_cd                              -- 분할상환구분코드
          , lon_itca_md_cd                          -- 대출이자계산방법코드
          , int_pam_cycl_cd                         -- 이자납입주기코드
          , prnc_pam_cycl_cd                        -- 원금납입주기코드
          , mtpa_dor_am                             -- 월납입금액
          , int_end_ymd                             -- 이자종료일자
          , pypa_end_ymd                            -- 분납종료일자
          , lspa_ymd                                -- 최종납입일자
          , inm_rt_apl_md_cd                        -- 금리적용방법코드
          , inm_rt_fxng_sc_cd                       -- 금리고정구분코드
          , tota_adm_rt_apl_ymd                     -- 총가산금리적용일자
          , adm_rt                                  -- 가산금리
          , tota_cnt                                -- 총횟수
          , pam_cnt                                 -- 납입횟수
          , lon_peri_day_tc                         -- 대출기간일수
          , lon_anac_cd                             -- 대출계정코드
          , fund_cd                                 -- 펀드코드
          , new_lon_am                              -- 신규대출금액
          , int_am                                  -- 이자금액
          , rep_am                                  -- 상환금액
          , fxte_rep_am                             -- 정기상환금액
          , vlnt_rep_am                             -- 임의상환금액
          , rdce_am                                 -- 감면금액
      from w0_shl.da_agmmln
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
