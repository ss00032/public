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


__author__     = "윤혁준"
__copyright__  = "Copyright 2021, Shinhan Datadam"
__credits__    = ["윤혁준"]
__version__    = "1.0"
__maintainer__ = "윤혁준"
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
pgm_id = 'ILCD_SCOMB0016_TG'

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
s3_file_prefix = f'icd_scomb0016_/icd_scomb0016_{execution_kst}'

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
        delete from l0_shc.scomb0016
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shc.scomb0016
                (
                  aws_ls_dt                               -- aws적재일시
                , bsn_pd_cd                               -- 영업상품코드
                , bg_cz_bsn_pd_cd                         -- 대분류영업상품코드
                , mi_cz_bsn_pd_cd                         -- 중분류영업상품코드
                , sl_cz_bsn_pd_cd                         -- 소분류영업상품코드
                , sls_tol_bj_pd_f                         -- 매출집계대상상품여부
                , pd_tol_zcd                              -- 상품집계분류코드
                , cre_crd_pd_f                            -- 신용카드상품여부
                , chk_cre_pd_f                            -- 체크신용상품여부
                , mns_cv_pd_f                             -- 마이너스현금서비스상품여부
                , lon_ucs_pd_f                            -- 론비회원상품여부
                , vv_pd_f                                 -- 리볼빙상품여부
                , mtg_pd_f                                -- 모기지론상품여부
                , bcc_pd_f                                -- bc카드상품여부
                , shb_lon_pd_f                            -- 신한은행론상품여부
                , lon_tol_zcd                             -- 론집계분류코드
                , pa_pd_zcd                               -- pa상품분류코드
                , vs_pd_f                                 -- 해외상품여부
                , erms_pd_ccd                             -- erms상품구분코드
                , mst_sgm_ccd                             -- 마스터세그먼트구분코드
                , rcr_lon_f                               -- 회복론여부
                , mns_lon_f                               -- 마이너스론여부
                , chk_pd_f                                -- 체크상품여부
                , erms_bj_f                               -- erms대상여부
                , bsl_pd_zcd                              -- 바젤상품분류코드
                , df_pd_f                                 -- 대환상품여부
                , lm_un_tka_pd_f                          -- 한도미차감상품여부
                , di_pd_zcd                               -- 매각상품분류코드
                , cre_pd_zcd                              -- 신용상품분류코드
                , cre_tsi_pd_zcd                          -- 신용전이상품분류코드
                , ni_rg_xct_id                            -- 최초등록수행id
                , ni_rg_dt                                -- 최초등록일시
                , ls_alt_xct_id                           -- 최종수정수행id
                , ls_alt_dt                               -- 최종수정일시
                , ls_ld_dt                                -- 최종적재일시
                , sls_tol_bj_pd_tf                        -- 매출집계대상상품tf
                , cre_crd_pd_tf                           -- 신용카드상품tf
                , chk_cre_pd_tf                           -- 체크신용상품tf
                , mns_cv_pd_tf                            -- 마이너스현금서비스상품tf
                , lon_ucs_pd_tf                           -- 론비회원상품tf
                , vv_pd_tf                                -- 리볼빙상품tf
                , mtg_pd_tf                               -- 모기지론상품tf
                , bcc_pd_tf                               -- bc카드상품tf
                , shb_lon_pd_tf                           -- 신한은행론상품tf
                , vs_pd_tf                                -- 해외상품tf
                , rcr_lon_tf                              -- 회복론tf
                , mns_lon_tf                              -- 마이너스론tf
                , chk_pd_tf                               -- 체크상품tf
                , erms_bj_tf                              -- erms대상tf
                , df_pd_tf                                -- 대환상품tf
                , lm_un_tka_pd_tf                         -- 한도미차감상품tf
                , cus_lon_tf                              -- 회원론tf
                , sls_apv_ti_f                            -- 매출승인전송여부
                , sls_apv_ti_tf                           -- 매출승인전송tf
                , sls_lm_un_tka_f                         -- 매출한도미차감여부
                , sls_lm_un_tka_tf                        -- 매출한도미차감tf
                , rcp_apv_ti_f                            -- 입금승인전송여부
                , rcp_apv_ti_tf                           -- 입금승인전송tf
                , rcp_xt_lm_ccd                           -- 입금예외한도구분코드
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , bsn_pd_cd                               -- 영업상품코드
          , bg_cz_bsn_pd_cd                         -- 대분류영업상품코드
          , mi_cz_bsn_pd_cd                         -- 중분류영업상품코드
          , sl_cz_bsn_pd_cd                         -- 소분류영업상품코드
          , sls_tol_bj_pd_f                         -- 매출집계대상상품여부
          , pd_tol_zcd                              -- 상품집계분류코드
          , cre_crd_pd_f                            -- 신용카드상품여부
          , chk_cre_pd_f                            -- 체크신용상품여부
          , mns_cv_pd_f                             -- 마이너스현금서비스상품여부
          , lon_ucs_pd_f                            -- 론비회원상품여부
          , vv_pd_f                                 -- 리볼빙상품여부
          , mtg_pd_f                                -- 모기지론상품여부
          , bcc_pd_f                                -- bc카드상품여부
          , shb_lon_pd_f                            -- 신한은행론상품여부
          , lon_tol_zcd                             -- 론집계분류코드
          , pa_pd_zcd                               -- pa상품분류코드
          , vs_pd_f                                 -- 해외상품여부
          , erms_pd_ccd                             -- erms상품구분코드
          , mst_sgm_ccd                             -- 마스터세그먼트구분코드
          , rcr_lon_f                               -- 회복론여부
          , mns_lon_f                               -- 마이너스론여부
          , chk_pd_f                                -- 체크상품여부
          , erms_bj_f                               -- erms대상여부
          , bsl_pd_zcd                              -- 바젤상품분류코드
          , df_pd_f                                 -- 대환상품여부
          , lm_un_tka_pd_f                          -- 한도미차감상품여부
          , di_pd_zcd                               -- 매각상품분류코드
          , cre_pd_zcd                              -- 신용상품분류코드
          , cre_tsi_pd_zcd                          -- 신용전이상품분류코드
          , ni_rg_xct_id                            -- 최초등록수행id
          , ni_rg_dt                                -- 최초등록일시
          , ls_alt_xct_id                           -- 최종수정수행id
          , ls_alt_dt                               -- 최종수정일시
          , ls_ld_dt                                -- 최종적재일시
          , sls_tol_bj_pd_tf                        -- 매출집계대상상품tf
          , cre_crd_pd_tf                           -- 신용카드상품tf
          , chk_cre_pd_tf                           -- 체크신용상품tf
          , mns_cv_pd_tf                            -- 마이너스현금서비스상품tf
          , lon_ucs_pd_tf                           -- 론비회원상품tf
          , vv_pd_tf                                -- 리볼빙상품tf
          , mtg_pd_tf                               -- 모기지론상품tf
          , bcc_pd_tf                               -- bc카드상품tf
          , shb_lon_pd_tf                           -- 신한은행론상품tf
          , vs_pd_tf                                -- 해외상품tf
          , rcr_lon_tf                              -- 회복론tf
          , mns_lon_tf                              -- 마이너스론tf
          , chk_pd_tf                               -- 체크상품tf
          , erms_bj_tf                              -- erms대상tf
          , df_pd_tf                                -- 대환상품tf
          , lm_un_tka_pd_tf                         -- 한도미차감상품tf
          , cus_lon_tf                              -- 회원론tf
          , sls_apv_ti_f                            -- 매출승인전송여부
          , sls_apv_ti_tf                           -- 매출승인전송tf
          , sls_lm_un_tka_f                         -- 매출한도미차감여부
          , sls_lm_un_tka_tf                        -- 매출한도미차감tf
          , rcp_apv_ti_f                            -- 입금승인전송여부
          , rcp_apv_ti_tf                           -- 입금승인전송tf
          , rcp_xt_lm_ccd                           -- 입금예외한도구분코드
      from w0_shc.scomb0016
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
