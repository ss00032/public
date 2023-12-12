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
pgm_id = 'ILCD_SACDA0003_TG'

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
s3_file_prefix = f'icd_sacda0003_/icd_sacda0003_{execution_kst}'

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
        delete from l0_shc.sacda0003
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shc.sacda0003
                (
                  aws_ls_dt                               -- aws적재일시
                , acd_bj_crd_rpl_n                        -- 사고대상카드대체번호
                , acd_ag_d                                -- 사고할당일자
                , acd_tcd                                 -- 사고유형코드
                , acd_si_sn                               -- 사고전표순번
                , mct_n                                   -- 가맹점번호
                , ts_ucd                                  -- 거래결과코드
                , hdg_icd                                 -- 취급기관코드
                , acd_si_ccd                              -- 사고전표구분코드
                , acd_si_rid                              -- 사고전표접수일자
                , acd_si_pi_d                             -- 사고전표반영일자
                , acd_nr_rid                              -- 사고관리접수일자
                , acd_si_ps_d                             -- 사고전표처리일자
                , sls_ps_de_d                             -- 매출처리확정일자
                , sls_ps_de_pgm_nm                        -- 매출처리확정프로그램명
                , lsp_cf_f                                -- 매출전표확인여부
                , mct_cse_tcd                             -- 가맹점합의유형코드
                , mct_cse_d                               -- 가맹점합의일자
                , mct_cse_dam_cus_ecd                     -- 가맹점합의피해회원관계코드
                , nn_ir_f                                 -- 무이자여부
                , rl_tm_imt_ps_bj_f                       -- 실시간귀책처리대상여부
                , rl_tm_imt_ps_pi_d                       -- 실시간귀책처리반영일자
                , acd_ppa                                 -- 사고원금금액
                , acd_ir_at                               -- 사고이자금액
                , acd_fe_at                               -- 사고수수료금액
                , acd_dlg_at                              -- 사고연체료금액
                , acd_law_ep_at                           -- 사고법비용금액
                , acd_ei_at                               -- 사고선수금액
                , acd_uc_ir_at                            -- 사고미수이자금액
                , chc_f                                   -- 체크카드여부
                , chc_fd_bj_f                             -- 체크카드환불대상여부
                , chk_fd_ick_cf_d                         -- 체크환불담당자확인일자
                , chk_fd_ps_d                             -- 체크환불처리일자
                , iz_lm_rgd                               -- 예수한도등록일자
                , iz_lm_sn                                -- 예수한도순번
                , ni_rg_xct_id                            -- 최초등록수행id
                , ni_rg_dt                                -- 최초등록일시
                , ls_alt_xct_id                           -- 최종수정수행id
                , ls_alt_dt                               -- 최종수정일시
                , ls_ld_dt                                -- 최종적재일시
                , lsp_cf_tf                               -- 매출전표확인tf
                , nn_ir_tf                                -- 무이자tf
                , rl_tm_imt_ps_bj_tf                      -- 실시간귀책처리대상tf
                , chc_tf                                  -- 체크카드tf
                , chc_fd_bj_tf                            -- 체크카드환불대상tf
                , iz_rg_ncd                               -- 예수등록사유코드
                , iz_lma                                  -- 예수한도금액
                , iz_of_d                                 -- 예수정리일자
                , iz_of_sn                                -- 예수정리순번
                , iz_of_at                                -- 예수정리금액
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , acd_bj_crd_rpl_n                        -- 사고대상카드대체번호
          , acd_ag_d                                -- 사고할당일자
          , acd_tcd                                 -- 사고유형코드
          , acd_si_sn                               -- 사고전표순번
          , mct_n                                   -- 가맹점번호
          , ts_ucd                                  -- 거래결과코드
          , hdg_icd                                 -- 취급기관코드
          , acd_si_ccd                              -- 사고전표구분코드
          , acd_si_rid                              -- 사고전표접수일자
          , acd_si_pi_d                             -- 사고전표반영일자
          , acd_nr_rid                              -- 사고관리접수일자
          , acd_si_ps_d                             -- 사고전표처리일자
          , sls_ps_de_d                             -- 매출처리확정일자
          , sls_ps_de_pgm_nm                        -- 매출처리확정프로그램명
          , lsp_cf_f                                -- 매출전표확인여부
          , mct_cse_tcd                             -- 가맹점합의유형코드
          , mct_cse_d                               -- 가맹점합의일자
          , mct_cse_dam_cus_ecd                     -- 가맹점합의피해회원관계코드
          , nn_ir_f                                 -- 무이자여부
          , rl_tm_imt_ps_bj_f                       -- 실시간귀책처리대상여부
          , rl_tm_imt_ps_pi_d                       -- 실시간귀책처리반영일자
          , acd_ppa                                 -- 사고원금금액
          , acd_ir_at                               -- 사고이자금액
          , acd_fe_at                               -- 사고수수료금액
          , acd_dlg_at                              -- 사고연체료금액
          , acd_law_ep_at                           -- 사고법비용금액
          , acd_ei_at                               -- 사고선수금액
          , acd_uc_ir_at                            -- 사고미수이자금액
          , chc_f                                   -- 체크카드여부
          , chc_fd_bj_f                             -- 체크카드환불대상여부
          , chk_fd_ick_cf_d                         -- 체크환불담당자확인일자
          , chk_fd_ps_d                             -- 체크환불처리일자
          , iz_lm_rgd                               -- 예수한도등록일자
          , iz_lm_sn                                -- 예수한도순번
          , ni_rg_xct_id                            -- 최초등록수행id
          , ni_rg_dt                                -- 최초등록일시
          , ls_alt_xct_id                           -- 최종수정수행id
          , ls_alt_dt                               -- 최종수정일시
          , ls_ld_dt                                -- 최종적재일시
          , lsp_cf_tf                               -- 매출전표확인tf
          , nn_ir_tf                                -- 무이자tf
          , rl_tm_imt_ps_bj_tf                      -- 실시간귀책처리대상tf
          , chc_tf                                  -- 체크카드tf
          , chc_fd_bj_tf                            -- 체크카드환불대상tf
          , iz_rg_ncd                               -- 예수등록사유코드
          , iz_lma                                  -- 예수한도금액
          , iz_of_d                                 -- 예수정리일자
          , iz_of_sn                                -- 예수정리순번
          , iz_of_at                                -- 예수정리금액
      from w0_shc.sacda0003
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
