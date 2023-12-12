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
pgm_id = 'ALCD_SMCTA3001_TG'

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
s3_file_prefix = f'acd_smcta3001_/acd_smcta3001_{execution_kst}'

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
        delete from l0_shc.smcta3001
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shc.smcta3001
                (
                  aws_ls_dt                               -- aws적재일시
                , mct_ry_cd                               -- 가맹점업종코드
                , mct_ry_nm                               -- 가맹점업종명
                , mct_eng_ry_nm                           -- 가맹점영문업종명
                , ta_sao_at                               -- 기준색출금액
                , vst_bj_f                                -- 방문대상여부
                , bad_sao_bj_f                            -- 불량색출대상여부
                , mct_bse_fe_rt                           -- 가맹점기본수수료율
                , mct_chk_fe_rt                           -- 가맹점체크수수료율
                , nns_mct_bse_fe_rt                       -- nns가맹점기본수수료율
                , nns_mct_chc_fe_rt                       -- nns가맹점체크카드수수료율
                , ns_fe_rt                                -- 할부수수료율
                , ec_fe_rt                                -- 정산수수료율
                , mct_ais_ccd                             -- 가맹점실사구분코드
                , sup_bad_f                               -- 우량불량여부
                , bsn_gcd                                 -- 영업등급코드
                , mct_ry_zcd                              -- 가맹점업종분류코드
                , bg_cz_bsn_pd_cd                         -- 대분류영업상품코드
                , gss_ccd                                 -- 주유소구분코드
                , sd_sdc_kcd                              -- 발송문서종류코드
                , mct_sdl_cd                              -- 가맹점발송처코드
                , pio_ais_f                               -- 사전실사여부
                , jn_pt_ry_zcd                            -- 가입신청업종분류코드
                , cu_ec_ry_cd                             -- 공동이용정산업종코드
                , to_apv_lma                              -- 총승인한도금액
                , min_ns_ms_cn                            -- 최소할부개월수
                , max_ns_ms_cn                            -- 최대할부개월수
                , sdd_idy_cz_ry_cd                        -- 표준산업분류업종코드
                , aml_mct_gp_cd                           -- 자금세탁방지가맹점그룹코드
                , ry_un_se_f                              -- 업종미사용여부
                , au_de_bj_ry_f                           -- 자동확정대상업종여부
                , lm_mf_bj_ry_f                           -- 한도변경대상업종여부
                , ns_lm_ry_f                              -- 할부한도업종여부
                , cm_ddc_xl_f                             -- 소득공제제외여부
                , obd_cib_bj_ry_f                         -- 아웃바운드콜대상업종여부
                , acs_ry_ccd                              -- acs업종구분코드
                , ni_rg_xct_id                            -- 최초등록수행id
                , ni_rg_dt                                -- 최초등록일시
                , ls_alt_xct_id                           -- 최종수정수행id
                , ls_alt_dt                               -- 최종수정일시
                , ls_ld_dt                                -- 최종적재일시
                , vst_bj_tf                               -- 방문대상tf
                , bad_sao_bj_tf                           -- 불량색출대상tf
                , sup_bad_tf                              -- 우량불량tf
                , pio_ais_tf                              -- 사전실사tf
                , ry_un_se_tf                             -- 업종미사용tf
                , au_de_bj_ry_tf                          -- 자동확정대상업종tf
                , lm_mf_bj_ry_tf                          -- 한도변경대상업종tf
                , ns_lm_ry_tf                             -- 할부한도업종tf
                , cm_ddc_xl_tf                            -- 소득공제제외tf
                , obd_cib_bj_ry_tf                        -- 아웃바운드콜대상업종tf
                , mct_ry_gp_cd                            -- 가맹점업종그룹코드
                , el_tf                                   -- 삭제tf
                , el_d                                    -- 삭제일자
                , elg_bse_fe_rt                           -- 적격기본수수료율
                , elg_chk_fe_rt                           -- 적격체크수수료율
                , rype_sdd_cre_fe_rt                      -- 업종별표준신용수수료율
                , rype_sdd_chk_fe_rt                      -- 업종별표준체크수수료율
                , ry_aq_ce_rsk_kcd                        -- 업종매입취소위험종류코드
                , aq_ce_ry_rsk_ef_ncd                     -- 매입취소업종위험적용사유코드
                , ns_lma                                  -- 할부한도금액
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , mct_ry_cd                               -- 가맹점업종코드
          , mct_ry_nm                               -- 가맹점업종명
          , mct_eng_ry_nm                           -- 가맹점영문업종명
          , ta_sao_at                               -- 기준색출금액
          , vst_bj_f                                -- 방문대상여부
          , bad_sao_bj_f                            -- 불량색출대상여부
          , mct_bse_fe_rt                           -- 가맹점기본수수료율
          , mct_chk_fe_rt                           -- 가맹점체크수수료율
          , nns_mct_bse_fe_rt                       -- nns가맹점기본수수료율
          , nns_mct_chc_fe_rt                       -- nns가맹점체크카드수수료율
          , ns_fe_rt                                -- 할부수수료율
          , ec_fe_rt                                -- 정산수수료율
          , mct_ais_ccd                             -- 가맹점실사구분코드
          , sup_bad_f                               -- 우량불량여부
          , bsn_gcd                                 -- 영업등급코드
          , mct_ry_zcd                              -- 가맹점업종분류코드
          , bg_cz_bsn_pd_cd                         -- 대분류영업상품코드
          , gss_ccd                                 -- 주유소구분코드
          , sd_sdc_kcd                              -- 발송문서종류코드
          , mct_sdl_cd                              -- 가맹점발송처코드
          , pio_ais_f                               -- 사전실사여부
          , jn_pt_ry_zcd                            -- 가입신청업종분류코드
          , cu_ec_ry_cd                             -- 공동이용정산업종코드
          , to_apv_lma                              -- 총승인한도금액
          , min_ns_ms_cn                            -- 최소할부개월수
          , max_ns_ms_cn                            -- 최대할부개월수
          , sdd_idy_cz_ry_cd                        -- 표준산업분류업종코드
          , aml_mct_gp_cd                           -- 자금세탁방지가맹점그룹코드
          , ry_un_se_f                              -- 업종미사용여부
          , au_de_bj_ry_f                           -- 자동확정대상업종여부
          , lm_mf_bj_ry_f                           -- 한도변경대상업종여부
          , ns_lm_ry_f                              -- 할부한도업종여부
          , cm_ddc_xl_f                             -- 소득공제제외여부
          , obd_cib_bj_ry_f                         -- 아웃바운드콜대상업종여부
          , acs_ry_ccd                              -- acs업종구분코드
          , ni_rg_xct_id                            -- 최초등록수행id
          , ni_rg_dt                                -- 최초등록일시
          , ls_alt_xct_id                           -- 최종수정수행id
          , ls_alt_dt                               -- 최종수정일시
          , ls_ld_dt                                -- 최종적재일시
          , vst_bj_tf                               -- 방문대상tf
          , bad_sao_bj_tf                           -- 불량색출대상tf
          , sup_bad_tf                              -- 우량불량tf
          , pio_ais_tf                              -- 사전실사tf
          , ry_un_se_tf                             -- 업종미사용tf
          , au_de_bj_ry_tf                          -- 자동확정대상업종tf
          , lm_mf_bj_ry_tf                          -- 한도변경대상업종tf
          , ns_lm_ry_tf                             -- 할부한도업종tf
          , cm_ddc_xl_tf                            -- 소득공제제외tf
          , obd_cib_bj_ry_tf                        -- 아웃바운드콜대상업종tf
          , mct_ry_gp_cd                            -- 가맹점업종그룹코드
          , el_tf                                   -- 삭제tf
          , el_d                                    -- 삭제일자
          , elg_bse_fe_rt                           -- 적격기본수수료율
          , elg_chk_fe_rt                           -- 적격체크수수료율
          , rype_sdd_cre_fe_rt                      -- 업종별표준신용수수료율
          , rype_sdd_chk_fe_rt                      -- 업종별표준체크수수료율
          , ry_aq_ce_rsk_kcd                        -- 업종매입취소위험종류코드
          , aq_ce_ry_rsk_ef_ncd                     -- 매입취소업종위험적용사유코드
          , ns_lma                                  -- 할부한도금액
      from w0_shc.smcta3001
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
