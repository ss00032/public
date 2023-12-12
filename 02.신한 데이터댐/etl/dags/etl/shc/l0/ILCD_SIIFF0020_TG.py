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
pgm_id = 'ILCD_SIIFF0020_TG'

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
s3_file_prefix = f'icd_siiff0020_/icd_siiff0020_{execution_kst}'

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
        delete from l0_shc.siiff0020
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shc.siiff0020
                (
                  aws_ls_dt                               -- aws적재일시
                , fms_vs_acd_uqe_n                        -- fms해외사고고유번호
                , fms_acd_sn                              -- fms사고순번
                , fms_apv_n                               -- fms승인번호
                , ts_d                                    -- 거래일자
                , ts_tm                                   -- 거래시간
                , mti_ts_tm                               -- 세부거래시간
                , fms_ts_ctm_key_n                        -- fms거래암호key번호
                , apva                                    -- 승인금액
                , apv_fca                                 -- 승인외화금액
                , fms_apv_ucd                             -- fms승인결과코드
                , fms_apv_ce_cd                           -- fms승인취소코드
                , apv_ce_tf                               -- 승인취소tf
                , fms_mct_n                               -- fms가맹점번호
                , mct_nm                                  -- 가맹점명
                , mct_brn                                 -- 가맹점사업자등록번호
                , mct_ry_cd                               -- 가맹점업종코드
                , mct_ry_nm                               -- 가맹점업종명
                , fms_vs_mct_tcc_cd                       -- fms해외가맹점tcc코드
                , mct_pon                                 -- 가맹점전화번호
                , pos_nty_moe                             -- pos_entry_mode
                , ie_nm                                   -- 기관명
                , bn_nm                                   -- 지점명
                , apv_tmn_id                              -- 승인단말기id
                , bn_pon                                  -- 지점전화번호
                , fms_cty_nm                              -- fms도시명
                , fms_dms_vs_ccd                          -- fms국내해외구분코드
                , apv_ntn_cd                              -- 승인국가코드
                , fms_ts_put_fcd                          -- fms거래입력형태코드
                , nn_apv_tf                               -- 무승인tf
                , aut_rp_o_se_tf                          -- 승인계수신본인사용tf
                , o_se_tf                                 -- 본인사용tf
                , acd_at_dol_tcd                          -- 사고금액분담유형코드
                , ls_dcs_ucd1                             -- 최종판정결과코드1
                , cus_ima                                 -- 회원귀책금액
                , mct_ima                                 -- 가맹점귀책금액
                , fur_ima                                 -- 부정사용자귀책금액
                , los_om_at                               -- 손실보상금액
                , o_se_ccd                                -- 본인사용구분코드
                , ls_dcs_ucd2                             -- 최종판정결과코드2
                , acd_tf                                  -- 사고tf
                , fms_dtc_tf                              -- fms적발tf
                , fms_oms_tf                              -- fms누락tf
                , fms_dtc_dgt                             -- fms적발회차
                , fms_oms_dgt                             -- fms누락회차
                , fms_vs_acd_tc_tf                        -- fms해외사고이관tf
                , fms_alrt_or_vl                          -- fms_alert발생값
                , ma_lma                                  -- 잔여한도금액
                , ti_tf                                   -- 전송tf
                , ru_rgd                                  -- 결과등록일자
                , ru_rgm                                  -- 결과등록시간
                , alt_d                                   -- 수정일자
                , alt_tm                                  -- 수정시간
                , ls_ld_dt                                -- 최종적재일시
                , crd_rpl_n                               -- 카드대체번호
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , fms_vs_acd_uqe_n                        -- fms해외사고고유번호
          , fms_acd_sn                              -- fms사고순번
          , fms_apv_n                               -- fms승인번호
          , ts_d                                    -- 거래일자
          , ts_tm                                   -- 거래시간
          , mti_ts_tm                               -- 세부거래시간
          , fms_ts_ctm_key_n                        -- fms거래암호key번호
          , apva                                    -- 승인금액
          , apv_fca                                 -- 승인외화금액
          , fms_apv_ucd                             -- fms승인결과코드
          , fms_apv_ce_cd                           -- fms승인취소코드
          , apv_ce_tf                               -- 승인취소tf
          , fms_mct_n                               -- fms가맹점번호
          , mct_nm                                  -- 가맹점명
          , mct_brn                                 -- 가맹점사업자등록번호
          , mct_ry_cd                               -- 가맹점업종코드
          , mct_ry_nm                               -- 가맹점업종명
          , fms_vs_mct_tcc_cd                       -- fms해외가맹점tcc코드
          , mct_pon                                 -- 가맹점전화번호
          , pos_nty_moe                             -- pos_entry_mode
          , ie_nm                                   -- 기관명
          , bn_nm                                   -- 지점명
          , apv_tmn_id                              -- 승인단말기id
          , bn_pon                                  -- 지점전화번호
          , fms_cty_nm                              -- fms도시명
          , fms_dms_vs_ccd                          -- fms국내해외구분코드
          , apv_ntn_cd                              -- 승인국가코드
          , fms_ts_put_fcd                          -- fms거래입력형태코드
          , nn_apv_tf                               -- 무승인tf
          , aut_rp_o_se_tf                          -- 승인계수신본인사용tf
          , o_se_tf                                 -- 본인사용tf
          , acd_at_dol_tcd                          -- 사고금액분담유형코드
          , ls_dcs_ucd1                             -- 최종판정결과코드1
          , cus_ima                                 -- 회원귀책금액
          , mct_ima                                 -- 가맹점귀책금액
          , fur_ima                                 -- 부정사용자귀책금액
          , los_om_at                               -- 손실보상금액
          , o_se_ccd                                -- 본인사용구분코드
          , ls_dcs_ucd2                             -- 최종판정결과코드2
          , acd_tf                                  -- 사고tf
          , fms_dtc_tf                              -- fms적발tf
          , fms_oms_tf                              -- fms누락tf
          , fms_dtc_dgt                             -- fms적발회차
          , fms_oms_dgt                             -- fms누락회차
          , fms_vs_acd_tc_tf                        -- fms해외사고이관tf
          , fms_alrt_or_vl                          -- fms_alert발생값
          , ma_lma                                  -- 잔여한도금액
          , ti_tf                                   -- 전송tf
          , ru_rgd                                  -- 결과등록일자
          , ru_rgm                                  -- 결과등록시간
          , alt_d                                   -- 수정일자
          , alt_tm                                  -- 수정시간
          , ls_ld_dt                                -- 최종적재일시
          , crd_rpl_n                               -- 카드대체번호
      from w0_shc.siiff0020
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
