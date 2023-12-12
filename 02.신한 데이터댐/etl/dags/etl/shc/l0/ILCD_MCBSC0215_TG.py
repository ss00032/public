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
pgm_id = 'ILCD_MCBSC0215_TG'

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
s3_file_prefix = f'icd_mcbsc0215_/icd_mcbsc0215_{execution_kst}'

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
        delete from l0_shc.mcbsc0215
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shc.mcbsc0215
                (
                  aws_ls_dt                               -- aws적재일시
                , ta_ym                                   -- 기준년월
                , sgdmd                                   -- 그룹md번호
                , ln_n                                    -- 대출번호
                , ns_fru_tol_sn                           -- 할부실적집계순번
                , ls_ld_dt                                -- 최종적재일시
                , ln_d                                    -- 대출일자
                , afo_n                                   -- 제휴사번호
                , afs_n                                   -- 제휴점번호
                , rv_hcd                                  -- 접수지점코드
                , ln_hcd                                  -- 대출지점코드
                , nn_ir_ccd                               -- 무이자구분코드
                , lna                                     -- 대출금액
                , pm_at                                   -- 지급금액
                , ns_fru_fst_py_sn_irrt                   -- 할부실적제1상환순번이자율
                , ns_fru_sec_py_sn_irrt                   -- 할부실적제2상환순번이자율
                , un_acz_ira                              -- 미실현이자금액
                , afs_fe_rt                               -- 제휴점수수료율
                , afs_fea                                 -- 제휴점수수료금액
                , ln_trm_ms_cn                            -- 대출기간개월수
                , rcp_mcd                                 -- 입금방법코드
                , fnc_cln_ccd                             -- 오토금융고객구분코드
                , ns_fru_lon_ll_cd                        -- 할부실적론담보코드
                , fnc_py_mti_ccd                          -- 오토금융상환세부구분코드
                , ni_st_d                                 -- 최초결제일자
                , smd_at                                  -- 인지대금액
                , ei_rt                                   -- 선수율
                , tow_n                                   -- 출고번호
                , fnc_pd_dl_n                             -- 오토금융상품상세번호
                , bsn_pd_cd                               -- 영업상품코드
                , mo_ccd                                  -- 판촉구분코드
                , aup_f                                   -- 오토플러스여부
                , pv_py_mcd                               -- 대표상환방법코드
                , gr_at                                   -- 유예금액
                , sug_rbs_f                               -- 대위변제여부
                , aup_crd_iss_cd                          -- 오토플러스카드발급코드
                , ca_nr_n                                 -- ca관리번호
                , sls_ts_n                                -- 매출거래번호
                , ca_fe_rt                                -- ca수수료율
                , li_ey_ict_rt1                           -- 판매사원인센티브율1
                , ns_fru_acn_si_d                         -- 할부실적회계전표일자
                , iss_ccd                                 -- 발급구분코드
                , aup_ei_at                               -- 오토플러스선수금액
                , li_ey_ad_ict_rt                         -- 판매사원추가인센티브율
                , fnc_mkt_jcd                             -- 오토금융마케팅대상코드
                , irr                                     -- 내부수익율
                , as_sm_sc                                -- as합계점수
                , whx_ict_at                              -- 원천징수인센티브금액
                , pfp_at                                  -- 성과급금액
                , ns_fru_ict_inc_irr                      -- 할부실적인센티브포함내부수익율
                , ns_fru_ict_un_inc_irr                   -- 할부실적인센티브미포함내부수익율
                , rlt_irrt                                -- 실제이자율
                , vh_pri_at                               -- 차량가격금액
                , irg_pt_at                               -- 보험료신청금액
                , inv_irg_at                              -- 산업재보험료금액
                , uca_nr_afs_n                            -- 중고차관리제휴점번호
                , fhn_bsn_tf                              -- 직접영업tf
                , cre_qy_ta_irr                           -- 신용조회기준내부수익율
                , ctc_evl_ta_irr                          -- 약정심사기준내부수익율
                , be_ict_rt                               -- 가능인센티브율
                , dle_mkt_ep_rt                           -- 딜러마케팅비용율
                , ddc_unn_gy_ucd                          -- 공제조합보증결과코드
                , gyf_rt                                  -- 보증료율
                , gyf_at                                  -- 보증료금액
                , gy_afo_n                                -- 보증제휴사번호
                , as_gcd                                  -- as등급코드
                , ncr_li_acv_fea                          -- 신차판매활성수수료금액
                , ns_fru_oth_epa                          -- 할부실적부대비용금액
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , ta_ym                                   -- 기준년월
          , sgdmd                                   -- 그룹md번호
          , ln_n                                    -- 대출번호
          , ns_fru_tol_sn                           -- 할부실적집계순번
          , ls_ld_dt                                -- 최종적재일시
          , ln_d                                    -- 대출일자
          , afo_n                                   -- 제휴사번호
          , afs_n                                   -- 제휴점번호
          , rv_hcd                                  -- 접수지점코드
          , ln_hcd                                  -- 대출지점코드
          , nn_ir_ccd                               -- 무이자구분코드
          , lna                                     -- 대출금액
          , pm_at                                   -- 지급금액
          , ns_fru_fst_py_sn_irrt                   -- 할부실적제1상환순번이자율
          , ns_fru_sec_py_sn_irrt                   -- 할부실적제2상환순번이자율
          , un_acz_ira                              -- 미실현이자금액
          , afs_fe_rt                               -- 제휴점수수료율
          , afs_fea                                 -- 제휴점수수료금액
          , ln_trm_ms_cn                            -- 대출기간개월수
          , rcp_mcd                                 -- 입금방법코드
          , fnc_cln_ccd                             -- 오토금융고객구분코드
          , ns_fru_lon_ll_cd                        -- 할부실적론담보코드
          , fnc_py_mti_ccd                          -- 오토금융상환세부구분코드
          , ni_st_d                                 -- 최초결제일자
          , smd_at                                  -- 인지대금액
          , ei_rt                                   -- 선수율
          , tow_n                                   -- 출고번호
          , fnc_pd_dl_n                             -- 오토금융상품상세번호
          , bsn_pd_cd                               -- 영업상품코드
          , mo_ccd                                  -- 판촉구분코드
          , aup_f                                   -- 오토플러스여부
          , pv_py_mcd                               -- 대표상환방법코드
          , gr_at                                   -- 유예금액
          , sug_rbs_f                               -- 대위변제여부
          , aup_crd_iss_cd                          -- 오토플러스카드발급코드
          , ca_nr_n                                 -- ca관리번호
          , sls_ts_n                                -- 매출거래번호
          , ca_fe_rt                                -- ca수수료율
          , li_ey_ict_rt1                           -- 판매사원인센티브율1
          , ns_fru_acn_si_d                         -- 할부실적회계전표일자
          , iss_ccd                                 -- 발급구분코드
          , aup_ei_at                               -- 오토플러스선수금액
          , li_ey_ad_ict_rt                         -- 판매사원추가인센티브율
          , fnc_mkt_jcd                             -- 오토금융마케팅대상코드
          , irr                                     -- 내부수익율
          , as_sm_sc                                -- as합계점수
          , whx_ict_at                              -- 원천징수인센티브금액
          , pfp_at                                  -- 성과급금액
          , ns_fru_ict_inc_irr                      -- 할부실적인센티브포함내부수익율
          , ns_fru_ict_un_inc_irr                   -- 할부실적인센티브미포함내부수익율
          , rlt_irrt                                -- 실제이자율
          , vh_pri_at                               -- 차량가격금액
          , irg_pt_at                               -- 보험료신청금액
          , inv_irg_at                              -- 산업재보험료금액
          , uca_nr_afs_n                            -- 중고차관리제휴점번호
          , fhn_bsn_tf                              -- 직접영업tf
          , cre_qy_ta_irr                           -- 신용조회기준내부수익율
          , ctc_evl_ta_irr                          -- 약정심사기준내부수익율
          , be_ict_rt                               -- 가능인센티브율
          , dle_mkt_ep_rt                           -- 딜러마케팅비용율
          , ddc_unn_gy_ucd                          -- 공제조합보증결과코드
          , gyf_rt                                  -- 보증료율
          , gyf_at                                  -- 보증료금액
          , gy_afo_n                                -- 보증제휴사번호
          , as_gcd                                  -- as등급코드
          , ncr_li_acv_fea                          -- 신차판매활성수수료금액
          , ns_fru_oth_epa                          -- 할부실적부대비용금액
      from w0_shc.mcbsc0215
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
