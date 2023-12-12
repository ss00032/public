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
pgm_id = 'ILCD_SWOAC0019_TG'

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
s3_file_prefix = f'icd_swoac0019_/icd_swoac0019_{execution_kst}'

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
        delete from l0_shc.swoac0019
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shc.swoac0019
                (
                  aws_ls_dt                               -- aws적재일시
                , ta_ym                                   -- 기준년월
                , lln_rid                                 -- 론대출접수일자
                , lln_rv_sn                               -- 론대출접수순번
                , sls_ts_n                                -- 매출거래번호
                , sgdmd                                   -- 그룹md번호
                , cus_ccd                                 -- 회원구분코드
                , vwy_ccd                                 -- 다원화구분코드
                , ctc_nr_n                                -- 약정관리번호
                , lns_d                                   -- 론상담일자
                , lns_sn                                  -- 론상담순번
                , lln_nr_n                                -- 론대출관리번호
                , lln_st_sn                               -- 론대출결제순번
                , lln_rv_tm                               -- 론대출접수시간
                , bsn_pd_cd                               -- 영업상품코드
                , lon_py_mcd                              -- 론상환방법코드
                , lon_rv_pcd                              -- 론접수경로코드
                , lon_pt_ccd                              -- 론신청구분코드
                , lon_pm_mcd                              -- 론지급방법코드
                , lln_ps_scd                              -- 론대출처리상태코드
                , lln_ps_su_ucd                           -- 론대출처리상태결과코드
                , lln_ms_cn                               -- 론대출개월수
                , lln_dfm_ms_cn                           -- 론대출거치개월수
                , lln_d                                   -- 론대출일자
                , lln_de_d                                -- 론대출확정일자
                , pm_d                                    -- 지급일자
                , lln_xp_d                                -- 론대출만기일자
                , lln_pyf_d                               -- 론대출완납일자
                , lln_pm_f                                -- 론대출지급여부
                , lln_ce_d                                -- 론대출취소일자
                , bil_dgt                                 -- 청구회차
                , bil_st_dd                               -- 청구결제일
                , ni_lln_st_d                             -- 최초론대출결제일자
                , ls_lln_st_d                             -- 최종론대출결제일자
                , lln_pt_at                               -- 론대출신청금액
                , lln_lma                                 -- 론대출한도금액
                , lln_rpl_psa                             -- 론대출대체처리금액
                , lln_de_at                               -- 론대출확정금액
                , cum_rca                                 -- 누적입금금액
                , lln_al                                  -- 론대출잔액
                , lln_pm_at                               -- 론대출지급금액
                , lln_m_rca                               -- 론대출월입금금액
                , lln_fea                                 -- 론대출수수료금액
                , lln_irrt                                -- 론대출이자율
                , lln_fe_rt                               -- 론대출수수료율
                , lln_dfm_irrt                            -- 론대출거치이자율
                , rv_hcd                                  -- 접수지점코드
                , ln_hcd                                  -- 대출지점코드
                , pm_hcd                                  -- 지급지점코드
                , rcp_mcd                                 -- 입금방법코드
                , nr_hcd                                  -- 관리지점코드
                , lsp_rid                                 -- 매출전표접수일자
                , lsp_n                                   -- 매출전표번호
                , bse_irrt                                -- 기본이자율
                , bse_fe_rt                               -- 기본수수료율
                , bse_dfm_irrt                            -- 기본거치이자율
                , xp_dd_awd_xr                            -- 만기일시상환율
                , lln_dbp_fea                             -- 론대출선취수수료금액
                , lln_dap_fea                             -- 론대출후취수수료금액
                , lln_smd_at                              -- 론대출인지대금액
                , ll_wor_at                               -- 담보가치금액
                , cdm_fea                                 -- cd기수수료금액
                , ats_lon_rq_icd                          -- 자동화기기론요청기관코드
                , lon_hpy_ccd                             -- 론우대구분코드
                , irt_ccd                                 -- 금리구분코드
                , mny_ppo_ccd                             -- 자금용도구분코드
                , lln_pt_ce_d                             -- 론대출신청취소일자
                , lln_ce_rn_ccd                           -- 론대출취소사유구분코드
                , ls_bil_gel_ccd                          -- 최종청구연락처구분코드
                , ls_spf_ap_mcd                           -- 최종명세서수령방법코드
                , lon_gy_ccd1                             -- 론보증구분코드1
                , lon_gy_ccd2                             -- 론보증구분코드2
                , lon_stu_sd_cd                           -- 론약관발송코드
                , cre_if_pus_ge_f                         -- 신용정보활용동의여부
                , bil_gr_sm_ms_cn                         -- 청구유예합계개월수
                , com_nc_rq_n                             -- 공통결재요청번호
                , mo_n                                    -- 판촉번호
                , xt_si_ccd                               -- 예외전표구분코드
                , cha_msg_tsn_rq_n                        -- 문자메시지송신요청번호
                , ol_bsn_pd_cd                            -- 구영업상품코드
                , tmn_id                                  -- 단말기id
                , crd_sv_n                                -- 카드서비스번호
                , mo_off_n                                -- 판촉오퍼번호
                , ni_rg_xct_id                            -- 최초등록수행id
                , ni_rg_dt                                -- 최초등록일시
                , ls_alt_xct_id                           -- 최종수정수행id
                , ls_alt_dt                               -- 최종수정일시
                , ls_ld_dt                                -- 최종적재일시
                , lln_pm_tf                               -- 론대출지급tf
                , cre_if_pus_ge_tf                        -- 신용정보활용동의tf
                , lln_de_tf                               -- 론대출확정tf
                , lln_ce_tf                               -- 론대출취소tf
                , rgl_ef_ni_ln_d                          -- 규제적용최초대출일자
                , irt_rdc_be_f_cd                         -- 금리인하가능여부코드
                , ic_ts_kcd                               -- ic거래종류코드
                , ln_reo_d                                -- 대출철회일자
                , rl_cm_evd_lmu_bj_ccd                    -- 실소득증빙한도상향대상구분코드
                , lln_pt_crd_rpl_n                        -- 론대출신청카드대체번호
                , cm_if_prv_ie_ccd                        -- 소득정보제공기관구분코드
                , lln_hpy_irrt                            -- 론대출우대이자율
                , lln_ssl_irrt                            -- 론대출특판이자율
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , ta_ym                                   -- 기준년월
          , lln_rid                                 -- 론대출접수일자
          , lln_rv_sn                               -- 론대출접수순번
          , sls_ts_n                                -- 매출거래번호
          , sgdmd                                   -- 그룹md번호
          , cus_ccd                                 -- 회원구분코드
          , vwy_ccd                                 -- 다원화구분코드
          , ctc_nr_n                                -- 약정관리번호
          , lns_d                                   -- 론상담일자
          , lns_sn                                  -- 론상담순번
          , lln_nr_n                                -- 론대출관리번호
          , lln_st_sn                               -- 론대출결제순번
          , lln_rv_tm                               -- 론대출접수시간
          , bsn_pd_cd                               -- 영업상품코드
          , lon_py_mcd                              -- 론상환방법코드
          , lon_rv_pcd                              -- 론접수경로코드
          , lon_pt_ccd                              -- 론신청구분코드
          , lon_pm_mcd                              -- 론지급방법코드
          , lln_ps_scd                              -- 론대출처리상태코드
          , lln_ps_su_ucd                           -- 론대출처리상태결과코드
          , lln_ms_cn                               -- 론대출개월수
          , lln_dfm_ms_cn                           -- 론대출거치개월수
          , lln_d                                   -- 론대출일자
          , lln_de_d                                -- 론대출확정일자
          , pm_d                                    -- 지급일자
          , lln_xp_d                                -- 론대출만기일자
          , lln_pyf_d                               -- 론대출완납일자
          , lln_pm_f                                -- 론대출지급여부
          , lln_ce_d                                -- 론대출취소일자
          , bil_dgt                                 -- 청구회차
          , bil_st_dd                               -- 청구결제일
          , ni_lln_st_d                             -- 최초론대출결제일자
          , ls_lln_st_d                             -- 최종론대출결제일자
          , lln_pt_at                               -- 론대출신청금액
          , lln_lma                                 -- 론대출한도금액
          , lln_rpl_psa                             -- 론대출대체처리금액
          , lln_de_at                               -- 론대출확정금액
          , cum_rca                                 -- 누적입금금액
          , lln_al                                  -- 론대출잔액
          , lln_pm_at                               -- 론대출지급금액
          , lln_m_rca                               -- 론대출월입금금액
          , lln_fea                                 -- 론대출수수료금액
          , lln_irrt                                -- 론대출이자율
          , lln_fe_rt                               -- 론대출수수료율
          , lln_dfm_irrt                            -- 론대출거치이자율
          , rv_hcd                                  -- 접수지점코드
          , ln_hcd                                  -- 대출지점코드
          , pm_hcd                                  -- 지급지점코드
          , rcp_mcd                                 -- 입금방법코드
          , nr_hcd                                  -- 관리지점코드
          , lsp_rid                                 -- 매출전표접수일자
          , lsp_n                                   -- 매출전표번호
          , bse_irrt                                -- 기본이자율
          , bse_fe_rt                               -- 기본수수료율
          , bse_dfm_irrt                            -- 기본거치이자율
          , xp_dd_awd_xr                            -- 만기일시상환율
          , lln_dbp_fea                             -- 론대출선취수수료금액
          , lln_dap_fea                             -- 론대출후취수수료금액
          , lln_smd_at                              -- 론대출인지대금액
          , ll_wor_at                               -- 담보가치금액
          , cdm_fea                                 -- cd기수수료금액
          , ats_lon_rq_icd                          -- 자동화기기론요청기관코드
          , lon_hpy_ccd                             -- 론우대구분코드
          , irt_ccd                                 -- 금리구분코드
          , mny_ppo_ccd                             -- 자금용도구분코드
          , lln_pt_ce_d                             -- 론대출신청취소일자
          , lln_ce_rn_ccd                           -- 론대출취소사유구분코드
          , ls_bil_gel_ccd                          -- 최종청구연락처구분코드
          , ls_spf_ap_mcd                           -- 최종명세서수령방법코드
          , lon_gy_ccd1                             -- 론보증구분코드1
          , lon_gy_ccd2                             -- 론보증구분코드2
          , lon_stu_sd_cd                           -- 론약관발송코드
          , cre_if_pus_ge_f                         -- 신용정보활용동의여부
          , bil_gr_sm_ms_cn                         -- 청구유예합계개월수
          , com_nc_rq_n                             -- 공통결재요청번호
          , mo_n                                    -- 판촉번호
          , xt_si_ccd                               -- 예외전표구분코드
          , cha_msg_tsn_rq_n                        -- 문자메시지송신요청번호
          , ol_bsn_pd_cd                            -- 구영업상품코드
          , tmn_id                                  -- 단말기id
          , crd_sv_n                                -- 카드서비스번호
          , mo_off_n                                -- 판촉오퍼번호
          , ni_rg_xct_id                            -- 최초등록수행id
          , ni_rg_dt                                -- 최초등록일시
          , ls_alt_xct_id                           -- 최종수정수행id
          , ls_alt_dt                               -- 최종수정일시
          , ls_ld_dt                                -- 최종적재일시
          , lln_pm_tf                               -- 론대출지급tf
          , cre_if_pus_ge_tf                        -- 신용정보활용동의tf
          , lln_de_tf                               -- 론대출확정tf
          , lln_ce_tf                               -- 론대출취소tf
          , rgl_ef_ni_ln_d                          -- 규제적용최초대출일자
          , irt_rdc_be_f_cd                         -- 금리인하가능여부코드
          , ic_ts_kcd                               -- ic거래종류코드
          , ln_reo_d                                -- 대출철회일자
          , rl_cm_evd_lmu_bj_ccd                    -- 실소득증빙한도상향대상구분코드
          , lln_pt_crd_rpl_n                        -- 론대출신청카드대체번호
          , cm_if_prv_ie_ccd                        -- 소득정보제공기관구분코드
          , lln_hpy_irrt                            -- 론대출우대이자율
          , lln_ssl_irrt                            -- 론대출특판이자율
      from w0_shc.swoac0019
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
