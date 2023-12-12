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


__author__     = "임효석"
__copyright__  = "Copyright 2021, Shinhan Datadam"
__credits__    = ["임효석"]
__version__    = "1.0"
__maintainer__ = "임효석"
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
pgm_id = 'ALCD_MTDIA5008_TG'

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
s3_file_prefix = f'acd_mtdia5008_/acd_mtdia5008_{execution_kst}'

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
        delete from l0_shc.mtdia5008
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shc.mtdia5008
                (
                  aws_ls_dt                               -- aws적재일시
                , crd_rpl_n                               -- 카드대체번호
                , ls_ld_dt                                -- 최종적재일시
                , sgdmd                                   -- 그룹md번호
                , sgdci                                   -- ci번호
                , sgdmd_dfr_n                             -- 고객구별그룹md번호
                , sgdci_dfr_n                             -- 고객구별ci번호
                , cry_k_sgdmd                             -- 소지자그룹md번호
                , cry_k_sgdci                             -- 소지자ci번호
                , cus_ccd                                 -- 회원구분코드
                , crd_tcd                                 -- 카드유형코드
                , o_fml_ccd                               -- 본인가족구분코드
                , crd_su_tcd                              -- 카드상태유형코드
                , bsu_n                                   -- 사업장번호
                , st_sn                                   -- 결제순번
                , crd_ico_ccd                             -- 카드발급사구분코드
                , crd_de_d                                -- 카드확정일자
                , isc_crd_de_d                            -- 기명카드확정일자
                , crd_vd_ym                               -- 카드유효년월
                , crd_pd_n                                -- 카드상품번호
                , crd_pdg_tcd                             -- 카드상품군유형코드
                , cct1_crd_pd_n                           -- 개념1카드상품번호
                , cct2_crd_pd_n                           -- 개념2카드상품번호
                , se_rst_scd                              -- 사용제한상태코드
                , crd_acc_se_rst_scd                      -- 카드계정사용제한상태코드
                , cus_se_rst_scd                          -- 회원사용제한상태코드
                , crp_bsu_se_rst_scd                      -- 법인사업장사용제한상태코드
                , crp_usk_se_rst_scd                      -- 법인이용자사용제한상태코드
                , cln_se_rst_scd                          -- 고객사용제한상태코드
                , cus_lm_ga_ccd                           -- 회원한도등급구분코드
                , o_crd_tf                                -- 본인카드tf
                , fml_crd_tf                              -- 가족카드tf
                , cre_crd_tf                              -- 신용카드tf
                , chc_tf                                  -- 체크카드tf
                , bcc_tf                                  -- bc카드tf
                , wf_crd_tf                               -- 복지카드tf
                , trf_crd_tf                              -- 교통카드tf
                , crd_afl_tf                              -- 카드제휴tf
                , gov_phc_tf                              -- 정부구매카드tf
                , fnc_crd_tf                              -- 오토금융카드tf
                , bsi_crd_tf                              -- 비지니스카드tf
                , afe_exo_tf                              -- 연회비면제tf
                , tc_tf                                   -- 이관tf
                , el_crd_tf                               -- 삭제카드tf
                , xp_crd_tf                               -- 만기카드tf
                , ivd_crd_tf                              -- 무효카드tf
                , ivd_bl_rr_crd_tf                        -- 무효bl등재카드tf
                , sp_bl_rr_crd_tf                         -- 정지bl등재카드tf
                , lm_rst_ef_crd_tf                        -- 한도제한적용카드tf
                , rlp_crd_tf                              -- 실질카드tf
                , lat_crd_tf                              -- 신규카드tf
                , scs_crd_tf                              -- 탈회카드tf
                , me_crd_tf                               -- 해지카드tf
                , sln_lot_crd_tf                          -- 도난분실카드tf
                , ts_sp_crd_tf                            -- 거래정지카드tf
                , ris_iac_ccd                             -- 재발급휴면구분코드
                , ris_crd_tf                              -- 재발급카드tf
                , rw_bj_crd_tf                            -- 갱신대상카드tf
                , au_rw_crd_tf                            -- 자동갱신카드tf
                , crd_rw_be_tf                            -- 카드갱신가능tf
                , ies_rw_tf                               -- 조기갱신tf
                , imd_iss_pt_tf                           -- 즉시발급신청tf
                , cut_iss_crd_tf                          -- 부정발급카드tf
                , ear_nn_isc_crd_tf                       -- 초기무기명카드tf
                , bse_crd_tf                              -- 기본카드tf
                , cs_ruf_giv_tf                           -- 현금기능부여tf
                , crd_de_ce_tf                            -- 카드확정취소tf
                , rw_xl_tf                                -- 갱신제외tf
                , crd_clt_mcd                             -- 카드모집방법코드
                , pv_hcd                                  -- 대표지점코드
                , clt_hcd                                 -- 모집지점코드
                , nr_hcd                                  -- 관리지점코드
                , cll_tcd                                 -- 모집인유형코드
                , crd_lok_ccd                             -- 카드외형구분코드
                , trf_ruf_ccd                             -- 교통기능구분코드
                , crd_pht_ccd                             -- 카드사진구분코드
                , crd_sd_mcd                              -- 카드발송방법코드
                , crd_brd_cd                              -- 카드브랜드코드
                , crd_su_ccd                              -- 카드상태구분코드
                , crd_ic_ppo_ccd                          -- 카드ic용도구분코드
                , crd_gcd                                 -- 카드등급코드
                , crd_ps_lcd                              -- 카드처리상세코드
                , crp_afl_pd_cd                           -- 법인제휴상품코드
                , nmn_ccd                                 -- 명의구분코드
                , crd_pt_ps_tcd                           -- 카드신청처리유형코드
                , scs_ccd                                 -- 탈회구분코드
                , ico_cz_ccd                              -- 발급사분류구분코드
                , pv_iss_ncd                              -- 대표발급사유코드
                , ls_iss_ncd                              -- 최종발급사유코드
                , chc_iss_ms_cn                           -- 체크카드발급개월수
                , afe_exo_ncd                             -- 연회비면제사유코드
                , iss_ccd                                 -- 발급구분코드
                , crd_pt_d                                -- 카드신청일자
                , crd_iss_d                               -- 카드발급일자
                , ent_rid                                 -- 입회접수일자
                , ent_rv_sn                               -- 입회접수순번
                , iss_elp_dd_cn                           -- 발급경과일수
                , iss_elp_ms_cn                           -- 발급경과개월수
                , afe_bil_cy_m                            -- 연회비청구주기월
                , bls_scd                                 -- 요주의상태코드
                , max_crd_bl_scd                          -- 최대카드bl상태코드
                , crd_bl_rr_tf                            -- 카드bl등재tf
                , crd_bl_ls_rr_d                          -- 카드bl최종등재일자
                , sln_lot_bl_rr_d                         -- 도난분실bl등재일자
                , crd_acc_rpl_n                           -- 카드계정대체번호
                , mbf_crd_rpl_n                           -- 변경전카드대체번호
                , ls_crd_rpl_n                            -- 최종카드대체번호
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , crd_rpl_n                               -- 카드대체번호
          , ls_ld_dt                                -- 최종적재일시
          , sgdmd                                   -- 그룹md번호
          , sgdci                                   -- ci번호
          , sgdmd_dfr_n                             -- 고객구별그룹md번호
          , sgdci_dfr_n                             -- 고객구별ci번호
          , cry_k_sgdmd                             -- 소지자그룹md번호
          , cry_k_sgdci                             -- 소지자ci번호
          , cus_ccd                                 -- 회원구분코드
          , crd_tcd                                 -- 카드유형코드
          , o_fml_ccd                               -- 본인가족구분코드
          , crd_su_tcd                              -- 카드상태유형코드
          , bsu_n                                   -- 사업장번호
          , st_sn                                   -- 결제순번
          , crd_ico_ccd                             -- 카드발급사구분코드
          , crd_de_d                                -- 카드확정일자
          , isc_crd_de_d                            -- 기명카드확정일자
          , crd_vd_ym                               -- 카드유효년월
          , crd_pd_n                                -- 카드상품번호
          , crd_pdg_tcd                             -- 카드상품군유형코드
          , cct1_crd_pd_n                           -- 개념1카드상품번호
          , cct2_crd_pd_n                           -- 개념2카드상품번호
          , se_rst_scd                              -- 사용제한상태코드
          , crd_acc_se_rst_scd                      -- 카드계정사용제한상태코드
          , cus_se_rst_scd                          -- 회원사용제한상태코드
          , crp_bsu_se_rst_scd                      -- 법인사업장사용제한상태코드
          , crp_usk_se_rst_scd                      -- 법인이용자사용제한상태코드
          , cln_se_rst_scd                          -- 고객사용제한상태코드
          , cus_lm_ga_ccd                           -- 회원한도등급구분코드
          , o_crd_tf                                -- 본인카드tf
          , fml_crd_tf                              -- 가족카드tf
          , cre_crd_tf                              -- 신용카드tf
          , chc_tf                                  -- 체크카드tf
          , bcc_tf                                  -- bc카드tf
          , wf_crd_tf                               -- 복지카드tf
          , trf_crd_tf                              -- 교통카드tf
          , crd_afl_tf                              -- 카드제휴tf
          , gov_phc_tf                              -- 정부구매카드tf
          , fnc_crd_tf                              -- 오토금융카드tf
          , bsi_crd_tf                              -- 비지니스카드tf
          , afe_exo_tf                              -- 연회비면제tf
          , tc_tf                                   -- 이관tf
          , el_crd_tf                               -- 삭제카드tf
          , xp_crd_tf                               -- 만기카드tf
          , ivd_crd_tf                              -- 무효카드tf
          , ivd_bl_rr_crd_tf                        -- 무효bl등재카드tf
          , sp_bl_rr_crd_tf                         -- 정지bl등재카드tf
          , lm_rst_ef_crd_tf                        -- 한도제한적용카드tf
          , rlp_crd_tf                              -- 실질카드tf
          , lat_crd_tf                              -- 신규카드tf
          , scs_crd_tf                              -- 탈회카드tf
          , me_crd_tf                               -- 해지카드tf
          , sln_lot_crd_tf                          -- 도난분실카드tf
          , ts_sp_crd_tf                            -- 거래정지카드tf
          , ris_iac_ccd                             -- 재발급휴면구분코드
          , ris_crd_tf                              -- 재발급카드tf
          , rw_bj_crd_tf                            -- 갱신대상카드tf
          , au_rw_crd_tf                            -- 자동갱신카드tf
          , crd_rw_be_tf                            -- 카드갱신가능tf
          , ies_rw_tf                               -- 조기갱신tf
          , imd_iss_pt_tf                           -- 즉시발급신청tf
          , cut_iss_crd_tf                          -- 부정발급카드tf
          , ear_nn_isc_crd_tf                       -- 초기무기명카드tf
          , bse_crd_tf                              -- 기본카드tf
          , cs_ruf_giv_tf                           -- 현금기능부여tf
          , crd_de_ce_tf                            -- 카드확정취소tf
          , rw_xl_tf                                -- 갱신제외tf
          , crd_clt_mcd                             -- 카드모집방법코드
          , pv_hcd                                  -- 대표지점코드
          , clt_hcd                                 -- 모집지점코드
          , nr_hcd                                  -- 관리지점코드
          , cll_tcd                                 -- 모집인유형코드
          , crd_lok_ccd                             -- 카드외형구분코드
          , trf_ruf_ccd                             -- 교통기능구분코드
          , crd_pht_ccd                             -- 카드사진구분코드
          , crd_sd_mcd                              -- 카드발송방법코드
          , crd_brd_cd                              -- 카드브랜드코드
          , crd_su_ccd                              -- 카드상태구분코드
          , crd_ic_ppo_ccd                          -- 카드ic용도구분코드
          , crd_gcd                                 -- 카드등급코드
          , crd_ps_lcd                              -- 카드처리상세코드
          , crp_afl_pd_cd                           -- 법인제휴상품코드
          , nmn_ccd                                 -- 명의구분코드
          , crd_pt_ps_tcd                           -- 카드신청처리유형코드
          , scs_ccd                                 -- 탈회구분코드
          , ico_cz_ccd                              -- 발급사분류구분코드
          , pv_iss_ncd                              -- 대표발급사유코드
          , ls_iss_ncd                              -- 최종발급사유코드
          , chc_iss_ms_cn                           -- 체크카드발급개월수
          , afe_exo_ncd                             -- 연회비면제사유코드
          , iss_ccd                                 -- 발급구분코드
          , crd_pt_d                                -- 카드신청일자
          , crd_iss_d                               -- 카드발급일자
          , ent_rid                                 -- 입회접수일자
          , ent_rv_sn                               -- 입회접수순번
          , iss_elp_dd_cn                           -- 발급경과일수
          , iss_elp_ms_cn                           -- 발급경과개월수
          , afe_bil_cy_m                            -- 연회비청구주기월
          , bls_scd                                 -- 요주의상태코드
          , max_crd_bl_scd                          -- 최대카드bl상태코드
          , crd_bl_rr_tf                            -- 카드bl등재tf
          , crd_bl_ls_rr_d                          -- 카드bl최종등재일자
          , sln_lot_bl_rr_d                         -- 도난분실bl등재일자
          , crd_acc_rpl_n                           -- 카드계정대체번호
          , mbf_crd_rpl_n                           -- 변경전카드대체번호
          , ls_crd_rpl_n                            -- 최종카드대체번호
      from w0_shc.mtdia5008
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
