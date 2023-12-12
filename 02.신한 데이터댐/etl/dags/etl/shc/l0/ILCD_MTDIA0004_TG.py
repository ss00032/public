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
pgm_id = 'ILCD_MTDIA0004_TG'

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
s3_file_prefix = f'icd_mtdia0004_/icd_mtdia0004_{execution_kst}'

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
        delete from l0_shc.mtdia0004
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shc.mtdia0004
                (
                  aws_ls_dt                               -- aws적재일시
                , ta_ym                                   -- 기준년월
                , sgdmd                                   -- 그룹md번호
                , ls_ld_dt                                -- 최종적재일시
                , cln_dtn_ccd                             -- 고객식별구분코드
                , crp_cln_ccd                             -- 법인고객구분코드
                , shp_la_co_cd                            -- 신한그룹관계사코드
                , crp_gp_zcd                              -- 법인그룹분류코드
                , crp_ops_gp_cd                           -- 법인소속그룹코드
                , rmm_vm_hqa_ccd                          -- rm_vm본부구분코드
                , rmm_vm_hqa_de_ncd                       -- rm_vm본부확정사유코드
                , rmm_vm_hqa_de_d                         -- rm_vm본부확정일자
                , rms_jbg_cd                              -- rms직군코드
                , sex_ccd                                 -- 성별구분코드
                , cln_age                                 -- 고객연령
                , age_ccd                                 -- 연령구분코드
                , cre_cln_bj_tf                           -- 신용고객대상tf
                , fml_crd_cry_tf                          -- 가족카드소지tf
                , fml_crd_only_cus_tf                     -- 가족카드only회원tf
                , soh_tf                                  -- 소호tf
                , bcc_cry_tf                              -- bc카드소지tf
                , wf_crd_cry_tf                           -- 복지카드소지tf
                , bsi_crd_cus_tf                          -- 비지니스카드회원tf
                , bsi_crd_only_cus_tf                     -- 비지니스카드only회원tf
                , cre_crd_cln_tf                          -- 신용카드고객tf
                , chc_cln_tf                              -- 체크카드고객tf
                , prp_crd_cln_tf                          -- 선불카드고객tf
                , lnp_cus_tf                              -- 론패스회원tf
                , lon_cus_tf                              -- 론회원tf
                , vv_cln_tf                               -- 리볼빙고객tf
                , cmt_lat_vv_cln_tf                       -- 당월신규리볼빙고객tf
                , mms_cus_tf                              -- 멤버쉽회원tf
                , fnc_psn_cus_tf                          -- 오토금융개인회원tf
                , fnc_crp_cus_tf                          -- 오토금융법인회원tf
                , mcw_cln_tf                              -- 가맹점주고객tf
                , onl_cln_tf                              -- 온라인고객tf
                , shp_sff_tf                              -- 신한그룹직원tf
                , shb_lat_cln_tf                          -- 신한은행신규고객tf
                , cre_crd_xp_cln_tf                       -- 신용카드만기고객tf
                , cre_crd_ivd_cln_tf                      -- 신용카드무효고객tf
                , cre_crd_dt_sp_cln_tf                    -- 신용카드일시정지고객tf
                , cre_crd_rlp_cln_tf                      -- 신용카드실질고객tf
                , chc_xp_cln_tf                           -- 체크카드만기고객tf
                , chc_ivd_cln_tf                          -- 체크카드무효고객tf
                , chc_dt_sp_cln_tf                        -- 체크카드일시정지고객tf
                , chc_rlp_cln_tf                          -- 체크카드실질고객tf
                , el_cln_tf                               -- 삭제고객tf
                , xp_cln_tf                               -- 만기고객tf
                , ivd_cln_tf                              -- 무효고객tf
                , dt_sp_cln_tf                            -- 일시정지고객tf
                , rlp_cln_tf                              -- 실질고객tf
                , lat_cln_tf                              -- 신규고객tf
                , evl_jbg_cd                              -- 심사직군코드
                , evl_jbt_cd                              -- 심사직위코드
                , xp_cus_ccd                              -- 만기회원구분코드
                , ni_de_d                                 -- 최초확정일자
                , cre_crd_ni_de_d                         -- 신용카드최초확정일자
                , chc_ni_de_d                             -- 체크카드최초확정일자
                , eco_scl_cd                              -- 기업규모코드
                , cus_seg_lcd                             -- 회원세분화상세코드
                , afo_ge_reo_tf                           -- 제휴사동의철회tf
                , wko_aid_sud_tf                          -- 워크아웃지원중단tf
                , cou_nr_tf                               -- 법정관리tf
                , cco_tps_clb_gcd                         -- 당사탑스클럽등급코드
                , shp_tps_clb_gcd                         -- 신한그룹탑스클럽등급코드
                , cco_tps_clb_cus_tf                      -- 당사탑스클럽회원tf
                , shp_tps_clb_tf                          -- 신한그룹탑스클럽tf
                , m12_cco_tps_clb_cus_tf                  -- 12개월당사탑스클럽회원tf
                , m12_shp_tps_cus_tf                      -- 12개월신한그룹탑스회원tf
                , ni_crd_iss_d                            -- 최초카드발급일자
                , ni_de_d_ta_iss_elp_ms_cn                -- 최초확정일자기준발급경과개월수
                , ric_de_ta_iss_elp_ms_cn                 -- 재유입확정기준발급경과개월수
                , vd_xts_ta_iss_elp_ms_cn                 -- 유효연장기준발급경과개월수
                , ls_iss_d_ta_iss_elp_ms_cn               -- 최종발급일자기준발급경과개월수
                , crd_clt_mcd                             -- 카드모집방법코드
                , cln_rq_vwy_tf                           -- 고객요청다원화tf
                , shb_hcd                                 -- 신한은행지점코드
                , nr_hcd                                  -- 관리지점코드
                , clt_hcd                                 -- 모집지점코드
                , dy_gr_ccd                               -- 연체유예구분코드
                , dy_gr_trm_dd_cn                         -- 연체유예기간일수
                , pv_bl_scd                               -- 대표bl상태코드
                , st_mcd                                  -- 결제방법코드
                , st_bcd                                  -- 결제은행코드
                , rcp_mcd                                 -- 입금방법코드
                , cre_crd_st_dd_cd                        -- 신용카드결제일코드
                , chc_st_dd_cd                            -- 체크카드결제일코드
                , pv_st_sn                                -- 대표결제순번
                , bil_ae_gel_ccd                          -- 청구지연락처구분코드
                , ar_gel_sn                               -- 주소연락처순번
                , pon_gel_sn                              -- 전화번호연락처순번
                , elc_ar_gel_sn                           -- 전자주소연락처순번
                , bil_ae_zpn                              -- 청구지우편번호
                , hm_zpn                                  -- 자택우편번호
                , wp_zpn                                  -- 직장우편번호
                , ni_ue_d                                 -- 최초이용일자
                , ni_lma                                  -- 최초한도금액
                , p_lma                                   -- 일시불한도금액
                , ns_lma                                  -- 할부한도금액
                , cv_lma                                  -- 현금서비스한도금액
                , crd_lln_lm_at                           -- 카드론대출한도금액
                , nn_ir_ns_ma_lma                         -- 무이자할부잔여한도금액
                , ig_lma                                  -- 통합한도금액
                , to_lma                                  -- 총한도금액
                , ni_p_ue_d                               -- 최초일시불이용일자
                , ni_ns_ue_d                              -- 최초할부이용일자
                , ni_cv_ue_d                              -- 최초현금서비스이용일자
                , ni_crd_lon_ue_d                         -- 최초카드론이용일자
                , rc_ue_d                                 -- 최근이용일자
                , rc_p_ue_d                               -- 최근일시불이용일자
                , rc_ns_ue_d                              -- 최근할부이용일자
                , rc_cv_ue_d                              -- 최근현금서비스이용일자
                , rc_crd_lon_ue_d                         -- 최근카드론이용일자
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , ta_ym                                   -- 기준년월
          , sgdmd                                   -- 그룹md번호
          , ls_ld_dt                                -- 최종적재일시
          , cln_dtn_ccd                             -- 고객식별구분코드
          , crp_cln_ccd                             -- 법인고객구분코드
          , shp_la_co_cd                            -- 신한그룹관계사코드
          , crp_gp_zcd                              -- 법인그룹분류코드
          , crp_ops_gp_cd                           -- 법인소속그룹코드
          , rmm_vm_hqa_ccd                          -- rm_vm본부구분코드
          , rmm_vm_hqa_de_ncd                       -- rm_vm본부확정사유코드
          , rmm_vm_hqa_de_d                         -- rm_vm본부확정일자
          , rms_jbg_cd                              -- rms직군코드
          , sex_ccd                                 -- 성별구분코드
          , cln_age                                 -- 고객연령
          , age_ccd                                 -- 연령구분코드
          , cre_cln_bj_tf                           -- 신용고객대상tf
          , fml_crd_cry_tf                          -- 가족카드소지tf
          , fml_crd_only_cus_tf                     -- 가족카드only회원tf
          , soh_tf                                  -- 소호tf
          , bcc_cry_tf                              -- bc카드소지tf
          , wf_crd_cry_tf                           -- 복지카드소지tf
          , bsi_crd_cus_tf                          -- 비지니스카드회원tf
          , bsi_crd_only_cus_tf                     -- 비지니스카드only회원tf
          , cre_crd_cln_tf                          -- 신용카드고객tf
          , chc_cln_tf                              -- 체크카드고객tf
          , prp_crd_cln_tf                          -- 선불카드고객tf
          , lnp_cus_tf                              -- 론패스회원tf
          , lon_cus_tf                              -- 론회원tf
          , vv_cln_tf                               -- 리볼빙고객tf
          , cmt_lat_vv_cln_tf                       -- 당월신규리볼빙고객tf
          , mms_cus_tf                              -- 멤버쉽회원tf
          , fnc_psn_cus_tf                          -- 오토금융개인회원tf
          , fnc_crp_cus_tf                          -- 오토금융법인회원tf
          , mcw_cln_tf                              -- 가맹점주고객tf
          , onl_cln_tf                              -- 온라인고객tf
          , shp_sff_tf                              -- 신한그룹직원tf
          , shb_lat_cln_tf                          -- 신한은행신규고객tf
          , cre_crd_xp_cln_tf                       -- 신용카드만기고객tf
          , cre_crd_ivd_cln_tf                      -- 신용카드무효고객tf
          , cre_crd_dt_sp_cln_tf                    -- 신용카드일시정지고객tf
          , cre_crd_rlp_cln_tf                      -- 신용카드실질고객tf
          , chc_xp_cln_tf                           -- 체크카드만기고객tf
          , chc_ivd_cln_tf                          -- 체크카드무효고객tf
          , chc_dt_sp_cln_tf                        -- 체크카드일시정지고객tf
          , chc_rlp_cln_tf                          -- 체크카드실질고객tf
          , el_cln_tf                               -- 삭제고객tf
          , xp_cln_tf                               -- 만기고객tf
          , ivd_cln_tf                              -- 무효고객tf
          , dt_sp_cln_tf                            -- 일시정지고객tf
          , rlp_cln_tf                              -- 실질고객tf
          , lat_cln_tf                              -- 신규고객tf
          , evl_jbg_cd                              -- 심사직군코드
          , evl_jbt_cd                              -- 심사직위코드
          , xp_cus_ccd                              -- 만기회원구분코드
          , ni_de_d                                 -- 최초확정일자
          , cre_crd_ni_de_d                         -- 신용카드최초확정일자
          , chc_ni_de_d                             -- 체크카드최초확정일자
          , eco_scl_cd                              -- 기업규모코드
          , cus_seg_lcd                             -- 회원세분화상세코드
          , afo_ge_reo_tf                           -- 제휴사동의철회tf
          , wko_aid_sud_tf                          -- 워크아웃지원중단tf
          , cou_nr_tf                               -- 법정관리tf
          , cco_tps_clb_gcd                         -- 당사탑스클럽등급코드
          , shp_tps_clb_gcd                         -- 신한그룹탑스클럽등급코드
          , cco_tps_clb_cus_tf                      -- 당사탑스클럽회원tf
          , shp_tps_clb_tf                          -- 신한그룹탑스클럽tf
          , m12_cco_tps_clb_cus_tf                  -- 12개월당사탑스클럽회원tf
          , m12_shp_tps_cus_tf                      -- 12개월신한그룹탑스회원tf
          , ni_crd_iss_d                            -- 최초카드발급일자
          , ni_de_d_ta_iss_elp_ms_cn                -- 최초확정일자기준발급경과개월수
          , ric_de_ta_iss_elp_ms_cn                 -- 재유입확정기준발급경과개월수
          , vd_xts_ta_iss_elp_ms_cn                 -- 유효연장기준발급경과개월수
          , ls_iss_d_ta_iss_elp_ms_cn               -- 최종발급일자기준발급경과개월수
          , crd_clt_mcd                             -- 카드모집방법코드
          , cln_rq_vwy_tf                           -- 고객요청다원화tf
          , shb_hcd                                 -- 신한은행지점코드
          , nr_hcd                                  -- 관리지점코드
          , clt_hcd                                 -- 모집지점코드
          , dy_gr_ccd                               -- 연체유예구분코드
          , dy_gr_trm_dd_cn                         -- 연체유예기간일수
          , pv_bl_scd                               -- 대표bl상태코드
          , st_mcd                                  -- 결제방법코드
          , st_bcd                                  -- 결제은행코드
          , rcp_mcd                                 -- 입금방법코드
          , cre_crd_st_dd_cd                        -- 신용카드결제일코드
          , chc_st_dd_cd                            -- 체크카드결제일코드
          , pv_st_sn                                -- 대표결제순번
          , bil_ae_gel_ccd                          -- 청구지연락처구분코드
          , ar_gel_sn                               -- 주소연락처순번
          , pon_gel_sn                              -- 전화번호연락처순번
          , elc_ar_gel_sn                           -- 전자주소연락처순번
          , bil_ae_zpn                              -- 청구지우편번호
          , hm_zpn                                  -- 자택우편번호
          , wp_zpn                                  -- 직장우편번호
          , ni_ue_d                                 -- 최초이용일자
          , ni_lma                                  -- 최초한도금액
          , p_lma                                   -- 일시불한도금액
          , ns_lma                                  -- 할부한도금액
          , cv_lma                                  -- 현금서비스한도금액
          , crd_lln_lm_at                           -- 카드론대출한도금액
          , nn_ir_ns_ma_lma                         -- 무이자할부잔여한도금액
          , ig_lma                                  -- 통합한도금액
          , to_lma                                  -- 총한도금액
          , ni_p_ue_d                               -- 최초일시불이용일자
          , ni_ns_ue_d                              -- 최초할부이용일자
          , ni_cv_ue_d                              -- 최초현금서비스이용일자
          , ni_crd_lon_ue_d                         -- 최초카드론이용일자
          , rc_ue_d                                 -- 최근이용일자
          , rc_p_ue_d                               -- 최근일시불이용일자
          , rc_ns_ue_d                              -- 최근할부이용일자
          , rc_cv_ue_d                              -- 최근현금서비스이용일자
          , rc_crd_lon_ue_d                         -- 최근카드론이용일자
      from w0_shc.mtdia0004
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
