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
pgm_id = 'ILCD_SIIFA0001_TG'

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
s3_file_prefix = f'icd_siifa0001_/icd_siifa0001_{execution_kst}'

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
        delete from l0_shc.siifa0001
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shc.siifa0001
                (
                  aws_ls_dt                               -- aws적재일시
                , acd_bj_crd_rpl_n                        -- 사고대상카드대체번호
                , acd_ag_d                                -- 사고할당일자
                , acd_tcd                                 -- 사고유형코드
                , acd_ag_sn                               -- 사고할당순번
                , acd_rr_dt                               -- 사고등재일시
                , to_acd_ppa                              -- 총사고원금금액
                , to_acd_at                               -- 총사고금액
                , rpt_do_rid                              -- 보고서접수일자
                , rv_trm_dd_cn                            -- 접수기간일수
                , ls_nc_d                                 -- 최종결재일자
                , de_ps_d                                 -- 확정처리일자
                , ps_trm_dd_cn                            -- 처리기간일수
                , los_om_at                               -- 손실보상금액
                , fur_ima                                 -- 부정사용자귀책금액
                , mct_ima                                 -- 가맹점귀책금액
                , cus_bla                                 -- 회원청구금액
                , to_wra                                  -- 총회수금액
                , cll_wra                                 -- 모집인회수금액
                , smn_fim_wra                             -- 배송업체회수금액
                , fur_wra                                 -- 부정사용자회수금액
                , et_wra                                  -- 기타회수금액
                , exc_ex_f                                -- 우수사례여부
                , ls_dcs_ucd                              -- 최종판정결과코드
                , acd_ct                                  -- 사고건수
                , imt_tp_zcd                              -- 귀책유형분류코드
                , gl_d                                    -- 목표일자
                , xm_elp_dd_cn                            -- 조사경과일수
                , acd_xm_scd                              -- 사고조사상태코드
                , fur_sao_f                               -- 부정사용자색출여부
                , fru_xl_f                                -- 실적제외여부
                , acd_nr_fru_xl_ncd                       -- 사고관리실적제외사유코드
                , crd_rnt_ccd                             -- 카드대여구분코드
                , acd_af_reu_f                            -- 사고후재이용여부
                , ino_xl_f                                -- 지표제외여부
                , o_se_ccd                                -- 본인사용구분코드
                , acd_nr_ino_xl_ncd                       -- 사고관리지표제외사유코드
                , va_bj_f                                 -- 평가대상여부
                , ls_ld_dt                                -- 최종적재일시
                , exc_ex_tf                               -- 우수사례tf
                , fur_sao_tf                              -- 부정사용자색출tf
                , fru_xl_tf                               -- 실적제외tf
                , ino_xl_tf                               -- 지표제외tf
                , acd_af_reu_tf                           -- 사고후재이용tf
                , va_bj_tf                                -- 평가대상tf
                , apv_ct                                  -- 승인건수
                , ce_ct                                   -- 취소건수
                , rjt_ct                                  -- 거절건수
                , cus_bse_imt_rt                          -- 회원기본귀책율
                , cus_rlt_imt_rt                          -- 회원실제귀책율
                , scty_wkp_ccd                            -- 사회약자구분코드
                , acd_rv_mcd                              -- 사고접수방법코드
                , cus_cri_ccd                             -- 회원특성구분코드
                , ath_fil_tp_ccd_btm_vl                   -- 첨부파일유형구분코드비트맵값
                , rl_acd_ccd                              -- 실사고구분코드
                , if_shr_ge_ccd                           -- 정보공유동의구분코드
                , acd_mti_ccd                             -- 사고세부구분코드
                , acd_xm_fe_rg_f                          -- 사고조사수수료등록여부
                , acd_xm_fe_rg_tf                         -- 사고조사수수료등록tf
                , cus_ima_pnt_sea                         -- 회원귀책금액포인트사용금액
                , acd_xm_fea_pnt_sea                      -- 사고조사수수료금액포인트사용금액
                , pnt_se_f                                -- 포인트사용여부
                , pnt_se_tf                               -- 포인트사용tf
                , ns_f                                    -- 할부여부
                , ns_ir_bil_f                             -- 할부이자청구여부
                , ns_tf                                   -- 할부tf
                , ns_ir_bil_tf                            -- 할부이자청구tf
                , onl_ah_rg_f                             -- 온라인타인등록여부
                , onl_ah_rg_tf                            -- 온라인타인등록tf
                , sln_lot_mw5_ov_f                        -- 도난분실5만원초과여부
                , ris_f                                   -- 재발급여부
                , rc_m3_av_uea                            -- 최근3개월평균이용금액
                , aco_crd_ue_f                            -- 타사카드이용여부
                , ad_los_om_rt                            -- 추가손실보상율
                , ad_los_om_be_at                         -- 추가손실보상가능금액
                , ad_los_om_ef_at                         -- 추가손실보상적용금액
                , sln_lot_ad_om_f                         -- 도난분실추가보상여부
                , sln_lot_mw5_ov_tf                       -- 도난분실5만원초과tf
                , ris_tf                                  -- 재발급tf
                , aco_crd_ue_tf                           -- 타사카드이용tf
                , sln_lot_ad_om_tf                        -- 도난분실추가보상tf
                , lup_lot_dcl_f                           -- 일괄분실신고여부
                , lup_lot_dcl_tf                          -- 일괄분실신고tf
                , acd_aff_nr_f                            -- 사고사후관리여부
                , acd_aff_nr_tf                           -- 사고사후관리tf
                , rlp_cus_f                               -- 실질회원여부
                , rlp_cus_tf                              -- 실질회원tf
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , acd_bj_crd_rpl_n                        -- 사고대상카드대체번호
          , acd_ag_d                                -- 사고할당일자
          , acd_tcd                                 -- 사고유형코드
          , acd_ag_sn                               -- 사고할당순번
          , acd_rr_dt                               -- 사고등재일시
          , to_acd_ppa                              -- 총사고원금금액
          , to_acd_at                               -- 총사고금액
          , rpt_do_rid                              -- 보고서접수일자
          , rv_trm_dd_cn                            -- 접수기간일수
          , ls_nc_d                                 -- 최종결재일자
          , de_ps_d                                 -- 확정처리일자
          , ps_trm_dd_cn                            -- 처리기간일수
          , los_om_at                               -- 손실보상금액
          , fur_ima                                 -- 부정사용자귀책금액
          , mct_ima                                 -- 가맹점귀책금액
          , cus_bla                                 -- 회원청구금액
          , to_wra                                  -- 총회수금액
          , cll_wra                                 -- 모집인회수금액
          , smn_fim_wra                             -- 배송업체회수금액
          , fur_wra                                 -- 부정사용자회수금액
          , et_wra                                  -- 기타회수금액
          , exc_ex_f                                -- 우수사례여부
          , ls_dcs_ucd                              -- 최종판정결과코드
          , acd_ct                                  -- 사고건수
          , imt_tp_zcd                              -- 귀책유형분류코드
          , gl_d                                    -- 목표일자
          , xm_elp_dd_cn                            -- 조사경과일수
          , acd_xm_scd                              -- 사고조사상태코드
          , fur_sao_f                               -- 부정사용자색출여부
          , fru_xl_f                                -- 실적제외여부
          , acd_nr_fru_xl_ncd                       -- 사고관리실적제외사유코드
          , crd_rnt_ccd                             -- 카드대여구분코드
          , acd_af_reu_f                            -- 사고후재이용여부
          , ino_xl_f                                -- 지표제외여부
          , o_se_ccd                                -- 본인사용구분코드
          , acd_nr_ino_xl_ncd                       -- 사고관리지표제외사유코드
          , va_bj_f                                 -- 평가대상여부
          , ls_ld_dt                                -- 최종적재일시
          , exc_ex_tf                               -- 우수사례tf
          , fur_sao_tf                              -- 부정사용자색출tf
          , fru_xl_tf                               -- 실적제외tf
          , ino_xl_tf                               -- 지표제외tf
          , acd_af_reu_tf                           -- 사고후재이용tf
          , va_bj_tf                                -- 평가대상tf
          , apv_ct                                  -- 승인건수
          , ce_ct                                   -- 취소건수
          , rjt_ct                                  -- 거절건수
          , cus_bse_imt_rt                          -- 회원기본귀책율
          , cus_rlt_imt_rt                          -- 회원실제귀책율
          , scty_wkp_ccd                            -- 사회약자구분코드
          , acd_rv_mcd                              -- 사고접수방법코드
          , cus_cri_ccd                             -- 회원특성구분코드
          , ath_fil_tp_ccd_btm_vl                   -- 첨부파일유형구분코드비트맵값
          , rl_acd_ccd                              -- 실사고구분코드
          , if_shr_ge_ccd                           -- 정보공유동의구분코드
          , acd_mti_ccd                             -- 사고세부구분코드
          , acd_xm_fe_rg_f                          -- 사고조사수수료등록여부
          , acd_xm_fe_rg_tf                         -- 사고조사수수료등록tf
          , cus_ima_pnt_sea                         -- 회원귀책금액포인트사용금액
          , acd_xm_fea_pnt_sea                      -- 사고조사수수료금액포인트사용금액
          , pnt_se_f                                -- 포인트사용여부
          , pnt_se_tf                               -- 포인트사용tf
          , ns_f                                    -- 할부여부
          , ns_ir_bil_f                             -- 할부이자청구여부
          , ns_tf                                   -- 할부tf
          , ns_ir_bil_tf                            -- 할부이자청구tf
          , onl_ah_rg_f                             -- 온라인타인등록여부
          , onl_ah_rg_tf                            -- 온라인타인등록tf
          , sln_lot_mw5_ov_f                        -- 도난분실5만원초과여부
          , ris_f                                   -- 재발급여부
          , rc_m3_av_uea                            -- 최근3개월평균이용금액
          , aco_crd_ue_f                            -- 타사카드이용여부
          , ad_los_om_rt                            -- 추가손실보상율
          , ad_los_om_be_at                         -- 추가손실보상가능금액
          , ad_los_om_ef_at                         -- 추가손실보상적용금액
          , sln_lot_ad_om_f                         -- 도난분실추가보상여부
          , sln_lot_mw5_ov_tf                       -- 도난분실5만원초과tf
          , ris_tf                                  -- 재발급tf
          , aco_crd_ue_tf                           -- 타사카드이용tf
          , sln_lot_ad_om_tf                        -- 도난분실추가보상tf
          , lup_lot_dcl_f                           -- 일괄분실신고여부
          , lup_lot_dcl_tf                          -- 일괄분실신고tf
          , acd_aff_nr_f                            -- 사고사후관리여부
          , acd_aff_nr_tf                           -- 사고사후관리tf
          , rlp_cus_f                               -- 실질회원여부
          , rlp_cus_tf                              -- 실질회원tf
      from w0_shc.siifa0001
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
