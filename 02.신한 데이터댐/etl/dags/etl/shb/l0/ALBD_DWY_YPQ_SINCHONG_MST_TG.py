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


__author__     = "노재홍"
__copyright__  = "Copyright 2021, Shinhan Datadam"
__credits__    = ["노재홍"]
__version__    = "1.0"
__maintainer__ = "노재홍"
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
pgm_id = 'ALBD_DWY_YPQ_SINCHONG_MST_TG'

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
s3_file_prefix = f'abd_dwy_ypq_sinchong_mst_/abd_dwy_ypq_sinchong_mst_{execution_kst}'

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
        delete from l0_shb.dwy_ypq_sinchong_mst
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwy_ypq_sinchong_mst
                (
                  aws_ls_dt                               -- aws적재일시
                , sinc_no                                 -- 신청번호
                , sinc_rcpt_g                             -- 신청접수구분
                , teamdc_aprvno                           -- 단체협의대출승인번호
                , sinc_g                                  -- 신청구분
                , sinc_jgcng_upmu_c                       -- 신청조건변경업무code
                , prdt_c                                  -- 상품code
                , plcy_finc_c                             -- 정책금융code
                , supl_prdt_c1                            -- 부가상품code1
                , supl_prdt_c2                            -- 부가상품code2
                , supl_prdt_c3                            -- 부가상품code3
                , ys_sinc_jamt                            -- 여신신청정수금액
                , dc_adamt_incr_jamt                      -- 대출증액증가정수금액
                , dc_tmuni_c                              -- 대출기간단위code
                , dc_term                                 -- 대출기간
                , dc_dudt                                 -- 대출만기일자
                , unyong_g                                -- 운용구분
                , irt_g                                   -- 금리구분
                , mkirt_k                                 -- 시장금리종류
                , mrkt_giganm_k                           -- 시장기간물종류
                , sirt_g                                  -- 특별금리구분
                , dc_iyul                                 -- 대출이율
                , shnd_mth                                -- 상환방법
                , ys_bond_bojun_c                         -- 여신채권보전code
                , lg_b_dmb_c1                             -- 대분류담보code1
                , dmb_k1                                  -- 담보종류1
                , dmb_acno1                               -- 담보계좌번호1
                , usbdmb_jamt1                            -- 가용담보정수금액1
                , lg_b_dmb_c2                             -- 대분류담보code2
                , dmb_k2                                  -- 담보종류2
                , dmb_acno2                               -- 담보계좌번호2
                , usbdmb_jamt2                            -- 가용담보정수금액2
                , lg_b_dmb_c3                             -- 대분류담보code3
                , dmb_k3                                  -- 담보종류3
                , dmb_acno3                               -- 담보계좌번호3
                , usbdmb_jamt3                            -- 가용담보정수금액3
                , ypq_etc_jogn_c                          -- 개인신청기타조건code
                , ypq_path_c                              -- 개인신청경로code
                , grpco_jkw_no                            -- 그룹사직원번호
                , fund_yongdo_c                           -- 자금용도code
                , trt_hope_dt                             -- 취급희망일자
                , trt_hope_dd_term                        -- 취급희망일기간
                , dc_exe_rqst_dt                          -- 대출실행요청일자
                , sinc_trt_fert                           -- 신청취급수수료율
                , spos_sodk_habsan_recg_yn                -- 배우자소득합산인정여부
                , sodk_prof_add_gm_irt                    -- 소득증빙가산감면금리
                , rel_acno                                -- 관련계좌번호
                , br_jdgm_c                               -- 영업점판결code
                , bojn_dmbjg_rrno1                        -- 보증담보제공주민번호1
                , bojn_dmbjg_rrno2                        -- 보증담보제공주민번호2
                , tot_guar_cnt                            -- 총보증인수
                , tot_dc_jamt                             -- 총대출정수금액
                , tot_crdc_jamt                           -- 총신용대출정수금액
                , tot_dmbdc_jamt                          -- 총담보대출정수금액
                , tot_frdc_jamt                           -- 총외화대출정수금액
                , old_bojn_jamt                           -- 기존보증정수금액
                , ypq_org_c                               -- 개인신청기관code
                , iyul_gd                                 -- 이율등급
                , allw_gd                                 -- 충당금등급
                , bojeungamt_yc_yn                        -- 보증금액연체여부
                , torg_dc_bojn_jamt                       -- 타기관대출보증정수금액
                , insr_ent_g                              -- 보험가입구분
                , torg_dc_um                              -- 타기관대출유무
                , torg_dc_singo_jamt                      -- 타기관대출신고정수금액
                , torg_dc_kfb_jamt                        -- 타기관대출은행연합회정수금액
                , torg_dmbdc_jamt                         -- 타기관담보대출정수금액
                , torg_dc_singo_cnt                       -- 타기관대출신고건수
                , torg_dc_kfb_cnt                         -- 타기관대출은행연합회건수
                , kfb_cshsvc_jamt                         -- 은행연합회현금서비스정수금액
                , kfb_cshscv_cnt                          -- 은행연합회현금서비스건수
                , kfb_cus_bjorg_cnt                       -- 은행연합회고객보증기관수
                , euibo_obnk_ent_yn                       -- 의료보험당행가입여부
                , calc_iyul                               -- 계산이율
                , check_list_wrt_yn                       -- checklist작성여부
                , gchtm_cnt_mcnt                          -- 거치기간개월수
                , tbnk_crdc_shnd_jamt                     -- 타행신용대출상환정수금액
                , tbnk_dmbdc_shnd_jamt                    -- 타행담보대출상환정수금액
                , jdsh_feemj_g                            -- 중도상환수수료면제구분
                , pbsvt_udln_shnd_jamt                    -- 공무원우대loan상환정수금액
                , korail_lend_rcmd_doc_ser                -- 철도공사융자추천서일련번호
                , lgtm_gibu_sinc_yn                       -- 장기기부신청여부
                , due_m6lt_gm_jkyn                        -- 만기6개월이내감면적용여부
                , chaju_chg_gm_jkyn                       -- 차주변경감면적용여부
                , rev_mgln_sinc_no                        -- 리볼빙모기지loan신청번호
                , irt_inha_g                              -- 금리인하구분
                , crtfee_fee_budam_g                      -- 설정비비용부담구분
                , malso_fee_budam_g                       -- 말소비용부담구분
                , dmb_malso_fee_jamt                      -- 담보말소비용정수금액
                , gamj_fee_fee_budam_g                    -- 감정료비용부담구분
                , gamj_fee_jamt                           -- 감정비용정수금액
                , sigachj_fee_fee_budam_g                 -- 시가추정수수료비용부담구분
                , dmb_sigachj_fee_jamt                    -- 담보시가추정수수료정수금액
                , inji_fee_budam_g                        -- 인지대비용부담구분
                , inji_jamt                               -- 인지대정수금액
                , add_irt                                 -- 가산금리
                , work_mn_udae_dc_crdt_gd                 -- 근로자우대대출신용등급
                , crdt_hndo_smtime_inqr_yn                -- 신용한도동시조회여부
                , dc_sinc_rst_sms_recv_yn                 -- 대출신청결과sms수신여부
                , rlofc_no                                -- 중개업소번호
                , pay_iche_yn                             -- 급여이체여부
                , dc_psb_hndo_jamt                        -- 대출가능한도정수금액
                , afrcpt_dmb_yn                           -- 후취담보여부
                , bhsh_due_ilsi_shnd_jamt                 -- 분할상환만기일시상환정수금액
                , prm_budam_g                             -- 보험료부담구분
                , torg_dc_shnd_estm_org_cnt               -- 타기관대출상환예정기관수
                , irt_g2                                  -- 금리구분2
                , mkirt_k2                                -- 시장금리종류2
                , mrkt_giganm_k2                          -- 시장기간물종류2
                , sirt_g2                                 -- 특별금리구분2
                , dc_iyul2                                -- 대출이율2
                , br_mgr_alapv_gm_irt2                    -- 지점장전결감면금리2
                , dangi_chg_irt_rt                        -- 단기변동금리비율
                , calc_iyul2                              -- 계산이율2
                , add_irt2                                -- 가산금리2
                , rrno_cid                                -- 주민번호고객id
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , sinc_no                                 -- 신청번호
          , sinc_rcpt_g                             -- 신청접수구분
          , teamdc_aprvno                           -- 단체협의대출승인번호
          , sinc_g                                  -- 신청구분
          , sinc_jgcng_upmu_c                       -- 신청조건변경업무code
          , prdt_c                                  -- 상품code
          , plcy_finc_c                             -- 정책금융code
          , supl_prdt_c1                            -- 부가상품code1
          , supl_prdt_c2                            -- 부가상품code2
          , supl_prdt_c3                            -- 부가상품code3
          , ys_sinc_jamt                            -- 여신신청정수금액
          , dc_adamt_incr_jamt                      -- 대출증액증가정수금액
          , dc_tmuni_c                              -- 대출기간단위code
          , dc_term                                 -- 대출기간
          , dc_dudt                                 -- 대출만기일자
          , unyong_g                                -- 운용구분
          , irt_g                                   -- 금리구분
          , mkirt_k                                 -- 시장금리종류
          , mrkt_giganm_k                           -- 시장기간물종류
          , sirt_g                                  -- 특별금리구분
          , dc_iyul                                 -- 대출이율
          , shnd_mth                                -- 상환방법
          , ys_bond_bojun_c                         -- 여신채권보전code
          , lg_b_dmb_c1                             -- 대분류담보code1
          , dmb_k1                                  -- 담보종류1
          , dmb_acno1                               -- 담보계좌번호1
          , usbdmb_jamt1                            -- 가용담보정수금액1
          , lg_b_dmb_c2                             -- 대분류담보code2
          , dmb_k2                                  -- 담보종류2
          , dmb_acno2                               -- 담보계좌번호2
          , usbdmb_jamt2                            -- 가용담보정수금액2
          , lg_b_dmb_c3                             -- 대분류담보code3
          , dmb_k3                                  -- 담보종류3
          , dmb_acno3                               -- 담보계좌번호3
          , usbdmb_jamt3                            -- 가용담보정수금액3
          , ypq_etc_jogn_c                          -- 개인신청기타조건code
          , ypq_path_c                              -- 개인신청경로code
          , grpco_jkw_no                            -- 그룹사직원번호
          , fund_yongdo_c                           -- 자금용도code
          , trt_hope_dt                             -- 취급희망일자
          , trt_hope_dd_term                        -- 취급희망일기간
          , dc_exe_rqst_dt                          -- 대출실행요청일자
          , sinc_trt_fert                           -- 신청취급수수료율
          , spos_sodk_habsan_recg_yn                -- 배우자소득합산인정여부
          , sodk_prof_add_gm_irt                    -- 소득증빙가산감면금리
          , rel_acno                                -- 관련계좌번호
          , br_jdgm_c                               -- 영업점판결code
          , bojn_dmbjg_rrno1                        -- 보증담보제공주민번호1
          , bojn_dmbjg_rrno2                        -- 보증담보제공주민번호2
          , tot_guar_cnt                            -- 총보증인수
          , tot_dc_jamt                             -- 총대출정수금액
          , tot_crdc_jamt                           -- 총신용대출정수금액
          , tot_dmbdc_jamt                          -- 총담보대출정수금액
          , tot_frdc_jamt                           -- 총외화대출정수금액
          , old_bojn_jamt                           -- 기존보증정수금액
          , ypq_org_c                               -- 개인신청기관code
          , iyul_gd                                 -- 이율등급
          , allw_gd                                 -- 충당금등급
          , bojeungamt_yc_yn                        -- 보증금액연체여부
          , torg_dc_bojn_jamt                       -- 타기관대출보증정수금액
          , insr_ent_g                              -- 보험가입구분
          , torg_dc_um                              -- 타기관대출유무
          , torg_dc_singo_jamt                      -- 타기관대출신고정수금액
          , torg_dc_kfb_jamt                        -- 타기관대출은행연합회정수금액
          , torg_dmbdc_jamt                         -- 타기관담보대출정수금액
          , torg_dc_singo_cnt                       -- 타기관대출신고건수
          , torg_dc_kfb_cnt                         -- 타기관대출은행연합회건수
          , kfb_cshsvc_jamt                         -- 은행연합회현금서비스정수금액
          , kfb_cshscv_cnt                          -- 은행연합회현금서비스건수
          , kfb_cus_bjorg_cnt                       -- 은행연합회고객보증기관수
          , euibo_obnk_ent_yn                       -- 의료보험당행가입여부
          , calc_iyul                               -- 계산이율
          , check_list_wrt_yn                       -- checklist작성여부
          , gchtm_cnt_mcnt                          -- 거치기간개월수
          , tbnk_crdc_shnd_jamt                     -- 타행신용대출상환정수금액
          , tbnk_dmbdc_shnd_jamt                    -- 타행담보대출상환정수금액
          , jdsh_feemj_g                            -- 중도상환수수료면제구분
          , pbsvt_udln_shnd_jamt                    -- 공무원우대loan상환정수금액
          , korail_lend_rcmd_doc_ser                -- 철도공사융자추천서일련번호
          , lgtm_gibu_sinc_yn                       -- 장기기부신청여부
          , due_m6lt_gm_jkyn                        -- 만기6개월이내감면적용여부
          , chaju_chg_gm_jkyn                       -- 차주변경감면적용여부
          , rev_mgln_sinc_no                        -- 리볼빙모기지loan신청번호
          , irt_inha_g                              -- 금리인하구분
          , crtfee_fee_budam_g                      -- 설정비비용부담구분
          , malso_fee_budam_g                       -- 말소비용부담구분
          , dmb_malso_fee_jamt                      -- 담보말소비용정수금액
          , gamj_fee_fee_budam_g                    -- 감정료비용부담구분
          , gamj_fee_jamt                           -- 감정비용정수금액
          , sigachj_fee_fee_budam_g                 -- 시가추정수수료비용부담구분
          , dmb_sigachj_fee_jamt                    -- 담보시가추정수수료정수금액
          , inji_fee_budam_g                        -- 인지대비용부담구분
          , inji_jamt                               -- 인지대정수금액
          , add_irt                                 -- 가산금리
          , work_mn_udae_dc_crdt_gd                 -- 근로자우대대출신용등급
          , crdt_hndo_smtime_inqr_yn                -- 신용한도동시조회여부
          , dc_sinc_rst_sms_recv_yn                 -- 대출신청결과sms수신여부
          , rlofc_no                                -- 중개업소번호
          , pay_iche_yn                             -- 급여이체여부
          , dc_psb_hndo_jamt                        -- 대출가능한도정수금액
          , afrcpt_dmb_yn                           -- 후취담보여부
          , bhsh_due_ilsi_shnd_jamt                 -- 분할상환만기일시상환정수금액
          , prm_budam_g                             -- 보험료부담구분
          , torg_dc_shnd_estm_org_cnt               -- 타기관대출상환예정기관수
          , irt_g2                                  -- 금리구분2
          , mkirt_k2                                -- 시장금리종류2
          , mrkt_giganm_k2                          -- 시장기간물종류2
          , sirt_g2                                 -- 특별금리구분2
          , dc_iyul2                                -- 대출이율2
          , br_mgr_alapv_gm_irt2                    -- 지점장전결감면금리2
          , dangi_chg_irt_rt                        -- 단기변동금리비율
          , calc_iyul2                              -- 계산이율2
          , add_irt2                                -- 가산금리2
          , rrno_cid                                -- 주민번호고객id
      from w0_shb.dwy_ypq_sinchong_mst
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
