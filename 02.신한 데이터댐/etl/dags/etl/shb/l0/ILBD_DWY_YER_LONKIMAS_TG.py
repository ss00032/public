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
pgm_id = 'ILBD_DWY_YER_LONKIMAS_TG'

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
s3_file_prefix = f'ibd_dwy_yer_lonkimas_/ibd_dwy_yer_lonkimas_{execution_kst}'

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
        delete from l0_shb.dwy_yer_lonkimas
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwy_yer_lonkimas
                (
                  aws_ls_dt                               -- aws적재일시
                , ci_no                                   -- ci번호
                , cusno                                   -- 고객번호
                , acno                                    -- 계좌번호
                , aprvno                                  -- 승인번호
                , aprv_ser                                -- 승인일련번호
                , act_c                                   -- 계정code
                , smok_c                                  -- 세목code
                , kwa_c                                   -- 과목code
                , acser                                   -- 계좌일련번호
                , grbrno                                  -- 관리점번호
                , ys_ac_s                                 -- 여신계좌상태
                , giikss_dt                               -- 기한이익상실일자
                , dc_tamt                                 -- 대출누계금액
                , sel_tamt                                -- 매각누계금액
                , sgak_tamt                               -- 상각누계금액
                , prdt_c                                  -- 상품code
                , prcp_cmplt_dt                           -- 원금완제일자
                , iyul_gmrsn_c                            -- 이율감면사유code
                , c2bj_iyul                               -- 이차보전이율
                , lst_trxdt                               -- 최종거래일자
                , fst_exe_dt                              -- 최초실행일자
                , hndo_nus_fert                           -- 한도미사용수수료율
                , hndo_nus_fee_lcalc_dt                   -- 한도미사용수수료최종계산일자
                , dc_jan                                  -- 대출잔액
                , mgln_term_mcnt                          -- 모기지론기간월수
                , nus_fee_calc_sjdt                       -- 미사용수수료계산시작일자
                , misuija                                 -- 미수이자
                , vntr_fund_amt                           -- 벤처자금금액
                , awd_rt                                  -- 보상비율
                , awd_dep_feamt                           -- 보상예금수수료금액
                , awd_dep_clr_dt                          -- 보상예금정산일자
                , awd_udae_rt                             -- 보상우대율
                , bjorg_cono                              -- 보증기관법인번호
                , pens_ji_gjdt                            -- 연금지급기준일자
                , autoln_asg_amt                          -- 오토론분담금액
                , c2bj_amt                                -- 이차보전금액
                , lgtm_dc_cvt_yjdt                        -- 장기대출전환예정일자
                , plcy_c                                  -- 정책code
                , jdsh_feemj_yn                           -- 중도상환수수료면제여부
                , tot_exe_cnt                             -- 총실행수
                , sel_mfr_c                               -- 매각업체code
                , nus_feamt                               -- 미사용수수료금액
                , sgak_dt                                 -- 상각일자
                , sgak_c                                  -- 상각code
                , yakj_fee_k                              -- 약정수수료종류
                , actv_exe_cnt                            -- 활동실행수
                , cur_c                                   -- 통화code
                , bhshdc_prcdlay_apl_cnt                  -- 분할상환대출원금연체적용횟수
                , sgak_misuint                            -- 상각미수이자
                , ycint_exmp_endt                         -- 연체이자면제종료일자
                , exct_trx_g                              -- 예외거래구분
                , dc_u                                    -- 대출유형
                , dc_ji_cycl_mcnt                         -- 대출지급주기월수
                , reln_bf_ac_kwa_c                        -- 대환전계좌과목code
                , reln_bf_acser                           -- 대환전계좌일련번호
                , busmok_c                                -- 부세목code
                , shnd_mth                                -- 상환방법
                , yakj_sjdt                               -- 약정시작일자
                , yngi_dt                                 -- 연기일자
                , ynd_ac_kwa_c                            -- 연동계좌과목code
                , ynd_acser                               -- 연동계좌일련번호
                , int_seonhuchwi_g                        -- 이자선후취구분
                , int_cycl_mcnt                           -- 이자주기월수
                , msg_occpk_c                             -- 전문직종code
                , bf_plcy_c                               -- 전정책code
                , bf_ys_ac_s                              -- 전여신계좌상태
                , dudt_apnt_shwamt                        -- 만기일자지정상환금액
                , gmirt_apl_endt1                         -- 감면금리적용종료일자1
                , gmirt_apl_endt2                         -- 감면금리적용종료일자2
                , irt_g                                   -- 금리구분
                , term_ovr_irt                            -- 기간초과금리
                , drdt                                    -- 등록일자
                , exeno                                   -- 실행번호
                , prcp_int_amt                            -- 원리금액
                , apl_irt_iyul1                           -- 적용금리이율1
                , apl_irt_iyul2                           -- 적용금리이율2
                , apl_irt_iyul3                           -- 적용금리이율3
                , apl_irt_iyul4                           -- 적용금리이율4
                , apl_irt_iyul5                           -- 적용금리이율5
                , apl_irt_iyul6                           -- 적용금리이율6
                , jydt                                    -- 적용일자
                , jogn_cdt                                -- 조건변경일자
                , mdl_prcp_shnd_mth                       -- 중도원금상환방법
                , chaip_efct_rflx_yn                      -- 차입효과반영여부
                , tamt_ds_iyul_g                          -- 총액대상이율구분
                , plcy_elmt_irt                           -- 정책요소금리
                , frdc_jodal_irt                          -- 외화대출조달금리
                , ys_mas_s                                -- 여신원장상태
                , wonri_dc_gchtm_mcnt                     -- 원리금대출거치기간월수
                , dc_bas_dd                               -- 대출기준일
                , wonri_dc_gjdt                           -- 원리금대출기준일자
                , dc_dudt                                 -- 대출만기일자
                , unyong_g                                -- 운용구분
                , crtfee_fee_budam_g                      -- 설정비비용부담구분
                , hs_afrcpt_dmb_dtbnk_g                   -- 주택후취담보당타행구분
                , jistop_yn                               -- 지급정지여부
                , jistop_amt                              -- 지급정지금액
                , jdsh_feemj_sjdt                         -- 중도상환수수료면제시작일자
                , yakj_yngi_dt                            -- 약정연기일자
                , iche_ac_kwa_c                           -- 이체계좌과목code
                , iche_acser                              -- 이체계좌일련번호
                , ysirt_c                                 -- 여신금리code
                , irt_term_c                              -- 금리기간code
                , gmirt_apl_endt3                         -- 감면금리적용종료일자3
                , gm_irt1                                 -- 감면금리1
                , gm_irt2                                 -- 감면금리2
                , gm_irt3                                 -- 감면금리3
                , irt_cycl_term                           -- 금리주기기간
                , jdsh_fee_tamt                           -- 중도상환수수료누계금액
                , chaip_plcy_c                            -- 차입정책code
                , ac_cvt_re_balyn                         -- 계좌전환재발급여부
                , jdsh_fee_apl_g                          -- 중도상환수수료적용구분
                , rev_jtdc_ac_kwa_c                       -- 리볼빙종통대계좌과목code
                , rev_jtdc_acser                          -- 리볼빙종통대계좌일련번호
                , abs_yngi_dt                             -- abs연기일자
                , supl_prdt_c1                            -- 부가상품code1
                , supl_prdt_c2                            -- 부가상품code2
                , supl_prdt_c3                            -- 부가상품code3
                , sel_dt                                  -- 매각일자
                , psn_revv_prgr_s                         -- 개인회생진행상태
                , bankr_prgr_s                            -- 파산진행상태
                , wkout_prgr_s                            -- workout진행상태
                , mn3rd_reln_ac_kwa_c                     -- 제3자대환계좌과목code
                , mn3rd_reln_acser                        -- 제3자대환계좌일련번호
                , witak_bojn_no                           -- 위탁보증번호
                , act_g                                   -- 계정구분
                , iche_cnt                                -- 이체건수
                , sugg_feamt                              -- 권유수수료금액
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , ci_no                                   -- ci번호
          , cusno                                   -- 고객번호
          , acno                                    -- 계좌번호
          , aprvno                                  -- 승인번호
          , aprv_ser                                -- 승인일련번호
          , act_c                                   -- 계정code
          , smok_c                                  -- 세목code
          , kwa_c                                   -- 과목code
          , acser                                   -- 계좌일련번호
          , grbrno                                  -- 관리점번호
          , ys_ac_s                                 -- 여신계좌상태
          , giikss_dt                               -- 기한이익상실일자
          , dc_tamt                                 -- 대출누계금액
          , sel_tamt                                -- 매각누계금액
          , sgak_tamt                               -- 상각누계금액
          , prdt_c                                  -- 상품code
          , prcp_cmplt_dt                           -- 원금완제일자
          , iyul_gmrsn_c                            -- 이율감면사유code
          , c2bj_iyul                               -- 이차보전이율
          , lst_trxdt                               -- 최종거래일자
          , fst_exe_dt                              -- 최초실행일자
          , hndo_nus_fert                           -- 한도미사용수수료율
          , hndo_nus_fee_lcalc_dt                   -- 한도미사용수수료최종계산일자
          , dc_jan                                  -- 대출잔액
          , mgln_term_mcnt                          -- 모기지론기간월수
          , nus_fee_calc_sjdt                       -- 미사용수수료계산시작일자
          , misuija                                 -- 미수이자
          , vntr_fund_amt                           -- 벤처자금금액
          , awd_rt                                  -- 보상비율
          , awd_dep_feamt                           -- 보상예금수수료금액
          , awd_dep_clr_dt                          -- 보상예금정산일자
          , awd_udae_rt                             -- 보상우대율
          , bjorg_cono                              -- 보증기관법인번호
          , pens_ji_gjdt                            -- 연금지급기준일자
          , autoln_asg_amt                          -- 오토론분담금액
          , c2bj_amt                                -- 이차보전금액
          , lgtm_dc_cvt_yjdt                        -- 장기대출전환예정일자
          , plcy_c                                  -- 정책code
          , jdsh_feemj_yn                           -- 중도상환수수료면제여부
          , tot_exe_cnt                             -- 총실행수
          , sel_mfr_c                               -- 매각업체code
          , nus_feamt                               -- 미사용수수료금액
          , sgak_dt                                 -- 상각일자
          , sgak_c                                  -- 상각code
          , yakj_fee_k                              -- 약정수수료종류
          , actv_exe_cnt                            -- 활동실행수
          , cur_c                                   -- 통화code
          , bhshdc_prcdlay_apl_cnt                  -- 분할상환대출원금연체적용횟수
          , sgak_misuint                            -- 상각미수이자
          , ycint_exmp_endt                         -- 연체이자면제종료일자
          , exct_trx_g                              -- 예외거래구분
          , dc_u                                    -- 대출유형
          , dc_ji_cycl_mcnt                         -- 대출지급주기월수
          , reln_bf_ac_kwa_c                        -- 대환전계좌과목code
          , reln_bf_acser                           -- 대환전계좌일련번호
          , busmok_c                                -- 부세목code
          , shnd_mth                                -- 상환방법
          , yakj_sjdt                               -- 약정시작일자
          , yngi_dt                                 -- 연기일자
          , ynd_ac_kwa_c                            -- 연동계좌과목code
          , ynd_acser                               -- 연동계좌일련번호
          , int_seonhuchwi_g                        -- 이자선후취구분
          , int_cycl_mcnt                           -- 이자주기월수
          , msg_occpk_c                             -- 전문직종code
          , bf_plcy_c                               -- 전정책code
          , bf_ys_ac_s                              -- 전여신계좌상태
          , dudt_apnt_shwamt                        -- 만기일자지정상환금액
          , gmirt_apl_endt1                         -- 감면금리적용종료일자1
          , gmirt_apl_endt2                         -- 감면금리적용종료일자2
          , irt_g                                   -- 금리구분
          , term_ovr_irt                            -- 기간초과금리
          , drdt                                    -- 등록일자
          , exeno                                   -- 실행번호
          , prcp_int_amt                            -- 원리금액
          , apl_irt_iyul1                           -- 적용금리이율1
          , apl_irt_iyul2                           -- 적용금리이율2
          , apl_irt_iyul3                           -- 적용금리이율3
          , apl_irt_iyul4                           -- 적용금리이율4
          , apl_irt_iyul5                           -- 적용금리이율5
          , apl_irt_iyul6                           -- 적용금리이율6
          , jydt                                    -- 적용일자
          , jogn_cdt                                -- 조건변경일자
          , mdl_prcp_shnd_mth                       -- 중도원금상환방법
          , chaip_efct_rflx_yn                      -- 차입효과반영여부
          , tamt_ds_iyul_g                          -- 총액대상이율구분
          , plcy_elmt_irt                           -- 정책요소금리
          , frdc_jodal_irt                          -- 외화대출조달금리
          , ys_mas_s                                -- 여신원장상태
          , wonri_dc_gchtm_mcnt                     -- 원리금대출거치기간월수
          , dc_bas_dd                               -- 대출기준일
          , wonri_dc_gjdt                           -- 원리금대출기준일자
          , dc_dudt                                 -- 대출만기일자
          , unyong_g                                -- 운용구분
          , crtfee_fee_budam_g                      -- 설정비비용부담구분
          , hs_afrcpt_dmb_dtbnk_g                   -- 주택후취담보당타행구분
          , jistop_yn                               -- 지급정지여부
          , jistop_amt                              -- 지급정지금액
          , jdsh_feemj_sjdt                         -- 중도상환수수료면제시작일자
          , yakj_yngi_dt                            -- 약정연기일자
          , iche_ac_kwa_c                           -- 이체계좌과목code
          , iche_acser                              -- 이체계좌일련번호
          , ysirt_c                                 -- 여신금리code
          , irt_term_c                              -- 금리기간code
          , gmirt_apl_endt3                         -- 감면금리적용종료일자3
          , gm_irt1                                 -- 감면금리1
          , gm_irt2                                 -- 감면금리2
          , gm_irt3                                 -- 감면금리3
          , irt_cycl_term                           -- 금리주기기간
          , jdsh_fee_tamt                           -- 중도상환수수료누계금액
          , chaip_plcy_c                            -- 차입정책code
          , ac_cvt_re_balyn                         -- 계좌전환재발급여부
          , jdsh_fee_apl_g                          -- 중도상환수수료적용구분
          , rev_jtdc_ac_kwa_c                       -- 리볼빙종통대계좌과목code
          , rev_jtdc_acser                          -- 리볼빙종통대계좌일련번호
          , abs_yngi_dt                             -- abs연기일자
          , supl_prdt_c1                            -- 부가상품code1
          , supl_prdt_c2                            -- 부가상품code2
          , supl_prdt_c3                            -- 부가상품code3
          , sel_dt                                  -- 매각일자
          , psn_revv_prgr_s                         -- 개인회생진행상태
          , bankr_prgr_s                            -- 파산진행상태
          , wkout_prgr_s                            -- workout진행상태
          , mn3rd_reln_ac_kwa_c                     -- 제3자대환계좌과목code
          , mn3rd_reln_acser                        -- 제3자대환계좌일련번호
          , witak_bojn_no                           -- 위탁보증번호
          , act_g                                   -- 계정구분
          , iche_cnt                                -- 이체건수
          , sugg_feamt                              -- 권유수수료금액
      from w0_shb.dwy_yer_lonkimas
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
