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
pgm_id = 'ILBD_DWY_YSQ_SINCHUNG_TG'

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
s3_file_prefix = f'ibd_dwy_ysq_sinchung_/ibd_dwy_ysq_sinchung_{execution_kst}'

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
        delete from l0_shb.dwy_ysq_sinchung
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwy_ysq_sinchung
                (
                  aws_ls_dt                               -- aws적재일시
                , ci_no                                   -- ci번호
                , cusno                                   -- 고객번호
                , sinc_no                                 -- 신청번호
                , psn_corp_g                              -- 개인기업구분
                , ypq_path_c                              -- 개인신청경로code
                , sinc_jgcng_upmu_c                       -- 신청조건변경업무code
                , hinsr_pay_obnkjj_yn                     -- 건강보험급여당행지정여부
                , dmb_qty_cnt                             -- 담보수량건수
                , dmb_dstr_samt                           -- 담보배분합계금액
                , yhdmb_samt                              -- 유효담보합계금액
                , dc_sinc_rst_sms_recv_yn                 -- 대출신청결과sms수신여부
                , tbnk_reln_dc_ycdt_d10_ovr_yn            -- 타행대환대출연체일10일초과여부
                , rest_org_c                              -- 요양기관code
                , estb_m3_gt_pass_yn                      -- 개업3개월이상경과여부
                , lm6_eoiryo_ask_amt                      -- 최근6개월의료급여청구금액
                , lm2_yoyang_ask_amt                      -- 최근2개월요양급여청구금액
                , lm12_eoiryo_ask_amt                     -- 최근12개월의료급여청구금액
                , comm_estb_yn                            -- 공동창업여부
                , soho_ys_law_exct_c1                     -- soho여신규정예외code1
                , soho_ys_law_exct_c2                     -- soho여신규정예외code2
                , soho_ys_law_exct_c3                     -- soho여신규정예외code3
                , gitrt_drvt_crdt_cvt_tamt                -- 기취급파생상품신용환산총금액
                , gitrt_drvt_crdt_cvt_dmb_tamt            -- 기취급파생상품신용환산담보총금액
                , tbnk_tot_dcamt                          -- 타행총대출금액
                , tbnk_dmb_ys_tamt                        -- 타행담보여신총금액
                , tbnk_crdt_ys_tamt                       -- 타행신용여신총금액
                , tbnk_forg_cnt                           -- 타행금융기관수
                , tbnk_hmys_tamt                          -- 타행가계여신총금액
                , tbnk_corp_ys_tamt                       -- 타행기업여신총금액
                , tbnk_crdc_shwamt                        -- 타행신용대출상환금액
                , tbnk_dmbdc_shwamt                       -- 타행담보대출상환금액
                , obnk_chaju_tot_ys_amt                   -- 당행차주총여신금액
                , obnk_chaju_corp_ys_tamt                 -- 당행차주기업여신총금액
                , obnk_chaju_dmb_ys_amt                   -- 당행차주담보여신금액
                , obnk_chaju_crdt_ys_amt                  -- 당행차주신용여신금액
                , obnk_chaju_bojeungamt                   -- 당행차주보증금액
                , hlbl_ebnd_dmb_dc_amt                    -- 할인어음전자채권담보대출금액
                , hlbl_ebnd_dmbdc_dmb_amt                 -- 할인어음전자채권담보대출담보금액
                , hlbl_ebnd_dmbdc_etc_dmb_amt             -- 할인어음전자채권담보대출기타담보금액
                , sgt_imlc_amt                            -- 일람불수입신용장금액
                , sgt_imlc_dmb_amt                        -- 일람불수입신용장담보금액
                , sgt_imlc_etc_dmb_amt                    -- 일람불수입신용장기타담보금액
                , etc_colt_bf_buy_amt                     -- 기타추심전매입금액
                , etc_colt_bf_buy_dmb_amt                 -- 기타추심전매입담보금액
                , etc_colt_bf_buy_etc_dmb_amt             -- 기타추심전매입기타담보금액
                , kepco_ordr_ln_amt                       -- 한전발주론금액
                , kepco_ordr_ln_dmb_amt                   -- 한전발주론담보금액
                , kepco_ordr_ln_etc_amt                   -- 한전발주론기타금액
                , etc_same_mn_hndo_excpt_amt              -- 기타동일인한도제외금액
                , etc_same_hndo_excpt_crdt_amt            -- 기타동일한도제외신용금액
                , etc_same_mn_hndo_excpt_dmb_amt          -- 기타동일인한도제외담보금액
                , etc_same_mn_hndo_excpt_etc_amt          -- 기타동일인한도제외기타금액
                , same_mn_hndo_crdt_exp_amt               -- 동일인한도신용exposure금액
                , same_mn_hndo_tot_exp_amt                -- 동일인한도총exposure금액
                , hlbl_ebnd_dmbdc_exp_amt                 -- 할인어음전자채권담보대출exposure금액
                , hlbl_ebnd_dmbdc_crdt_exp_amt            -- 할인어음전자채권담보대출신용exposure금액
                , sgt_imlc_exp_amt                        -- 일람불수입신용장exposure금액
                , sgt_imlc_crdt_exp_amt                   -- 일람불수입신용장신용exposure금액
                , etc_colt_bf_buy_exp_amt                 -- 기타추심전매입exposure금액
                , etc_colt_bf_buy_crdt_exp_amt            -- 기타추심전매입신용exposure금액
                , kepco_ordr_ln_exp_amt                   -- 한전발주론exposure금액
                , kepco_ordr_ln_crdt_exp_amt              -- 한전발주론신용exposure금액
                , ly1_btw_buy_debt_amt                    -- 최근1년간매입채무금액
                , lm6_revn_amt                            -- 최근6개월매출금액
                , ltst_m3_revn_amt                        -- 최근3개월매출금액
                , estm_revn_amt                           -- 추정매출금액
                , crjs_wrt_omit_gnyn                      -- 신용조사작성생략가능여부
                , dw_data_gjdt                            -- dwdata기준일자
                , dw_lst_jukja_dt                         -- dw최종적재일자
                , soho_jgcng_upmu_c                       -- soho조건변경업무code
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , ci_no                                   -- ci번호
          , cusno                                   -- 고객번호
          , sinc_no                                 -- 신청번호
          , psn_corp_g                              -- 개인기업구분
          , ypq_path_c                              -- 개인신청경로code
          , sinc_jgcng_upmu_c                       -- 신청조건변경업무code
          , hinsr_pay_obnkjj_yn                     -- 건강보험급여당행지정여부
          , dmb_qty_cnt                             -- 담보수량건수
          , dmb_dstr_samt                           -- 담보배분합계금액
          , yhdmb_samt                              -- 유효담보합계금액
          , dc_sinc_rst_sms_recv_yn                 -- 대출신청결과sms수신여부
          , tbnk_reln_dc_ycdt_d10_ovr_yn            -- 타행대환대출연체일10일초과여부
          , rest_org_c                              -- 요양기관code
          , estb_m3_gt_pass_yn                      -- 개업3개월이상경과여부
          , lm6_eoiryo_ask_amt                      -- 최근6개월의료급여청구금액
          , lm2_yoyang_ask_amt                      -- 최근2개월요양급여청구금액
          , lm12_eoiryo_ask_amt                     -- 최근12개월의료급여청구금액
          , comm_estb_yn                            -- 공동창업여부
          , soho_ys_law_exct_c1                     -- soho여신규정예외code1
          , soho_ys_law_exct_c2                     -- soho여신규정예외code2
          , soho_ys_law_exct_c3                     -- soho여신규정예외code3
          , gitrt_drvt_crdt_cvt_tamt                -- 기취급파생상품신용환산총금액
          , gitrt_drvt_crdt_cvt_dmb_tamt            -- 기취급파생상품신용환산담보총금액
          , tbnk_tot_dcamt                          -- 타행총대출금액
          , tbnk_dmb_ys_tamt                        -- 타행담보여신총금액
          , tbnk_crdt_ys_tamt                       -- 타행신용여신총금액
          , tbnk_forg_cnt                           -- 타행금융기관수
          , tbnk_hmys_tamt                          -- 타행가계여신총금액
          , tbnk_corp_ys_tamt                       -- 타행기업여신총금액
          , tbnk_crdc_shwamt                        -- 타행신용대출상환금액
          , tbnk_dmbdc_shwamt                       -- 타행담보대출상환금액
          , obnk_chaju_tot_ys_amt                   -- 당행차주총여신금액
          , obnk_chaju_corp_ys_tamt                 -- 당행차주기업여신총금액
          , obnk_chaju_dmb_ys_amt                   -- 당행차주담보여신금액
          , obnk_chaju_crdt_ys_amt                  -- 당행차주신용여신금액
          , obnk_chaju_bojeungamt                   -- 당행차주보증금액
          , hlbl_ebnd_dmb_dc_amt                    -- 할인어음전자채권담보대출금액
          , hlbl_ebnd_dmbdc_dmb_amt                 -- 할인어음전자채권담보대출담보금액
          , hlbl_ebnd_dmbdc_etc_dmb_amt             -- 할인어음전자채권담보대출기타담보금액
          , sgt_imlc_amt                            -- 일람불수입신용장금액
          , sgt_imlc_dmb_amt                        -- 일람불수입신용장담보금액
          , sgt_imlc_etc_dmb_amt                    -- 일람불수입신용장기타담보금액
          , etc_colt_bf_buy_amt                     -- 기타추심전매입금액
          , etc_colt_bf_buy_dmb_amt                 -- 기타추심전매입담보금액
          , etc_colt_bf_buy_etc_dmb_amt             -- 기타추심전매입기타담보금액
          , kepco_ordr_ln_amt                       -- 한전발주론금액
          , kepco_ordr_ln_dmb_amt                   -- 한전발주론담보금액
          , kepco_ordr_ln_etc_amt                   -- 한전발주론기타금액
          , etc_same_mn_hndo_excpt_amt              -- 기타동일인한도제외금액
          , etc_same_hndo_excpt_crdt_amt            -- 기타동일한도제외신용금액
          , etc_same_mn_hndo_excpt_dmb_amt          -- 기타동일인한도제외담보금액
          , etc_same_mn_hndo_excpt_etc_amt          -- 기타동일인한도제외기타금액
          , same_mn_hndo_crdt_exp_amt               -- 동일인한도신용exposure금액
          , same_mn_hndo_tot_exp_amt                -- 동일인한도총exposure금액
          , hlbl_ebnd_dmbdc_exp_amt                 -- 할인어음전자채권담보대출exposure금액
          , hlbl_ebnd_dmbdc_crdt_exp_amt            -- 할인어음전자채권담보대출신용exposure금액
          , sgt_imlc_exp_amt                        -- 일람불수입신용장exposure금액
          , sgt_imlc_crdt_exp_amt                   -- 일람불수입신용장신용exposure금액
          , etc_colt_bf_buy_exp_amt                 -- 기타추심전매입exposure금액
          , etc_colt_bf_buy_crdt_exp_amt            -- 기타추심전매입신용exposure금액
          , kepco_ordr_ln_exp_amt                   -- 한전발주론exposure금액
          , kepco_ordr_ln_crdt_exp_amt              -- 한전발주론신용exposure금액
          , ly1_btw_buy_debt_amt                    -- 최근1년간매입채무금액
          , lm6_revn_amt                            -- 최근6개월매출금액
          , ltst_m3_revn_amt                        -- 최근3개월매출금액
          , estm_revn_amt                           -- 추정매출금액
          , crjs_wrt_omit_gnyn                      -- 신용조사작성생략가능여부
          , dw_data_gjdt                            -- dwdata기준일자
          , dw_lst_jukja_dt                         -- dw최종적재일자
          , soho_jgcng_upmu_c                       -- soho조건변경업무code
      from w0_shb.dwy_ysq_sinchung
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
