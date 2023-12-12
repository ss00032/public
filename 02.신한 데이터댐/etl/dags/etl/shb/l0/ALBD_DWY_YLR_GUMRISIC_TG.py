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
pgm_id = 'ALBD_DWY_YLR_GUMRISIC_TG'

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
s3_file_prefix = f'abd_dwy_ylr_gumrisic_/abd_dwy_ylr_gumrisic_{execution_kst}'

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
        delete from l0_shb.dwy_ylr_gumrisic
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwy_ylr_gumrisic
                (
                  aws_ls_dt                               -- aws적재일시
                , ci_no                                   -- ci번호
                , cusno                                   -- 고객번호
                , sinc_no                                 -- 신청번호
                , sangdam_sinc_jinhg_c                    -- 상담신청진행code
                , udirt_aprv_dsyn                         -- 우대금리승인대상여부
                , corp_scal_c                             -- 기업규모code
                , sfg_crval_model_g                       -- sfg신용평가모델구분
                , crdt_gd                                 -- 신용등급
                , bojeungamt                              -- 보증금액
                , indst_gd                                -- 산업등급
                , sindst_b                                -- 표준산업분류
                , lgd_rt                                  -- lgd비율
                , bdrt                                    -- 부도율
                , ys_term_bdrt                            -- 여신기간별부도율
                , prdt_c                                  -- 상품code
                , plcy_c                                  -- 정책code
                , unyong_g                                -- 운용구분
                , sinc_g                                  -- 신청구분
                , irt_g                                   -- 금리구분
                , mkirt_k                                 -- 시장금리종류
                , irt_term_c                              -- 금리기간code
                , shnd_mth                                -- 상환방법
                , bhsh_eq_g                               -- 분할상환균등구분
                , tamt_ds_apl_g                           -- 총액대상적용구분
                , dc_tmuni_c                              -- 대출기간단위code
                , dc_term                                 -- 대출기간
                , dc_dudt                                 -- 대출만기일자
                , gchtm_mcnt                              -- 거치기간월수
                , chaip_sincdt                            -- 차입신청일자
                , cur_c                                   -- 통화code
                , wcvt_amt                                -- 원화환산금액
                , apl_exrt                                -- 적용환율
                , sinc_amt                                -- 신청금액
                , irt_tax_rt                              -- 금리tax율
                , irt_edtax_rt                            -- 금리교육세율
                , crdt_gd_sprd_iyul                       -- 신용도spread이율
                , term_sprd_iyul                          -- 기간스프레드이율
                , rskcpt_fee_rt                           -- 위험자본비용율
                , upmu_adint_rt                           -- 업무원가율
                , dc_oprc_iyul                            -- 대출원가이율
                , fcst_margrt                             -- 예상margin율
                , sys_calc_iyul                           -- 시스템산출이율
                , adj_udae_irt                            -- 조정우대금리
                , decs_irt                                -- 결정금리
                , bas_irt                                 -- 기준금리
                , add_irt                                 -- 가산금리
                , chaip_each_yrtm_suip_int                -- 차입건별연간수입이자
                , plcy_irt                                -- 정책금리
                , saupb_spec_irt                          -- 사업부특별금리
                , chaip_iyul                              -- 차입이율
                , chaip_rt                                -- 차입비율
                , nocr_iyul                               -- 소요자기자본율이율
                , revn_amt                                -- 매출금액
                , due_adj_iyul                            -- 만기조정이율
                , nocr_crcft_rt                           -- 소요자기자본율상관계수율
                , gm_rt                                   -- 감면율
                , smamt_add_irt                           -- 소액가산금리
                , insr_rt                                 -- 보험율
                , chaip_efct_irt                          -- 차입효과금리
                , irt_adj_rsn_c                           -- 금리조정사유code
                , chaip_each_yrtm_pl_amt                  -- 차입건별연간손익금액
                , chaip_each_mmby_pl_amt                  -- 차입건별월별손익금액
                , glv_amt                                 -- 평가손익금액
                , irt_calc_dt                             -- 금리산출일자
                , aprvno                                  -- 승인번호
                , sidt                                    -- 승인일자
                , brno                                    -- 점번호
                , irt_yssuiks_bas_yymm                    -- 금리예상수익성기준년월
                , irt_cus_suiks_bas_yymm                  -- 금리고객수익성기준년월
                , yngi_irt_bas_ml_chgyn                   -- 연기금리기준물변경여부
                , gu_mkirt_k                              -- 구시장금리종류
                , gu_irt_term_c                           -- 구금리기간code
                , sys_irt_adcs_g                          -- 시스템금리전결구분
                , hand_irt_adcs_g                         -- 수기금리전결구분
                , cdt                                     -- 변경일자
                , edtax_gaman_bf_decs_irt                 -- 교육세감안전결정금리
                , irt_inha_req_yn                         -- 금리인하요구여부
                , irt_inha_req_rsn_c                      -- 금리인하요구사유code
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , ci_no                                   -- ci번호
          , cusno                                   -- 고객번호
          , sinc_no                                 -- 신청번호
          , sangdam_sinc_jinhg_c                    -- 상담신청진행code
          , udirt_aprv_dsyn                         -- 우대금리승인대상여부
          , corp_scal_c                             -- 기업규모code
          , sfg_crval_model_g                       -- sfg신용평가모델구분
          , crdt_gd                                 -- 신용등급
          , bojeungamt                              -- 보증금액
          , indst_gd                                -- 산업등급
          , sindst_b                                -- 표준산업분류
          , lgd_rt                                  -- lgd비율
          , bdrt                                    -- 부도율
          , ys_term_bdrt                            -- 여신기간별부도율
          , prdt_c                                  -- 상품code
          , plcy_c                                  -- 정책code
          , unyong_g                                -- 운용구분
          , sinc_g                                  -- 신청구분
          , irt_g                                   -- 금리구분
          , mkirt_k                                 -- 시장금리종류
          , irt_term_c                              -- 금리기간code
          , shnd_mth                                -- 상환방법
          , bhsh_eq_g                               -- 분할상환균등구분
          , tamt_ds_apl_g                           -- 총액대상적용구분
          , dc_tmuni_c                              -- 대출기간단위code
          , dc_term                                 -- 대출기간
          , dc_dudt                                 -- 대출만기일자
          , gchtm_mcnt                              -- 거치기간월수
          , chaip_sincdt                            -- 차입신청일자
          , cur_c                                   -- 통화code
          , wcvt_amt                                -- 원화환산금액
          , apl_exrt                                -- 적용환율
          , sinc_amt                                -- 신청금액
          , irt_tax_rt                              -- 금리tax율
          , irt_edtax_rt                            -- 금리교육세율
          , crdt_gd_sprd_iyul                       -- 신용도spread이율
          , term_sprd_iyul                          -- 기간스프레드이율
          , rskcpt_fee_rt                           -- 위험자본비용율
          , upmu_adint_rt                           -- 업무원가율
          , dc_oprc_iyul                            -- 대출원가이율
          , fcst_margrt                             -- 예상margin율
          , sys_calc_iyul                           -- 시스템산출이율
          , adj_udae_irt                            -- 조정우대금리
          , decs_irt                                -- 결정금리
          , bas_irt                                 -- 기준금리
          , add_irt                                 -- 가산금리
          , chaip_each_yrtm_suip_int                -- 차입건별연간수입이자
          , plcy_irt                                -- 정책금리
          , saupb_spec_irt                          -- 사업부특별금리
          , chaip_iyul                              -- 차입이율
          , chaip_rt                                -- 차입비율
          , nocr_iyul                               -- 소요자기자본율이율
          , revn_amt                                -- 매출금액
          , due_adj_iyul                            -- 만기조정이율
          , nocr_crcft_rt                           -- 소요자기자본율상관계수율
          , gm_rt                                   -- 감면율
          , smamt_add_irt                           -- 소액가산금리
          , insr_rt                                 -- 보험율
          , chaip_efct_irt                          -- 차입효과금리
          , irt_adj_rsn_c                           -- 금리조정사유code
          , chaip_each_yrtm_pl_amt                  -- 차입건별연간손익금액
          , chaip_each_mmby_pl_amt                  -- 차입건별월별손익금액
          , glv_amt                                 -- 평가손익금액
          , irt_calc_dt                             -- 금리산출일자
          , aprvno                                  -- 승인번호
          , sidt                                    -- 승인일자
          , brno                                    -- 점번호
          , irt_yssuiks_bas_yymm                    -- 금리예상수익성기준년월
          , irt_cus_suiks_bas_yymm                  -- 금리고객수익성기준년월
          , yngi_irt_bas_ml_chgyn                   -- 연기금리기준물변경여부
          , gu_mkirt_k                              -- 구시장금리종류
          , gu_irt_term_c                           -- 구금리기간code
          , sys_irt_adcs_g                          -- 시스템금리전결구분
          , hand_irt_adcs_g                         -- 수기금리전결구분
          , cdt                                     -- 변경일자
          , edtax_gaman_bf_decs_irt                 -- 교육세감안전결정금리
          , irt_inha_req_yn                         -- 금리인하요구여부
          , irt_inha_req_rsn_c                      -- 금리인하요구사유code
      from w0_shb.dwy_ylr_gumrisic
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
