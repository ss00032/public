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
pgm_id = 'ILBD_DWY_YER_LONGRHIS_TG'

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
s3_file_prefix = f'ibd_dwy_yer_longrhis_/ibd_dwy_yer_longrhis_{execution_kst}'

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
        delete from l0_shb.dwy_yer_longrhis
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwy_yer_longrhis
                (
                  aws_ls_dt                               -- aws적재일시
                , acno                                    -- acno
                , trx_sno                                 -- trx_sno
                , kwa_c                                   -- kwa_c
                , acser                                   -- acser
                , bas_exrt                                -- bas_exrt
                , gisdt                                   -- gisdt
                , usdcvt_rt                               -- usdcvt_rt
                , trd_bas_rt                              -- trd_bas_rt
                , buy_adj_rt                              -- buy_adj_rt
                , misuija                                 -- misuija
                , cotax_amt                               -- cotax_amt
                , fcur_ynd_ac_kwa_c                       -- fcur_ynd_ac_kwa_c
                , fcur_ynd_amt                            -- fcur_ynd_amt
                , fcur_ynd_cur_c                          -- fcur_ynd_cur_c
                , pramt                                   -- pramt
                , wonri_yc_gjdt                           -- wonri_yc_gjdt
                , int_amt                                 -- int_amt
                , fnd_ar_jdsh_feamt                       -- fnd_ar_jdsh_feamt
                , cdln_feamt                              -- cdln_feamt
                , pstn_exrt                               -- pstn_exrt
                , trn                                     -- trn
                , hsong_yn                                -- hsong_yn
                , cc_tryn                                 -- cc_tryn
                , fcur_int                                -- fcur_int
                , wcur_ynd_ac_kwa_c                       -- wcur_ynd_ac_kwa_c
                , wcur_ynd_amt                            -- wcur_ynd_amt
                , int_exrt                                -- int_exrt
                , jogish_yn                               -- jogish_yn
                , exeno                                   -- exeno
                , opp_cur_c                               -- opp_cur_c
                , tramt                                   -- tramt
                , trxdt                                   -- trxdt
                , trxaf_dc_jan                            -- trxaf_dc_jan
                , birt_chgyn                              -- birt_chgyn
                , gjamt                                   -- gjamt
                , nxt_int_npdt                            -- nxt_int_npdt
                , trm_sno                                 -- trm_sno
                , dch_amt                                 -- dch_amt
                , mgaf_yn                                 -- mgaf_yn
                , sunnp_occr_dcnt                         -- sunnp_occr_dcnt
                , sunnp_apl_dcnt                          -- sunnp_apl_dcnt
                , yakj_feamt                              -- yakj_feamt
                , ys_trx_k                                -- ys_trx_k
                , yc_yn                                   -- yc_yn
                , yc_clr_yn                               -- yc_clr_yn
                , prcdlay_gjdt                            -- prcdlay_gjdt
                , wdch_amt                                -- wdch_amt
                , iyul                                    -- iyul
                , jdsh_feamt                              -- jdsh_feamt
                , hndo_yakj_fert                          -- hndo_yakj_fert
                , hwanchl_int                             -- hwanchl_int
                , wthd_rsn_c                              -- wthd_rsn_c
                , fdch_amt                                -- fdch_amt
                , irt_g                                   -- irt_g
                , irt_term_c                              -- irt_term_c
                , ysirt_c                                 -- ysirt_c
                , iyul_tbl_sno                            -- iyul_tbl_sno
                , gmrsn_c                                 -- gmrsn_c
                , irt_cycl_term                           -- irt_cycl_term
                , fee_samt                                -- fee_samt
                , int_cycl_mcnt                           -- int_cycl_mcnt
                , bf_nxt_int_ip_dt                        -- bf_nxt_int_ip_dt
                , chan_u                                  -- chan_u
                , prn_sryn                                -- prn_sryn
                , tjum_amt                                -- tjum_amt
                , csh_amt                                 -- csh_amt
                , bf_bhsh_tamt                            -- bf_bhsh_tamt
                , c2bj_int_ip_yn                          -- c2bj_int_ip_yn
                , jdsh_feemj_yn                           -- jdsh_feemj_yn
                , finc_amt                                -- finc_amt
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , acno                                    -- acno
          , trx_sno                                 -- trx_sno
          , kwa_c                                   -- kwa_c
          , acser                                   -- acser
          , bas_exrt                                -- bas_exrt
          , gisdt                                   -- gisdt
          , usdcvt_rt                               -- usdcvt_rt
          , trd_bas_rt                              -- trd_bas_rt
          , buy_adj_rt                              -- buy_adj_rt
          , misuija                                 -- misuija
          , cotax_amt                               -- cotax_amt
          , fcur_ynd_ac_kwa_c                       -- fcur_ynd_ac_kwa_c
          , fcur_ynd_amt                            -- fcur_ynd_amt
          , fcur_ynd_cur_c                          -- fcur_ynd_cur_c
          , pramt                                   -- pramt
          , wonri_yc_gjdt                           -- wonri_yc_gjdt
          , int_amt                                 -- int_amt
          , fnd_ar_jdsh_feamt                       -- fnd_ar_jdsh_feamt
          , cdln_feamt                              -- cdln_feamt
          , pstn_exrt                               -- pstn_exrt
          , trn                                     -- trn
          , hsong_yn                                -- hsong_yn
          , cc_tryn                                 -- cc_tryn
          , fcur_int                                -- fcur_int
          , wcur_ynd_ac_kwa_c                       -- wcur_ynd_ac_kwa_c
          , wcur_ynd_amt                            -- wcur_ynd_amt
          , int_exrt                                -- int_exrt
          , jogish_yn                               -- jogish_yn
          , exeno                                   -- exeno
          , opp_cur_c                               -- opp_cur_c
          , tramt                                   -- tramt
          , trxdt                                   -- trxdt
          , trxaf_dc_jan                            -- trxaf_dc_jan
          , birt_chgyn                              -- birt_chgyn
          , gjamt                                   -- gjamt
          , nxt_int_npdt                            -- nxt_int_npdt
          , trm_sno                                 -- trm_sno
          , dch_amt                                 -- dch_amt
          , mgaf_yn                                 -- mgaf_yn
          , sunnp_occr_dcnt                         -- sunnp_occr_dcnt
          , sunnp_apl_dcnt                          -- sunnp_apl_dcnt
          , yakj_feamt                              -- yakj_feamt
          , ys_trx_k                                -- ys_trx_k
          , yc_yn                                   -- yc_yn
          , yc_clr_yn                               -- yc_clr_yn
          , prcdlay_gjdt                            -- prcdlay_gjdt
          , wdch_amt                                -- wdch_amt
          , iyul                                    -- iyul
          , jdsh_feamt                              -- jdsh_feamt
          , hndo_yakj_fert                          -- hndo_yakj_fert
          , hwanchl_int                             -- hwanchl_int
          , wthd_rsn_c                              -- wthd_rsn_c
          , fdch_amt                                -- fdch_amt
          , irt_g                                   -- irt_g
          , irt_term_c                              -- irt_term_c
          , ysirt_c                                 -- ysirt_c
          , iyul_tbl_sno                            -- iyul_tbl_sno
          , gmrsn_c                                 -- gmrsn_c
          , irt_cycl_term                           -- irt_cycl_term
          , fee_samt                                -- fee_samt
          , int_cycl_mcnt                           -- int_cycl_mcnt
          , bf_nxt_int_ip_dt                        -- bf_nxt_int_ip_dt
          , chan_u                                  -- chan_u
          , prn_sryn                                -- prn_sryn
          , tjum_amt                                -- tjum_amt
          , csh_amt                                 -- csh_amt
          , bf_bhsh_tamt                            -- bf_bhsh_tamt
          , c2bj_int_ip_yn                          -- c2bj_int_ip_yn
          , jdsh_feemj_yn                           -- jdsh_feemj_yn
          , finc_amt                                -- finc_amt
      from w0_shb.dwy_yer_longrhis
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
