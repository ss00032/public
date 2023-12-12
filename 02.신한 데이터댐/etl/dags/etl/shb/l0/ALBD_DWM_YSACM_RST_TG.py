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
pgm_id = 'ALBD_DWM_YSACM_RST_TG'

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
s3_file_prefix = f'abd_dwm_ysacm_rst_/abd_dwm_ysacm_rst_{execution_kst}'

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
        delete from l0_shb.dwm_ysacm_rst
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwm_ysacm_rst
                (
                  aws_ls_dt                               -- aws적재일시
                , dw_bas_nyymm                            -- dw기준n년월
                , ci_no                                   -- ci번호
                , cusno                                   -- 고객번호
                , acno                                    -- 계좌번호
                , act_c                                   -- 계정code
                , smok_c                                  -- 세목code
                , busmok_c                                -- 부세목code
                , cur_c                                   -- 통화code
                , exeno                                   -- 실행번호
                , grbrno                                  -- 관리점번호
                , prdt_c                                  -- 상품code
                , plcy_c                                  -- 정책code
                , kwa_c                                   -- 과목code
                , ac_s                                    -- 계좌상태
                , yakj_sjdt                               -- 약정시작일자
                , yakj_dudt                               -- 약정만기일자
                , yakj_hjedt                              -- 약정해제일자
                , fst_exe_dt                              -- 최초실행일자
                , lst_trxdt                               -- 최종거래일자
                , dc_dudt                                 -- 대출만기일자
                , sidt                                    -- 승인일자
                , prcp_cmplt_dt                           -- 원금완제일자
                , unyong_g                                -- 운용구분
                , due_arr_janjon_cnt_mcnt                 -- 만기도래잔존개월수
                , irt_g                                   -- 금리구분
                , ysirt_c                                 -- 여신금리code
                , ys_irt_term_c                           -- 여신금리기간code
                , dep_dmb_acno                            -- 예금담보계좌번호
                , ys_bond_bojun_c                         -- 여신채권보전code
                , crtfee_fee_budam_g                      -- 설정비비용부담구분
                , jbdan_dc_k                              -- 집단대출종류
                , shnd_mth                                -- 상환방법
                , fund_yongdo_c                           -- 자금용도code
                , aprvno                                  -- 승인번호
                , aprv_ser                                -- 승인일련번호
                , reln_yn                                 -- 대환여부
                , apl_iyul                                -- 적용이율
                , wcvt_jan                                -- 원화환산잔액
                , mi_hwa_cvt_jan                          -- 미화환산잔액
                , jan                                     -- 잔액
                , bfmm_ac_jan                             -- 전월계좌잔액
                , mgbf_jan                                -- 마감전잔액
                , m_avjn                                  -- 월평잔
                , wcvt_m_avjn                             -- 원화환산월평잔
                , bfmm_ac_avjn                            -- 전월계좌평잔
                , tmpd_avjn                               -- 기중평잔
                , lm3_ac_avjn                             -- 최근3개월계좌평잔
                , lm6_ac_avjn                             -- 최근6개월계좌평잔
                , lm12_ac_avjn                            -- 최근12개월계좌평잔
                , hdamt                                   -- 한도금액
                , wcvt_hdamt                              -- 원화환산한도금액
                , ycamt                                   -- 연체금액
                , yc_cnt                                  -- 연체횟수
                , yc_dcnt                                 -- 연체일수
                , yc_mcnt                                 -- 연체월수
                , lst_int_npdt                            -- 최종이자납입일자
                , prcdlay_dcnt                            -- 원금연체일수
                , prcdlay_mcnt                            -- 원금연체월수
                , mmmd_yc_dcnt                            -- 월중연체일수
                , lm6_tot_ys_yc_dcnt                      -- 최근6개월총여신연체일수
                , lm12_tot_ys_yc_dcnt                     -- 최근12개월총여신연체일수
                , dw_resc_fld_prdt_b                      -- dw원천영역상품분류
                , dw_lst_jukja_dt                         -- dw최종적재일자
                , dmb_k                                   -- 담보종류
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , dw_bas_nyymm                            -- dw기준n년월
          , ci_no                                   -- ci번호
          , cusno                                   -- 고객번호
          , acno                                    -- 계좌번호
          , act_c                                   -- 계정code
          , smok_c                                  -- 세목code
          , busmok_c                                -- 부세목code
          , cur_c                                   -- 통화code
          , exeno                                   -- 실행번호
          , grbrno                                  -- 관리점번호
          , prdt_c                                  -- 상품code
          , plcy_c                                  -- 정책code
          , kwa_c                                   -- 과목code
          , ac_s                                    -- 계좌상태
          , yakj_sjdt                               -- 약정시작일자
          , yakj_dudt                               -- 약정만기일자
          , yakj_hjedt                              -- 약정해제일자
          , fst_exe_dt                              -- 최초실행일자
          , lst_trxdt                               -- 최종거래일자
          , dc_dudt                                 -- 대출만기일자
          , sidt                                    -- 승인일자
          , prcp_cmplt_dt                           -- 원금완제일자
          , unyong_g                                -- 운용구분
          , due_arr_janjon_cnt_mcnt                 -- 만기도래잔존개월수
          , irt_g                                   -- 금리구분
          , ysirt_c                                 -- 여신금리code
          , ys_irt_term_c                           -- 여신금리기간code
          , dep_dmb_acno                            -- 예금담보계좌번호
          , ys_bond_bojun_c                         -- 여신채권보전code
          , crtfee_fee_budam_g                      -- 설정비비용부담구분
          , jbdan_dc_k                              -- 집단대출종류
          , shnd_mth                                -- 상환방법
          , fund_yongdo_c                           -- 자금용도code
          , aprvno                                  -- 승인번호
          , aprv_ser                                -- 승인일련번호
          , reln_yn                                 -- 대환여부
          , apl_iyul                                -- 적용이율
          , wcvt_jan                                -- 원화환산잔액
          , mi_hwa_cvt_jan                          -- 미화환산잔액
          , jan                                     -- 잔액
          , bfmm_ac_jan                             -- 전월계좌잔액
          , mgbf_jan                                -- 마감전잔액
          , m_avjn                                  -- 월평잔
          , wcvt_m_avjn                             -- 원화환산월평잔
          , bfmm_ac_avjn                            -- 전월계좌평잔
          , tmpd_avjn                               -- 기중평잔
          , lm3_ac_avjn                             -- 최근3개월계좌평잔
          , lm6_ac_avjn                             -- 최근6개월계좌평잔
          , lm12_ac_avjn                            -- 최근12개월계좌평잔
          , hdamt                                   -- 한도금액
          , wcvt_hdamt                              -- 원화환산한도금액
          , ycamt                                   -- 연체금액
          , yc_cnt                                  -- 연체횟수
          , yc_dcnt                                 -- 연체일수
          , yc_mcnt                                 -- 연체월수
          , lst_int_npdt                            -- 최종이자납입일자
          , prcdlay_dcnt                            -- 원금연체일수
          , prcdlay_mcnt                            -- 원금연체월수
          , mmmd_yc_dcnt                            -- 월중연체일수
          , lm6_tot_ys_yc_dcnt                      -- 최근6개월총여신연체일수
          , lm12_tot_ys_yc_dcnt                     -- 최근12개월총여신연체일수
          , dw_resc_fld_prdt_b                      -- dw원천영역상품분류
          , dw_lst_jukja_dt                         -- dw최종적재일자
          , dmb_k                                   -- 담보종류
      from w0_shb.dwm_ysacm_rst
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
