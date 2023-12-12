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


__author__     = "이일주"
__copyright__  = "Copyright 2021, Shinhan Datadam"
__credits__    = ["이일주"]
__version__    = "1.0"
__maintainer__ = "이일주"
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
pgm_id = 'ALID_GBA038S00_TG'

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
s3_file_prefix = f'aid_gba038s00_{execution_kst}'

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
        delete from l0_shi.gba038s00
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shi.gba038s00
                (
                  aws_ls_dt                               -- aws적재일시
                , proc_ymd                                -- 처리일자
                , acct_no                                 -- 계좌번호
                , cust_no                                 -- 고객번호
                , grp_md_no                               -- 그룹md번호
                , ci_valu                                 -- ci값
                , est_ymd                                 -- 개설일자
                , mang_tp_code                            -- 관리구분코드
                , acct_mang_bran_code                     -- 계좌관리지점코드
                , fna_ctac_bran_code                      -- fna연계지점코드
                , mthr_dbrn_code                          -- 모부점코드
                , fna_acct_tp_code                        -- fna계좌구분코드
                , bib_tp_code                             -- bib구분코드
                , cma_acct_tp_code                        -- cma계좌구분코드
                , bank_coop_bizs_tp_code                  -- 은행제휴업무구분코드
                , forg_code                               -- 금융기관코드
                , coop_est_forg_bran_code                 -- 제휴개설금융기관지점코드
                , secr_tc_cmbn_grad_code                  -- 증권탑스클럽통합등급코드
                , extl_realnm_cnfm_tp_code                -- 대외실명확인구분코드
                , grup_tp_code                            -- 단체구분코드
                , txon_tp_code                            -- 과세구분코드
                , natv_frgner_tp_code                     -- 내국인외국인구분코드
                , resimn_tp_code                          -- 거주자구분코드
                , curr_type_code                          -- 통화유형코드
                , invtmn_clas_code                        -- 투자자분류코드
                , ntax_incmn_tp_code                      -- 국세청소득자구분코드
                , tc_cmbn_grad_code                       -- 탑스클럽통합등급코드
                , gold_svc_regi_tp_code                   -- 골드서비스등록구분코드
                , on_acct_yn                              -- 온라인계좌여부
                , secur_tp_code                           -- 유가증권구분코드
                , acct_cmsn_tp_code                       -- 계좌수수료구분코드
                , conf_cmsn_yn                            -- 협의수수료여부
                , mrgn_collct_tp_code                     -- 증거금징수구분코드
                , crd_acct_tp_code                        -- 신용계좌구분코드
                , mrgn_estb_scop_tp_code                  -- 증거금설정범위구분코드
                , other_gds_mrgn_proc_yn                  -- 타상품증거금처리여부
                , est_dbrn_code                           -- 개설부점코드
                , regi_dt                                 -- 등록일시
                , modi_dt                                 -- 수정일시
                , reqmn_memb                              -- 권유자사번
                , txof_only_acct_tp_code                  -- 비과세전용계좌구분코드
                , isa_afil_type_code                      -- isa가입유형코드
                , for_stk_regi_yn                         -- 해외주식등록여부
                , bank_coop_nff_est_tp_code               -- 은행제휴비대면개설구분코드
                , nff_tmp_cust_no                         -- 비대면임시고객번호
                , nff_acct_est_ver_tp_code                -- 비대면계좌개설버전구분코드
                , nff_acct_est_reqst_co_tp_code           -- 비대면계좌개설요청회사구분코드
                , nff_highv_lowv_tp_code                  -- 비대면고가저가구분코드
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , proc_ymd                                -- 처리일자
          , acct_no                                 -- 계좌번호
          , cust_no                                 -- 고객번호
          , grp_md_no                               -- 그룹md번호
          , ci_valu                                 -- ci값
          , est_ymd                                 -- 개설일자
          , mang_tp_code                            -- 관리구분코드
          , acct_mang_bran_code                     -- 계좌관리지점코드
          , fna_ctac_bran_code                      -- fna연계지점코드
          , mthr_dbrn_code                          -- 모부점코드
          , fna_acct_tp_code                        -- fna계좌구분코드
          , bib_tp_code                             -- bib구분코드
          , cma_acct_tp_code                        -- cma계좌구분코드
          , bank_coop_bizs_tp_code                  -- 은행제휴업무구분코드
          , forg_code                               -- 금융기관코드
          , coop_est_forg_bran_code                 -- 제휴개설금융기관지점코드
          , secr_tc_cmbn_grad_code                  -- 증권탑스클럽통합등급코드
          , extl_realnm_cnfm_tp_code                -- 대외실명확인구분코드
          , grup_tp_code                            -- 단체구분코드
          , txon_tp_code                            -- 과세구분코드
          , natv_frgner_tp_code                     -- 내국인외국인구분코드
          , resimn_tp_code                          -- 거주자구분코드
          , curr_type_code                          -- 통화유형코드
          , invtmn_clas_code                        -- 투자자분류코드
          , ntax_incmn_tp_code                      -- 국세청소득자구분코드
          , tc_cmbn_grad_code                       -- 탑스클럽통합등급코드
          , gold_svc_regi_tp_code                   -- 골드서비스등록구분코드
          , on_acct_yn                              -- 온라인계좌여부
          , secur_tp_code                           -- 유가증권구분코드
          , acct_cmsn_tp_code                       -- 계좌수수료구분코드
          , conf_cmsn_yn                            -- 협의수수료여부
          , mrgn_collct_tp_code                     -- 증거금징수구분코드
          , crd_acct_tp_code                        -- 신용계좌구분코드
          , mrgn_estb_scop_tp_code                  -- 증거금설정범위구분코드
          , other_gds_mrgn_proc_yn                  -- 타상품증거금처리여부
          , est_dbrn_code                           -- 개설부점코드
          , regi_dt                                 -- 등록일시
          , modi_dt                                 -- 수정일시
          , reqmn_memb                              -- 권유자사번
          , txof_only_acct_tp_code                  -- 비과세전용계좌구분코드
          , isa_afil_type_code                      -- isa가입유형코드
          , for_stk_regi_yn                         -- 해외주식등록여부
          , bank_coop_nff_est_tp_code               -- 은행제휴비대면개설구분코드
          , nff_tmp_cust_no                         -- 비대면임시고객번호
          , nff_acct_est_ver_tp_code                -- 비대면계좌개설버전구분코드
          , nff_acct_est_reqst_co_tp_code           -- 비대면계좌개설요청회사구분코드
          , nff_highv_lowv_tp_code                  -- 비대면고가저가구분코드
      from w0_shi.gba038s00
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
