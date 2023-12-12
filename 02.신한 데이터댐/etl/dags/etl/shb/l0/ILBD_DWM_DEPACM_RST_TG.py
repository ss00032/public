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
pgm_id = 'ILBD_DWM_DEPACM_RST_TG'

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
s3_file_prefix = f'ibd_dwm_depacm_rst_/ibd_dwm_depacm_rst_{execution_kst}'

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
        delete from l0_shb.dwm_depacm_rst
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwm_depacm_rst
                (
                  aws_ls_dt                               -- aws적재일시
                , ci_no                                   -- ci번호
                , cusno                                   -- 고객번호
                , acno                                    -- 계좌번호
                , cur_c                                   -- 통화code
                , sno                                     -- 순번
                , act_c                                   -- 계정code
                , smok_c                                  -- 세목code
                , busmok_c                                -- 부세목code
                , prdt_c                                  -- 상품code
                , kwa_c                                   -- 과목code
                , now_dep_ac_s                            -- 현수신계좌상태
                , mkdt                                    -- 신규일자
                , hji_dt                                  -- 해지일자
                , dudt                                    -- 만기일자
                , jibul_psb_jan                           -- 지불가능잔액
                , pldg_cret_tamt                          -- 질권설정총금액
                , invs_obj_c                              -- 투자목적code
                , dep_dc_ac_yn                            -- 수신대출계좌여부
                , cont_g                                  -- 계약구분
                , due_bas_camp                            -- 만기기본계약금액
                , dep_conttm_g                            -- 수신계약기간구분
                , ac_cont_s                               -- 계좌계약상태
                , trn1_jkrip_amt                          -- 1회차적립금액
                , irt_cont_mth                            -- 금리계약방법
                , irt_turn_cycl                           -- 금리회전주기
                , nosign_mk_yn                            -- 무기명신규여부
                , fprc_amt                                -- 액면금액
                , bas_iyul                                -- 기본이율
                , addiyul                                 -- 가산이율
                , bonus_iyul                              -- 보너스이율
                , pay_ip_lst_dt                           -- 급여입금최종일자
                , visa_kjac_yn                            -- 비자카드결제계좌여부
                , fna_secr_ac_yn                          -- fna증권계좌여부
                , secr_kjac_yn                            -- 증권결제계좌여부
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
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , ci_no                                   -- ci번호
          , cusno                                   -- 고객번호
          , acno                                    -- 계좌번호
          , cur_c                                   -- 통화code
          , sno                                     -- 순번
          , act_c                                   -- 계정code
          , smok_c                                  -- 세목code
          , busmok_c                                -- 부세목code
          , prdt_c                                  -- 상품code
          , kwa_c                                   -- 과목code
          , now_dep_ac_s                            -- 현수신계좌상태
          , mkdt                                    -- 신규일자
          , hji_dt                                  -- 해지일자
          , dudt                                    -- 만기일자
          , jibul_psb_jan                           -- 지불가능잔액
          , pldg_cret_tamt                          -- 질권설정총금액
          , invs_obj_c                              -- 투자목적code
          , dep_dc_ac_yn                            -- 수신대출계좌여부
          , cont_g                                  -- 계약구분
          , due_bas_camp                            -- 만기기본계약금액
          , dep_conttm_g                            -- 수신계약기간구분
          , ac_cont_s                               -- 계좌계약상태
          , trn1_jkrip_amt                          -- 1회차적립금액
          , irt_cont_mth                            -- 금리계약방법
          , irt_turn_cycl                           -- 금리회전주기
          , nosign_mk_yn                            -- 무기명신규여부
          , fprc_amt                                -- 액면금액
          , bas_iyul                                -- 기본이율
          , addiyul                                 -- 가산이율
          , bonus_iyul                              -- 보너스이율
          , pay_ip_lst_dt                           -- 급여입금최종일자
          , visa_kjac_yn                            -- 비자카드결제계좌여부
          , fna_secr_ac_yn                          -- fna증권계좌여부
          , secr_kjac_yn                            -- 증권결제계좌여부
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
      from w0_shb.dwm_depacm_rst
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
