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
pgm_id = 'ILID_AAA001M00_TG'

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
s3_file_prefix = f'iid_aaa001m00_{execution_kst}'

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
        delete from l0_shi.aaa001m00
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shi.aaa001m00
                (
                  aws_ls_dt                               -- aws적재일시
                , cust_no                                 -- 고객번호
                , grp_md_no                               -- 그룹md번호
                , ci_valu                                 -- ci값
                , qual_org_invtmn_yn                      -- 적격기관투자자여부
                , regi_ymd                                -- 등록일자
                , crd_evlt_regi_yn                        -- 신용평가등록여부
                , crd_bad_tp_code                         -- 신용불량구분코드
                , acct_est_rejt_tp_code                   -- 계좌개설거부구분코드
                , frgner_ord_invtmn_clas_code             -- 외국인주문투자자분류코드
                , cust_info_infm_tp_code                  -- 고객정보통보구분코드
                , invt_prop_code                          -- 투자성향코드
                , txpf_tp_code                            -- 세금우대구분코드
                , bank_trns_dd1_lim_amt                   -- 은행이체1일한도금액
                , bank_trns_one_lim_amt                   -- 은행이체1회한도금액
                , otp_vrfy_vendor_code                    -- otp인증벤더코드
                , otp_seq                                 -- otp순번
                , otp_isu_tp_code                         -- otp발급구분코드
                , tc_cmbn_grad_code                       -- 탑스클럽통합등급코드
                , secr_tc_cmbn_grad_code                  -- 증권탑스클럽통합등급코드
                , rp_stbd_altn_cnsnt_yn                   -- rp종목교체동의여부
                , fss_p100_collct_trg_yn                  -- 금감원100퍼센트징수대상여부
                , dfr_frz_yn                              -- 미수동결여부
                , secr_cust_invtmn_tp_code                -- 증권고객투자자구분코드
                , info_ofer_cnsnt_yn                      -- 정보제공동의여부
                , invt_req_hope_yn                        -- 투자권유희망여부
                , last_regi_ymd                           -- 최종등록일자
                , last_regi_seq                           -- 최종등록순번
                , evlt_score                              -- 평가점수
                , cust_invt_grad_code                     -- 고객투자등급코드
                , drv_invt_grad_code                      -- 파생투자등급코드
                , last_oprt_ymd                           -- 최종출력일자
                , regi_dt                                 -- 등록일시
                , modi_dt                                 -- 수정일시
                , invtmn_cfrm_ending_yn                   -- 투자자확인서만료여부
                , strt_ymd                                -- 시작일자
                , end_ymd                                 -- 종료일자
                , elw_bsis_entr_grad                      -- elw기본예탁금등급
                , othr_elw_regi_ymd                       -- 타사elw등록일자
                , wrap_trust_invt_grad_code               -- 랩신탁투자등급코드
                , qual_invtmn_yn                          -- 적격투자자여부
                , dfr_cnsnt_yn                            -- 미수동의여부
                , cust_invtmn_tp_excpt_code               -- 고객투자자구분예외코드
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , cust_no                                 -- 고객번호
          , grp_md_no                               -- 그룹md번호
          , ci_valu                                 -- ci값
          , qual_org_invtmn_yn                      -- 적격기관투자자여부
          , regi_ymd                                -- 등록일자
          , crd_evlt_regi_yn                        -- 신용평가등록여부
          , crd_bad_tp_code                         -- 신용불량구분코드
          , acct_est_rejt_tp_code                   -- 계좌개설거부구분코드
          , frgner_ord_invtmn_clas_code             -- 외국인주문투자자분류코드
          , cust_info_infm_tp_code                  -- 고객정보통보구분코드
          , invt_prop_code                          -- 투자성향코드
          , txpf_tp_code                            -- 세금우대구분코드
          , bank_trns_dd1_lim_amt                   -- 은행이체1일한도금액
          , bank_trns_one_lim_amt                   -- 은행이체1회한도금액
          , otp_vrfy_vendor_code                    -- otp인증벤더코드
          , otp_seq                                 -- otp순번
          , otp_isu_tp_code                         -- otp발급구분코드
          , tc_cmbn_grad_code                       -- 탑스클럽통합등급코드
          , secr_tc_cmbn_grad_code                  -- 증권탑스클럽통합등급코드
          , rp_stbd_altn_cnsnt_yn                   -- rp종목교체동의여부
          , fss_p100_collct_trg_yn                  -- 금감원100퍼센트징수대상여부
          , dfr_frz_yn                              -- 미수동결여부
          , secr_cust_invtmn_tp_code                -- 증권고객투자자구분코드
          , info_ofer_cnsnt_yn                      -- 정보제공동의여부
          , invt_req_hope_yn                        -- 투자권유희망여부
          , last_regi_ymd                           -- 최종등록일자
          , last_regi_seq                           -- 최종등록순번
          , evlt_score                              -- 평가점수
          , cust_invt_grad_code                     -- 고객투자등급코드
          , drv_invt_grad_code                      -- 파생투자등급코드
          , last_oprt_ymd                           -- 최종출력일자
          , regi_dt                                 -- 등록일시
          , modi_dt                                 -- 수정일시
          , invtmn_cfrm_ending_yn                   -- 투자자확인서만료여부
          , strt_ymd                                -- 시작일자
          , end_ymd                                 -- 종료일자
          , elw_bsis_entr_grad                      -- elw기본예탁금등급
          , othr_elw_regi_ymd                       -- 타사elw등록일자
          , wrap_trust_invt_grad_code               -- 랩신탁투자등급코드
          , qual_invtmn_yn                          -- 적격투자자여부
          , dfr_cnsnt_yn                            -- 미수동의여부
          , cust_invtmn_tp_excpt_code               -- 고객투자자구분예외코드
      from w0_shi.aaa001m00
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
