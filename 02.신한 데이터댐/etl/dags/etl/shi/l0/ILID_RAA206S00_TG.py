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
pgm_id = 'ILID_RAA206S00_TG'

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
s3_file_prefix = f'iid_raa206s00_{execution_kst}'

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
        delete from l0_shi.raa206s00
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shi.raa206s00
                (
                  aws_ls_dt                               -- aws적재일시
                , base_ymd                                -- 기준일자
                , grp_md_no                               -- 그룹md번호
                , ci_valu                                 -- ci값
                , gend_code                               -- 성별코드
                , age                                     -- 연령
                , agerg_code                              -- 연령대코드
                , a5_unit_agerg_code                      -- 5세단위연령대코드
                , eld_invtmn_yn                           -- 고령투자자여부
                , secr_cust_invtmn_tp_code                -- 증권고객투자자구분코드
                , drem_yn                                 -- 임직원여부
                , staff_fami_yn                           -- 직원가족여부
                , mang_cust_yn                            -- 관리고객여부
                , orig_shs_cust_yn                        -- 원신한고객여부
                , norm_acct_hold_yn                       -- 정상계좌보유여부
                , spcty_info_regi_yn                      -- 특수정보등록여부
                , lndo_regi_acct_hold_yn                  -- 대출등록계좌보유여부
                , tcom_coop_crd_card_hold_yn              -- 당사제휴신용카드보유여부
                , tcom_coop_check_hold_yn                 -- 당사제휴체크카드보유여부
                , cmbn_tc_cmbn_grad_code                  -- 통합탑스클럽통합등급코드
                , mdeal_ch_code                           -- 주거래채널코드
                , cust_invt_grad_code                     -- 고객투자등급코드
                , anys_mrkt_seg_2_code                    -- 분석시장세분화2코드
                , anys_mrkt_seg_3_code                    -- 분석시장세분화3코드
                , analds_prob_score                       -- 분석국내주식가망점수
                , analfs_prob_score                       -- 분석해외주식가망점수
                , analdb_prob_score                       -- 분석국내채권가망점수
                , analfb_prob_score                       -- 분석해외채권가망점수
                , anys_bncert_prob_score                  -- 분석수익증권가망점수
                , analeds_prob_score                      -- 분석elsdls가망점수
                , analedb_prob_score                      -- 분석elbdlb가망점수
                , anys_wrap_prob_score                    -- 분석랩가망점수
                , dom_stk_deal_expc_yn                    -- 국내주식거래경험여부
                , for_stk_deal_expc_yn                    -- 해외주식거래경험여부
                , bncert_deal_expc_yn                     -- 수익증권거래경험여부
                , accu_fund_deal_expc_yn                  -- 적립식펀드거래경험여부
                , dom_bd_deal_expc_yn                     -- 국내채권거래경험여부
                , for_bd_deal_expc_yn                     -- 해외채권거래경험여부
                , els_deal_expc_yn                        -- els거래경험여부
                , dls_deal_expc_yn                        -- dls거래경험여부
                , elb_deal_expc_yn                        -- elb거래경험여부
                , dlb_deal_expc_yn                        -- dlb거래경험여부
                , wrap_deal_expc_yn                       -- 랩거래경험여부
                , fuop_deal_expc_yn                       -- 선물옵션거래경험여부
                , trust_gds_deal_expc_yn                  -- 신탁상품거래경험여부
                , crd_lndo_deal_expc_yn                   -- 신용대출거래경험여부
                , sploan_deal_expc_yn                     -- 스탁파워론거래경험여부
                , pnsn_sv_gds_deal_expc_yn                -- 연금저축상품거래경험여부
                , irp_gds_deal_expc_yn                    -- irp상품거래경험여부
                , plny_for_stk_deal_expc_yn               -- 플랜예스해외주식거래경험여부
                , regi_dt                                 -- 등록일시
                , modi_dt                                 -- 수정일시
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , base_ymd                                -- 기준일자
          , grp_md_no                               -- 그룹md번호
          , ci_valu                                 -- ci값
          , gend_code                               -- 성별코드
          , age                                     -- 연령
          , agerg_code                              -- 연령대코드
          , a5_unit_agerg_code                      -- 5세단위연령대코드
          , eld_invtmn_yn                           -- 고령투자자여부
          , secr_cust_invtmn_tp_code                -- 증권고객투자자구분코드
          , drem_yn                                 -- 임직원여부
          , staff_fami_yn                           -- 직원가족여부
          , mang_cust_yn                            -- 관리고객여부
          , orig_shs_cust_yn                        -- 원신한고객여부
          , norm_acct_hold_yn                       -- 정상계좌보유여부
          , spcty_info_regi_yn                      -- 특수정보등록여부
          , lndo_regi_acct_hold_yn                  -- 대출등록계좌보유여부
          , tcom_coop_crd_card_hold_yn              -- 당사제휴신용카드보유여부
          , tcom_coop_check_hold_yn                 -- 당사제휴체크카드보유여부
          , cmbn_tc_cmbn_grad_code                  -- 통합탑스클럽통합등급코드
          , mdeal_ch_code                           -- 주거래채널코드
          , cust_invt_grad_code                     -- 고객투자등급코드
          , anys_mrkt_seg_2_code                    -- 분석시장세분화2코드
          , anys_mrkt_seg_3_code                    -- 분석시장세분화3코드
          , analds_prob_score                       -- 분석국내주식가망점수
          , analfs_prob_score                       -- 분석해외주식가망점수
          , analdb_prob_score                       -- 분석국내채권가망점수
          , analfb_prob_score                       -- 분석해외채권가망점수
          , anys_bncert_prob_score                  -- 분석수익증권가망점수
          , analeds_prob_score                      -- 분석elsdls가망점수
          , analedb_prob_score                      -- 분석elbdlb가망점수
          , anys_wrap_prob_score                    -- 분석랩가망점수
          , dom_stk_deal_expc_yn                    -- 국내주식거래경험여부
          , for_stk_deal_expc_yn                    -- 해외주식거래경험여부
          , bncert_deal_expc_yn                     -- 수익증권거래경험여부
          , accu_fund_deal_expc_yn                  -- 적립식펀드거래경험여부
          , dom_bd_deal_expc_yn                     -- 국내채권거래경험여부
          , for_bd_deal_expc_yn                     -- 해외채권거래경험여부
          , els_deal_expc_yn                        -- els거래경험여부
          , dls_deal_expc_yn                        -- dls거래경험여부
          , elb_deal_expc_yn                        -- elb거래경험여부
          , dlb_deal_expc_yn                        -- dlb거래경험여부
          , wrap_deal_expc_yn                       -- 랩거래경험여부
          , fuop_deal_expc_yn                       -- 선물옵션거래경험여부
          , trust_gds_deal_expc_yn                  -- 신탁상품거래경험여부
          , crd_lndo_deal_expc_yn                   -- 신용대출거래경험여부
          , sploan_deal_expc_yn                     -- 스탁파워론거래경험여부
          , pnsn_sv_gds_deal_expc_yn                -- 연금저축상품거래경험여부
          , irp_gds_deal_expc_yn                    -- irp상품거래경험여부
          , plny_for_stk_deal_expc_yn               -- 플랜예스해외주식거래경험여부
          , regi_dt                                 -- 등록일시
          , modi_dt                                 -- 수정일시
      from w0_shi.raa206s00
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
