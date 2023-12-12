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
pgm_id = 'ILID_JDC022T00_TG'

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
s3_file_prefix = f'iid_jdc022t00_{execution_kst}'

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
        delete from l0_shi.jdc022t00
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shi.jdc022t00
                (
                  aws_ls_dt                               -- aws적재일시
                , base_ym                                 -- 기준년월
                , grp_md_no                               -- 그룹md번호
                , rpst_cust_no                            -- 대표고객번호
                , ci_valu                                 -- ci값
                , pers_cust_yn                            -- 개인고객여부
                , corp_cust_yn                            -- 법인고객여부
                , gend_code                               -- 성별코드
                , age                                     -- 연령
                , agerg_code                              -- 연령대코드
                , a5_unit_agerg_code                      -- 5세단위연령대코드
                , secr_cust_invtmn_tp_code                -- 증권고객투자자구분코드
                , max_cust_athl_grad_code                 -- 최대고객활동성등급코드
                , min_cust_athl_grad_code                 -- 최소고객활동성등급코드
                , frst_acct_est_yn                        -- 최초계좌개설여부
                , frst_acct_est_snce_pass_mcnt            -- 최초계좌개설이후경과월수
                , frst_acct_est_ymd                       -- 최초계좌개설일자
                , rpst_acct_no                            -- 대표계좌번호
                , rpst_visor_memb                         -- 대표관리자사번
                , rpst_mang_dbrn_code                     -- 대표관리부점코드
                , mang_cust_yn                            -- 관리고객여부
                , visor_chg_yn                            -- 관리자변경여부
                , mang_dbrn_chg_yn                        -- 관리부점변경여부
                , frgner_cust_yn                          -- 외국인고객여부
                , athl_cust_yn                            -- 활동성고객여부
                , lmon_athl_cust_yn                       -- 전월활동성고객여부
                , acty_acct_hold_yn                       -- 활성계좌보유여부
                , norm_acct_hold_cnt                      -- 정상계좌보유수
                , cntn_deal_cust_yn                       -- 지속거래고객여부
                , tops_svc_grad_code                      -- tops서비스등급코드
                , tops_rsut_grad_code                     -- tops실적등급코드
                , tops_grad_tran_tp_code                  -- tops등급이동구분코드
                , hd_tops_svc_grad_code                   -- 지주사tops서비스등급코드
                , hd_tops_rsut_grad_code                  -- 지주사tops실적등급코드
                , conf_cmsn_yn                            -- 협의수수료여부
                , rtal_mang_cust_yn                       -- 리테일관리고객여부
                , samc_cust_yn                            -- 스마트자산관리센터고객여부
                , saly_trns_yn                            -- 급여이체여부
                , wrap_hold_yn                            -- 랩보유여부
                , new_acct_est_yn                         -- 신규계좌개설여부
                , recn_y3_best_asst_amt                   -- 최근3년최고자산금액
                , recn_y3_best_asst_ym                    -- 최근3년최고자산년월
                , recn_y3_best_avgrm_amt                  -- 최근3년최고평잔금액
                , recn_y3_best_avgrm_ym                   -- 최근3년최고평잔년월
                , drem_yn                                 -- 임직원여부
                , staff_fami_yn                           -- 직원가족여부
                , itdt_cust_yn                            -- 소개고객여부
                , mjtshol_yn                              -- 대주주여부
                , fuop_acct_hold_yn                       -- 선물옵션계좌보유여부
                , crd_acct_hold_yn                        -- 신용계좌보유여부
                , lndo_regi_acct_hold_yn                  -- 대출등록계좌보유여부
                , spcty_info_regi_yn                      -- 특수정보등록여부
                , tcom_coop_crd_card_hold_yn              -- 당사제휴신용카드보유여부
                , tcom_coop_check_hold_yn                 -- 당사제휴체크카드보유여부
                , other_sec_deal_yn                       -- 타증권사거래여부
                , regi_dt                                 -- 등록일시
                , modi_dt                                 -- 수정일시
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , base_ym                                 -- 기준년월
          , grp_md_no                               -- 그룹md번호
          , rpst_cust_no                            -- 대표고객번호
          , ci_valu                                 -- ci값
          , pers_cust_yn                            -- 개인고객여부
          , corp_cust_yn                            -- 법인고객여부
          , gend_code                               -- 성별코드
          , age                                     -- 연령
          , agerg_code                              -- 연령대코드
          , a5_unit_agerg_code                      -- 5세단위연령대코드
          , secr_cust_invtmn_tp_code                -- 증권고객투자자구분코드
          , max_cust_athl_grad_code                 -- 최대고객활동성등급코드
          , min_cust_athl_grad_code                 -- 최소고객활동성등급코드
          , frst_acct_est_yn                        -- 최초계좌개설여부
          , frst_acct_est_snce_pass_mcnt            -- 최초계좌개설이후경과월수
          , frst_acct_est_ymd                       -- 최초계좌개설일자
          , rpst_acct_no                            -- 대표계좌번호
          , rpst_visor_memb                         -- 대표관리자사번
          , rpst_mang_dbrn_code                     -- 대표관리부점코드
          , mang_cust_yn                            -- 관리고객여부
          , visor_chg_yn                            -- 관리자변경여부
          , mang_dbrn_chg_yn                        -- 관리부점변경여부
          , frgner_cust_yn                          -- 외국인고객여부
          , athl_cust_yn                            -- 활동성고객여부
          , lmon_athl_cust_yn                       -- 전월활동성고객여부
          , acty_acct_hold_yn                       -- 활성계좌보유여부
          , norm_acct_hold_cnt                      -- 정상계좌보유수
          , cntn_deal_cust_yn                       -- 지속거래고객여부
          , tops_svc_grad_code                      -- tops서비스등급코드
          , tops_rsut_grad_code                     -- tops실적등급코드
          , tops_grad_tran_tp_code                  -- tops등급이동구분코드
          , hd_tops_svc_grad_code                   -- 지주사tops서비스등급코드
          , hd_tops_rsut_grad_code                  -- 지주사tops실적등급코드
          , conf_cmsn_yn                            -- 협의수수료여부
          , rtal_mang_cust_yn                       -- 리테일관리고객여부
          , samc_cust_yn                            -- 스마트자산관리센터고객여부
          , saly_trns_yn                            -- 급여이체여부
          , wrap_hold_yn                            -- 랩보유여부
          , new_acct_est_yn                         -- 신규계좌개설여부
          , recn_y3_best_asst_amt                   -- 최근3년최고자산금액
          , recn_y3_best_asst_ym                    -- 최근3년최고자산년월
          , recn_y3_best_avgrm_amt                  -- 최근3년최고평잔금액
          , recn_y3_best_avgrm_ym                   -- 최근3년최고평잔년월
          , drem_yn                                 -- 임직원여부
          , staff_fami_yn                           -- 직원가족여부
          , itdt_cust_yn                            -- 소개고객여부
          , mjtshol_yn                              -- 대주주여부
          , fuop_acct_hold_yn                       -- 선물옵션계좌보유여부
          , crd_acct_hold_yn                        -- 신용계좌보유여부
          , lndo_regi_acct_hold_yn                  -- 대출등록계좌보유여부
          , spcty_info_regi_yn                      -- 특수정보등록여부
          , tcom_coop_crd_card_hold_yn              -- 당사제휴신용카드보유여부
          , tcom_coop_check_hold_yn                 -- 당사제휴체크카드보유여부
          , other_sec_deal_yn                       -- 타증권사거래여부
          , regi_dt                                 -- 등록일시
          , modi_dt                                 -- 수정일시
      from w0_shi.jdc022t00
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
