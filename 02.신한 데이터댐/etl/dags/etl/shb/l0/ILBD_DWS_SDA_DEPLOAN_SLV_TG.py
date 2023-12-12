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
pgm_id = 'ILBD_DWS_SDA_DEPLOAN_SLV_TG'

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
s3_file_prefix = f'ibd_dws_sda_deploan_slv_/ibd_dws_sda_deploan_slv_{execution_kst}'

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
        delete from l0_shb.dws_sda_deploan_slv
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dws_sda_deploan_slv
                (
                  aws_ls_dt                               -- aws적재일시
                , ci_no                                   -- ci번호
                , cusno                                   -- 고객번호
                , acno                                    -- 계좌번호
                , kwa_c                                   -- 과목code
                , acser                                   -- 계좌일련번호
                , grbrno                                  -- 관리점번호
                , dc_aprvno                               -- 대출승인번호
                , dep_dc_ac_s                             -- 수신대출계좌상태
                , sinc_g                                  -- 신청구분
                , prdt_c                                  -- 상품code
                , plcy_c                                  -- 정책code
                , act_c                                   -- 계정code
                , smok_c                                  -- 세목code
                , busmok_c                                -- 부세목code
                , sts_fund_c                              -- 신탁펀드code
                , dc_hdamt                                -- 대출한도금액
                , usbl_siamt                              -- 가용승인금액
                , fst_hndo_dr_dt                          -- 최초한도등록일자
                , fst_dc_dt                               -- 최초대출일자
                , dc_exe_dt                               -- 대출실행일자
                , dc_dudt                                 -- 대출기일일자
                , dc_kjdt                                 -- 대출결제일자
                , lst_dc_dr_dt                            -- 최종대출등록일자
                , irt_g                                   -- 금리구분
                , ysirt_c                                 -- 여신금리code
                , mkirt_k                                 -- 시장금리종류
                , irt_term_c                              -- 금리기간code
                , dc_add_irt                              -- 대출가산금리
                , dmb_dc_wght_avr_irt                     -- 담보대출가중평균금리
                , lst_cus_apl_dc_irt                      -- 최종고객적용대출금리
                , yc_irt_u                                -- 연체금리유형
                , ys_bond_bojun_c                         -- 여신채권보전code
                , dmb_k                                   -- 담보종류
                , dc_unyung_c                             -- 대출운영code
                , dc_turn_term                            -- 대출회전기간
                , dc_hapd_c                               -- 대출합동code
                , cdln_trt_fert                           -- 카드론취급수수료율
                , plcy_in_irt                             -- 정책내부금리
                , saupb_in_irt                            -- 사업부내부금리
                , fnd_ar_in_irt                           -- 자금부내부금리
                , dep_dc_adcs_g                           -- 수신대출전결구분
                , ys_yakj_g                               -- 여신약정구분
                , inji_budam_g                            -- 인지대부담구분
                , hndo_feejs_g                            -- 한도수수료징수구분
                , hndo_fert                               -- 한도수수료율
                , dmb_acno                                -- 담보계좌번호
                , trx_sno                                 -- 거래순번
                , ab_trn                                  -- 자산유동화회차
                , pbsvt_org_c                             -- 공무원기관code
                , lst_cdt                                 -- 최종변경일자
                , act_g                                   -- 계정구분
                , teamdc_aprvno                           -- 단체협의대출승인번호
                , corp_scal_c                             -- 기업규모code
                , sinc_rcpt_g                             -- 신청접수구분
                , fil_dt2                                 -- 연기실행 전 기존만기일자
                , fil_cnt2                                -- 구분코드
                , fil_105_iyul1                           -- 가산금리
                , fil_105_iyul2                           -- 담보대출가중평균금리
                , fil_105_iyul3                           -- 최종고객적용대출금리
                , fil_dt3                                 -- 연기실행 전 실행일자
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , ci_no                                   -- ci번호
          , cusno                                   -- 고객번호
          , acno                                    -- 계좌번호
          , kwa_c                                   -- 과목code
          , acser                                   -- 계좌일련번호
          , grbrno                                  -- 관리점번호
          , dc_aprvno                               -- 대출승인번호
          , dep_dc_ac_s                             -- 수신대출계좌상태
          , sinc_g                                  -- 신청구분
          , prdt_c                                  -- 상품code
          , plcy_c                                  -- 정책code
          , act_c                                   -- 계정code
          , smok_c                                  -- 세목code
          , busmok_c                                -- 부세목code
          , sts_fund_c                              -- 신탁펀드code
          , dc_hdamt                                -- 대출한도금액
          , usbl_siamt                              -- 가용승인금액
          , fst_hndo_dr_dt                          -- 최초한도등록일자
          , fst_dc_dt                               -- 최초대출일자
          , dc_exe_dt                               -- 대출실행일자
          , dc_dudt                                 -- 대출기일일자
          , dc_kjdt                                 -- 대출결제일자
          , lst_dc_dr_dt                            -- 최종대출등록일자
          , irt_g                                   -- 금리구분
          , ysirt_c                                 -- 여신금리code
          , mkirt_k                                 -- 시장금리종류
          , irt_term_c                              -- 금리기간code
          , dc_add_irt                              -- 대출가산금리
          , dmb_dc_wght_avr_irt                     -- 담보대출가중평균금리
          , lst_cus_apl_dc_irt                      -- 최종고객적용대출금리
          , yc_irt_u                                -- 연체금리유형
          , ys_bond_bojun_c                         -- 여신채권보전code
          , dmb_k                                   -- 담보종류
          , dc_unyung_c                             -- 대출운영code
          , dc_turn_term                            -- 대출회전기간
          , dc_hapd_c                               -- 대출합동code
          , cdln_trt_fert                           -- 카드론취급수수료율
          , plcy_in_irt                             -- 정책내부금리
          , saupb_in_irt                            -- 사업부내부금리
          , fnd_ar_in_irt                           -- 자금부내부금리
          , dep_dc_adcs_g                           -- 수신대출전결구분
          , ys_yakj_g                               -- 여신약정구분
          , inji_budam_g                            -- 인지대부담구분
          , hndo_feejs_g                            -- 한도수수료징수구분
          , hndo_fert                               -- 한도수수료율
          , dmb_acno                                -- 담보계좌번호
          , trx_sno                                 -- 거래순번
          , ab_trn                                  -- 자산유동화회차
          , pbsvt_org_c                             -- 공무원기관code
          , lst_cdt                                 -- 최종변경일자
          , act_g                                   -- 계정구분
          , teamdc_aprvno                           -- 단체협의대출승인번호
          , corp_scal_c                             -- 기업규모code
          , sinc_rcpt_g                             -- 신청접수구분
          , fil_dt2                                 -- 연기실행 전 기존만기일자
          , fil_cnt2                                -- 구분코드
          , fil_105_iyul1                           -- 가산금리
          , fil_105_iyul2                           -- 담보대출가중평균금리
          , fil_105_iyul3                           -- 최종고객적용대출금리
          , fil_dt3                                 -- 연기실행 전 실행일자
      from w0_shb.dws_sda_deploan_slv
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
