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
pgm_id = 'ILBD_DWS_SDT_GOJEONG_SLV_TG'

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
s3_file_prefix = f'ibd_dws_sdt_gojeong_slv_/ibd_dws_sdt_gojeong_slv_{execution_kst}'

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
        delete from l0_shb.dws_sdt_gojeong_slv
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dws_sdt_gojeong_slv
                (
                  aws_ls_dt                               -- aws적재일시
                , acno                                    -- 계좌번호
                , trxno                                   -- 거래번호
                , kwa_c                                   -- 과목code
                , trx_oprt_dt                             -- 거래조작일자
                , trx_oprt_time                           -- 거래조작시각
                , trxdt                                   -- 거래일자
                , gsdt_tryn                               -- 기산일거래여부
                , upmu_g                                  -- 업무구분
                , dep_trx_g                               -- 수신거래구분
                , tramt                                   -- 거래금액
                , trxaf_jan                               -- 거래후잔액
                , ynd_amt                                 -- 연동금액
                , ynd_ip_bnk_c                            -- 연동입금은행code
                , ji_psb_jan                              -- 지급가능잔액
                , invs_pramt                              -- 투자원금금액
                , fund_wcur_pramt                         -- 펀드원화원금금액
                , napip_prcp_ji_amt                       -- 납입원금지급금액
                , tax_std_amt                             -- 과세표준금액
                , oprc_prcp_amt                           -- 원가원금금액
                , oprc_prcp_ji_amt                        -- 원가원금지급금액
                , jkyo_c                                  -- 적요code
                , chan_u                                  -- 채널유형
                , ip_trn                                  -- 입금회차
                , ji_ds_stt_trn                           -- 지급대상시작회차
                , ji_ds_end_trn                           -- 지급대상종료회차
                , ji_psb_acnt                             -- 지급가능좌수
                , ji_fcst_acnt                            -- 지급예상좌수
                , aprvno                                  -- 승인번호
                , orgn_trxdt                              -- 원거래일자
                , lst_jidt                                -- 최종지급일자
                , bncr_prcp_uprc                          -- 수익증권원금단가
                , addiyul                                 -- 가산이율
                , trn_apl_iyul                            -- 회차적용이율
                , lst_chg_brno                            -- 최종변경점번호
                , lst_cdt                                 -- 최종변경일자
                , lst_chg_time                            -- 최종변경시각
                , dw_data_gjdt                            -- dwdata기준일자
                , dw_lst_jukja_dt                         -- dw최종적재일자
                , fil_yn1                                 -- filler여부1
                , fil_5_no3                               -- filler5번호3
                , fil_18_int1                             -- filler18이자1
                , fil_10_iyul2                            -- filler10이율2
                , ip_rqstr_silno_cid                      -- 입금의뢰인실명번호고객id
                , fil_dt4                                 -- 은행거래신청서 징구일
                , fil_20_ctnt2                            -- s-solutiin 키
                , fil_yn6                                 -- filler여부6
                , fil_yn7                                 -- fil_yn7
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , acno                                    -- 계좌번호
          , trxno                                   -- 거래번호
          , kwa_c                                   -- 과목code
          , trx_oprt_dt                             -- 거래조작일자
          , trx_oprt_time                           -- 거래조작시각
          , trxdt                                   -- 거래일자
          , gsdt_tryn                               -- 기산일거래여부
          , upmu_g                                  -- 업무구분
          , dep_trx_g                               -- 수신거래구분
          , tramt                                   -- 거래금액
          , trxaf_jan                               -- 거래후잔액
          , ynd_amt                                 -- 연동금액
          , ynd_ip_bnk_c                            -- 연동입금은행code
          , ji_psb_jan                              -- 지급가능잔액
          , invs_pramt                              -- 투자원금금액
          , fund_wcur_pramt                         -- 펀드원화원금금액
          , napip_prcp_ji_amt                       -- 납입원금지급금액
          , tax_std_amt                             -- 과세표준금액
          , oprc_prcp_amt                           -- 원가원금금액
          , oprc_prcp_ji_amt                        -- 원가원금지급금액
          , jkyo_c                                  -- 적요code
          , chan_u                                  -- 채널유형
          , ip_trn                                  -- 입금회차
          , ji_ds_stt_trn                           -- 지급대상시작회차
          , ji_ds_end_trn                           -- 지급대상종료회차
          , ji_psb_acnt                             -- 지급가능좌수
          , ji_fcst_acnt                            -- 지급예상좌수
          , aprvno                                  -- 승인번호
          , orgn_trxdt                              -- 원거래일자
          , lst_jidt                                -- 최종지급일자
          , bncr_prcp_uprc                          -- 수익증권원금단가
          , addiyul                                 -- 가산이율
          , trn_apl_iyul                            -- 회차적용이율
          , lst_chg_brno                            -- 최종변경점번호
          , lst_cdt                                 -- 최종변경일자
          , lst_chg_time                            -- 최종변경시각
          , dw_data_gjdt                            -- dwdata기준일자
          , dw_lst_jukja_dt                         -- dw최종적재일자
          , fil_yn1                                 -- filler여부1
          , fil_5_no3                               -- filler5번호3
          , fil_18_int1                             -- filler18이자1
          , fil_10_iyul2                            -- filler10이율2
          , ip_rqstr_silno_cid                      -- 입금의뢰인실명번호고객id
          , fil_dt4                                 -- 은행거래신청서 징구일
          , fil_20_ctnt2                            -- s-solutiin 키
          , fil_yn6                                 -- filler여부6
          , fil_yn7                                 -- fil_yn7
      from w0_shb.dws_sdt_gojeong_slv
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
