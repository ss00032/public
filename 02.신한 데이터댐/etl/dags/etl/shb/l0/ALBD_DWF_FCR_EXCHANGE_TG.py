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
pgm_id = 'ALBD_DWF_FCR_EXCHANGE_TG'

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
s3_file_prefix = f'abd_dwf_fcr_exchange_/abd_dwf_fcr_exchange_{execution_kst}'

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
        delete from l0_shb.dwf_fcr_exchange
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwf_fcr_exchange
                (
                  aws_ls_dt                               -- aws적재일시
                , ci_no                                   -- ci번호
                , cusno                                   -- 고객번호
                , acno                                    -- 계좌번호
                , kwa_c                                   -- 계좌과목code
                , acser                                   -- 계좌일련번호
                , for_mas_s                               -- 외환원장상태
                , lst_trxdt                               -- 최종거래일자
                , exchg_trx_k                             -- 환전거래종류
                , exchg_g                                 -- 환전구분
                , grbrno                                  -- 관리점번호
                , trxbrno                                 -- 거래점번호
                , trxdt                                   -- 거래일자
                , oprt_dt                                 -- 조작일자
                , hsdt                                    -- 후송일자
                , gisdt                                   -- 기산일자
                , feeud_rsn_c                             -- 수수료환율우대사유code
                , silno_g                                 -- 실명번호구분
                , cus_adr1                                -- 고객주소1
                , cus_adr2                                -- 고객주소2
                , ntlty_nat_c                             -- 국적국가code
                , usdcvt_amt                              -- 대미환산금액
                , usdcvt_pstn_amt                         -- 대미환산position금액
                , wcur_samt                               -- 원화합계금액
                , wdch_amt                                -- 원화대체금액
                , wcsh_amt                                -- 원화현금금액
                , ipji_g                                  -- 입지구분
                , tbnkchk_amt                             -- 타점권금액
                , trxno                                   -- 거래번호
                , fnc_trxno                               -- 무역외거래번호
                , chan_u                                  -- 채널유형
                , exchg_bonus_k                           -- 환전보너스종류
                , exchg_bonus_famt                        -- 환전보너스외화금액
                , sinc_juche_c                            -- 신청주체code
                , ynd_ac_kwa_c                            -- 연동계좌과목code
                , ynd_acser                               -- 연동계좌일련번호
                , bnk_g1                                  -- 은행구분1
                , fcur_ynd_acno                           -- 외화연동계좌번호
                , bnk_g2                                  -- 은행구분2
                , wcur_ynd_acno                           -- 원화연동계좌번호
                , dhb_acno                                -- 동화계좌번호
                , fund_resc_guknae_gukwoi_g               -- 자금원천국내국외구분
                , fnc_rsn_c                               -- 무역외사유code
                , silno_g2                                -- 실명번호구분2
                , bnk_g3                                  -- 은행구분3
                , wcur_fcur_g                             -- 원화외화구분
                , ip_acno                                 -- 입금계좌번호
                , for_ref_no                              -- 외환참조번호
                , exrt_jkdt                               -- 환율적용일자
                , fil_30_ctnt                             -- 환전팩판매실적
                , new_fnc_rsn_c                           -- 신사유코드
                , fil_cnt7                                -- 디지털 창구 여부
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , ci_no                                   -- ci번호
          , cusno                                   -- 고객번호
          , acno                                    -- 계좌번호
          , kwa_c                                   -- 계좌과목code
          , acser                                   -- 계좌일련번호
          , for_mas_s                               -- 외환원장상태
          , lst_trxdt                               -- 최종거래일자
          , exchg_trx_k                             -- 환전거래종류
          , exchg_g                                 -- 환전구분
          , grbrno                                  -- 관리점번호
          , trxbrno                                 -- 거래점번호
          , trxdt                                   -- 거래일자
          , oprt_dt                                 -- 조작일자
          , hsdt                                    -- 후송일자
          , gisdt                                   -- 기산일자
          , feeud_rsn_c                             -- 수수료환율우대사유code
          , silno_g                                 -- 실명번호구분
          , cus_adr1                                -- 고객주소1
          , cus_adr2                                -- 고객주소2
          , ntlty_nat_c                             -- 국적국가code
          , usdcvt_amt                              -- 대미환산금액
          , usdcvt_pstn_amt                         -- 대미환산position금액
          , wcur_samt                               -- 원화합계금액
          , wdch_amt                                -- 원화대체금액
          , wcsh_amt                                -- 원화현금금액
          , ipji_g                                  -- 입지구분
          , tbnkchk_amt                             -- 타점권금액
          , trxno                                   -- 거래번호
          , fnc_trxno                               -- 무역외거래번호
          , chan_u                                  -- 채널유형
          , exchg_bonus_k                           -- 환전보너스종류
          , exchg_bonus_famt                        -- 환전보너스외화금액
          , sinc_juche_c                            -- 신청주체code
          , ynd_ac_kwa_c                            -- 연동계좌과목code
          , ynd_acser                               -- 연동계좌일련번호
          , bnk_g1                                  -- 은행구분1
          , fcur_ynd_acno                           -- 외화연동계좌번호
          , bnk_g2                                  -- 은행구분2
          , wcur_ynd_acno                           -- 원화연동계좌번호
          , dhb_acno                                -- 동화계좌번호
          , fund_resc_guknae_gukwoi_g               -- 자금원천국내국외구분
          , fnc_rsn_c                               -- 무역외사유code
          , silno_g2                                -- 실명번호구분2
          , bnk_g3                                  -- 은행구분3
          , wcur_fcur_g                             -- 원화외화구분
          , ip_acno                                 -- 입금계좌번호
          , for_ref_no                              -- 외환참조번호
          , exrt_jkdt                               -- 환율적용일자
          , fil_30_ctnt                             -- 환전팩판매실적
          , new_fnc_rsn_c                           -- 신사유코드
          , fil_cnt7                                -- 디지털 창구 여부
      from w0_shb.dwf_fcr_exchange
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
