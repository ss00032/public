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
pgm_id = 'ALCD_SIIFC0226_TG'

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
s3_file_prefix = f'acd_siifc0226_/acd_siifc0226_{execution_kst}'

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
        delete from l0_shc.siifc0226
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shc.siifc0226
                (
                  aws_ls_dt                               -- aws적재일시
                , sra_pnt_se_d                            -- 신한그룹통합리워드앱포인트사용일자
                , sra_pnt_se_sn                           -- 신한그룹통합리워드앱포인트사용순번
                , sra_ig_sgdmd                            -- 신한그룹통합리워드앱통합그룹md번호
                , sra_ig_sgdci                            -- 신한그룹통합리워드앱통합ci번호
                , sra_pnt_kcd                             -- 신한그룹통합리워드앱포인트종류코드
                , sra_afo_cd                              -- 신한그룹통합리워드앱제휴사코드
                , sra_pnt_chl_ccd                         -- 신한그룹통합리워드앱포인트채널구분코드
                , sra_mct_n                               -- 신한그룹통합리워드앱가맹점번호
                , sra_mct_nm                              -- 신한그룹통합리워드앱가맹점명
                , sra_pnt_kd_lcd                          -- 신한그룹통합리워드앱포인트종류상세코드
                , sra_se_pnt                              -- 신한그룹통합리워드앱사용포인트
                , sra_se_af_ma_pnt                        -- 신한그룹통합리워드앱사용후잔여포인트
                , sra_pnt_se_mcd                          -- 신한그룹통합리워드앱포인트사용방법코드
                , sra_ce_f                                -- 신한그룹통합리워드앱취소여부
                , sra_r_pnt_se_d                          -- 신한그룹통합리워드앱원포인트사용일자
                , sra_r_pnt_se_sn                         -- 신한그룹통합리워드앱원포인트사용순번
                , sra_cno_se_d                            -- 신한그룹통합리워드앱계열사사용일자
                , sra_cno_se_tm                           -- 신한그룹통합리워드앱계열사사용시간
                , sra_shfc_ts_tcd                         -- 신한그룹통합리워드앱신한판클럽거래유형코드
                , sra_lsp_rid                             -- 신한그룹통합리워드앱매출전표접수일자
                , sra_lsp_n                               -- 신한그룹통합리워드앱매출전표번호
                , sra_shfc_ts_ec_tcd                      -- 신한그룹통합리워드앱신한판클럽거래정산유형코드
                , sra_af_ps_f                             -- 신한그룹통합리워드앱후처리여부
                , sra_shfc_ts_ps_ucd                      -- 신한그룹통합리워드앱신한판클럽거래처리결과코드
                , sra_ts_rqk_xct_id                       -- 신한그룹통합리워드앱거래요청자수행id
                , sra_ce_tf                               -- 신한그룹통합리워드앱취소tf
                , sra_af_ps_tf                            -- 신한그룹통합리워드앱후처리tf
                , ni_rg_dt                                -- 최초등록일시
                , ls_alt_dt                               -- 최종수정일시
                , ls_ld_dt                                -- 최종적재일시
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , sra_pnt_se_d                            -- 신한그룹통합리워드앱포인트사용일자
          , sra_pnt_se_sn                           -- 신한그룹통합리워드앱포인트사용순번
          , sra_ig_sgdmd                            -- 신한그룹통합리워드앱통합그룹md번호
          , sra_ig_sgdci                            -- 신한그룹통합리워드앱통합ci번호
          , sra_pnt_kcd                             -- 신한그룹통합리워드앱포인트종류코드
          , sra_afo_cd                              -- 신한그룹통합리워드앱제휴사코드
          , sra_pnt_chl_ccd                         -- 신한그룹통합리워드앱포인트채널구분코드
          , sra_mct_n                               -- 신한그룹통합리워드앱가맹점번호
          , sra_mct_nm                              -- 신한그룹통합리워드앱가맹점명
          , sra_pnt_kd_lcd                          -- 신한그룹통합리워드앱포인트종류상세코드
          , sra_se_pnt                              -- 신한그룹통합리워드앱사용포인트
          , sra_se_af_ma_pnt                        -- 신한그룹통합리워드앱사용후잔여포인트
          , sra_pnt_se_mcd                          -- 신한그룹통합리워드앱포인트사용방법코드
          , sra_ce_f                                -- 신한그룹통합리워드앱취소여부
          , sra_r_pnt_se_d                          -- 신한그룹통합리워드앱원포인트사용일자
          , sra_r_pnt_se_sn                         -- 신한그룹통합리워드앱원포인트사용순번
          , sra_cno_se_d                            -- 신한그룹통합리워드앱계열사사용일자
          , sra_cno_se_tm                           -- 신한그룹통합리워드앱계열사사용시간
          , sra_shfc_ts_tcd                         -- 신한그룹통합리워드앱신한판클럽거래유형코드
          , sra_lsp_rid                             -- 신한그룹통합리워드앱매출전표접수일자
          , sra_lsp_n                               -- 신한그룹통합리워드앱매출전표번호
          , sra_shfc_ts_ec_tcd                      -- 신한그룹통합리워드앱신한판클럽거래정산유형코드
          , sra_af_ps_f                             -- 신한그룹통합리워드앱후처리여부
          , sra_shfc_ts_ps_ucd                      -- 신한그룹통합리워드앱신한판클럽거래처리결과코드
          , sra_ts_rqk_xct_id                       -- 신한그룹통합리워드앱거래요청자수행id
          , sra_ce_tf                               -- 신한그룹통합리워드앱취소tf
          , sra_af_ps_tf                            -- 신한그룹통합리워드앱후처리tf
          , ni_rg_dt                                -- 최초등록일시
          , ls_alt_dt                               -- 최종수정일시
          , ls_ld_dt                                -- 최종적재일시
      from w0_shc.siifc0226
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
