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


__author__     = "노재홍"
__copyright__  = "Copyright 2021, Shinhan Datadam"
__credits__    = ["노재홍"]
__version__    = "1.0"
__maintainer__ = "노재홍"
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
pgm_id = 'ALBD_DWY_YPQ_SINCUST_MST_TG'

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
s3_file_prefix = f'abd_dwy_ypq_sincust_mst_/abd_dwy_ypq_sincust_mst_{execution_kst}'

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
        delete from l0_shb.dwy_ypq_sincust_mst
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwy_ypq_sincust_mst
                (
                  aws_ls_dt                               -- aws적재일시
                , ci_no                                   -- ci번호
                , cusno                                   -- 고객번호
                , sinc_no                                 -- 신청번호
                , cus_mkdt                                -- 고객신규일자
                , naeoigukin_g                            -- 내외국인구분
                , cus_age                                 -- 고객연령
                , ault_minr_sinc_um                       -- 성년인정미성년자신청유무
                , hmadr_dong_nm                           -- 자택주소동명
                , jugeo_k                                 -- 주거종류
                , now_adr_rsdc_term                       -- 현주소거주기간
                , towork_g                                -- 맞벌이구분
                , spos_inmt_yn                            -- 배우자동거여부
                , fath_inmt_yn                            -- 부친동거여부
                , mo_inmt_yn                              -- 모동거여부
                , chld_inmt_yn                            -- 자녀동거여부
                , brthr_sstr_inmt_yn                      -- 형제자매동거여부
                , etc_inmt_yn                             -- 기타동거여부
                , chld_inwon_cnt                          -- 자녀인원수
                , occp_g                                  -- 직업구분
                , emp_g                                   -- 임직원구분
                , occp_c                                  -- 직업code
                , jkwi_c                                  -- 직위code
                , emp_type_g                              -- 고용형태구분
                , ipsa_dt                                 -- 입사일자
                , wrk_term                                -- 근무기간
                , jugeo_g                                 -- 주거구분
                , etc_yr_sodk_um                          -- 기타연소득유무
                , sdamt_hiyn                              -- 소득금액확인여부
                , pens_sodk_um                            -- 연금소득유무
                , int_sodk_um                             -- 이자소득유무
                , bdang_sodk_um                           -- 배당소득유무
                , leas_sodk_um                            -- 임대소득유무
                , etc_sodk_um                             -- 기타소득유무
                , pay_iche_jamt                           -- 급여이체정수금액
                , yr_sodk_jamt                            -- 연소득정수금액
                , jonghap_sdtax_jamt                      -- 종합소득세정수금액
                , tax_jamt                                -- 재산세정수금액
                , ppens_jamt                              -- 공적연금정수금액
                , etc_yr_sodk_jsamt                       -- 기타연소득정수합계금액
                , dw_lst_jukja_dt                         -- dw최종적재일자
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , ci_no                                   -- ci번호
          , cusno                                   -- 고객번호
          , sinc_no                                 -- 신청번호
          , cus_mkdt                                -- 고객신규일자
          , naeoigukin_g                            -- 내외국인구분
          , cus_age                                 -- 고객연령
          , ault_minr_sinc_um                       -- 성년인정미성년자신청유무
          , hmadr_dong_nm                           -- 자택주소동명
          , jugeo_k                                 -- 주거종류
          , now_adr_rsdc_term                       -- 현주소거주기간
          , towork_g                                -- 맞벌이구분
          , spos_inmt_yn                            -- 배우자동거여부
          , fath_inmt_yn                            -- 부친동거여부
          , mo_inmt_yn                              -- 모동거여부
          , chld_inmt_yn                            -- 자녀동거여부
          , brthr_sstr_inmt_yn                      -- 형제자매동거여부
          , etc_inmt_yn                             -- 기타동거여부
          , chld_inwon_cnt                          -- 자녀인원수
          , occp_g                                  -- 직업구분
          , emp_g                                   -- 임직원구분
          , occp_c                                  -- 직업code
          , jkwi_c                                  -- 직위code
          , emp_type_g                              -- 고용형태구분
          , ipsa_dt                                 -- 입사일자
          , wrk_term                                -- 근무기간
          , jugeo_g                                 -- 주거구분
          , etc_yr_sodk_um                          -- 기타연소득유무
          , sdamt_hiyn                              -- 소득금액확인여부
          , pens_sodk_um                            -- 연금소득유무
          , int_sodk_um                             -- 이자소득유무
          , bdang_sodk_um                           -- 배당소득유무
          , leas_sodk_um                            -- 임대소득유무
          , etc_sodk_um                             -- 기타소득유무
          , pay_iche_jamt                           -- 급여이체정수금액
          , yr_sodk_jamt                            -- 연소득정수금액
          , jonghap_sdtax_jamt                      -- 종합소득세정수금액
          , tax_jamt                                -- 재산세정수금액
          , ppens_jamt                              -- 공적연금정수금액
          , etc_yr_sodk_jsamt                       -- 기타연소득정수합계금액
          , dw_lst_jukja_dt                         -- dw최종적재일자
      from w0_shb.dwy_ypq_sincust_mst
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
