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
pgm_id = 'ILCD_MTFUA0014_TG'

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
s3_file_prefix = f'icd_mtfua0014_/icd_mtfua0014_{execution_kst}'

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
        delete from l0_shc.mtfua0014
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shc.mtfua0014
                (
                  aws_ls_dt                               -- aws적재일시
                , ta_ym                                   -- 기준년월
                , sgdmd                                   -- 그룹md번호
                , ls_ld_dt                                -- 최종적재일시
                , sgdmd_dfr_n                             -- 고객구별그룹md번호
                , dpr_hga                                 -- 백화점취급금액
                , oln_hga                                 -- 주유취급금액
                , cmu_bt_hga                              -- 통신이체취급금액
                , trf_hga                                 -- 교통취급금액
                , apo_hga                                 -- 아파트관리비취급금액
                , iu_hga                                  -- 보험취급금액
                , blp_dum_hga                             -- 대형할인마트취급금액
                , hsp_hga                                 -- 홈쇼핑취급금액
                , car_ivv_hga                             -- 자동차관련취급금액
                , sms_hga                                 -- sms취급금액
                , et_cl_hga                               -- 기타신판취급금액
                , tdmk_iu_hga                             -- tdmk보험취급금액
                , aia_hga                                 -- aia생명취급금액
                , dum_hga                                 -- 할인마트취급금액
                , in_cv_hga                               -- 인터넷현금서비스취급금액
                , ars_cv_hga                              -- ars현금서비스취급금액
                , cdm_cv_hga                              -- cd기현금서비스취급금액
                , elc_cmc_hga                             -- 전자상거래취급금액
                , gs_hsp_hga                              -- gs홈쇼핑취급금액
                , in_cv_ue_ct                             -- 인터넷현금서비스이용건수
                , ars_cv_ue_ct                            -- ars현금서비스이용건수
                , cdm_cv_ue_ct                            -- cd기현금서비스이용건수
                , dpr_ue_tf                               -- 백화점이용tf
                , cmu_bt_ue_tf                            -- 통신이체이용tf
                , trf_ue_tf                               -- 교통이용tf
                , apo_ue_tf                               -- 아파트관리비이용tf
                , iu_ue_tf                                -- 보험이용tf
                , blp_dum_ue_tf                           -- 대형할인마트이용tf
                , hsp_ue_tf                               -- 홈쇼핑이용tf
                , car_ivv_ue_tf                           -- 자동차관련이용tf
                , sms_ue_tf                               -- sms이용tf
                , et_cl_ue_tf                             -- 기타신판이용tf
                , tdmk_iu_ue_tf                           -- tdmk보험이용tf
                , aia_ue_tf                               -- aia생명이용tf
                , dum_ue_tf                               -- 할인마트이용tf
                , gs_hsp_ue_tf                            -- gs홈쇼핑이용tf
                , oln_ue_tf                               -- 주유이용tf
                , in_cv_ue_tf                             -- 인터넷현금서비스이용tf
                , ars_cv_ue_tf                            -- ars현금서비스이용tf
                , cdm_cv_ue_tf                            -- cd기현금서비스이용tf
                , elc_cmc_ue_tf                           -- 전자상거래이용tf
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , ta_ym                                   -- 기준년월
          , sgdmd                                   -- 그룹md번호
          , ls_ld_dt                                -- 최종적재일시
          , sgdmd_dfr_n                             -- 고객구별그룹md번호
          , dpr_hga                                 -- 백화점취급금액
          , oln_hga                                 -- 주유취급금액
          , cmu_bt_hga                              -- 통신이체취급금액
          , trf_hga                                 -- 교통취급금액
          , apo_hga                                 -- 아파트관리비취급금액
          , iu_hga                                  -- 보험취급금액
          , blp_dum_hga                             -- 대형할인마트취급금액
          , hsp_hga                                 -- 홈쇼핑취급금액
          , car_ivv_hga                             -- 자동차관련취급금액
          , sms_hga                                 -- sms취급금액
          , et_cl_hga                               -- 기타신판취급금액
          , tdmk_iu_hga                             -- tdmk보험취급금액
          , aia_hga                                 -- aia생명취급금액
          , dum_hga                                 -- 할인마트취급금액
          , in_cv_hga                               -- 인터넷현금서비스취급금액
          , ars_cv_hga                              -- ars현금서비스취급금액
          , cdm_cv_hga                              -- cd기현금서비스취급금액
          , elc_cmc_hga                             -- 전자상거래취급금액
          , gs_hsp_hga                              -- gs홈쇼핑취급금액
          , in_cv_ue_ct                             -- 인터넷현금서비스이용건수
          , ars_cv_ue_ct                            -- ars현금서비스이용건수
          , cdm_cv_ue_ct                            -- cd기현금서비스이용건수
          , dpr_ue_tf                               -- 백화점이용tf
          , cmu_bt_ue_tf                            -- 통신이체이용tf
          , trf_ue_tf                               -- 교통이용tf
          , apo_ue_tf                               -- 아파트관리비이용tf
          , iu_ue_tf                                -- 보험이용tf
          , blp_dum_ue_tf                           -- 대형할인마트이용tf
          , hsp_ue_tf                               -- 홈쇼핑이용tf
          , car_ivv_ue_tf                           -- 자동차관련이용tf
          , sms_ue_tf                               -- sms이용tf
          , et_cl_ue_tf                             -- 기타신판이용tf
          , tdmk_iu_ue_tf                           -- tdmk보험이용tf
          , aia_ue_tf                               -- aia생명이용tf
          , dum_ue_tf                               -- 할인마트이용tf
          , gs_hsp_ue_tf                            -- gs홈쇼핑이용tf
          , oln_ue_tf                               -- 주유이용tf
          , in_cv_ue_tf                             -- 인터넷현금서비스이용tf
          , ars_cv_ue_tf                            -- ars현금서비스이용tf
          , cdm_cv_ue_tf                            -- cd기현금서비스이용tf
          , elc_cmc_ue_tf                           -- 전자상거래이용tf
      from w0_shc.mtfua0014
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
