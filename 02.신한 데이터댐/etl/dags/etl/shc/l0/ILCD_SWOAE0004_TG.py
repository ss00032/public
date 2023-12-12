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
pgm_id = 'ILCD_SWOAE0004_TG'

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
s3_file_prefix = f'icd_swoae0004_/icd_swoae0004_{execution_kst}'

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
        delete from l0_shc.swoae0004
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shc.swoae0004
                (
                  aws_ls_dt                               -- aws적재일시
                , mct_n                                   -- 가맹점번호
                , spf_gel_ccd                             -- 명세서연락처구분코드
                , mct_zpn                                 -- 가맹점우편번호
                , mct_bse_ar                              -- 가맹점기본주소
                , mct_gds_bud_ccd                         -- 가맹점gds건물구분코드
                , mct_adm_gds_apb_cd                      -- 가맹점행정gds동코드
                , mct_cou_gds_apb_cd                      -- 가맹점법정gds동코드
                , gds_mct_bse_hsn_vl                      -- gds가맹점기본번지값
                , gds_mct_ssi_hsn_vl                      -- gds가맹점보조번지값
                , gds_mct_bud_nm                          -- gds가맹점건물명
                , mct_gds_apt_n                           -- 가맹점gds아파트번호
                , gds_mct_bud_apb_vl                      -- gds가맹점건물동값
                , gds_mct_bud_romn_nm                     -- gds가맹점건물호수명
                , gds_mct_bud_flr_nm                      -- gds가맹점건물층명
                , mct_gds_mti_hsn_n                       -- 가맹점gds세부번지번호
                , gds_mct_las_nm                          -- gds가맹점반명
                , gds_src_cd                              -- gds소스코드
                , wid_cty_cd                              -- 광역도시코드
                , gds_dsr_cd                              -- gds시군구코드
                , gds_brg_cd                              -- gds구군코드
                , mct_xc_vl                               -- 가맹점x좌표값
                , mct_yc_vl                               -- 가맹점y좌표값
                , mct_gds_cdt_ucd                         -- 가맹점gds좌표결과코드
                , ls_ld_dt                                -- 최종적재일시
                , lal_mct_xc_vl                           -- 경위도가맹점x좌표값
                , lal_mct_yc_vl                           -- 경위도가맹점y좌표값
                , mct_gds_cdt_ucd2                        -- 가맹점gds좌표결과코드2
                , utmr_mct_xc_vl                          -- utm가맹점x좌표값
                , utmr_mct_yc_vl                          -- utm가맹점y좌표값
                , mct_gds_cdt_ucd3                        -- 가맹점gds좌표결과코드3
                , mct_ntn_bn_psi_n10                      -- 가맹점국가지점위치번호10
                , mct_ntn_bn_psi_n50                      -- 가맹점국가지점위치번호50
                , gds_ri_cd                               -- gds리코드
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , mct_n                                   -- 가맹점번호
          , spf_gel_ccd                             -- 명세서연락처구분코드
          , mct_zpn                                 -- 가맹점우편번호
          , mct_bse_ar                              -- 가맹점기본주소
          , mct_gds_bud_ccd                         -- 가맹점gds건물구분코드
          , mct_adm_gds_apb_cd                      -- 가맹점행정gds동코드
          , mct_cou_gds_apb_cd                      -- 가맹점법정gds동코드
          , gds_mct_bse_hsn_vl                      -- gds가맹점기본번지값
          , gds_mct_ssi_hsn_vl                      -- gds가맹점보조번지값
          , gds_mct_bud_nm                          -- gds가맹점건물명
          , mct_gds_apt_n                           -- 가맹점gds아파트번호
          , gds_mct_bud_apb_vl                      -- gds가맹점건물동값
          , gds_mct_bud_romn_nm                     -- gds가맹점건물호수명
          , gds_mct_bud_flr_nm                      -- gds가맹점건물층명
          , mct_gds_mti_hsn_n                       -- 가맹점gds세부번지번호
          , gds_mct_las_nm                          -- gds가맹점반명
          , gds_src_cd                              -- gds소스코드
          , wid_cty_cd                              -- 광역도시코드
          , gds_dsr_cd                              -- gds시군구코드
          , gds_brg_cd                              -- gds구군코드
          , mct_xc_vl                               -- 가맹점x좌표값
          , mct_yc_vl                               -- 가맹점y좌표값
          , mct_gds_cdt_ucd                         -- 가맹점gds좌표결과코드
          , ls_ld_dt                                -- 최종적재일시
          , lal_mct_xc_vl                           -- 경위도가맹점x좌표값
          , lal_mct_yc_vl                           -- 경위도가맹점y좌표값
          , mct_gds_cdt_ucd2                        -- 가맹점gds좌표결과코드2
          , utmr_mct_xc_vl                          -- utm가맹점x좌표값
          , utmr_mct_yc_vl                          -- utm가맹점y좌표값
          , mct_gds_cdt_ucd3                        -- 가맹점gds좌표결과코드3
          , mct_ntn_bn_psi_n10                      -- 가맹점국가지점위치번호10
          , mct_ntn_bn_psi_n50                      -- 가맹점국가지점위치번호50
          , gds_ri_cd                               -- gds리코드
      from w0_shc.swoae0004
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
