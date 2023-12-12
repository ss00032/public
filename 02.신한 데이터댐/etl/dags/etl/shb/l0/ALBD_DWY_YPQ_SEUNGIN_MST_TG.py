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
pgm_id = 'ALBD_DWY_YPQ_SEUNGIN_MST_TG'

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
s3_file_prefix = f'abd_dwy_ypq_seungin_mst_/abd_dwy_ypq_seungin_mst_{execution_kst}'

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
        delete from l0_shb.dwy_ypq_seungin_mst
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwy_ypq_seungin_mst
                (
                  aws_ls_dt                               -- aws적재일시
                , sinc_no                                 -- 신청번호
                , ypq_bnk_g                               -- 개인신청은행구분
                , sincdt                                  -- 신청일자
                , sinc_time                               -- 신청시각
                , sinc_g                                  -- 신청구분
                , dup_sinc_yn                             -- 중복신청여부
                , ltst_dup_sincdt                         -- 최근중복신청일자
                , ltst_dup_sinc_gun_jdgm_his              -- 최근중복신청건판결내역
                , ltst_dup_sinc_gun_lst_s_no              -- 최근중복신청건최종상태번호
                , dup_sccnt                               -- 중복신청건수
                , lst_s_no                                -- 최종상태번호
                , lst_s_no_dt                             -- 최종상태번호일자
                , rcvr_shb_jkwi_c                         -- 접수자신한은행직위code
                , rcpt_time                               -- 접수시각
                , sinc_brno                               -- 신청점번호
                , aprv_brno                               -- 승인점번호
                , lst_jdgm_b_his                          -- 최종판결분류내역
                , lst_jdgm_rst_his                        -- 최종판결결과내역
                , lst_jdgm_dt                             -- 최종판결일자
                , fst_jdgm_rst_his                        -- 최초판결결과내역
                , chg_sincin_shb_jkwi_c                   -- 변경신청인신한은행직위code
                , chg_sincin_nm                           -- 변경신청인명
                , chg_sincdt                              -- 변경신청일자
                , chg_sinc_time                           -- 변경신청시각
                , jdgm_crt_b_his                          -- 판결정정분류내역
                , jdgm_crt_rsn1                           -- 판결정정사유내역1
                , jdgm_crt_rsn2                           -- 판결정정사유내역2
                , jdgm_crt_rsn3                           -- 판결정정사유내역3
                , jdgm_crt_mn_shb_jkwi_c                  -- 판결정정자신한은행직위code
                , jdgm_crt_mn_nm                          -- 판결정정자명
                , jdgm_crt_dt                             -- 판결정정일자
                , jdgm_crt_time                           -- 판결정정시각
                , lst_trxdt                               -- 최종거래일자
                , prdt_c                                  -- 상품code
                , aprv_plcy_finc_c                        -- 승인정책금융code
                , dc_aprv_jamt                            -- 대출승인정수금액
                , aprv_iyul                               -- 승인이율
                , aprv_irt_g                              -- 승인금리구분
                , sirt_g                                  -- 특별금리구분
                , dc_tmuni_c                              -- 대출기간단위code
                , dc_term                                 -- 대출기간
                , dc_dudt                                 -- 대출만기일자
                , unyong_g                                -- 운용구분
                , shnd_mth                                -- 상환방법
                , aprv_trt_fert                           -- 승인취급수수료율
                , aprv_bjiyul                             -- 승인본지점이율
                , sidt                                    -- 승인일자
                , doc_jinggu_yn                           -- 서류징구여부
                , hiyn                                    -- 확인여부
                , ntyn                                    -- 통지여부
                , ys_bojn_jamt1                           -- 여신보증정수금액1
                , ys_bojn_jamt2                           -- 여신보증정수금액2
                , dc_exe_yn                               -- 대출실행여부
                , exe_ac_kwa_c                            -- 실행계좌과목code
                , exe_acser                               -- 실행계좌일련번호
                , dc_exe_jamt                             -- 대출실행정수금액
                , dc_exe_dt                               -- 대출실행일자
                , dc_shnd_dt                              -- 대출상환일자
                , lst_jdgm_strg_id                        -- 최종판결strategyid
                , bf_lst_s_no                             -- 직전최종상태번호
                , auto_end_dt                             -- 자동완결일자
                , beh_rsk_valt_pnt                        -- 행동위험평가점수
                , bnk_cus_crdt_rsk_gd                     -- 은행고객신용위험등급
                , sfg_crdt_rsk_gd                         -- sfg신용위험등급
                , dc_sinc_eungdae_his                     -- 대출신청응대내역
                , bnk_cus_crdt_rsk_pnt                    -- 은행고객신용위험점수
                , sfg_crdt_rsk_pnt                        -- sfg신용위험점수
                , hmdc_junkl_g                            -- 가계대출전결구분
                , bas_yymm                                -- 기준년월
                , aprv_iyul2                              -- 승인이율2
                , aprv_irt_g2                             -- 승인금리구분2
                , sirt_g2                                 -- 특별금리구분2
                , rrno_cid                                -- 주민번호고객id
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , sinc_no                                 -- 신청번호
          , ypq_bnk_g                               -- 개인신청은행구분
          , sincdt                                  -- 신청일자
          , sinc_time                               -- 신청시각
          , sinc_g                                  -- 신청구분
          , dup_sinc_yn                             -- 중복신청여부
          , ltst_dup_sincdt                         -- 최근중복신청일자
          , ltst_dup_sinc_gun_jdgm_his              -- 최근중복신청건판결내역
          , ltst_dup_sinc_gun_lst_s_no              -- 최근중복신청건최종상태번호
          , dup_sccnt                               -- 중복신청건수
          , lst_s_no                                -- 최종상태번호
          , lst_s_no_dt                             -- 최종상태번호일자
          , rcvr_shb_jkwi_c                         -- 접수자신한은행직위code
          , rcpt_time                               -- 접수시각
          , sinc_brno                               -- 신청점번호
          , aprv_brno                               -- 승인점번호
          , lst_jdgm_b_his                          -- 최종판결분류내역
          , lst_jdgm_rst_his                        -- 최종판결결과내역
          , lst_jdgm_dt                             -- 최종판결일자
          , fst_jdgm_rst_his                        -- 최초판결결과내역
          , chg_sincin_shb_jkwi_c                   -- 변경신청인신한은행직위code
          , chg_sincin_nm                           -- 변경신청인명
          , chg_sincdt                              -- 변경신청일자
          , chg_sinc_time                           -- 변경신청시각
          , jdgm_crt_b_his                          -- 판결정정분류내역
          , jdgm_crt_rsn1                           -- 판결정정사유내역1
          , jdgm_crt_rsn2                           -- 판결정정사유내역2
          , jdgm_crt_rsn3                           -- 판결정정사유내역3
          , jdgm_crt_mn_shb_jkwi_c                  -- 판결정정자신한은행직위code
          , jdgm_crt_mn_nm                          -- 판결정정자명
          , jdgm_crt_dt                             -- 판결정정일자
          , jdgm_crt_time                           -- 판결정정시각
          , lst_trxdt                               -- 최종거래일자
          , prdt_c                                  -- 상품code
          , aprv_plcy_finc_c                        -- 승인정책금융code
          , dc_aprv_jamt                            -- 대출승인정수금액
          , aprv_iyul                               -- 승인이율
          , aprv_irt_g                              -- 승인금리구분
          , sirt_g                                  -- 특별금리구분
          , dc_tmuni_c                              -- 대출기간단위code
          , dc_term                                 -- 대출기간
          , dc_dudt                                 -- 대출만기일자
          , unyong_g                                -- 운용구분
          , shnd_mth                                -- 상환방법
          , aprv_trt_fert                           -- 승인취급수수료율
          , aprv_bjiyul                             -- 승인본지점이율
          , sidt                                    -- 승인일자
          , doc_jinggu_yn                           -- 서류징구여부
          , hiyn                                    -- 확인여부
          , ntyn                                    -- 통지여부
          , ys_bojn_jamt1                           -- 여신보증정수금액1
          , ys_bojn_jamt2                           -- 여신보증정수금액2
          , dc_exe_yn                               -- 대출실행여부
          , exe_ac_kwa_c                            -- 실행계좌과목code
          , exe_acser                               -- 실행계좌일련번호
          , dc_exe_jamt                             -- 대출실행정수금액
          , dc_exe_dt                               -- 대출실행일자
          , dc_shnd_dt                              -- 대출상환일자
          , lst_jdgm_strg_id                        -- 최종판결strategyid
          , bf_lst_s_no                             -- 직전최종상태번호
          , auto_end_dt                             -- 자동완결일자
          , beh_rsk_valt_pnt                        -- 행동위험평가점수
          , bnk_cus_crdt_rsk_gd                     -- 은행고객신용위험등급
          , sfg_crdt_rsk_gd                         -- sfg신용위험등급
          , dc_sinc_eungdae_his                     -- 대출신청응대내역
          , bnk_cus_crdt_rsk_pnt                    -- 은행고객신용위험점수
          , sfg_crdt_rsk_pnt                        -- sfg신용위험점수
          , hmdc_junkl_g                            -- 가계대출전결구분
          , bas_yymm                                -- 기준년월
          , aprv_iyul2                              -- 승인이율2
          , aprv_irt_g2                             -- 승인금리구분2
          , sirt_g2                                 -- 특별금리구분2
          , rrno_cid                                -- 주민번호고객id
      from w0_shb.dwy_ypq_seungin_mst
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
