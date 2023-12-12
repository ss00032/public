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
pgm_id = 'ALBD_DWX_RIB_IBMAS_TG'

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
s3_file_prefix = f'abd_dwx_rib_ibmas_/abd_dwx_rib_ibmas_{execution_kst}'

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
        delete from l0_shb.dwx_rib_ibmas
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwx_rib_ibmas
                (
                  aws_ls_dt                               -- aws적재일시
                , dw_bas_ddt                              -- dw기준d일자
                , ci_no                                   -- ci번호
                , cusno                                   -- 고객번호
                , mkdt                                    -- 신규일자
                , mk_brno                                 -- 신규점
                , mk_term_jno                             -- 신규단말저널일련번호
                , hjidt                                   -- 해지일자
                , hji_brno                                -- 해지점
                , hji_term_jno                            -- 해지단말저널일련번호
                , cert_state                              -- 인증서상태
                , cert_fst_bldttm                         -- 인증서최초발급일시
                , cert_upd_dttm                           -- 인증서갱신일시
                , lst_login_dttm                          -- 최종로그인일시
                , lst_logout_dttm                         -- 최종로그아웃일시
                , lst_iche_dt                             -- 최종이체일자
                , lst_iche_cnt                            -- 최종이체일이체건수
                , lst_iche_amt                            -- 최종이체일이체금액
                , lst_channel                             -- 최종접속채널
                , ibmas_memo_rej_yn_c1                    -- 쪽지거절여부
                , ibmas_memo_rej_dt_c8                    -- 쪽지거절일자
                , dw_data_gjdt                            -- dwdata기준일자
                , dw_lst_jukja_dt                         -- dw최종적재일자
                , ibmas_wlist_add_c1                      -- 뱅킹용pc_사전등록여부
                , ibmas_access_except_c1                  -- 예외pc접속_알림여부
                , ibmas_foreign_ip_c1                     -- 최종로그인_해외ip여부
                , ibmas_graphic_cert_sayong_gbn           -- 그래픽인증사용여부 / 0 : 사용안함, 1 : 사용
                , ibmas_id_mk_brno_c4                     -- id신규점번호
                , ibmas_id_mkdt_c8                        -- id신규일
                , ibmas_id_mk_acno_c12                    -- id신규시인증계좌번호
                , ibmas_id_mk_channel_c2                  -- id신규매체
                , ibmas_lst_mac1_c32                      -- 최종접속 mac1
                , ibmas_lst_mac3_c32                      -- 최종접속 mac3
                , ibmas_wlist_add_dt_c8                   -- 기기지정서비스 등록일자
                , ibmas_wlist_add_cnt_c3                  -- 기기지정서비스 등록기기 cnt
                , ibmas_wlist_add_chk_c1                  -- 기기지정서비스 추가인증 중복선택 여부
                , ibmas_addchk_agree_yn_c1                -- 전자금융사기예방서비스 동의여부
                , ibmas_addchk_agree_dt_c8                -- 전자금융사기예방서비스 동의일자
                , ibmas_wlist_add_dup_c1                  -- 기기지정서비스 추가인증 중복선택 여부
                , ibmas_view_closedt_c8                   -- 보안카드 이용 누적 횟수 제한일자
                , ibmas_cert_chkbit_c1                    -- 인증서 우회추가체크 관리
                , ibmas_trans_chkbit_c1                   -- 이체 우회추가체크 관리
                , ibmas_force_chkbit_c1                   -- 강제 우회추가체크 관리
                , ibmas_receive_acno_c16                  -- 최종수취인계좌번호
                , ibmas_login_addchk_c1                   -- 로그인 후 추가인증 실행여부
                , ibmas_login_wlist_c1                    -- 로그인시 등록된 기기여부
                , ibmas_iche_wlist_c1                     -- 이체 추가인증 거래의 등록 기기여부
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , dw_bas_ddt                              -- dw기준d일자
          , ci_no                                   -- ci번호
          , cusno                                   -- 고객번호
          , mkdt                                    -- 신규일자
          , mk_brno                                 -- 신규점
          , mk_term_jno                             -- 신규단말저널일련번호
          , hjidt                                   -- 해지일자
          , hji_brno                                -- 해지점
          , hji_term_jno                            -- 해지단말저널일련번호
          , cert_state                              -- 인증서상태
          , cert_fst_bldttm                         -- 인증서최초발급일시
          , cert_upd_dttm                           -- 인증서갱신일시
          , lst_login_dttm                          -- 최종로그인일시
          , lst_logout_dttm                         -- 최종로그아웃일시
          , lst_iche_dt                             -- 최종이체일자
          , lst_iche_cnt                            -- 최종이체일이체건수
          , lst_iche_amt                            -- 최종이체일이체금액
          , lst_channel                             -- 최종접속채널
          , ibmas_memo_rej_yn_c1                    -- 쪽지거절여부
          , ibmas_memo_rej_dt_c8                    -- 쪽지거절일자
          , dw_data_gjdt                            -- dwdata기준일자
          , dw_lst_jukja_dt                         -- dw최종적재일자
          , ibmas_wlist_add_c1                      -- 뱅킹용pc_사전등록여부
          , ibmas_access_except_c1                  -- 예외pc접속_알림여부
          , ibmas_foreign_ip_c1                     -- 최종로그인_해외ip여부
          , ibmas_graphic_cert_sayong_gbn           -- 그래픽인증사용여부 / 0 : 사용안함, 1 : 사용
          , ibmas_id_mk_brno_c4                     -- id신규점번호
          , ibmas_id_mkdt_c8                        -- id신규일
          , ibmas_id_mk_acno_c12                    -- id신규시인증계좌번호
          , ibmas_id_mk_channel_c2                  -- id신규매체
          , ibmas_lst_mac1_c32                      -- 최종접속 mac1
          , ibmas_lst_mac3_c32                      -- 최종접속 mac3
          , ibmas_wlist_add_dt_c8                   -- 기기지정서비스 등록일자
          , ibmas_wlist_add_cnt_c3                  -- 기기지정서비스 등록기기 cnt
          , ibmas_wlist_add_chk_c1                  -- 기기지정서비스 추가인증 중복선택 여부
          , ibmas_addchk_agree_yn_c1                -- 전자금융사기예방서비스 동의여부
          , ibmas_addchk_agree_dt_c8                -- 전자금융사기예방서비스 동의일자
          , ibmas_wlist_add_dup_c1                  -- 기기지정서비스 추가인증 중복선택 여부
          , ibmas_view_closedt_c8                   -- 보안카드 이용 누적 횟수 제한일자
          , ibmas_cert_chkbit_c1                    -- 인증서 우회추가체크 관리
          , ibmas_trans_chkbit_c1                   -- 이체 우회추가체크 관리
          , ibmas_force_chkbit_c1                   -- 강제 우회추가체크 관리
          , ibmas_receive_acno_c16                  -- 최종수취인계좌번호
          , ibmas_login_addchk_c1                   -- 로그인 후 추가인증 실행여부
          , ibmas_login_wlist_c1                    -- 로그인시 등록된 기기여부
          , ibmas_iche_wlist_c1                     -- 이체 추가인증 거래의 등록 기기여부
      from w0_shb.dwx_rib_ibmas
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
