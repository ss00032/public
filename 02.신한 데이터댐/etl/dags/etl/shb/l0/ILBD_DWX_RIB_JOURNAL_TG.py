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
pgm_id = 'ILBD_DWX_RIB_JOURNAL_TG'

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
s3_file_prefix = f'ibd_dwx_rib_journal_/ibd_dwx_rib_journal_{execution_kst}'

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
        delete from l0_shb.dwx_rib_journal
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwx_rib_journal
                (
                  aws_ls_dt                               -- aws적재일시
                , jnal_tran_date                          -- 거래일자
                , jnal_tran_time                          -- 거래시간
                , jnal_tran_sno                           -- 일련번호
                , jnal_ci_no                              -- ci번호
                , jnal_cusno                              -- 고객번호
                , jnal_state                              -- 전문상태
                , jnal_svc_code                           -- 서비스코드
                , jnal_media_code                         -- 매체코드
                , jnal_rqst_recv_gbn                      -- 요청메인수신구분
                , jnal_rqst_send_gbn                      -- 요청메인송신구분
                , jnal_resp_recv_gbn                      -- 응답메인수신구분
                , jnal_resp_send_gbn                      -- 응답메인송신구분
                , jnal_rqst_proc_dt                       -- 요청메인처리일자
                , jnal_rqst_proc_tm                       -- 요청메인처리시간
                , jnal_resp_proc_dt                       -- 응답메인처리일자
                , jnal_resp_proc_tm                       -- 응답메인처리시간
                , jnal_rqst_send_state                    -- 요청전송상태
                , jnal_resp_send_state                    -- 응답전송상태
                , rwc_upmu_kbn                            -- 웹공통_업무구분
                , rwc_svc_code                            -- 웹공통_서비스코드
                , rwc_sysgbn                              -- 웹공통_온라인시스템구분
                , rwc_web_date                            -- 웹공통_웹서버일자
                , rwc_web_time                            -- 웹공통_웹서버시간
                , rwc_web_domain                          -- 웹공통_도메인명
                , rwc_ipaddr                              -- 웹공통_ip주소
                , rwc_language                            -- 웹공통_언어구분
                , rwc_channel_gbn                         -- 웹공통_채널유형
                , rwc_ci_no                               -- 웹공통_ci번호
                , rwc_cifno                               -- 웹공통_고객번호
                , rwc_sec_chk                             -- 웹공통_보안매체체크
                , rwc_ichepswd_chk                        -- 웹공통_이체비번체크
                , rwc_yeyak_iche                          -- 웹공통_예약이체구분
                , rwc_ef_date                             -- 웹공통_저널생성일자
                , rwc_ef_time                             -- 웹공통_저널생성시간
                , rwc_ef_yoil                             -- 웹공통_저널생성요일
                , rwc_resultcd                            -- 웹공통_처리결과
                , rwc_endmark                             -- 웹공통_endmark
                , rwc_ef_serial                           -- 웹공통_원거래번호
                , rmc_err_code                            -- 전문헤더_에러코드
                , rjc_amt                                 -- 저널공통_거래금액
                , rjc_susuryo                             -- 저널공통_수수료
                , rjc_jiacno                              -- 저널공통_지급계좌
                , rjc_ipbankcd                            -- 저널공통_입금은행코드
                , rjc_ipacno                              -- 저널공통_입금계좌번호
                , jnal_err_code                           -- 오류코드
                , rwc_sub_channel                         -- 웹공통_서브채널
                , rwc_mas_slv_gbn                         -- 마스터슬레이브구분
                , bio_cert_kbn                            -- 바이오인증구분
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , jnal_tran_date                          -- 거래일자
          , jnal_tran_time                          -- 거래시간
          , jnal_tran_sno                           -- 일련번호
          , jnal_ci_no                              -- ci번호
          , jnal_cusno                              -- 고객번호
          , jnal_state                              -- 전문상태
          , jnal_svc_code                           -- 서비스코드
          , jnal_media_code                         -- 매체코드
          , jnal_rqst_recv_gbn                      -- 요청메인수신구분
          , jnal_rqst_send_gbn                      -- 요청메인송신구분
          , jnal_resp_recv_gbn                      -- 응답메인수신구분
          , jnal_resp_send_gbn                      -- 응답메인송신구분
          , jnal_rqst_proc_dt                       -- 요청메인처리일자
          , jnal_rqst_proc_tm                       -- 요청메인처리시간
          , jnal_resp_proc_dt                       -- 응답메인처리일자
          , jnal_resp_proc_tm                       -- 응답메인처리시간
          , jnal_rqst_send_state                    -- 요청전송상태
          , jnal_resp_send_state                    -- 응답전송상태
          , rwc_upmu_kbn                            -- 웹공통_업무구분
          , rwc_svc_code                            -- 웹공통_서비스코드
          , rwc_sysgbn                              -- 웹공통_온라인시스템구분
          , rwc_web_date                            -- 웹공통_웹서버일자
          , rwc_web_time                            -- 웹공통_웹서버시간
          , rwc_web_domain                          -- 웹공통_도메인명
          , rwc_ipaddr                              -- 웹공통_ip주소
          , rwc_language                            -- 웹공통_언어구분
          , rwc_channel_gbn                         -- 웹공통_채널유형
          , rwc_ci_no                               -- 웹공통_ci번호
          , rwc_cifno                               -- 웹공통_고객번호
          , rwc_sec_chk                             -- 웹공통_보안매체체크
          , rwc_ichepswd_chk                        -- 웹공통_이체비번체크
          , rwc_yeyak_iche                          -- 웹공통_예약이체구분
          , rwc_ef_date                             -- 웹공통_저널생성일자
          , rwc_ef_time                             -- 웹공통_저널생성시간
          , rwc_ef_yoil                             -- 웹공통_저널생성요일
          , rwc_resultcd                            -- 웹공통_처리결과
          , rwc_endmark                             -- 웹공통_endmark
          , rwc_ef_serial                           -- 웹공통_원거래번호
          , rmc_err_code                            -- 전문헤더_에러코드
          , rjc_amt                                 -- 저널공통_거래금액
          , rjc_susuryo                             -- 저널공통_수수료
          , rjc_jiacno                              -- 저널공통_지급계좌
          , rjc_ipbankcd                            -- 저널공통_입금은행코드
          , rjc_ipacno                              -- 저널공통_입금계좌번호
          , jnal_err_code                           -- 오류코드
          , rwc_sub_channel                         -- 웹공통_서브채널
          , rwc_mas_slv_gbn                         -- 마스터슬레이브구분
          , bio_cert_kbn                            -- 바이오인증구분
      from w0_shb.dwx_rib_journal
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
