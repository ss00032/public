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
pgm_id = 'ALBD_DWS_SDT_YUDONG_SLV_TG'

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
s3_file_prefix = f'abd_dws_sdt_yudong_slv_/abd_dws_sdt_yudong_slv_{execution_kst}'

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
        delete from l0_shb.dws_sdt_yudong_slv
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dws_sdt_yudong_slv
                (
                  aws_ls_dt                               -- aws적재일시
                , acno                                    -- 계좌번호
                , trxno                                   -- 거래번호
                , trxdt                                   -- 거래일자
                , upmu_g                                  -- 업무구분
                , dep_trx_g                               -- 수신거래구분
                , ipji_g                                  -- 입지구분
                , tramt                                   -- 거래금액
                , csh_amt                                 -- 현금금액
                , dch_amt                                 -- 대체금액
                , trxaf_jan                               -- 거래후잔액
                , jkyo_c                                  -- 적요code
                , chan_u                                  -- 채널유형
                , trxbrno                                 -- 거래점번호
                , ynd_amt                                 -- 연동금액
                , ynd_ip_bnk_c                            -- 연동입금은행code
                , ynd_bnk_giro_g                          -- 연동은행지로번호
                , ynd_opp_ac_no                           -- 연동상대계좌번호
                , crt_can_g                               -- 정정취소구분
                , tbnk_trx_bnk_c                          -- 타행거래은행code
                , trx_oprt_dt                             -- 거래조작일자
                , trx_oprt_time                           -- 거래조작시각
                , trx_tbnkchk_c1                          -- 거래타점권code1
                , trx_tjum_amt1                           -- 거래타점금액1
                , trx_tbnkchk_c2                          -- 거래타점권code2
                , trx_tjum_amt2                           -- 거래타점금액2
                , trx_tbnkchk_c3                          -- 거래타점권code3
                , trx_tjum_amt3                           -- 거래타점금액3
                , ip_rqstr_nm                             -- 입금의뢰인명
                , ip_rqstr_nat_c                          -- 입금의뢰인국가code
                , ip_rqstr_silno_g                        -- 입금의뢰인실명번호구분
                , dw_lst_jukja_dt                         -- dw최종적재일자
                , orgn_trxno                              -- 원거래번호
                , cd_bal_trn                              -- 카드발급회차
                , tbnk_trx_brno                           -- 타행거래점번호
                , trx_cmmt_ctnt                           -- 적요내용
                , ip_rqstr_cid                            -- 입금요청인고객id
                , ynd_chk_amtty_g                         -- 연동수표권종구분
                , chk_ynd_stt_no                          -- 수표연동시작번호
                , chk_ynd_scnt                            -- 수표연동매수
                , chkbil_g                                -- 수표어음구분
                , chk_billno                              -- 수표어음번호
                , bd_trx_ser                              -- 별단거래일련번호
                , lst_chg_scr_id                          -- 최종변경화면id
                , lst_chg_brno                            -- 최종변경점번호
                , fil_2_cnt5                              -- filler2수5
                , gijang_excpt_dsyn                       -- 기장제외대상여부
                , ynd_ip_dep_main_nm                      -- 연동입금예금주명
                , cms_no                                  -- cms번호
                , busl_ys_wthd_rsn_c                      -- 회수사유코드
                , ynd_chk_amtty_g2                        -- 연동수표권종구분2
                , chk_ynd_scnt2                           -- 수표연동매수2
                , ynd_chk_amtty_g3                        -- 연동수표권종구분3
                , chk_ynd_scnt3                           -- 수표연동매수3
                , fil_100_ctnt3                           -- fil_100_ctnt3
                , fil_2_cnt6                              -- fil_2_cnt6
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , acno                                    -- 계좌번호
          , trxno                                   -- 거래번호
          , trxdt                                   -- 거래일자
          , upmu_g                                  -- 업무구분
          , dep_trx_g                               -- 수신거래구분
          , ipji_g                                  -- 입지구분
          , tramt                                   -- 거래금액
          , csh_amt                                 -- 현금금액
          , dch_amt                                 -- 대체금액
          , trxaf_jan                               -- 거래후잔액
          , jkyo_c                                  -- 적요code
          , chan_u                                  -- 채널유형
          , trxbrno                                 -- 거래점번호
          , ynd_amt                                 -- 연동금액
          , ynd_ip_bnk_c                            -- 연동입금은행code
          , ynd_bnk_giro_g                          -- 연동은행지로번호
          , ynd_opp_ac_no                           -- 연동상대계좌번호
          , crt_can_g                               -- 정정취소구분
          , tbnk_trx_bnk_c                          -- 타행거래은행code
          , trx_oprt_dt                             -- 거래조작일자
          , trx_oprt_time                           -- 거래조작시각
          , trx_tbnkchk_c1                          -- 거래타점권code1
          , trx_tjum_amt1                           -- 거래타점금액1
          , trx_tbnkchk_c2                          -- 거래타점권code2
          , trx_tjum_amt2                           -- 거래타점금액2
          , trx_tbnkchk_c3                          -- 거래타점권code3
          , trx_tjum_amt3                           -- 거래타점금액3
          , ip_rqstr_nm                             -- 입금의뢰인명
          , ip_rqstr_nat_c                          -- 입금의뢰인국가code
          , ip_rqstr_silno_g                        -- 입금의뢰인실명번호구분
          , dw_lst_jukja_dt                         -- dw최종적재일자
          , orgn_trxno                              -- 원거래번호
          , cd_bal_trn                              -- 카드발급회차
          , tbnk_trx_brno                           -- 타행거래점번호
          , trx_cmmt_ctnt                           -- 적요내용
          , ip_rqstr_cid                            -- 입금요청인고객id
          , ynd_chk_amtty_g                         -- 연동수표권종구분
          , chk_ynd_stt_no                          -- 수표연동시작번호
          , chk_ynd_scnt                            -- 수표연동매수
          , chkbil_g                                -- 수표어음구분
          , chk_billno                              -- 수표어음번호
          , bd_trx_ser                              -- 별단거래일련번호
          , lst_chg_scr_id                          -- 최종변경화면id
          , lst_chg_brno                            -- 최종변경점번호
          , fil_2_cnt5                              -- filler2수5
          , gijang_excpt_dsyn                       -- 기장제외대상여부
          , ynd_ip_dep_main_nm                      -- 연동입금예금주명
          , cms_no                                  -- cms번호
          , busl_ys_wthd_rsn_c                      -- 회수사유코드
          , ynd_chk_amtty_g2                        -- 연동수표권종구분2
          , chk_ynd_scnt2                           -- 수표연동매수2
          , ynd_chk_amtty_g3                        -- 연동수표권종구분3
          , chk_ynd_scnt3                           -- 수표연동매수3
          , fil_100_ctnt3                           -- fil_100_ctnt3
          , fil_2_cnt6                              -- fil_2_cnt6
      from w0_shb.dws_sdt_yudong_slv
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
