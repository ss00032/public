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


__author__     = "이일주"
__copyright__  = "Copyright 2021, Shinhan Datadam"
__credits__    = ["이일주"]
__version__    = "1.0"
__maintainer__ = "이일주"
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
pgm_id = 'ILLD_DA_CONMON_CLS_TG'

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
s3_file_prefix = f'ild_da_conmon_cls_/ild_da_conmon_cls_{execution_kst}'

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
        delete from l0_shl.da_conmon_cls
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shl.da_conmon_cls
                (
                  aws_ls_dt                               -- aws적재일시
                , clos_ym                                 -- 마감년월
                , inon_no                                 -- 보험계약번호
                , ins_sbsn_good_smcl_cd                   -- 보험영업상품소분류코드
                , chnn_asrt_stg1_cd                       -- 채널분류1단계코드
                , chnn_asrt_stg2_cd                       -- 채널분류2단계코드
                , chnn_asrt_stg3_cd                       -- 채널분류3단계코드
                , chnn_asrt_stg4_cd                       -- 채널분류4단계코드
                , won_cin_fe                              -- 원화환산보험료
                , indv_asct_sc_cd                         -- 개인단체계약구분코드
                , ftp_fe_pam_dt                           -- 초회보험료납입일시
                , ha_n64                                  -- 해시_64
                , mnpr_inty_cd                            -- 주계약보종코드
                , mnpr_con_ymd                            -- 주계약계약일자
                , mnpr_eprt_ymd                           -- 주계약만기일자
                , mnpr_inft_tc_sc_cd                      -- 주계약제1보험기간구분코드
                , mnpr_inft_tc                            -- 주계약제1보험기간
                , mnpr_pmpe_tc                            -- 주계약납입기간
                , mnpr_pam_cycl_cd                        -- 주계약납입주기코드
                , mnpr_ent_am                             -- 주계약가입금액
                , mnpr_inp_fe                             -- 주계약보험료
                , mnpr_frst_inp_fe                        -- 주계약최초보험료
                , etnc_ymd                                -- 소멸일자
                , rei_ymd                                 -- 부활일자
                , ctst_cd                                 -- 계약상태코드
                , con_dtpt_stat_cd                        -- 계약상세상태코드
                , con_dtpt_stat_chg_dt                    -- 계약상세상태변경일시
                , trns_dy_sc_cd                           -- 이체일구분코드
                , sum_inp_fe                              -- 합계보험료
                , mtpa_str_sum_inp_fe                     -- 월납기준합계보험료
                , lst_fur_ymd                             -- 최종지급일자
                , apai_cmlt_inp_fe                        -- 기납입누계보험료
                , adpa_cmlt_inp_fe                        -- 추가납입누계보험료
                , lspa_ym                                 -- 최종납입년월
                , lspa_ordr                               -- 최종납입회차
                , lspa_ymd                                -- 최종납입일자
                , ann_swt_ymd                             -- 연금전환일자
                , frst_con_ymd                            -- 최초계약일자
                , mone_curr_cd                            -- 화폐통화코드
                , wc_ftp_fe                               -- 원화환산초회보험료
                , wc_apai_cmlt_inp_fe                     -- 원화환산기납입누계보험료
                , wc_adpa_cmlt_inp_fe                     -- 원화환산추가납입누계보험료
                , wc_sum_inp_fe                           -- 원화환산합계보험료
                , ech_rt_str_ymd                          -- 환율기준일자
                , wc_mnpr_ent_am                          -- 원화환산주계약가입금액
                , wc_mnpr_inp_fe                          -- 원화환산주계약보험료
                , wc_mnpr_frst_inp_fe                     -- 원화환산주계약최초보험료
                , won_fxng_pam_pet_am                     -- 원화고정납입신청금액
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , clos_ym                                 -- 마감년월
          , inon_no                                 -- 보험계약번호
          , ins_sbsn_good_smcl_cd                   -- 보험영업상품소분류코드
          , chnn_asrt_stg1_cd                       -- 채널분류1단계코드
          , chnn_asrt_stg2_cd                       -- 채널분류2단계코드
          , chnn_asrt_stg3_cd                       -- 채널분류3단계코드
          , chnn_asrt_stg4_cd                       -- 채널분류4단계코드
          , won_cin_fe                              -- 원화환산보험료
          , indv_asct_sc_cd                         -- 개인단체계약구분코드
          , ftp_fe_pam_dt                           -- 초회보험료납입일시
          , ha_n64                                  -- 해시_64
          , mnpr_inty_cd                            -- 주계약보종코드
          , mnpr_con_ymd                            -- 주계약계약일자
          , mnpr_eprt_ymd                           -- 주계약만기일자
          , mnpr_inft_tc_sc_cd                      -- 주계약제1보험기간구분코드
          , mnpr_inft_tc                            -- 주계약제1보험기간
          , mnpr_pmpe_tc                            -- 주계약납입기간
          , mnpr_pam_cycl_cd                        -- 주계약납입주기코드
          , mnpr_ent_am                             -- 주계약가입금액
          , mnpr_inp_fe                             -- 주계약보험료
          , mnpr_frst_inp_fe                        -- 주계약최초보험료
          , etnc_ymd                                -- 소멸일자
          , rei_ymd                                 -- 부활일자
          , ctst_cd                                 -- 계약상태코드
          , con_dtpt_stat_cd                        -- 계약상세상태코드
          , con_dtpt_stat_chg_dt                    -- 계약상세상태변경일시
          , trns_dy_sc_cd                           -- 이체일구분코드
          , sum_inp_fe                              -- 합계보험료
          , mtpa_str_sum_inp_fe                     -- 월납기준합계보험료
          , lst_fur_ymd                             -- 최종지급일자
          , apai_cmlt_inp_fe                        -- 기납입누계보험료
          , adpa_cmlt_inp_fe                        -- 추가납입누계보험료
          , lspa_ym                                 -- 최종납입년월
          , lspa_ordr                               -- 최종납입회차
          , lspa_ymd                                -- 최종납입일자
          , ann_swt_ymd                             -- 연금전환일자
          , frst_con_ymd                            -- 최초계약일자
          , mone_curr_cd                            -- 화폐통화코드
          , wc_ftp_fe                               -- 원화환산초회보험료
          , wc_apai_cmlt_inp_fe                     -- 원화환산기납입누계보험료
          , wc_adpa_cmlt_inp_fe                     -- 원화환산추가납입누계보험료
          , wc_sum_inp_fe                           -- 원화환산합계보험료
          , ech_rt_str_ymd                          -- 환율기준일자
          , wc_mnpr_ent_am                          -- 원화환산주계약가입금액
          , wc_mnpr_inp_fe                          -- 원화환산주계약보험료
          , wc_mnpr_frst_inp_fe                     -- 원화환산주계약최초보험료
          , won_fxng_pam_pet_am                     -- 원화고정납입신청금액
      from w0_shl.da_conmon_cls
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
