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
pgm_id = 'ILBD_DWY_YER_LONSIMAS_TG'

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
s3_file_prefix = f'ibd_dwy_yer_lonsimas_/ibd_dwy_yer_lonsimas_{execution_kst}'

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
        delete from l0_shb.dwy_yer_lonsimas
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwy_yer_lonsimas
                (
                  aws_ls_dt                               -- aws적재일시
                , acno                                    -- 계좌번호
                , exeno                                   -- 실행번호
                , kwa_c                                   -- 과목code
                , acser                                   -- 계좌일련번호
                , sunnp_dcnt                              -- 선납일수
                , exe_amt                                 -- 실행금액
                , exe_dudt                                -- 실행만기일자
                , exe_jan                                 -- 실행잔액
                , wonrish_tot_trn                         -- 원리금상환총회차
                , wonrish_trn                             -- 원리금상환회차
                , lst_int_ip_dt                           -- 최종이자수입일자
                , salln_yc_yuye_yn                        -- 판매론연체유예여부
                , gu_exeno                                -- 구실행번호
                , tec_jipyak_smco_yn                      -- 기술집약중소기업여부
                , corp_scal_c                             -- 기업규모code
                , supay_ac_yn                             -- 대지급계좌여부
                , mediln_rtrt_yn                          -- 메디칼론재취급여부
                , misuija                                 -- 미수이자
                , vntr_corp_yn                            -- 벤처기업여부
                , bojun_npdt                              -- 보전납입일자
                , sgak_amt                                -- 상각금액
                , sgbond_rsv_amt                          -- 상각채권준비금액
                , sgbond_rsvamt_mvi_amt                   -- 상각채권준비금전입금액
                , sungp_prm_amt                           -- 선급보험료금액
                , exe_dt                                  -- 실행일자
                , apl_iyul                                -- 적용이율
                , chaip_efct_rflx_yn                      -- 차입효과반영여부
                , tamthd_siljk_excpt_dryn                 -- 총액한도실적제외등록여부
                , tamthd_siljk_entr_yn                    -- 총액한도실적편입여부
                , lst_trxdt                               -- 최종거래일자
                , fund_chgyn                              -- 펀드변경여부
                , fund_cdt                                -- 펀드변경일자
                , fund_pramt                              -- 펀드원금금액
                , fund_int                                -- 펀드이자
                , real_cnsr_bizno                         -- 실수요자사업자번호
                , yc_cnt                                  -- 연체횟수
                , frnt_buy_feamt                          -- 창구매입수수료금액
                , bond_exchg_drokj_yn                     -- 채권교체등록중여부
                , ys_ac_s                                 -- 여신계좌상태
                , irt_g                                   -- 금리구분
                , ysirt_c                                 -- 여신금리code
                , irt_term_c                              -- 금리기간code
                , bf_ys_ac_s                              -- 전여신계좌상태
                , irt_cycl_term                           -- 금리주기기간
                , cdln_ta_bnk_iche_yn                     -- 카드론타은행이체여부
                , gu_smok_c_ctnt                          -- 구세목code내용
                , gu_mng_bnk_c                            -- 구관리은행code
                , bok_rpt_dt                              -- 한국은행보고일자
                , misu_int_spbond_entr_yn                 -- 미수이자특수채권편입여부
                , prcp_cmplt_dt                           -- 원금완제일자
                , iche_yn                                 -- 이체여부
                , iche_wthd_ynd_hjae_yn                   -- 이체회수연동해제여부
                , c2bj_gihan_dt                           -- 이차보전기한일자
                , fil_18_amt1                             -- fil_18_amt1
                , fil_18_amt2                             -- fil_18_amt2
                , fil_18_amt3                             -- fil_18_amt3
                , fil_10_ctnt2                            -- filler10내용2[연체기준일]
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , acno                                    -- 계좌번호
          , exeno                                   -- 실행번호
          , kwa_c                                   -- 과목code
          , acser                                   -- 계좌일련번호
          , sunnp_dcnt                              -- 선납일수
          , exe_amt                                 -- 실행금액
          , exe_dudt                                -- 실행만기일자
          , exe_jan                                 -- 실행잔액
          , wonrish_tot_trn                         -- 원리금상환총회차
          , wonrish_trn                             -- 원리금상환회차
          , lst_int_ip_dt                           -- 최종이자수입일자
          , salln_yc_yuye_yn                        -- 판매론연체유예여부
          , gu_exeno                                -- 구실행번호
          , tec_jipyak_smco_yn                      -- 기술집약중소기업여부
          , corp_scal_c                             -- 기업규모code
          , supay_ac_yn                             -- 대지급계좌여부
          , mediln_rtrt_yn                          -- 메디칼론재취급여부
          , misuija                                 -- 미수이자
          , vntr_corp_yn                            -- 벤처기업여부
          , bojun_npdt                              -- 보전납입일자
          , sgak_amt                                -- 상각금액
          , sgbond_rsv_amt                          -- 상각채권준비금액
          , sgbond_rsvamt_mvi_amt                   -- 상각채권준비금전입금액
          , sungp_prm_amt                           -- 선급보험료금액
          , exe_dt                                  -- 실행일자
          , apl_iyul                                -- 적용이율
          , chaip_efct_rflx_yn                      -- 차입효과반영여부
          , tamthd_siljk_excpt_dryn                 -- 총액한도실적제외등록여부
          , tamthd_siljk_entr_yn                    -- 총액한도실적편입여부
          , lst_trxdt                               -- 최종거래일자
          , fund_chgyn                              -- 펀드변경여부
          , fund_cdt                                -- 펀드변경일자
          , fund_pramt                              -- 펀드원금금액
          , fund_int                                -- 펀드이자
          , real_cnsr_bizno                         -- 실수요자사업자번호
          , yc_cnt                                  -- 연체횟수
          , frnt_buy_feamt                          -- 창구매입수수료금액
          , bond_exchg_drokj_yn                     -- 채권교체등록중여부
          , ys_ac_s                                 -- 여신계좌상태
          , irt_g                                   -- 금리구분
          , ysirt_c                                 -- 여신금리code
          , irt_term_c                              -- 금리기간code
          , bf_ys_ac_s                              -- 전여신계좌상태
          , irt_cycl_term                           -- 금리주기기간
          , cdln_ta_bnk_iche_yn                     -- 카드론타은행이체여부
          , gu_smok_c_ctnt                          -- 구세목code내용
          , gu_mng_bnk_c                            -- 구관리은행code
          , bok_rpt_dt                              -- 한국은행보고일자
          , misu_int_spbond_entr_yn                 -- 미수이자특수채권편입여부
          , prcp_cmplt_dt                           -- 원금완제일자
          , iche_yn                                 -- 이체여부
          , iche_wthd_ynd_hjae_yn                   -- 이체회수연동해제여부
          , c2bj_gihan_dt                           -- 이차보전기한일자
          , fil_18_amt1                             -- fil_18_amt1
          , fil_18_amt2                             -- fil_18_amt2
          , fil_18_amt3                             -- fil_18_amt3
          , fil_10_ctnt2                            -- filler10내용2[연체기준일]
      from w0_shb.dwy_yer_lonsimas
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
