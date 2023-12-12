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
pgm_id = 'ILBD_DWS_SDA_MAIN_MAS_M_TG'

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
s3_file_prefix = f'ibd_dws_sda_main_mas_m_/ibd_dws_sda_main_mas_m_{execution_kst}'

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
        delete from l0_shb.dws_sda_main_mas_m
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dws_sda_main_mas_m
                (
                  aws_ls_dt                               -- aws적재일시
                , dw_bas_nyymm                            -- dw기준n년월
                , ci_no                                   -- ci번호
                , cusno                                   -- 고객번호
                , acno                                    -- 계좌번호
                , kwa_c                                   -- 과목code
                , acser                                   -- 계좌일련번호
                , now_dep_ac_s                            -- 현수신계좌상태
                , prdt_c                                  -- 상품code
                , act_c                                   -- 계정code
                , smok_c                                  -- 세목code
                , busmok_c                                -- 부세목code
                , mkdt                                    -- 신규일자
                , dudt                                    -- 만기일자
                , cus_trxdt                               -- 고객거래일
                , hji_dt                                  -- 해지일자
                , ac_jan                                  -- 계좌잔액
                , janjon_acnt                             -- 잔존좌수
                , silnm_conf_dt                           -- 실명확인일자
                , trx_chan_u                              -- 거래채널유형
                , cur_c                                   -- 통화code
                , invs_obj_c                              -- 투자목적code
                , wcur_invs_obj_c                         -- 원화투자목적code
                , wcur_birsdcja_g                         -- 원화비거주자구분
                , kfb_prdt_c                              -- 은행연합회상품code
                , taxud_prdt_k                            -- 세금우대상품종류
                , taxud_hdamt                             -- 세금우대한도금액
                , jibul_psb_jan                           -- 지불가능잔액
                , pldg_cret_tamt                          -- 질권설정총금액
                , ji_limt_amt                             -- 지급제한금액
                , japjwa_entr_amt                         -- 잡좌편입금액
                , e_bbk_mk_yn                             -- 전자통장신규여부
                , bbk_nus_g                               -- 통장미사용구분
                , bbk_char_c                              -- 통장성격code
                , lst_trxdt                               -- 최종거래일자
                , bbk_bkkp_nm                             -- 통장부기명
                , prdt_bkkp_nm                            -- 상품부기명
                , apchuk_mgr_apl_g                        -- 압축기장적용구분
                , cusgd_prn_dt                            -- 고객등급인자일자
                , rep_acno                                -- 대표계좌번호
                , ltacno                                  -- 평생계좌번호
                , jmud_yn                                 -- 전문직우대여부
                , bw_jkw_yn                               -- 법원직원여부
                , tcher_udae_yn                           -- 선생님우대여부
                , cd_mktg_dsyn                            -- 카드마케팅대상여부
                , feeud_pay_ac_yn                         -- 수수료우대급여계좌여부
                , amway_feejg_yn                          -- 암웨이수수료징구여부
                , police_rqst_sago_yn                     -- 경찰청의뢰사고여부
                , cc_oi_jistop_yn                         -- cc외지급정지여부
                , dj_trx_hji_yn                           -- 당좌거래해지여부
                , ip_jistop_yn                            -- 입금지급정지여부
                , crime_sago_yn                           -- 범죄사고여부
                , ingam_loss_yn                           -- 인감분실여부
                , jeonbumr_yn                             -- 전부명령여부
                , comm_nm_ac_yn                           -- 공동명의계좌여부
                , dep_dc_ac_yn                            -- 수신대출계좌여부
                , budae_svc_his_yn                        -- 부대서비스내역여부
                , linkac_dryn                             -- 연결계좌등록여부
                , taxud_cvt_his_yn                        -- 세금우대전환내역여부
                , japik_excpt_dryn                        -- 잡익제외등록여부
                , obnk_pldg_dr_yn                         -- 당행질권등록여부
                , torg_pldg_dr_yn                         -- 타기관질권등록여부
                , jongtong_dmb_jegong_yn                  -- 종통담보제공여부
                , inet_inqr_excpt_svc_yn                  -- 인터넷조회제외서비스여부
                , bkrp_sungo_hjae_dt                      -- 파산선고해제일자
                , psn_dbtr_revv_hjae_dt                   -- 개인채무자회생해제일자
                , cnsr_bkrp_hjae_dt                       -- 소비자파산해제일자
                , psn_wkout_hjae_dt                       -- 개인워크아웃해제일자
                , ip_ji_stop_hjae_dt                      -- 입금지급정지해제일자
                , ilbu_apryu_amt                          -- 일부압류금액
                , ilbu_apryu_cnt                          -- 일부압류건수
                , tamt_apryu_cnt                          -- 전액압류건수
                , ji_stop_cnt                             -- 지급정지건수
                , prcp_jistop_cnt                         -- 원금지급정지건수
                , bbk_loss_yn                             -- 통장분실여부
                , fil_5_no5                               -- filler5번호5(거래중지계좌)
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , dw_bas_nyymm                            -- dw기준n년월
          , ci_no                                   -- ci번호
          , cusno                                   -- 고객번호
          , acno                                    -- 계좌번호
          , kwa_c                                   -- 과목code
          , acser                                   -- 계좌일련번호
          , now_dep_ac_s                            -- 현수신계좌상태
          , prdt_c                                  -- 상품code
          , act_c                                   -- 계정code
          , smok_c                                  -- 세목code
          , busmok_c                                -- 부세목code
          , mkdt                                    -- 신규일자
          , dudt                                    -- 만기일자
          , cus_trxdt                               -- 고객거래일
          , hji_dt                                  -- 해지일자
          , ac_jan                                  -- 계좌잔액
          , janjon_acnt                             -- 잔존좌수
          , silnm_conf_dt                           -- 실명확인일자
          , trx_chan_u                              -- 거래채널유형
          , cur_c                                   -- 통화code
          , invs_obj_c                              -- 투자목적code
          , wcur_invs_obj_c                         -- 원화투자목적code
          , wcur_birsdcja_g                         -- 원화비거주자구분
          , kfb_prdt_c                              -- 은행연합회상품code
          , taxud_prdt_k                            -- 세금우대상품종류
          , taxud_hdamt                             -- 세금우대한도금액
          , jibul_psb_jan                           -- 지불가능잔액
          , pldg_cret_tamt                          -- 질권설정총금액
          , ji_limt_amt                             -- 지급제한금액
          , japjwa_entr_amt                         -- 잡좌편입금액
          , e_bbk_mk_yn                             -- 전자통장신규여부
          , bbk_nus_g                               -- 통장미사용구분
          , bbk_char_c                              -- 통장성격code
          , lst_trxdt                               -- 최종거래일자
          , bbk_bkkp_nm                             -- 통장부기명
          , prdt_bkkp_nm                            -- 상품부기명
          , apchuk_mgr_apl_g                        -- 압축기장적용구분
          , cusgd_prn_dt                            -- 고객등급인자일자
          , rep_acno                                -- 대표계좌번호
          , ltacno                                  -- 평생계좌번호
          , jmud_yn                                 -- 전문직우대여부
          , bw_jkw_yn                               -- 법원직원여부
          , tcher_udae_yn                           -- 선생님우대여부
          , cd_mktg_dsyn                            -- 카드마케팅대상여부
          , feeud_pay_ac_yn                         -- 수수료우대급여계좌여부
          , amway_feejg_yn                          -- 암웨이수수료징구여부
          , police_rqst_sago_yn                     -- 경찰청의뢰사고여부
          , cc_oi_jistop_yn                         -- cc외지급정지여부
          , dj_trx_hji_yn                           -- 당좌거래해지여부
          , ip_jistop_yn                            -- 입금지급정지여부
          , crime_sago_yn                           -- 범죄사고여부
          , ingam_loss_yn                           -- 인감분실여부
          , jeonbumr_yn                             -- 전부명령여부
          , comm_nm_ac_yn                           -- 공동명의계좌여부
          , dep_dc_ac_yn                            -- 수신대출계좌여부
          , budae_svc_his_yn                        -- 부대서비스내역여부
          , linkac_dryn                             -- 연결계좌등록여부
          , taxud_cvt_his_yn                        -- 세금우대전환내역여부
          , japik_excpt_dryn                        -- 잡익제외등록여부
          , obnk_pldg_dr_yn                         -- 당행질권등록여부
          , torg_pldg_dr_yn                         -- 타기관질권등록여부
          , jongtong_dmb_jegong_yn                  -- 종통담보제공여부
          , inet_inqr_excpt_svc_yn                  -- 인터넷조회제외서비스여부
          , bkrp_sungo_hjae_dt                      -- 파산선고해제일자
          , psn_dbtr_revv_hjae_dt                   -- 개인채무자회생해제일자
          , cnsr_bkrp_hjae_dt                       -- 소비자파산해제일자
          , psn_wkout_hjae_dt                       -- 개인워크아웃해제일자
          , ip_ji_stop_hjae_dt                      -- 입금지급정지해제일자
          , ilbu_apryu_amt                          -- 일부압류금액
          , ilbu_apryu_cnt                          -- 일부압류건수
          , tamt_apryu_cnt                          -- 전액압류건수
          , ji_stop_cnt                             -- 지급정지건수
          , prcp_jistop_cnt                         -- 원금지급정지건수
          , bbk_loss_yn                             -- 통장분실여부
          , fil_5_no5                               -- filler5번호5(거래중지계좌)
      from w0_shb.dws_sda_main_mas_m
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
