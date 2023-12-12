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
pgm_id = 'ILBD_DWS_SDA_GOJEONG_MAS_TG'

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
s3_file_prefix = f'ibd_dws_sda_gojeong_mas_/ibd_dws_sda_gojeong_mas_{execution_kst}'

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
        delete from l0_shb.dws_sda_gojeong_mas
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dws_sda_gojeong_mas
                (
                  aws_ls_dt                               -- aws적재일시
                , ci_no                                   -- ci번호
                , cusno                                   -- 고객번호
                , acno                                    -- 계좌번호
                , kwa_c                                   -- 과목code
                , acser                                   -- 계좌일련번호
                , ac_fst_mkdt                             -- 계좌최초신규일자
                , trx_resv_dt                             -- 거래예약일자
                , resv_exe_dt                             -- 예약실행일자
                , invs_prcp_ac_jan                        -- 투자원금계좌잔액
                , bncr_prcp_uprc                          -- 수익증권원금단가
                , napip_tamt                              -- 납입누계금액
                , napip_cnt                               -- 납입횟수
                , ji_cnt                                  -- 지급횟수
                , ilbu_hji_cnt                            -- 일부해지건수
                , iyn_dudt                                -- 이연만기일자
                , hjej_dt                                 -- 해지의제일자
                , bosu_lst_jidt                           -- 보수최종지급일자
                , pens_ji_sjdt                            -- 연금지급시작일자
                , qtyr_hndo_mng_dt                        -- 분기한도관리일자
                , liyul_dsdt_af_int_amt                   -- 최종이율결정일후이자금액
                , ji_int_samt                             -- 지급이자합계금액
                , ksaf_jan                                -- 결산후잔액
                , sts_contmv_mkdt                         -- 신탁계약이전신규일자
                , pens_ji_tot_amt                         -- 연금지급총금액
                , csh_bdang_um                            -- 현금배당유무
                , pass_term_yakiyul_jkyn                  -- 경과기간별약정이율적용여부
                , invs_desc_hovr_yn                       -- 투자설명교부여부
                , bill_keep_s                             -- 어음보관상태
                , cy_fst_acno                             -- 청약최초계좌번호
                , varamt_jongtong_acno                    -- 변액종통계좌번호
                , team_cont_saupjuche_no                  -- 단체계약사업체번호
                , bjinqrsvc_yn                            -- 빗장조회서비스여부
                , pens_ji_sinc_yn                         -- 연금지급신청여부
                , autoich_yn                              -- 자동이체여부
                , next_prdt_c                             -- 차기상품code
                , napip_prcp_samt                         -- 납입원금합계금액
                , oprc_prcp_samt                          -- 원가원금합계금액
                , cont_g                                  -- 계약구분
                , ac_cont_s                               -- 계좌계약상태
                , cont_mkdt                               -- 계약신규일자
                , cont_dudt                               -- 계약만기일자
                , dep_conttm_g                            -- 수신계약기간구분
                , conttm_mcnt                             -- 계약기간월수
                , cont_dcnt                               -- 계약기간일수
                , trn1_jkrip_amt                          -- 1회차적립금액
                , trn1_jiamt                              -- 1회차지급금액
                , m_ji_pramt                              -- 월지급원금금액
                , mk_fst_amt                              -- 신규최초금액
                , ji_cycl_c                               -- 지급주기code
                , ji_cycl                                 -- 지급주기
                , ip_cycl_c                               -- 입금주기code
                , ip_cycl                                 -- 입금주기
                , udiyul_ji_g                             -- 우대이율지급구분
                , irt_turn_cycl                           -- 금리회전주기
                , irt_turn_cycl_c                         -- 금리회전주기code
                , siyn                                    -- 승인여부
                , nosign_mk_yn                            -- 무기명신규여부
                , fprc_amt                                -- 액면금액
                , halin_revn_amt                          -- 할인매출금액
                , invs_prdt_trx_mth_g                     -- 투자상품거래방식구분
                , int_ji_mth                              -- 이자지급방법
                , cont_drdt                               -- 계약등록일자
                , cont_endt                               -- 계약종료일자
                , bas_iyul                                -- 기본이율
                , addiyul                                 -- 가산이율
                , bonus_iyul                              -- 보너스이율
                , secr_add_iyul                           -- 증권가산이율
                , cd_add_iyul                             -- 카드가산이율
                , mrkt_in_iyul                            -- 시장내부이율
                , fnd_ar_adj_iyul                         -- 자금부조정이율
                , plcy_adj_iyul                           -- 정책조정이율
                , saupb_adj_iyul                          -- 사업부조정이율
                , sdd_prmm_iyul                           -- 유동성프리미엄이율
                , lst_cdt                                 -- 최종변경일자
                , lst_chg_brno                            -- 최종변경점번호
                , fund_wcur_pramt                         -- 펀드원화원금금액
                , fil_2_no2                               -- 금리변동통지서비스신청구분
                , fil_yn1                                 -- 소득공제신청여부
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , ci_no                                   -- ci번호
          , cusno                                   -- 고객번호
          , acno                                    -- 계좌번호
          , kwa_c                                   -- 과목code
          , acser                                   -- 계좌일련번호
          , ac_fst_mkdt                             -- 계좌최초신규일자
          , trx_resv_dt                             -- 거래예약일자
          , resv_exe_dt                             -- 예약실행일자
          , invs_prcp_ac_jan                        -- 투자원금계좌잔액
          , bncr_prcp_uprc                          -- 수익증권원금단가
          , napip_tamt                              -- 납입누계금액
          , napip_cnt                               -- 납입횟수
          , ji_cnt                                  -- 지급횟수
          , ilbu_hji_cnt                            -- 일부해지건수
          , iyn_dudt                                -- 이연만기일자
          , hjej_dt                                 -- 해지의제일자
          , bosu_lst_jidt                           -- 보수최종지급일자
          , pens_ji_sjdt                            -- 연금지급시작일자
          , qtyr_hndo_mng_dt                        -- 분기한도관리일자
          , liyul_dsdt_af_int_amt                   -- 최종이율결정일후이자금액
          , ji_int_samt                             -- 지급이자합계금액
          , ksaf_jan                                -- 결산후잔액
          , sts_contmv_mkdt                         -- 신탁계약이전신규일자
          , pens_ji_tot_amt                         -- 연금지급총금액
          , csh_bdang_um                            -- 현금배당유무
          , pass_term_yakiyul_jkyn                  -- 경과기간별약정이율적용여부
          , invs_desc_hovr_yn                       -- 투자설명교부여부
          , bill_keep_s                             -- 어음보관상태
          , cy_fst_acno                             -- 청약최초계좌번호
          , varamt_jongtong_acno                    -- 변액종통계좌번호
          , team_cont_saupjuche_no                  -- 단체계약사업체번호
          , bjinqrsvc_yn                            -- 빗장조회서비스여부
          , pens_ji_sinc_yn                         -- 연금지급신청여부
          , autoich_yn                              -- 자동이체여부
          , next_prdt_c                             -- 차기상품code
          , napip_prcp_samt                         -- 납입원금합계금액
          , oprc_prcp_samt                          -- 원가원금합계금액
          , cont_g                                  -- 계약구분
          , ac_cont_s                               -- 계좌계약상태
          , cont_mkdt                               -- 계약신규일자
          , cont_dudt                               -- 계약만기일자
          , dep_conttm_g                            -- 수신계약기간구분
          , conttm_mcnt                             -- 계약기간월수
          , cont_dcnt                               -- 계약기간일수
          , trn1_jkrip_amt                          -- 1회차적립금액
          , trn1_jiamt                              -- 1회차지급금액
          , m_ji_pramt                              -- 월지급원금금액
          , mk_fst_amt                              -- 신규최초금액
          , ji_cycl_c                               -- 지급주기code
          , ji_cycl                                 -- 지급주기
          , ip_cycl_c                               -- 입금주기code
          , ip_cycl                                 -- 입금주기
          , udiyul_ji_g                             -- 우대이율지급구분
          , irt_turn_cycl                           -- 금리회전주기
          , irt_turn_cycl_c                         -- 금리회전주기code
          , siyn                                    -- 승인여부
          , nosign_mk_yn                            -- 무기명신규여부
          , fprc_amt                                -- 액면금액
          , halin_revn_amt                          -- 할인매출금액
          , invs_prdt_trx_mth_g                     -- 투자상품거래방식구분
          , int_ji_mth                              -- 이자지급방법
          , cont_drdt                               -- 계약등록일자
          , cont_endt                               -- 계약종료일자
          , bas_iyul                                -- 기본이율
          , addiyul                                 -- 가산이율
          , bonus_iyul                              -- 보너스이율
          , secr_add_iyul                           -- 증권가산이율
          , cd_add_iyul                             -- 카드가산이율
          , mrkt_in_iyul                            -- 시장내부이율
          , fnd_ar_adj_iyul                         -- 자금부조정이율
          , plcy_adj_iyul                           -- 정책조정이율
          , saupb_adj_iyul                          -- 사업부조정이율
          , sdd_prmm_iyul                           -- 유동성프리미엄이율
          , lst_cdt                                 -- 최종변경일자
          , lst_chg_brno                            -- 최종변경점번호
          , fund_wcur_pramt                         -- 펀드원화원금금액
          , fil_2_no2                               -- 금리변동통지서비스신청구분
          , fil_yn1                                 -- 소득공제신청여부
      from w0_shb.dws_sda_gojeong_mas
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
