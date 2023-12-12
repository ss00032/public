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
pgm_id = 'ALBD_DWY_YDI_YEONCHE_MAS_M_TG'

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
s3_file_prefix = f'abd_dwy_ydi_yeonche_mas_m_/abd_dwy_ydi_yeonche_mas_m_{execution_kst}'

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
        delete from l0_shb.dwy_ydi_yeonche_mas_m
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwy_ydi_yeonche_mas_m
                (
                  aws_ls_dt                               -- aws적재일시
                , dw_bas_nyymm                            -- dw기준n년월
                , ci_no                                   -- ci번호
                , cusno                                   -- 고객번호
                , acno                                    -- 계좌번호
                , exeno                                   -- 실행번호
                , for_refno                               -- 외환레퍼런스번호
                , prdt_c                                  -- 상품code
                , plcy_c                                  -- 정책code
                , act_c                                   -- 계정code
                , smok_c                                  -- 세목code
                , busmok_c                                -- 부세목code
                , grpco_c                                 -- 그룹사code
                , job_gjdt                                -- 작업기준일자
                , act_g                                   -- 계정구분
                , grbrno                                  -- 관리점번호
                , junkl_g                                 -- 전결구분
                , hapd_c                                  -- 합동code
                , prcp_bojon_g                            -- 원금보존구분
                , prcp_bojon_rt                           -- 원금보존비율
                , cur_c                                   -- 통화code
                , dep_dmbdc_yn                            -- 예금담보대출여부
                , yakj_iyul                               -- 약정이율
                , apl_exrt                                -- 적용환율
                , sidt                                    -- 승인일자
                , aprvno                                  -- 승인번호
                , nxt_int_npdt                            -- 다음이자납입일자
                , hndo_ykjdt                              -- 한도약정일자
                , hndo_dudt                               -- 한도만기일자
                , dc_hdamt                                -- 대출한도금액
                , dc_jan                                  -- 대출잔액
                , hdov_sjdt                               -- 한도초과시작일자
                , giikss_dt                               -- 기한이익상실일자
                , prcdlay_gjdt                            -- 원금연체기준일자
                , wonri_yc_gjdt                           -- 원리금연체기준일자
                , yc_dcnt                                 -- 연체일수
                , yc_mcnt                                 -- 연체월수
                , yc_cnt                                  -- 연체횟수
                , prcdlay_dcnt                            -- 원금연체일수
                , prcdlay_mcnt                            -- 원금연체월수
                , yc_g                                    -- 연체구분
                , ycamt                                   -- 연체금액
                , ta_sel_org_g                            -- 대외매각기관구분
                , ta_sel_af_grbrno                        -- 대외매각후관리점번호
                , now_val_halin_offset_amt                -- 현재가치할인차감금액
                , kjdt                                    -- 결제일자
                , ac_s                                    -- 계좌상태
                , corp_bdaj_yn                            -- 기업채권조정여부
                , nxt_bhsh_dt                             -- 다음분할상환일자
                , dc_exe_dt                               -- 대출실행일자
                , mofc_mvo_dt                             -- 본부이관일자
                , mofc_mvo_brno                           -- 본부이관점번호
                , budo_yc_dc_g                            -- 부도연체대출구분
                , budo_yc_mofc_g                          -- 부도연체본부구분
                , budo_yc_ys_g                            -- 부도연체여신구분
                , shnd_mth                                -- 상환방법
                , shnd_dt                                 -- 상환일자
                , frdc_jan                                -- 외화대출잔액
                , frdc_hdamt                              -- 외화대출한도금액
                , prcdlay_bhsh_gjdt                       -- 원금연체분할상환기준일자
                , fund_yongdo_c                           -- 자금용도code
                , lst_yngi_dt                             -- 최종연기일자
                , lst_int_npdt                            -- 최종이자납입일자
                , bond_adj_g                              -- 채권조정구분
                , bond_adj_prgr_s                         -- 채권조정진행상태
                , bulrinf_yn                              -- 불량정보여부
                , cdln_auto_ynj_jeyn                      -- 카드론자동연장제외여부
                , ci_safeloan_ent_yn                      -- ci_safeloan가입여부
                , corp_scal_c                             -- 기업규모code
                , cus_scal_g                              -- 고객규모구분
                , iche_ac_jan                             -- 이체계좌잔액
                , iche_acno                               -- 이체계좌번호
                , jbdan_dc_mng_no                         -- 집단대출관리번호
                , job_yymm                                -- 작업년월
                , kwa_c                                   -- 과목code
                , misuija                                 -- 미수이자
                , rel_acno                                -- 관련계좌번호
                , reln_bf_jan                             -- 대환전잔액
                , reln_bf_yc_dcnt                         -- 대환전연체일수
                , reln_bf_ycamt                           -- 대환전연체금액
                , reln_yn                                 -- 대환여부
                , seonhuchwi_g                            -- 선후취구분_이수구분
                , sindst_b                                -- 표준산업분류
                , trxpsn_psnt_c                           -- 거래자인격code
                , unyong_g                                -- 운용구분
                , wkout_prgr_s                            -- workout진행상태
                , wkout_sincdt                            -- workout신청일자
                , ys_bond_bojun_c                         -- 여신채권보전code
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , dw_bas_nyymm                            -- dw기준n년월
          , ci_no                                   -- ci번호
          , cusno                                   -- 고객번호
          , acno                                    -- 계좌번호
          , exeno                                   -- 실행번호
          , for_refno                               -- 외환레퍼런스번호
          , prdt_c                                  -- 상품code
          , plcy_c                                  -- 정책code
          , act_c                                   -- 계정code
          , smok_c                                  -- 세목code
          , busmok_c                                -- 부세목code
          , grpco_c                                 -- 그룹사code
          , job_gjdt                                -- 작업기준일자
          , act_g                                   -- 계정구분
          , grbrno                                  -- 관리점번호
          , junkl_g                                 -- 전결구분
          , hapd_c                                  -- 합동code
          , prcp_bojon_g                            -- 원금보존구분
          , prcp_bojon_rt                           -- 원금보존비율
          , cur_c                                   -- 통화code
          , dep_dmbdc_yn                            -- 예금담보대출여부
          , yakj_iyul                               -- 약정이율
          , apl_exrt                                -- 적용환율
          , sidt                                    -- 승인일자
          , aprvno                                  -- 승인번호
          , nxt_int_npdt                            -- 다음이자납입일자
          , hndo_ykjdt                              -- 한도약정일자
          , hndo_dudt                               -- 한도만기일자
          , dc_hdamt                                -- 대출한도금액
          , dc_jan                                  -- 대출잔액
          , hdov_sjdt                               -- 한도초과시작일자
          , giikss_dt                               -- 기한이익상실일자
          , prcdlay_gjdt                            -- 원금연체기준일자
          , wonri_yc_gjdt                           -- 원리금연체기준일자
          , yc_dcnt                                 -- 연체일수
          , yc_mcnt                                 -- 연체월수
          , yc_cnt                                  -- 연체횟수
          , prcdlay_dcnt                            -- 원금연체일수
          , prcdlay_mcnt                            -- 원금연체월수
          , yc_g                                    -- 연체구분
          , ycamt                                   -- 연체금액
          , ta_sel_org_g                            -- 대외매각기관구분
          , ta_sel_af_grbrno                        -- 대외매각후관리점번호
          , now_val_halin_offset_amt                -- 현재가치할인차감금액
          , kjdt                                    -- 결제일자
          , ac_s                                    -- 계좌상태
          , corp_bdaj_yn                            -- 기업채권조정여부
          , nxt_bhsh_dt                             -- 다음분할상환일자
          , dc_exe_dt                               -- 대출실행일자
          , mofc_mvo_dt                             -- 본부이관일자
          , mofc_mvo_brno                           -- 본부이관점번호
          , budo_yc_dc_g                            -- 부도연체대출구분
          , budo_yc_mofc_g                          -- 부도연체본부구분
          , budo_yc_ys_g                            -- 부도연체여신구분
          , shnd_mth                                -- 상환방법
          , shnd_dt                                 -- 상환일자
          , frdc_jan                                -- 외화대출잔액
          , frdc_hdamt                              -- 외화대출한도금액
          , prcdlay_bhsh_gjdt                       -- 원금연체분할상환기준일자
          , fund_yongdo_c                           -- 자금용도code
          , lst_yngi_dt                             -- 최종연기일자
          , lst_int_npdt                            -- 최종이자납입일자
          , bond_adj_g                              -- 채권조정구분
          , bond_adj_prgr_s                         -- 채권조정진행상태
          , bulrinf_yn                              -- 불량정보여부
          , cdln_auto_ynj_jeyn                      -- 카드론자동연장제외여부
          , ci_safeloan_ent_yn                      -- ci_safeloan가입여부
          , corp_scal_c                             -- 기업규모code
          , cus_scal_g                              -- 고객규모구분
          , iche_ac_jan                             -- 이체계좌잔액
          , iche_acno                               -- 이체계좌번호
          , jbdan_dc_mng_no                         -- 집단대출관리번호
          , job_yymm                                -- 작업년월
          , kwa_c                                   -- 과목code
          , misuija                                 -- 미수이자
          , rel_acno                                -- 관련계좌번호
          , reln_bf_jan                             -- 대환전잔액
          , reln_bf_yc_dcnt                         -- 대환전연체일수
          , reln_bf_ycamt                           -- 대환전연체금액
          , reln_yn                                 -- 대환여부
          , seonhuchwi_g                            -- 선후취구분_이수구분
          , sindst_b                                -- 표준산업분류
          , trxpsn_psnt_c                           -- 거래자인격code
          , unyong_g                                -- 운용구분
          , wkout_prgr_s                            -- workout진행상태
          , wkout_sincdt                            -- workout신청일자
          , ys_bond_bojun_c                         -- 여신채권보전code
      from w0_shb.dwy_ydi_yeonche_mas_m
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
