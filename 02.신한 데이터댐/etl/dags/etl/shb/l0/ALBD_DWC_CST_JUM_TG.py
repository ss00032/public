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
pgm_id = 'ALBD_DWC_CST_JUM_TG'

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
s3_file_prefix = f'abd_dwc_cst_jum_/abd_dwc_cst_jum_{execution_kst}'

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
        delete from l0_shb.dwc_cst_jum
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dwc_cst_jum
                (
                  aws_ls_dt                               -- aws적재일시
                , grpco_c                                 -- 그룹사code
                , brno                                    -- 점번호
                , br_s                                    -- 점상태
                , girono                                  -- 지로번호
                , forg_for_c                              -- 금융기관외환code
                , fbrno                                   -- 외환점번호
                , hng_br_swd_nm                           -- 한글점약어명
                , hng_br_nm                               -- 한글점명
                , bbk_prn_6_br_nm                         -- 통장인자6점명
                , bbk_prn_8_br_nm                         -- 통장인자8점명
                , eng_br_nm                               -- 영문점명
                , ci_no                                   -- ci번호
                , cusno                                   -- 고객번호
                , bizno                                   -- 사업자번호
                , br_grp_g                                -- 점그룹구분
                , saupb_g                                 -- 사업부구분
                , br_mng_g                                -- 점관리구분
                , br_k                                    -- 점종류
                , trm_psb_trx_g                           -- 단말가능거래구분
                , trm_psb_hday_g                          -- 단말가능휴일구분
                , area_c                                  -- 지역code
                , clrhs_c                                 -- 교환소code
                , dj_bojnamt_gpji_c                       -- 당좌보증금급지code
                , txmofc_c                                -- 세무서code
                , bok_gukgo_girono                        -- 한국은행국고지로번호
                , bok_chaip_girono                        -- 한국은행차입지로번호
                , ag_mo_brno                              -- 출장소모점번호
                , gukgo_mo_brno                           -- 국고모점번호
                , bill_ech_mo_brno                        -- 어음교환모점번호
                , ji_rsvamt_mo_brno                       -- 지급준비금모점번호
                , chaip_mo_brno                           -- 차입모점번호
                , suinji_mo_brno                          -- 수입인지모점번호
                , rcstamp_mo_brno                         -- 수입증지모점번호
                , bpr_mo_brno                             -- bpr모점번호
                , gukgo_brno                              -- 국고수납점번호
                , hanjibung_mo_brno                       -- 한지붕모점번호
                , hanjibung_psn_cus_brno                  -- 한지붕개인고객점번호
                , hanjibung_pb_cus_brno                   -- 한지붕pb고객점번호
                , hanjibung_corp_cus_brno                 -- 한지붕기업고객점번호
                , hanjibung_jonghap_finc_brno             -- 한지붕종합금융점번호
                , hanjibung_spcl_cus_brno                 -- 한지붕특수고객점번호
                , unty_brno                               -- 통합점번호
                , new_gu_bnk_g                            -- 신구은행구분
                , new_gu_brno                             -- 신구점번호
                , bs_jonjae_yn                            -- bs존재여부
                , for_ct_yn                               -- 외환center여부
                , inet_exchg_yn                           -- internet환전여부
                , oknet_exchg_rcp_br_yn                   -- oknet환전수령점여부
                , for_iju_ct_unyung_yn                    -- 외환이주center운영여부
                , exchghs_unyung_yn                       -- 환전소운영여부
                , spcl_cur_exchg_gnyn                     -- 특수통화환전가능여부
                , postno                                  -- 우편번호
                , hng_all_adr                             -- 한글전체주소
                , eng_all_adr                             -- 영문전체주소
                , brmngr_nm                               -- 지점장명
                , tel1no                                  -- 전화지역번호
                , tel2no                                  -- 전화국번호
                , tel3no                                  -- 전화통신일련번호
                , saupb_in_calc_gun                       -- 사업부내계수군
                , saupb_in_grp_g                          -- 사업부내그룹구분
                , opnbr_dt                                -- 개점일자
                , mgbr_dt                                 -- 폐점일자
                , bk_bill_bpr_ihaeng_dt                   -- 보관어음bpr이행일자
                , brno1                                   -- 커뮤니티 모점번호
                , brno2                                   -- 점번호2
                , brno3                                   -- 점번호3
                , brno4                                   -- 점번호4
                , brno5                                   -- 점번호5
                , drdt                                    -- 등록일자
                , dr_brno                                 -- 등록점번호
                , cdt                                     -- 변경일자
                , chg_brno                                -- 변경점번호
                , dw_lst_jukja_dt                         -- dw최종적재일자
                , fil_3_cnt1                              -- 커뮤니티 그룹번호
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , grpco_c                                 -- 그룹사code
          , brno                                    -- 점번호
          , br_s                                    -- 점상태
          , girono                                  -- 지로번호
          , forg_for_c                              -- 금융기관외환code
          , fbrno                                   -- 외환점번호
          , hng_br_swd_nm                           -- 한글점약어명
          , hng_br_nm                               -- 한글점명
          , bbk_prn_6_br_nm                         -- 통장인자6점명
          , bbk_prn_8_br_nm                         -- 통장인자8점명
          , eng_br_nm                               -- 영문점명
          , ci_no                                   -- ci번호
          , cusno                                   -- 고객번호
          , bizno                                   -- 사업자번호
          , br_grp_g                                -- 점그룹구분
          , saupb_g                                 -- 사업부구분
          , br_mng_g                                -- 점관리구분
          , br_k                                    -- 점종류
          , trm_psb_trx_g                           -- 단말가능거래구분
          , trm_psb_hday_g                          -- 단말가능휴일구분
          , area_c                                  -- 지역code
          , clrhs_c                                 -- 교환소code
          , dj_bojnamt_gpji_c                       -- 당좌보증금급지code
          , txmofc_c                                -- 세무서code
          , bok_gukgo_girono                        -- 한국은행국고지로번호
          , bok_chaip_girono                        -- 한국은행차입지로번호
          , ag_mo_brno                              -- 출장소모점번호
          , gukgo_mo_brno                           -- 국고모점번호
          , bill_ech_mo_brno                        -- 어음교환모점번호
          , ji_rsvamt_mo_brno                       -- 지급준비금모점번호
          , chaip_mo_brno                           -- 차입모점번호
          , suinji_mo_brno                          -- 수입인지모점번호
          , rcstamp_mo_brno                         -- 수입증지모점번호
          , bpr_mo_brno                             -- bpr모점번호
          , gukgo_brno                              -- 국고수납점번호
          , hanjibung_mo_brno                       -- 한지붕모점번호
          , hanjibung_psn_cus_brno                  -- 한지붕개인고객점번호
          , hanjibung_pb_cus_brno                   -- 한지붕pb고객점번호
          , hanjibung_corp_cus_brno                 -- 한지붕기업고객점번호
          , hanjibung_jonghap_finc_brno             -- 한지붕종합금융점번호
          , hanjibung_spcl_cus_brno                 -- 한지붕특수고객점번호
          , unty_brno                               -- 통합점번호
          , new_gu_bnk_g                            -- 신구은행구분
          , new_gu_brno                             -- 신구점번호
          , bs_jonjae_yn                            -- bs존재여부
          , for_ct_yn                               -- 외환center여부
          , inet_exchg_yn                           -- internet환전여부
          , oknet_exchg_rcp_br_yn                   -- oknet환전수령점여부
          , for_iju_ct_unyung_yn                    -- 외환이주center운영여부
          , exchghs_unyung_yn                       -- 환전소운영여부
          , spcl_cur_exchg_gnyn                     -- 특수통화환전가능여부
          , postno                                  -- 우편번호
          , hng_all_adr                             -- 한글전체주소
          , eng_all_adr                             -- 영문전체주소
          , brmngr_nm                               -- 지점장명
          , tel1no                                  -- 전화지역번호
          , tel2no                                  -- 전화국번호
          , tel3no                                  -- 전화통신일련번호
          , saupb_in_calc_gun                       -- 사업부내계수군
          , saupb_in_grp_g                          -- 사업부내그룹구분
          , opnbr_dt                                -- 개점일자
          , mgbr_dt                                 -- 폐점일자
          , bk_bill_bpr_ihaeng_dt                   -- 보관어음bpr이행일자
          , brno1                                   -- 커뮤니티 모점번호
          , brno2                                   -- 점번호2
          , brno3                                   -- 점번호3
          , brno4                                   -- 점번호4
          , brno5                                   -- 점번호5
          , drdt                                    -- 등록일자
          , dr_brno                                 -- 등록점번호
          , cdt                                     -- 변경일자
          , chg_brno                                -- 변경점번호
          , dw_lst_jukja_dt                         -- dw최종적재일자
          , fil_3_cnt1                              -- 커뮤니티 그룹번호
      from w0_shb.dwc_cst_jum
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
