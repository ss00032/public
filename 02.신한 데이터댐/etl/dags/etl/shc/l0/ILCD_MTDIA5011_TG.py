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
pgm_id = 'ILCD_MTDIA5011_TG'

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
s3_file_prefix = f'icd_mtdia5011_/icd_mtdia5011_{execution_kst}'

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
        delete from l0_shc.mtdia5011
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shc.mtdia5011
                (
                  aws_ls_dt                               -- aws적재일시
                , mct_n                                   -- 가맹점번호
                , ls_ld_dt                                -- 최종적재일시
                , rpk_sgdmd                               -- 대표자그룹md번호
                , mct_nm                                  -- 가맹점명
                , cam_mct_tf                              -- 캠페인가맹점tf
                , mct_mi_zcd                              -- 가맹점중분류코드
                , mkt_mct_zcd                             -- 마케팅가맹점분류코드
                , mkt_mct_bg_zcd                          -- 마케팅가맹점대분류코드
                , mkt_mct_mi_zcd                          -- 마케팅가맹점중분류코드
                , mkt_mct_sl_zcd                          -- 마케팅가맹점소분류코드
                , mct_nr_hcd                              -- 가맹점관리지점코드
                , mct_jdc_hcd                             -- 가맹점관할지점코드
                , nr_shb_hcd                              -- 관리신한은행지점코드
                , cu_ec_ry_cd                             -- 공동이용정산업종코드
                , psn_etk_tf                              -- 개인사업자tf
                , lat_mct_tf                              -- 신규가맹점tf
                , opa_mct_tf                              -- 가동가맹점tf
                , rlp_mct_tf                              -- 실질가맹점tf
                , me_mct_tf                               -- 해지가맹점tf
                , cco_mct_tf                              -- 당사가맹점tf
                , pg_mct_tf                               -- pg가맹점tf
                , ais_tf                                  -- 실사tf
                , mct_atv_tf                              -- 가맹점활동tf
                , mct_rav_tf                              -- 가맹점재활동tf
                , cvy_mct_tf                              -- 양도가맹점tf
                , pbp_bj_tf                               -- 대금지급대상tf
                , cu_mct_tf                               -- 공동이용가맹점tf
                , gnr_spm_tf                              -- 일반특약tf
                , elc_spm_tf                              -- 전자특약tf
                , jn_apv_ti_tf                            -- 가입승인전송tf
                , bad_mct_tf                              -- 불량가맹점tf
                , mkt_if_ge_tf                            -- 마케팅정보동의tf
                , ic_crd_ill_cf_tf                        -- ic카드설치확인tf
                , pos_sur_tf                              -- pos보안tf
                , dup_mct_tf                              -- 중복가맹점tf
                , cu_tf                                   -- 공동이용tf
                , pha_mct_tf                              -- 구매가맹점tf
                , mct_ccd                                 -- 가맹점구분코드
                , bsn_gcd                                 -- 영업등급코드
                , gss_ccd                                 -- 주유소구분코드
                , bg_cz_bsn_pd_cd                         -- 대분류영업상품코드
                , mcc_cd                                  -- mcc코드
                , mct_ig_bil_ccd                          -- 가맹점통합청구구분코드
                , mct_bsn_scd                             -- 가맹점영업상태코드
                , mct_zpn                                 -- 가맹점우편번호
                , mct_are_d                               -- 가맹점계약일자
                , mct_me_d                                -- 가맹점해지일자
                , mct_ais_d                               -- 가맹점실사일자
                , mct_ts_sp_rgd                           -- 가맹점거래정지등록일자
                , mct_rav_d                               -- 가맹점재활동일자
                , st_ac_mf_d                              -- 결제계좌변경일자
                , mct_ec_ga_ef_d                          -- 가맹점정산등급적용일자
                , mct_fe_rt_blk_cd                        -- 가맹점수수료율구간코드
                , mct_ry_cd                               -- 가맹점업종코드
                , mct_bse_fe_rt                           -- 가맹점기본수수료율
                , cco_mct_cre_crd_fe_rt                   -- 당사가맹점신용카드수수료율
                , cco_chc_fe_rt                           -- 당사체크카드수수료율
                , nns_mct_cre_crd_fe_rt                   -- nns가맹점신용카드수수료율
                , nns_mct_chc_fe_rt                       -- nns가맹점체크카드수수료율
                , ry_brd_hcd                              -- 업종브랜드지점코드
                , brd_hcd                                 -- 브랜드지점코드
                , mct_ec_gcd                              -- 가맹점정산등급코드
                , mct_kcd                                 -- 가맹점종류코드
                , mct_bls_stg_cd                          -- 가맹점요주의단계코드
                , crd_sv_n                                -- 카드서비스번호
                , bcd                                     -- 은행코드
                , mct_to_lm_giv_at                        -- 가맹점총한도부여금액
                , mct_ns_lm_giv_at                        -- 가맹점할부한도부여금액
                , mct_ars_lm_giv_at                       -- 가맹점ars한도부여금액
                , mct_kyn_lm_giv_at                       -- 가맹점키인한도부여금액
                , mct_hwr_spm_lm_giv_at                   -- 가맹점수기특약한도부여금액
                , mct_ts_pe_lm_giv_at                     -- 가맹점거래건별한도부여금액
                , mct_dlm_giv_at                          -- 가맹점일한도부여금액
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , mct_n                                   -- 가맹점번호
          , ls_ld_dt                                -- 최종적재일시
          , rpk_sgdmd                               -- 대표자그룹md번호
          , mct_nm                                  -- 가맹점명
          , cam_mct_tf                              -- 캠페인가맹점tf
          , mct_mi_zcd                              -- 가맹점중분류코드
          , mkt_mct_zcd                             -- 마케팅가맹점분류코드
          , mkt_mct_bg_zcd                          -- 마케팅가맹점대분류코드
          , mkt_mct_mi_zcd                          -- 마케팅가맹점중분류코드
          , mkt_mct_sl_zcd                          -- 마케팅가맹점소분류코드
          , mct_nr_hcd                              -- 가맹점관리지점코드
          , mct_jdc_hcd                             -- 가맹점관할지점코드
          , nr_shb_hcd                              -- 관리신한은행지점코드
          , cu_ec_ry_cd                             -- 공동이용정산업종코드
          , psn_etk_tf                              -- 개인사업자tf
          , lat_mct_tf                              -- 신규가맹점tf
          , opa_mct_tf                              -- 가동가맹점tf
          , rlp_mct_tf                              -- 실질가맹점tf
          , me_mct_tf                               -- 해지가맹점tf
          , cco_mct_tf                              -- 당사가맹점tf
          , pg_mct_tf                               -- pg가맹점tf
          , ais_tf                                  -- 실사tf
          , mct_atv_tf                              -- 가맹점활동tf
          , mct_rav_tf                              -- 가맹점재활동tf
          , cvy_mct_tf                              -- 양도가맹점tf
          , pbp_bj_tf                               -- 대금지급대상tf
          , cu_mct_tf                               -- 공동이용가맹점tf
          , gnr_spm_tf                              -- 일반특약tf
          , elc_spm_tf                              -- 전자특약tf
          , jn_apv_ti_tf                            -- 가입승인전송tf
          , bad_mct_tf                              -- 불량가맹점tf
          , mkt_if_ge_tf                            -- 마케팅정보동의tf
          , ic_crd_ill_cf_tf                        -- ic카드설치확인tf
          , pos_sur_tf                              -- pos보안tf
          , dup_mct_tf                              -- 중복가맹점tf
          , cu_tf                                   -- 공동이용tf
          , pha_mct_tf                              -- 구매가맹점tf
          , mct_ccd                                 -- 가맹점구분코드
          , bsn_gcd                                 -- 영업등급코드
          , gss_ccd                                 -- 주유소구분코드
          , bg_cz_bsn_pd_cd                         -- 대분류영업상품코드
          , mcc_cd                                  -- mcc코드
          , mct_ig_bil_ccd                          -- 가맹점통합청구구분코드
          , mct_bsn_scd                             -- 가맹점영업상태코드
          , mct_zpn                                 -- 가맹점우편번호
          , mct_are_d                               -- 가맹점계약일자
          , mct_me_d                                -- 가맹점해지일자
          , mct_ais_d                               -- 가맹점실사일자
          , mct_ts_sp_rgd                           -- 가맹점거래정지등록일자
          , mct_rav_d                               -- 가맹점재활동일자
          , st_ac_mf_d                              -- 결제계좌변경일자
          , mct_ec_ga_ef_d                          -- 가맹점정산등급적용일자
          , mct_fe_rt_blk_cd                        -- 가맹점수수료율구간코드
          , mct_ry_cd                               -- 가맹점업종코드
          , mct_bse_fe_rt                           -- 가맹점기본수수료율
          , cco_mct_cre_crd_fe_rt                   -- 당사가맹점신용카드수수료율
          , cco_chc_fe_rt                           -- 당사체크카드수수료율
          , nns_mct_cre_crd_fe_rt                   -- nns가맹점신용카드수수료율
          , nns_mct_chc_fe_rt                       -- nns가맹점체크카드수수료율
          , ry_brd_hcd                              -- 업종브랜드지점코드
          , brd_hcd                                 -- 브랜드지점코드
          , mct_ec_gcd                              -- 가맹점정산등급코드
          , mct_kcd                                 -- 가맹점종류코드
          , mct_bls_stg_cd                          -- 가맹점요주의단계코드
          , crd_sv_n                                -- 카드서비스번호
          , bcd                                     -- 은행코드
          , mct_to_lm_giv_at                        -- 가맹점총한도부여금액
          , mct_ns_lm_giv_at                        -- 가맹점할부한도부여금액
          , mct_ars_lm_giv_at                       -- 가맹점ars한도부여금액
          , mct_kyn_lm_giv_at                       -- 가맹점키인한도부여금액
          , mct_hwr_spm_lm_giv_at                   -- 가맹점수기특약한도부여금액
          , mct_ts_pe_lm_giv_at                     -- 가맹점거래건별한도부여금액
          , mct_dlm_giv_at                          -- 가맹점일한도부여금액
      from w0_shc.mtdia5011
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
