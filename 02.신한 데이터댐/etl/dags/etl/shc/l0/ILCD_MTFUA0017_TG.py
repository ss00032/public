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


__author__     = "이종호"
__copyright__  = "Copyright 2021, Shinhan Datadam"
__credits__    = ["이종호"]
__version__    = "1.0"
__maintainer__ = "이종호"
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
pgm_id = 'ILCD_MTFUA0017_TG'

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
s3_file_prefix = f'icd_mtfua0017_/icd_mtfua0017_{execution_kst}'

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
        delete from l0_shc.mtfua0017
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shc.mtfua0017
                (
                  aws_ls_dt                               -- aws적재일시
                , ta_ym                                   -- 기준년월
                , mct_n                                   -- 가맹점번호
                , ls_ld_dt                                -- 최종적재일시
                , mct_ry_cd                               -- 가맹점업종코드
                , mct_kcd                                 -- 가맹점종류코드
                , cam_mct_tf                              -- 캠페인가맹점tf
                , cl_ue_cus_cn                            -- 신판이용회원수
                , p_ue_cus_cn                             -- 일시불이용회원수
                , ns_ue_cus_cn                            -- 할부이용회원수
                , nn_ir_ns_ue_cus_cn                      -- 무이자할부이용회원수
                , pr_dc_ue_cus_cn                         -- 선할인이용회원수
                , vv_cl_ue_cus_cn                         -- 리볼빙신판이용회원수
                , chc_cl_ue_cus_cn                        -- 체크카드신판이용회원수
                , chc_cre_p_ue_cus_cn                     -- 체크카드신용일시불이용회원수
                , chc_p_ue_cus_cn                         -- 체크카드일시불이용회원수
                , psn_cl_ue_cus_cn                        -- 개인신판이용회원수
                , psn_p_ue_cus_cn                         -- 개인일시불이용회원수
                , psn_ns_ue_cus_cn                        -- 개인할부이용회원수
                , psn_nn_ir_ns_ue_cus_cn                  -- 개인무이자할부이용회원수
                , psn_pr_dc_ue_cus_cn                     -- 개인선할인이용회원수
                , bsi_cl_ue_cus_cn                        -- 비지니스신판이용회원수
                , bsi_p_ue_cus_cn                         -- 비지니스일시불이용회원수
                , bsi_vv_cl_ue_cus_cn                     -- 비지니스리볼빙신판이용회원수
                , bsi_ns_ue_cus_cn                        -- 비지니스할부이용회원수
                , psn_vv_cl_ue_cus_cn                     -- 개인리볼빙신판이용회원수
                , psn_chc_cl_ue_cus_cn                    -- 개인체크카드신판이용회원수
                , psn_chc_cre_p_ue_cus_cn                 -- 개인체크카드신용일시불이용회원수
                , psn_chc_p_ue_cus_cn                     -- 개인체크카드일시불이용회원수
                , crp_cl_ue_cus_cn                        -- 법인신판이용회원수
                , crp_p_ue_cus_cn                         -- 법인일시불이용회원수
                , crp_ns_ue_cus_cn                        -- 법인할부이용회원수
                , crp_chc_cl_ue_cus_cn                    -- 법인체크카드신판이용회원수
                , crp_chc_p_ue_cus_cn                     -- 법인체크카드일시불이용회원수
                , pnmc_cl_ue_cus_cn                       -- 개인명의법인신판이용회원수
                , pnmc_chc_cl_cus_cn                      -- 개인명의법인체크카드신판회원수
                , ue_ct                                   -- 이용건수
                , cl_ue_ct                                -- 신판이용건수
                , p_ue_ct                                 -- 일시불이용건수
                , ns_ue_ct                                -- 할부이용건수
                , nn_ir_ns_ue_ct                          -- 무이자할부이용건수
                , pr_dc_ue_ct                             -- 선할인이용건수
                , vv_cl_ue_ct                             -- 리볼빙신판이용건수
                , chc_cl_ue_ct                            -- 체크카드신판이용건수
                , chc_cre_p_ue_ct                         -- 체크카드신용일시불이용건수
                , chc_p_ue_ct                             -- 체크카드일시불이용건수
                , psn_cl_ue_ct                            -- 개인신판이용건수
                , psn_p_ue_ct                             -- 개인일시불이용건수
                , psn_ns_ue_ct                            -- 개인할부이용건수
                , psn_nn_ir_ns_ue_ct                      -- 개인무이자할부이용건수
                , psn_pr_dc_ue_ct                         -- 개인선할인이용건수
                , bsi_cl_ue_ct                            -- 비지니스신판이용건수
                , bsi_p_ue_ct                             -- 비지니스일시불이용건수
                , bsi_vv_cl_ue_ct                         -- 비지니스리볼빙신판이용건수
                , bsi_ns_ue_ct                            -- 비지니스할부이용건수
                , psn_vv_cl_ue_ct                         -- 개인리볼빙신판이용건수
                , psn_chc_cl_ue_ct                        -- 개인체크카드신판이용건수
                , psn_chc_cre_p_ue_ct                     -- 개인체크카드신용일시불이용건수
                , psn_chc_p_ue_ct                         -- 개인체크카드일시불이용건수
                , crp_cl_ue_ct                            -- 법인신판이용건수
                , crp_p_ue_ct                             -- 법인일시불이용건수
                , crp_ns_ue_ct                            -- 법인할부이용건수
                , crp_chc_cl_ue_ct                        -- 법인체크카드신판이용건수
                , crp_chc_p_ue_ct                         -- 법인체크카드일시불이용건수
                , pnmc_cl_ue_ct                           -- 개인명의법인신판이용건수
                , pnmc_chc_cl_ue_ct                       -- 개인명의법인체크카드신판이용건수
                , hga                                     -- 취급금액
                , cl_hga                                  -- 신판취급금액
                , p_hga                                   -- 일시불취급금액
                , ns_hga                                  -- 할부취급금액
                , nn_ir_ns_hga                            -- 무이자할부취급금액
                , pr_dc_hga                               -- 선할인취급금액
                , vv_cl_hga                               -- 리볼빙신판취급금액
                , chc_cl_hga                              -- 체크카드신판취급금액
                , chc_cre_p_hga                           -- 체크카드신용일시불취급금액
                , chc_p_hga                               -- 체크카드일시불취급금액
                , psn_cl_hga                              -- 개인신판취급금액
                , psn_p_hga                               -- 개인일시불취급금액
                , psn_ns_hga                              -- 개인할부취급금액
                , psn_nn_ir_ns_hga                        -- 개인무이자할부취급금액
                , psn_pr_dc_hga                           -- 개인선할인취급금액
                , bsi_cl_hga                              -- 비지니스신판취급금액
                , bsi_p_hga                               -- 비지니스일시불취급금액
                , bsi_vv_cl_hga                           -- 비지니스리볼빙신판취급금액
                , bsi_ns_hga                              -- 비지니스할부취급금액
                , psn_vv_cl_hga                           -- 개인리볼빙신판취급금액
                , psn_chc_cl_hga                          -- 개인체크카드신판취급금액
                , psn_chc_cre_p_hga                       -- 개인체크카드신용일시불취급금액
                , psn_chc_p_hga                           -- 개인체크카드일시불취급금액
                , crp_cl_hga                              -- 법인신판취급금액
                , crp_p_hga                               -- 법인일시불취급금액
                , crp_ns_hga                              -- 법인할부취급금액
                , crp_chc_cl_hga                          -- 법인체크카드신판취급금액
                , crp_chc_p_hga                           -- 법인체크카드일시불취급금액
                , pnmc_cl_hga                             -- 개인명의법인신판취급금액
                , pnmc_chc_cl_hga                         -- 개인명의법인체크카드신판취급금액
                , itl_aq_ct                               -- 국제매입건수
                , itl_aq_ppa                              -- 국제매입원금금액
                , ue_crd_cn                               -- 이용카드수
                , ue_cus_cn                               -- 이용회원수
                , ue_ttn_cd                               -- 이용패턴코드
                , cl_ue_ttn_cd                            -- 신판이용패턴코드
                , p_ue_ttn_cd                             -- 일시불이용패턴코드
                , ns_ue_ttn_cd                            -- 할부이용패턴코드
                , nn_ir_ns_ue_ttn_cd                      -- 무이자할부이용패턴코드
                , pr_dc_ue_ttn_cd                         -- 선할인이용패턴코드
                , vv_cl_ue_ttn_cd                         -- 리볼빙신판이용패턴코드
                , chc_cl_ue_ttn_cd                        -- 체크카드신판이용패턴코드
                , chc_cre_p_ue_ttn_cd                     -- 체크카드신용일시불이용패턴코드
                , chc_p_ue_ttn_cd                         -- 체크카드일시불이용패턴코드
                , psn_cl_ue_ttn_cd                        -- 개인신판이용패턴코드
                , psn_p_ue_ttn_cd                         -- 개인일시불이용패턴코드
                , psn_ns_ue_ttn_cd                        -- 개인할부이용패턴코드
                , psn_nn_ir_ns_ue_ttn_cd                  -- 개인무이자할부이용패턴코드
                , psn_pr_dc_ue_ttn_cd                     -- 개인선할인이용패턴코드
                , psn_vv_cl_ue_ttn_cd                     -- 개인리볼빙신판이용패턴코드
                , psn_chc_cl_ue_ttn_cd                    -- 개인체크카드신판이용패턴코드
                , psn_chc_cre_p_ue_ttn_cd                 -- 개인체크카드신용일시불이용패턴코드
                , psn_chc_p_ue_ttn_cd                     -- 개인체크카드일시불이용패턴코드
                , crp_cl_ue_ttn_cd                        -- 법인신판이용패턴코드
                , crp_p_ue_ttn_cd                         -- 법인일시불이용패턴코드
                , crp_ns_ue_ttn_cd                        -- 법인할부이용패턴코드
                , crp_chc_cl_ue_ttn_cd                    -- 법인체크카드신판이용패턴코드
                , crp_chc_p_ue_ttn_cd                     -- 법인체크카드일시불이용패턴코드
                , pnmc_cl_ue_ttn_cd                       -- 개인명의법인신판이용패턴코드
                , pnmc_chc_cl_ue_ttn_cd                   -- 개인명의법인체크카드신판이용패턴코드
                , mon_ue_ct                               -- 월요일이용건수
                , tue_ue_ct                               -- 화요일이용건수
                , wed_ue_ct                               -- 수요일이용건수
                , thu_ue_ct                               -- 목요일이용건수
                , fri_ue_ct                               -- 금요일이용건수
                , sat_ue_ct                               -- 토요일이용건수
                , sun_ue_ct                               -- 일요일이용건수
                , mrn_ue_ct                               -- 오전이용건수
                , lch_ue_ct                               -- 중식이용건수
                , aft_ue_ct                               -- 오후이용건수
                , nht_ue_ct                               -- 야간이용건수
                , hr5_hr11_blk_ue_ct                      -- 5시11시구간이용건수
                , hr12_hr13_blk_ue_ct                     -- 12시13시구간이용건수
                , hr14_hr17_blk_ue_ct                     -- 14시17시구간이용건수
                , hr18_hr22_blk_ue_ct                     -- 18시22시구간이용건수
                , hr23_hr4_blk_ue_ct                      -- 23시4시구간이용건수
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , ta_ym                                   -- 기준년월
          , mct_n                                   -- 가맹점번호
          , ls_ld_dt                                -- 최종적재일시
          , mct_ry_cd                               -- 가맹점업종코드
          , mct_kcd                                 -- 가맹점종류코드
          , cam_mct_tf                              -- 캠페인가맹점tf
          , cl_ue_cus_cn                            -- 신판이용회원수
          , p_ue_cus_cn                             -- 일시불이용회원수
          , ns_ue_cus_cn                            -- 할부이용회원수
          , nn_ir_ns_ue_cus_cn                      -- 무이자할부이용회원수
          , pr_dc_ue_cus_cn                         -- 선할인이용회원수
          , vv_cl_ue_cus_cn                         -- 리볼빙신판이용회원수
          , chc_cl_ue_cus_cn                        -- 체크카드신판이용회원수
          , chc_cre_p_ue_cus_cn                     -- 체크카드신용일시불이용회원수
          , chc_p_ue_cus_cn                         -- 체크카드일시불이용회원수
          , psn_cl_ue_cus_cn                        -- 개인신판이용회원수
          , psn_p_ue_cus_cn                         -- 개인일시불이용회원수
          , psn_ns_ue_cus_cn                        -- 개인할부이용회원수
          , psn_nn_ir_ns_ue_cus_cn                  -- 개인무이자할부이용회원수
          , psn_pr_dc_ue_cus_cn                     -- 개인선할인이용회원수
          , bsi_cl_ue_cus_cn                        -- 비지니스신판이용회원수
          , bsi_p_ue_cus_cn                         -- 비지니스일시불이용회원수
          , bsi_vv_cl_ue_cus_cn                     -- 비지니스리볼빙신판이용회원수
          , bsi_ns_ue_cus_cn                        -- 비지니스할부이용회원수
          , psn_vv_cl_ue_cus_cn                     -- 개인리볼빙신판이용회원수
          , psn_chc_cl_ue_cus_cn                    -- 개인체크카드신판이용회원수
          , psn_chc_cre_p_ue_cus_cn                 -- 개인체크카드신용일시불이용회원수
          , psn_chc_p_ue_cus_cn                     -- 개인체크카드일시불이용회원수
          , crp_cl_ue_cus_cn                        -- 법인신판이용회원수
          , crp_p_ue_cus_cn                         -- 법인일시불이용회원수
          , crp_ns_ue_cus_cn                        -- 법인할부이용회원수
          , crp_chc_cl_ue_cus_cn                    -- 법인체크카드신판이용회원수
          , crp_chc_p_ue_cus_cn                     -- 법인체크카드일시불이용회원수
          , pnmc_cl_ue_cus_cn                       -- 개인명의법인신판이용회원수
          , pnmc_chc_cl_cus_cn                      -- 개인명의법인체크카드신판회원수
          , ue_ct                                   -- 이용건수
          , cl_ue_ct                                -- 신판이용건수
          , p_ue_ct                                 -- 일시불이용건수
          , ns_ue_ct                                -- 할부이용건수
          , nn_ir_ns_ue_ct                          -- 무이자할부이용건수
          , pr_dc_ue_ct                             -- 선할인이용건수
          , vv_cl_ue_ct                             -- 리볼빙신판이용건수
          , chc_cl_ue_ct                            -- 체크카드신판이용건수
          , chc_cre_p_ue_ct                         -- 체크카드신용일시불이용건수
          , chc_p_ue_ct                             -- 체크카드일시불이용건수
          , psn_cl_ue_ct                            -- 개인신판이용건수
          , psn_p_ue_ct                             -- 개인일시불이용건수
          , psn_ns_ue_ct                            -- 개인할부이용건수
          , psn_nn_ir_ns_ue_ct                      -- 개인무이자할부이용건수
          , psn_pr_dc_ue_ct                         -- 개인선할인이용건수
          , bsi_cl_ue_ct                            -- 비지니스신판이용건수
          , bsi_p_ue_ct                             -- 비지니스일시불이용건수
          , bsi_vv_cl_ue_ct                         -- 비지니스리볼빙신판이용건수
          , bsi_ns_ue_ct                            -- 비지니스할부이용건수
          , psn_vv_cl_ue_ct                         -- 개인리볼빙신판이용건수
          , psn_chc_cl_ue_ct                        -- 개인체크카드신판이용건수
          , psn_chc_cre_p_ue_ct                     -- 개인체크카드신용일시불이용건수
          , psn_chc_p_ue_ct                         -- 개인체크카드일시불이용건수
          , crp_cl_ue_ct                            -- 법인신판이용건수
          , crp_p_ue_ct                             -- 법인일시불이용건수
          , crp_ns_ue_ct                            -- 법인할부이용건수
          , crp_chc_cl_ue_ct                        -- 법인체크카드신판이용건수
          , crp_chc_p_ue_ct                         -- 법인체크카드일시불이용건수
          , pnmc_cl_ue_ct                           -- 개인명의법인신판이용건수
          , pnmc_chc_cl_ue_ct                       -- 개인명의법인체크카드신판이용건수
          , hga                                     -- 취급금액
          , cl_hga                                  -- 신판취급금액
          , p_hga                                   -- 일시불취급금액
          , ns_hga                                  -- 할부취급금액
          , nn_ir_ns_hga                            -- 무이자할부취급금액
          , pr_dc_hga                               -- 선할인취급금액
          , vv_cl_hga                               -- 리볼빙신판취급금액
          , chc_cl_hga                              -- 체크카드신판취급금액
          , chc_cre_p_hga                           -- 체크카드신용일시불취급금액
          , chc_p_hga                               -- 체크카드일시불취급금액
          , psn_cl_hga                              -- 개인신판취급금액
          , psn_p_hga                               -- 개인일시불취급금액
          , psn_ns_hga                              -- 개인할부취급금액
          , psn_nn_ir_ns_hga                        -- 개인무이자할부취급금액
          , psn_pr_dc_hga                           -- 개인선할인취급금액
          , bsi_cl_hga                              -- 비지니스신판취급금액
          , bsi_p_hga                               -- 비지니스일시불취급금액
          , bsi_vv_cl_hga                           -- 비지니스리볼빙신판취급금액
          , bsi_ns_hga                              -- 비지니스할부취급금액
          , psn_vv_cl_hga                           -- 개인리볼빙신판취급금액
          , psn_chc_cl_hga                          -- 개인체크카드신판취급금액
          , psn_chc_cre_p_hga                       -- 개인체크카드신용일시불취급금액
          , psn_chc_p_hga                           -- 개인체크카드일시불취급금액
          , crp_cl_hga                              -- 법인신판취급금액
          , crp_p_hga                               -- 법인일시불취급금액
          , crp_ns_hga                              -- 법인할부취급금액
          , crp_chc_cl_hga                          -- 법인체크카드신판취급금액
          , crp_chc_p_hga                           -- 법인체크카드일시불취급금액
          , pnmc_cl_hga                             -- 개인명의법인신판취급금액
          , pnmc_chc_cl_hga                         -- 개인명의법인체크카드신판취급금액
          , itl_aq_ct                               -- 국제매입건수
          , itl_aq_ppa                              -- 국제매입원금금액
          , ue_crd_cn                               -- 이용카드수
          , ue_cus_cn                               -- 이용회원수
          , ue_ttn_cd                               -- 이용패턴코드
          , cl_ue_ttn_cd                            -- 신판이용패턴코드
          , p_ue_ttn_cd                             -- 일시불이용패턴코드
          , ns_ue_ttn_cd                            -- 할부이용패턴코드
          , nn_ir_ns_ue_ttn_cd                      -- 무이자할부이용패턴코드
          , pr_dc_ue_ttn_cd                         -- 선할인이용패턴코드
          , vv_cl_ue_ttn_cd                         -- 리볼빙신판이용패턴코드
          , chc_cl_ue_ttn_cd                        -- 체크카드신판이용패턴코드
          , chc_cre_p_ue_ttn_cd                     -- 체크카드신용일시불이용패턴코드
          , chc_p_ue_ttn_cd                         -- 체크카드일시불이용패턴코드
          , psn_cl_ue_ttn_cd                        -- 개인신판이용패턴코드
          , psn_p_ue_ttn_cd                         -- 개인일시불이용패턴코드
          , psn_ns_ue_ttn_cd                        -- 개인할부이용패턴코드
          , psn_nn_ir_ns_ue_ttn_cd                  -- 개인무이자할부이용패턴코드
          , psn_pr_dc_ue_ttn_cd                     -- 개인선할인이용패턴코드
          , psn_vv_cl_ue_ttn_cd                     -- 개인리볼빙신판이용패턴코드
          , psn_chc_cl_ue_ttn_cd                    -- 개인체크카드신판이용패턴코드
          , psn_chc_cre_p_ue_ttn_cd                 -- 개인체크카드신용일시불이용패턴코드
          , psn_chc_p_ue_ttn_cd                     -- 개인체크카드일시불이용패턴코드
          , crp_cl_ue_ttn_cd                        -- 법인신판이용패턴코드
          , crp_p_ue_ttn_cd                         -- 법인일시불이용패턴코드
          , crp_ns_ue_ttn_cd                        -- 법인할부이용패턴코드
          , crp_chc_cl_ue_ttn_cd                    -- 법인체크카드신판이용패턴코드
          , crp_chc_p_ue_ttn_cd                     -- 법인체크카드일시불이용패턴코드
          , pnmc_cl_ue_ttn_cd                       -- 개인명의법인신판이용패턴코드
          , pnmc_chc_cl_ue_ttn_cd                   -- 개인명의법인체크카드신판이용패턴코드
          , mon_ue_ct                               -- 월요일이용건수
          , tue_ue_ct                               -- 화요일이용건수
          , wed_ue_ct                               -- 수요일이용건수
          , thu_ue_ct                               -- 목요일이용건수
          , fri_ue_ct                               -- 금요일이용건수
          , sat_ue_ct                               -- 토요일이용건수
          , sun_ue_ct                               -- 일요일이용건수
          , mrn_ue_ct                               -- 오전이용건수
          , lch_ue_ct                               -- 중식이용건수
          , aft_ue_ct                               -- 오후이용건수
          , nht_ue_ct                               -- 야간이용건수
          , hr5_hr11_blk_ue_ct                      -- 5시11시구간이용건수
          , hr12_hr13_blk_ue_ct                     -- 12시13시구간이용건수
          , hr14_hr17_blk_ue_ct                     -- 14시17시구간이용건수
          , hr18_hr22_blk_ue_ct                     -- 18시22시구간이용건수
          , hr23_hr4_blk_ue_ct                      -- 23시4시구간이용건수
      from w0_shc.mtfua0017
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
