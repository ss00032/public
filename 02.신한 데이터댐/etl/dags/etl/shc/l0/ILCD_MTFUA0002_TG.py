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
pgm_id = 'ILCD_MTFUA0002_TG'

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
s3_file_prefix = f'icd_mtfua0002_/icd_mtfua0002_{execution_kst}'

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
        delete from l0_shc.mtfua0002
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shc.mtfua0002
                (
                  aws_ls_dt                               -- aws적재일시
                , ta_ym                                   -- 기준년월
                , crd_rpl_n                               -- 카드대체번호
                , ls_ld_dt                                -- 최종적재일시
                , sgdmd                                   -- 그룹md번호
                , sgdmd_dfr_n                             -- 고객구별그룹md번호
                , cus_ccd                                 -- 회원구분코드
                , o_fml_ccd                               -- 본인가족구분코드
                , cry_k_sgdmd                             -- 소지자그룹md번호
                , o_crd_tf                                -- 본인카드tf
                , fml_crd_tf                              -- 가족카드tf
                , chc_tf                                  -- 체크카드tf
                , bcc_tf                                  -- bc카드tf
                , bsi_crd_tf                              -- 비지니스카드tf
                , fnc_crd_tf                              -- 오토금융카드tf
                , lat_crd_tf                              -- 신규카드tf
                , crd_su_tcd                              -- 카드상태유형코드
                , crd_tcd                                 -- 카드유형코드
                , cct2_crd_pd_n                           -- 개념2카드상품번호
                , crd_pd_n                                -- 카드상품번호
                , scs_ccd                                 -- 탈회구분코드
                , iss_elp_ms_cn                           -- 발급경과개월수
                , hga                                     -- 취급금액
                , cl_hga                                  -- 신판취급금액
                , cl_vs_hga                               -- 신판해외취급금액
                , cre_cl_hga                              -- 신용신판취급금액
                , p_hga                                   -- 일시불취급금액
                , p_vs_hga                                -- 일시불해외취급금액
                , vv_cl_hga                               -- 리볼빙신판취급금액
                , vv_cl_vs_hga                            -- 리볼빙신판해외취급금액
                , vv_hga1                                 -- 리볼빙취급금액1
                , vv_vs_hga1                              -- 리볼빙해외취급금액1
                , vv_hga2                                 -- 리볼빙취급금액2
                , ns_hga                                  -- 할부취급금액
                , ns_vs_hga                               -- 할부해외취급금액
                , nn_ir_ns_hga                            -- 무이자할부취급금액
                , cv_hga                                  -- 현금서비스취급금액
                , cv_vs_hga                               -- 현금서비스해외취급금액
                , vv_cv_hga                               -- 리볼빙현금서비스취급금액
                , vv_cv_vs_hga                            -- 리볼빙현금서비스해외취급금액
                , vv_mns_cv_hga                           -- 리볼빙마이너스현금서비스취급금액
                , vv_mns_cv_vs_hga                        -- 리볼빙마이너스현금서비스해외취급금액
                , spr_st_hga                              -- 분할결제취급금액
                , chc_hga                                 -- 체크카드취급금액
                , chc_vs_hga                              -- 체크카드해외취급금액
                , chc_cre_p_hga                           -- 체크카드신용일시불취급금액
                , chc_cl_vs_hga                           -- 체크카드신판해외취급금액
                , chc_cv_hga                              -- 체크카드현금서비스취급금액
                , chc_cv_vs_hga                           -- 체크카드현금서비스해외취급금액
                , ue_ct                                   -- 이용건수
                , cl_ue_ct                                -- 신판이용건수
                , cl_vs_ue_ct                             -- 신판해외이용건수
                , cre_cl_ue_ct                            -- 신용신판이용건수
                , p_ue_ct                                 -- 일시불이용건수
                , p_vs_ue_ct                              -- 일시불해외이용건수
                , vv_cl_ue_ct                             -- 리볼빙신판이용건수
                , vv_cl_vs_ue_ct                          -- 리볼빙신판해외이용건수
                , vv_ue_ct1                               -- 리볼빙이용건수1
                , vv_vs_ue_ct1                            -- 리볼빙해외이용건수1
                , vv_ue_ct2                               -- 리볼빙이용건수2
                , ns_ue_ct                                -- 할부이용건수
                , ns_vs_ue_ct                             -- 할부해외이용건수
                , nn_ir_ns_ue_ct                          -- 무이자할부이용건수
                , cv_ue_ct                                -- 현금서비스이용건수
                , cv_vs_ue_ct                             -- 현금서비스해외이용건수
                , vv_cv_ue_ct                             -- 리볼빙현금서비스이용건수
                , vv_cv_vs_ue_ct                          -- 리볼빙현금서비스해외이용건수
                , vv_mns_cv_ue_ct                         -- 리볼빙마이너스현금서비스이용건수
                , vv_mns_cv_vs_ue_ct                      -- 리볼빙마이너스현금서비스해외이용건수
                , spr_st_ue_ct                            -- 분할결제이용건수
                , chc_ue_ct                               -- 체크카드이용건수
                , chc_vs_ue_ct                            -- 체크카드해외이용건수
                , chc_cre_p_ue_ct                         -- 체크카드신용일시불이용건수
                , chc_cl_vs_ue_ct                         -- 체크카드신판해외이용건수
                , chc_cv_ue_ct                            -- 체크카드현금서비스이용건수
                , chc_cv_vs_ue_ct                         -- 체크카드현금서비스해외이용건수
                , ue_tf                                   -- 이용tf
                , cl_ue_tf                                -- 신판이용tf
                , cl_vs_ue_tf                             -- 신판해외이용tf
                , cre_cl_ue_tf                            -- 신용신판이용tf
                , p_ue_tf                                 -- 일시불이용tf
                , p_vs_ue_tf                              -- 일시불해외이용tf
                , vv_cl_ue_tf                             -- 리볼빙신판이용tf
                , vv_cl_vs_ue_tf                          -- 리볼빙신판해외이용tf
                , vv_ue_tf1                               -- 리볼빙이용tf1
                , vv_vs_ue_tf1                            -- 리볼빙해외이용tf1
                , vv_ue_tf2                               -- 리볼빙이용tf2
                , ns_ue_tf                                -- 할부이용tf
                , ns_vs_ue_tf                             -- 할부해외이용tf
                , nn_ir_ns_ue_tf                          -- 무이자할부이용tf
                , cv_ue_tf                                -- 현금서비스이용tf
                , cv_vs_ue_tf                             -- 현금서비스해외이용tf
                , vv_cv_ue_tf                             -- 리볼빙현금서비스이용tf
                , vv_cv_vs_ue_tf                          -- 리볼빙현금서비스해외이용tf
                , vv_mns_cv_ue_tf                         -- 리볼빙마이너스현금서비스이용tf
                , vv_mns_cv_vs_ue_tf                      -- 리볼빙마이너스현금서비스해외이용tf
                , spr_st_ue_tf                            -- 분할결제이용tf
                , chc_ue_tf                               -- 체크카드이용tf
                , chc_vs_ue_tf                            -- 체크카드해외이용tf
                , chc_cre_p_ue_tf                         -- 체크카드신용일시불이용tf
                , chc_cl_vs_ue_tf                         -- 체크카드신판해외이용tf
                , chc_cv_ue_tf                            -- 체크카드현금서비스이용tf
                , chc_cv_vs_ue_tf                         -- 체크카드현금서비스해외이용tf
                , ue_ttn_cd                               -- 이용패턴코드
                , cex_ue_ttn_cd                           -- 복합이용패턴코드
                , cl_ue_ttn_cd                            -- 신판이용패턴코드
                , cl_vs_ue_ttn_cd                         -- 신판해외이용패턴코드
                , cre_cl_ue_ttn_cd                        -- 신용신판이용패턴코드
                , p_ue_ttn_cd                             -- 일시불이용패턴코드
                , p_vs_ue_ttn_cd                          -- 일시불해외이용패턴코드
                , vv_cl_ue_ttn_cd                         -- 리볼빙신판이용패턴코드
                , vv_cl_vs_ue_ttn_cd                      -- 리볼빙신판해외이용패턴코드
                , vv_ue_ttn_cd1                           -- 리볼빙이용패턴코드1
                , vv_cl_vs_ue_ttn_cd1                     -- 리볼빙신판해외이용패턴코드1
                , vv_ue_ttn_cd2                           -- 리볼빙이용패턴코드2
                , ns_ue_ttn_cd                            -- 할부이용패턴코드
                , ns_vs_ue_ttn_cd                         -- 할부해외이용패턴코드
                , nn_ir_ns_ue_ttn_cd                      -- 무이자할부이용패턴코드
                , cv_ue_ttn_cd                            -- 현금서비스이용패턴코드
                , cv_vs_ue_ttn_cd                         -- 현금서비스해외이용패턴코드
                , vv_cv_ue_ttn_cd                         -- 리볼빙현금서비스이용패턴코드
                , vv_cv_vs_ue_ttn_cd                      -- 리볼빙현금서비스해외이용패턴코드
                , vv_mns_cv_ue_ttn_cd                     -- 리볼빙마이너스현금서비스이용패턴코드
                , vv_mns_cv_vs_ue_ttn_cd                  -- 리볼빙마이너스현금서비스해외이용패턴코드
                , spr_st_ue_ttn_cd                        -- 분할결제이용패턴코드
                , chc_ue_ttn_cd                           -- 체크카드이용패턴코드
                , chc_vs_ue_ttn_cd                        -- 체크카드해외이용패턴코드
                , chc_cre_p_ue_ttn_cd                     -- 체크카드신용일시불이용패턴코드
                , chc_cl_vs_ue_ttn_cd                     -- 체크카드신판해외이용패턴코드
                , chc_cv_ue_ttn_cd                        -- 체크카드현금서비스이용패턴코드
                , chc_cv_vs_ue_ttn_cd                     -- 체크카드현금서비스해외이용패턴코드
                , der_trf_chc_hga                         -- 후불교통체크카드취급금액
                , der_trf_chc_xl_hga                      -- 후불교통체크카드제외취급금액
                , der_trf_chc_ue_ct                       -- 후불교통체크카드이용건수
                , der_trf_chc_xl_ue_ct                    -- 후불교통체크카드제외이용건수
                , der_trf_chc_ue_tf                       -- 후불교통체크카드이용tf
                , der_trf_chc_xl_ue_tf                    -- 후불교통체크카드제외이용tf
                , der_trf_chc_ue_ttn_cd                   -- 후불교통체크카드이용패턴코드
                , der_trf_chc_xl_ue_ttn_cd                -- 후불교통체크카드제외이용패턴코드
                , crd_acc_rpl_n                           -- 카드계정대체번호
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , ta_ym                                   -- 기준년월
          , crd_rpl_n                               -- 카드대체번호
          , ls_ld_dt                                -- 최종적재일시
          , sgdmd                                   -- 그룹md번호
          , sgdmd_dfr_n                             -- 고객구별그룹md번호
          , cus_ccd                                 -- 회원구분코드
          , o_fml_ccd                               -- 본인가족구분코드
          , cry_k_sgdmd                             -- 소지자그룹md번호
          , o_crd_tf                                -- 본인카드tf
          , fml_crd_tf                              -- 가족카드tf
          , chc_tf                                  -- 체크카드tf
          , bcc_tf                                  -- bc카드tf
          , bsi_crd_tf                              -- 비지니스카드tf
          , fnc_crd_tf                              -- 오토금융카드tf
          , lat_crd_tf                              -- 신규카드tf
          , crd_su_tcd                              -- 카드상태유형코드
          , crd_tcd                                 -- 카드유형코드
          , cct2_crd_pd_n                           -- 개념2카드상품번호
          , crd_pd_n                                -- 카드상품번호
          , scs_ccd                                 -- 탈회구분코드
          , iss_elp_ms_cn                           -- 발급경과개월수
          , hga                                     -- 취급금액
          , cl_hga                                  -- 신판취급금액
          , cl_vs_hga                               -- 신판해외취급금액
          , cre_cl_hga                              -- 신용신판취급금액
          , p_hga                                   -- 일시불취급금액
          , p_vs_hga                                -- 일시불해외취급금액
          , vv_cl_hga                               -- 리볼빙신판취급금액
          , vv_cl_vs_hga                            -- 리볼빙신판해외취급금액
          , vv_hga1                                 -- 리볼빙취급금액1
          , vv_vs_hga1                              -- 리볼빙해외취급금액1
          , vv_hga2                                 -- 리볼빙취급금액2
          , ns_hga                                  -- 할부취급금액
          , ns_vs_hga                               -- 할부해외취급금액
          , nn_ir_ns_hga                            -- 무이자할부취급금액
          , cv_hga                                  -- 현금서비스취급금액
          , cv_vs_hga                               -- 현금서비스해외취급금액
          , vv_cv_hga                               -- 리볼빙현금서비스취급금액
          , vv_cv_vs_hga                            -- 리볼빙현금서비스해외취급금액
          , vv_mns_cv_hga                           -- 리볼빙마이너스현금서비스취급금액
          , vv_mns_cv_vs_hga                        -- 리볼빙마이너스현금서비스해외취급금액
          , spr_st_hga                              -- 분할결제취급금액
          , chc_hga                                 -- 체크카드취급금액
          , chc_vs_hga                              -- 체크카드해외취급금액
          , chc_cre_p_hga                           -- 체크카드신용일시불취급금액
          , chc_cl_vs_hga                           -- 체크카드신판해외취급금액
          , chc_cv_hga                              -- 체크카드현금서비스취급금액
          , chc_cv_vs_hga                           -- 체크카드현금서비스해외취급금액
          , ue_ct                                   -- 이용건수
          , cl_ue_ct                                -- 신판이용건수
          , cl_vs_ue_ct                             -- 신판해외이용건수
          , cre_cl_ue_ct                            -- 신용신판이용건수
          , p_ue_ct                                 -- 일시불이용건수
          , p_vs_ue_ct                              -- 일시불해외이용건수
          , vv_cl_ue_ct                             -- 리볼빙신판이용건수
          , vv_cl_vs_ue_ct                          -- 리볼빙신판해외이용건수
          , vv_ue_ct1                               -- 리볼빙이용건수1
          , vv_vs_ue_ct1                            -- 리볼빙해외이용건수1
          , vv_ue_ct2                               -- 리볼빙이용건수2
          , ns_ue_ct                                -- 할부이용건수
          , ns_vs_ue_ct                             -- 할부해외이용건수
          , nn_ir_ns_ue_ct                          -- 무이자할부이용건수
          , cv_ue_ct                                -- 현금서비스이용건수
          , cv_vs_ue_ct                             -- 현금서비스해외이용건수
          , vv_cv_ue_ct                             -- 리볼빙현금서비스이용건수
          , vv_cv_vs_ue_ct                          -- 리볼빙현금서비스해외이용건수
          , vv_mns_cv_ue_ct                         -- 리볼빙마이너스현금서비스이용건수
          , vv_mns_cv_vs_ue_ct                      -- 리볼빙마이너스현금서비스해외이용건수
          , spr_st_ue_ct                            -- 분할결제이용건수
          , chc_ue_ct                               -- 체크카드이용건수
          , chc_vs_ue_ct                            -- 체크카드해외이용건수
          , chc_cre_p_ue_ct                         -- 체크카드신용일시불이용건수
          , chc_cl_vs_ue_ct                         -- 체크카드신판해외이용건수
          , chc_cv_ue_ct                            -- 체크카드현금서비스이용건수
          , chc_cv_vs_ue_ct                         -- 체크카드현금서비스해외이용건수
          , ue_tf                                   -- 이용tf
          , cl_ue_tf                                -- 신판이용tf
          , cl_vs_ue_tf                             -- 신판해외이용tf
          , cre_cl_ue_tf                            -- 신용신판이용tf
          , p_ue_tf                                 -- 일시불이용tf
          , p_vs_ue_tf                              -- 일시불해외이용tf
          , vv_cl_ue_tf                             -- 리볼빙신판이용tf
          , vv_cl_vs_ue_tf                          -- 리볼빙신판해외이용tf
          , vv_ue_tf1                               -- 리볼빙이용tf1
          , vv_vs_ue_tf1                            -- 리볼빙해외이용tf1
          , vv_ue_tf2                               -- 리볼빙이용tf2
          , ns_ue_tf                                -- 할부이용tf
          , ns_vs_ue_tf                             -- 할부해외이용tf
          , nn_ir_ns_ue_tf                          -- 무이자할부이용tf
          , cv_ue_tf                                -- 현금서비스이용tf
          , cv_vs_ue_tf                             -- 현금서비스해외이용tf
          , vv_cv_ue_tf                             -- 리볼빙현금서비스이용tf
          , vv_cv_vs_ue_tf                          -- 리볼빙현금서비스해외이용tf
          , vv_mns_cv_ue_tf                         -- 리볼빙마이너스현금서비스이용tf
          , vv_mns_cv_vs_ue_tf                      -- 리볼빙마이너스현금서비스해외이용tf
          , spr_st_ue_tf                            -- 분할결제이용tf
          , chc_ue_tf                               -- 체크카드이용tf
          , chc_vs_ue_tf                            -- 체크카드해외이용tf
          , chc_cre_p_ue_tf                         -- 체크카드신용일시불이용tf
          , chc_cl_vs_ue_tf                         -- 체크카드신판해외이용tf
          , chc_cv_ue_tf                            -- 체크카드현금서비스이용tf
          , chc_cv_vs_ue_tf                         -- 체크카드현금서비스해외이용tf
          , ue_ttn_cd                               -- 이용패턴코드
          , cex_ue_ttn_cd                           -- 복합이용패턴코드
          , cl_ue_ttn_cd                            -- 신판이용패턴코드
          , cl_vs_ue_ttn_cd                         -- 신판해외이용패턴코드
          , cre_cl_ue_ttn_cd                        -- 신용신판이용패턴코드
          , p_ue_ttn_cd                             -- 일시불이용패턴코드
          , p_vs_ue_ttn_cd                          -- 일시불해외이용패턴코드
          , vv_cl_ue_ttn_cd                         -- 리볼빙신판이용패턴코드
          , vv_cl_vs_ue_ttn_cd                      -- 리볼빙신판해외이용패턴코드
          , vv_ue_ttn_cd1                           -- 리볼빙이용패턴코드1
          , vv_cl_vs_ue_ttn_cd1                     -- 리볼빙신판해외이용패턴코드1
          , vv_ue_ttn_cd2                           -- 리볼빙이용패턴코드2
          , ns_ue_ttn_cd                            -- 할부이용패턴코드
          , ns_vs_ue_ttn_cd                         -- 할부해외이용패턴코드
          , nn_ir_ns_ue_ttn_cd                      -- 무이자할부이용패턴코드
          , cv_ue_ttn_cd                            -- 현금서비스이용패턴코드
          , cv_vs_ue_ttn_cd                         -- 현금서비스해외이용패턴코드
          , vv_cv_ue_ttn_cd                         -- 리볼빙현금서비스이용패턴코드
          , vv_cv_vs_ue_ttn_cd                      -- 리볼빙현금서비스해외이용패턴코드
          , vv_mns_cv_ue_ttn_cd                     -- 리볼빙마이너스현금서비스이용패턴코드
          , vv_mns_cv_vs_ue_ttn_cd                  -- 리볼빙마이너스현금서비스해외이용패턴코드
          , spr_st_ue_ttn_cd                        -- 분할결제이용패턴코드
          , chc_ue_ttn_cd                           -- 체크카드이용패턴코드
          , chc_vs_ue_ttn_cd                        -- 체크카드해외이용패턴코드
          , chc_cre_p_ue_ttn_cd                     -- 체크카드신용일시불이용패턴코드
          , chc_cl_vs_ue_ttn_cd                     -- 체크카드신판해외이용패턴코드
          , chc_cv_ue_ttn_cd                        -- 체크카드현금서비스이용패턴코드
          , chc_cv_vs_ue_ttn_cd                     -- 체크카드현금서비스해외이용패턴코드
          , der_trf_chc_hga                         -- 후불교통체크카드취급금액
          , der_trf_chc_xl_hga                      -- 후불교통체크카드제외취급금액
          , der_trf_chc_ue_ct                       -- 후불교통체크카드이용건수
          , der_trf_chc_xl_ue_ct                    -- 후불교통체크카드제외이용건수
          , der_trf_chc_ue_tf                       -- 후불교통체크카드이용tf
          , der_trf_chc_xl_ue_tf                    -- 후불교통체크카드제외이용tf
          , der_trf_chc_ue_ttn_cd                   -- 후불교통체크카드이용패턴코드
          , der_trf_chc_xl_ue_ttn_cd                -- 후불교통체크카드제외이용패턴코드
          , crd_acc_rpl_n                           -- 카드계정대체번호
      from w0_shc.mtfua0002
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
