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
pgm_id = 'ILCD_SWOAC0020_TG'

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
s3_file_prefix = f'icd_swoac0020_/icd_swoac0020_{execution_kst}'

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
        delete from l0_shc.swoac0020
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shc.swoac0020
                (
                  aws_ls_dt                               -- aws적재일시
                , ta_ym                                   -- 기준년월
                , ln_n                                    -- 대출번호
                , sls_ts_n                                -- 매출거래번호
                , ctc_n                                   -- 약정번호
                , sgdmd                                   -- 그룹md번호
                , cus_ccd                                 -- 회원구분코드
                , fnc_ln_sn                               -- 오토금융대출순번
                , bsn_pd_cd                               -- 영업상품코드
                , fnc_pd_dl_n                             -- 오토금융상품상세번호
                , fnc_hdg_pd_cd                           -- 오토금융취급상품코드
                , ln_pd_zcd                               -- 대출상품분류코드
                , mfg_gp_cd                               -- 제품그룹코드
                , nn_ir_ccd                               -- 무이자구분코드
                , afo_n                                   -- 제휴사번호
                , afs_n                                   -- 제휴점번호
                , cnv_ccd                                 -- 협약구분코드
                , ln_trm_ms_cn                            -- 대출기간개월수
                , sub_nn_ir_ms_cn                         -- 부분무이자개월수
                , lna                                     -- 대출금액
                , pv_py_mcd                               -- 대표상환방법코드
                , bil_hcd                                 -- 청구지점코드
                , rv_hcd                                  -- 접수지점코드
                , ln_hcd                                  -- 대출지점코드
                , ln_d                                    -- 대출일자
                , xp_d                                    -- 만기일자
                , de_d                                    -- 확정일자
                , ni_st_d                                 -- 최초결제일자
                , ls_bil_d                                -- 최종청구일자
                , ls_bil_dgt                              -- 최종청구회차
                , ls_rip_d                                -- 최종수납일자
                , ls_rca                                  -- 최종입금금액
                , rsv_li_f                                -- 예약판매여부
                , ln_al                                   -- 대출잔액
                , ei_al                                   -- 선수잔액
                , pyf_f                                   -- 완납여부
                , pyf_d                                   -- 완납일자
                , dep_at                                  -- 예치금액
                , dep_at_cvs_f                            -- 예치금액전환여부
                , dep_at_bil_dgt                          -- 예치금액청구회차
                , qy_rst_f                                -- 조회제한여부
                , ce_f                                    -- 취소여부
                , ce_d                                    -- 취소일자
                , ln_su_mf_cd                             -- 대출상태변경코드
                , suc_f                                   -- 승계여부
                , pr_qy_f                                 -- 선조회여부
                , ol_ln_hcd                               -- 구대출지점코드
                , ftt_irt_f                               -- 변동금리여부
                , df_ln_uab_f                             -- 대환대출불가여부
                , cre_rcr_aid_dql_f                       -- 신용회복지원상실여부
                , ivl_ira                                 -- 실효이자금액
                , sug_rbs_xl_f                            -- 대위변제제외여부
                , sug_rbs_xl_d                            -- 대위변제제외일자
                , cex_pd_ccd                              -- 복합상품구분코드
                , bot_dql_f                               -- 기한이익상실여부
                , dlg_rkn_edd                             -- 연체료기산종료일자
                , pp_pry_ddc_f                            -- 원금우선공제여부
                , ni_rg_xct_id                            -- 최초등록수행id
                , ni_rg_dt                                -- 최초등록일시
                , ls_alt_xct_id                           -- 최종수정수행id
                , ls_alt_dt                               -- 최종수정일시
                , ls_ld_dt                                -- 최종적재일시
                , rsv_li_tf                               -- 예약판매tf
                , pyf_tf                                  -- 완납tf
                , dep_at_cvs_tf                           -- 예치금액전환tf
                , qy_rst_tf                               -- 조회제한tf
                , ce_tf                                   -- 취소tf
                , suc_tf                                  -- 승계tf
                , pr_qy_tf                                -- 선조회tf
                , ftt_irt_tf                              -- 변동금리tf
                , df_ln_uab_tf                            -- 대환대출불가tf
                , cre_rcr_aid_dql_tf                      -- 신용회복지원상실tf
                , sug_rbs_xl_tf                           -- 대위변제제외tf
                , bot_dql_tf                              -- 기한이익상실tf
                , pp_pry_ddc_tf                           -- 원금우선공제tf
                , aup_apv_crd_rpl_n                       -- 오토플러스승인카드대체번호
                , tvc_rpl_n                               -- 트래블카드대체번호
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , ta_ym                                   -- 기준년월
          , ln_n                                    -- 대출번호
          , sls_ts_n                                -- 매출거래번호
          , ctc_n                                   -- 약정번호
          , sgdmd                                   -- 그룹md번호
          , cus_ccd                                 -- 회원구분코드
          , fnc_ln_sn                               -- 오토금융대출순번
          , bsn_pd_cd                               -- 영업상품코드
          , fnc_pd_dl_n                             -- 오토금융상품상세번호
          , fnc_hdg_pd_cd                           -- 오토금융취급상품코드
          , ln_pd_zcd                               -- 대출상품분류코드
          , mfg_gp_cd                               -- 제품그룹코드
          , nn_ir_ccd                               -- 무이자구분코드
          , afo_n                                   -- 제휴사번호
          , afs_n                                   -- 제휴점번호
          , cnv_ccd                                 -- 협약구분코드
          , ln_trm_ms_cn                            -- 대출기간개월수
          , sub_nn_ir_ms_cn                         -- 부분무이자개월수
          , lna                                     -- 대출금액
          , pv_py_mcd                               -- 대표상환방법코드
          , bil_hcd                                 -- 청구지점코드
          , rv_hcd                                  -- 접수지점코드
          , ln_hcd                                  -- 대출지점코드
          , ln_d                                    -- 대출일자
          , xp_d                                    -- 만기일자
          , de_d                                    -- 확정일자
          , ni_st_d                                 -- 최초결제일자
          , ls_bil_d                                -- 최종청구일자
          , ls_bil_dgt                              -- 최종청구회차
          , ls_rip_d                                -- 최종수납일자
          , ls_rca                                  -- 최종입금금액
          , rsv_li_f                                -- 예약판매여부
          , ln_al                                   -- 대출잔액
          , ei_al                                   -- 선수잔액
          , pyf_f                                   -- 완납여부
          , pyf_d                                   -- 완납일자
          , dep_at                                  -- 예치금액
          , dep_at_cvs_f                            -- 예치금액전환여부
          , dep_at_bil_dgt                          -- 예치금액청구회차
          , qy_rst_f                                -- 조회제한여부
          , ce_f                                    -- 취소여부
          , ce_d                                    -- 취소일자
          , ln_su_mf_cd                             -- 대출상태변경코드
          , suc_f                                   -- 승계여부
          , pr_qy_f                                 -- 선조회여부
          , ol_ln_hcd                               -- 구대출지점코드
          , ftt_irt_f                               -- 변동금리여부
          , df_ln_uab_f                             -- 대환대출불가여부
          , cre_rcr_aid_dql_f                       -- 신용회복지원상실여부
          , ivl_ira                                 -- 실효이자금액
          , sug_rbs_xl_f                            -- 대위변제제외여부
          , sug_rbs_xl_d                            -- 대위변제제외일자
          , cex_pd_ccd                              -- 복합상품구분코드
          , bot_dql_f                               -- 기한이익상실여부
          , dlg_rkn_edd                             -- 연체료기산종료일자
          , pp_pry_ddc_f                            -- 원금우선공제여부
          , ni_rg_xct_id                            -- 최초등록수행id
          , ni_rg_dt                                -- 최초등록일시
          , ls_alt_xct_id                           -- 최종수정수행id
          , ls_alt_dt                               -- 최종수정일시
          , ls_ld_dt                                -- 최종적재일시
          , rsv_li_tf                               -- 예약판매tf
          , pyf_tf                                  -- 완납tf
          , dep_at_cvs_tf                           -- 예치금액전환tf
          , qy_rst_tf                               -- 조회제한tf
          , ce_tf                                   -- 취소tf
          , suc_tf                                  -- 승계tf
          , pr_qy_tf                                -- 선조회tf
          , ftt_irt_tf                              -- 변동금리tf
          , df_ln_uab_tf                            -- 대환대출불가tf
          , cre_rcr_aid_dql_tf                      -- 신용회복지원상실tf
          , sug_rbs_xl_tf                           -- 대위변제제외tf
          , bot_dql_tf                              -- 기한이익상실tf
          , pp_pry_ddc_tf                           -- 원금우선공제tf
          , aup_apv_crd_rpl_n                       -- 오토플러스승인카드대체번호
          , tvc_rpl_n                               -- 트래블카드대체번호
      from w0_shc.swoac0020
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
