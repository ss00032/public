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
pgm_id = 'ALID_JDA008T10_TG'

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
s3_file_prefix = f'aid_jda008t10_{execution_kst}'

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
        delete from l0_shi.jda008t10
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shi.jda008t10
                (
                  aws_ls_dt                               -- aws적재일시
                , base_ym                                 -- 기준년월
                , acct_no                                 -- 계좌번호
                , cust_no                                 -- 고객번호
                , grp_md_no                               -- 그룹md번호
                , ci_valu                                 -- ci값
                , est_ymd                                 -- 개설일자
                , mang_tp_code                            -- 관리구분코드
                , mang_ocr_ymd                            -- 관리발생일자
                , acct_mang_dbrn_code                     -- 계좌관리부점코드
                , mang_dbrn_chg_ymd                       -- 관리부점변경일자
                , realnm_ficnm_tp_code                    -- 실명가명구분코드
                , realnm_ficnm_cnfm_ymd                   -- 실명가명확인일자
                , realnm_cnfm_tp_code                     -- 실명확인구분코드
                , acid_yn                                 -- 사고여부
                , acid_ymd                                -- 사고일자
                , ingam_stat_code                         -- 인감상태코드
                , ingam_regi_ymd                          -- 인감등록일자
                , mast_infm_tp_code                       -- 원장통보구분코드
                , mast_infm_ctadd_code                    -- 원장통보연락처코드
                , post                                    -- 우편번호
                , nwant_mast_aply_ymd                     -- 불원원장신청일자
                , mast_retu_numt                          -- 원장반송횟수
                , retu_tp_code                            -- 반송구분코드
                , retu_ymd                                -- 반송일자
                , remq_cert_isu_ymd                       -- 잔고증명서발급일자
                , txon_tp_code                            -- 과세구분코드
                , orgn_collct_mand_tp_code                -- 원천징수위임구분코드
                , ntax_incmn_tp_code                      -- 국세청소득자구분코드
                , natv_frgner_tp_code                     -- 내국인외국인구분코드
                , natn_code                               -- 국가코드
                , resimn_tp_code                          -- 거주자구분코드
                , resi_natn_code                          -- 거주국가코드
                , realnm_cnfm_type_code                   -- 실명확인유형코드
                , grup_tp_code                            -- 단체구분코드
                , invtmn_clas_code                        -- 투자자분류코드
                , dvidtx_tp_code                          -- 배당세구분코드
                , trtax_tp_code                           -- 거래세구분코드
                , usfee_txon_tp_code                      -- 이용료과세구분코드
                , mrgn_collct_tp_code                     -- 증거금징수구분코드
                , curr_type_code                          -- 통화유형코드
                , secur_tp_code                           -- 유가증권구분코드
                , acct_trns_outm_engg_yn                  -- 계좌이체출금약정여부
                , ord_nacpt_tp_code                       -- 주문불응구분코드
                , card_isu_tp_code                        -- 카드발급구분코드
                , card_dlvr_ymd                           -- 카드교부일자
                , card_isu_seq                            -- 카드발급순번
                , psb_isu_tp_code                         -- 통장발급구분코드
                , psb_isu_ymd                             -- 통장발급일자
                , psb_isu_numt                            -- 통장발급횟수
                , invt_prop_code                          -- 투자성향코드
                , fna_acct_tp_code                        -- fna계좌구분코드
                , acct_cmsn_tp_code                       -- 계좌수수료구분코드
                , tc_cmbn_grad_code                       -- 탑스클럽통합등급코드
                , intg_acct_agmt_acce_tp_code             -- 종합계좌약관승인구분코드
                , intg_acct_agmt_acce_ymd                 -- 종합계좌약관승인일자
                , etc_prvs_lmt_tp_code                    -- 기타지급제한구분코드
                , bank_coop_bizs_tp_code                  -- 은행제휴업무구분코드
                , forg_code                               -- 금융기관코드
                , coop_bank_acct_no                       -- 제휴은행계좌번호
                , ctac_bank_acct_no                       -- 연계은행계좌번호
                , living_sv_tp_code                       -- 생계형저축구분코드
                , secr_acct_tp_code                       -- 증권계좌구분코드
                , sploan_tp_code                          -- 스탁파워론구분코드
                , scr_lndo_regi_yn                        -- 청약대출등록여부
                , agnt_proc_yn                            -- 대리인처리여부
                , atsw_regi_yn                            -- autoswing등록여부
                , wrap_regi_yn                            -- 랩등록여부
                , deal_moti_tp_code                       -- 거래동기구분코드
                , crd_acct_tp_code                        -- 신용계좌구분코드
                , crd_est_ymd                             -- 신용개설일자
                , crd_abnd_ymd                            -- 신용해지일자
                , crd_agrm_yn                             -- 신용약정서여부
                , mrgn_estb_scop_tp_code                  -- 증거금설정범위구분코드
                , opps_trde_rejt_cont_numt                -- 반대매매거부연속횟수
                , bank_remn_certi_isu_ymd                 -- 은행잔액증명발급일자
                , bank_prvs_stop_tp_code                  -- 은행지급정지구분코드
                , last_deal_ymd                           -- 최종거래일자
                , last_deal_no_ymd                        -- 최종거래번호일자
                , frst_trde_ymd                           -- 최초매매일자
                , last_prt_ymd                            -- 최종인쇄일자
                , acct_trns_rcpt_engg_yn                  -- 계좌이체입금약정여부
                , trde_rep_notilo_tp_code                 -- 매매보고서통보지구분코드
                , hts_acct_yn                             -- hts계좌여부
                , ars_acct_yn                             -- ars계좌여부
                , coop_est_forg_bran_code                 -- 제휴개설금융기관지점코드
                , coop_for_est_forg_bran_code             -- 제휴해외개설금융기관지점코드
                , bank_coop_for_acct_tp_code              -- 은행제휴해외계좌구분코드
                , mdesi_lmt_regi_yn                       -- 비지정제한등록여부
                , fna_ctac_bran_code                      -- fna연계지점코드
                , consn_rejt_tp_code                      -- 수탁거부구분코드
                , psb_acct_mast_infm_yn                   -- 통장계좌원장통보여부
                , email_retu_numt                         -- 이메일반송횟수
                , acct_est_tp_code                        -- 계좌개설구분코드
                , bank_dsbl_amt                           -- 은행불능금액
                , mthr_dbrn_code                          -- 모부점코드
                , bib_tp_code                             -- bib구분코드
                , rpst_invtmn_tp_code                     -- 대표투자자구분코드
                , bank_cdm_use_aval_code                  -- 은행cd기사용가능코드
                , dealmn_persn_code                       -- 거래자인격코드
                , cma_acct_tp_code                        -- cma계좌구분코드
                , last_lad_ymd                            -- 최종적재일자
                , regi_dt                                 -- 등록일시
                , modi_dt                                 -- 수정일시
                , grp_cmbn_cust_id                        -- 그룹통합고객id
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , base_ym                                 -- 기준년월
          , acct_no                                 -- 계좌번호
          , cust_no                                 -- 고객번호
          , grp_md_no                               -- 그룹md번호
          , ci_valu                                 -- ci값
          , est_ymd                                 -- 개설일자
          , mang_tp_code                            -- 관리구분코드
          , mang_ocr_ymd                            -- 관리발생일자
          , acct_mang_dbrn_code                     -- 계좌관리부점코드
          , mang_dbrn_chg_ymd                       -- 관리부점변경일자
          , realnm_ficnm_tp_code                    -- 실명가명구분코드
          , realnm_ficnm_cnfm_ymd                   -- 실명가명확인일자
          , realnm_cnfm_tp_code                     -- 실명확인구분코드
          , acid_yn                                 -- 사고여부
          , acid_ymd                                -- 사고일자
          , ingam_stat_code                         -- 인감상태코드
          , ingam_regi_ymd                          -- 인감등록일자
          , mast_infm_tp_code                       -- 원장통보구분코드
          , mast_infm_ctadd_code                    -- 원장통보연락처코드
          , post                                    -- 우편번호
          , nwant_mast_aply_ymd                     -- 불원원장신청일자
          , mast_retu_numt                          -- 원장반송횟수
          , retu_tp_code                            -- 반송구분코드
          , retu_ymd                                -- 반송일자
          , remq_cert_isu_ymd                       -- 잔고증명서발급일자
          , txon_tp_code                            -- 과세구분코드
          , orgn_collct_mand_tp_code                -- 원천징수위임구분코드
          , ntax_incmn_tp_code                      -- 국세청소득자구분코드
          , natv_frgner_tp_code                     -- 내국인외국인구분코드
          , natn_code                               -- 국가코드
          , resimn_tp_code                          -- 거주자구분코드
          , resi_natn_code                          -- 거주국가코드
          , realnm_cnfm_type_code                   -- 실명확인유형코드
          , grup_tp_code                            -- 단체구분코드
          , invtmn_clas_code                        -- 투자자분류코드
          , dvidtx_tp_code                          -- 배당세구분코드
          , trtax_tp_code                           -- 거래세구분코드
          , usfee_txon_tp_code                      -- 이용료과세구분코드
          , mrgn_collct_tp_code                     -- 증거금징수구분코드
          , curr_type_code                          -- 통화유형코드
          , secur_tp_code                           -- 유가증권구분코드
          , acct_trns_outm_engg_yn                  -- 계좌이체출금약정여부
          , ord_nacpt_tp_code                       -- 주문불응구분코드
          , card_isu_tp_code                        -- 카드발급구분코드
          , card_dlvr_ymd                           -- 카드교부일자
          , card_isu_seq                            -- 카드발급순번
          , psb_isu_tp_code                         -- 통장발급구분코드
          , psb_isu_ymd                             -- 통장발급일자
          , psb_isu_numt                            -- 통장발급횟수
          , invt_prop_code                          -- 투자성향코드
          , fna_acct_tp_code                        -- fna계좌구분코드
          , acct_cmsn_tp_code                       -- 계좌수수료구분코드
          , tc_cmbn_grad_code                       -- 탑스클럽통합등급코드
          , intg_acct_agmt_acce_tp_code             -- 종합계좌약관승인구분코드
          , intg_acct_agmt_acce_ymd                 -- 종합계좌약관승인일자
          , etc_prvs_lmt_tp_code                    -- 기타지급제한구분코드
          , bank_coop_bizs_tp_code                  -- 은행제휴업무구분코드
          , forg_code                               -- 금융기관코드
          , coop_bank_acct_no                       -- 제휴은행계좌번호
          , ctac_bank_acct_no                       -- 연계은행계좌번호
          , living_sv_tp_code                       -- 생계형저축구분코드
          , secr_acct_tp_code                       -- 증권계좌구분코드
          , sploan_tp_code                          -- 스탁파워론구분코드
          , scr_lndo_regi_yn                        -- 청약대출등록여부
          , agnt_proc_yn                            -- 대리인처리여부
          , atsw_regi_yn                            -- autoswing등록여부
          , wrap_regi_yn                            -- 랩등록여부
          , deal_moti_tp_code                       -- 거래동기구분코드
          , crd_acct_tp_code                        -- 신용계좌구분코드
          , crd_est_ymd                             -- 신용개설일자
          , crd_abnd_ymd                            -- 신용해지일자
          , crd_agrm_yn                             -- 신용약정서여부
          , mrgn_estb_scop_tp_code                  -- 증거금설정범위구분코드
          , opps_trde_rejt_cont_numt                -- 반대매매거부연속횟수
          , bank_remn_certi_isu_ymd                 -- 은행잔액증명발급일자
          , bank_prvs_stop_tp_code                  -- 은행지급정지구분코드
          , last_deal_ymd                           -- 최종거래일자
          , last_deal_no_ymd                        -- 최종거래번호일자
          , frst_trde_ymd                           -- 최초매매일자
          , last_prt_ymd                            -- 최종인쇄일자
          , acct_trns_rcpt_engg_yn                  -- 계좌이체입금약정여부
          , trde_rep_notilo_tp_code                 -- 매매보고서통보지구분코드
          , hts_acct_yn                             -- hts계좌여부
          , ars_acct_yn                             -- ars계좌여부
          , coop_est_forg_bran_code                 -- 제휴개설금융기관지점코드
          , coop_for_est_forg_bran_code             -- 제휴해외개설금융기관지점코드
          , bank_coop_for_acct_tp_code              -- 은행제휴해외계좌구분코드
          , mdesi_lmt_regi_yn                       -- 비지정제한등록여부
          , fna_ctac_bran_code                      -- fna연계지점코드
          , consn_rejt_tp_code                      -- 수탁거부구분코드
          , psb_acct_mast_infm_yn                   -- 통장계좌원장통보여부
          , email_retu_numt                         -- 이메일반송횟수
          , acct_est_tp_code                        -- 계좌개설구분코드
          , bank_dsbl_amt                           -- 은행불능금액
          , mthr_dbrn_code                          -- 모부점코드
          , bib_tp_code                             -- bib구분코드
          , rpst_invtmn_tp_code                     -- 대표투자자구분코드
          , bank_cdm_use_aval_code                  -- 은행cd기사용가능코드
          , dealmn_persn_code                       -- 거래자인격코드
          , cma_acct_tp_code                        -- cma계좌구분코드
          , last_lad_ymd                            -- 최종적재일자
          , regi_dt                                 -- 등록일시
          , modi_dt                                 -- 수정일시
          , grp_cmbn_cust_id                        -- 그룹통합고객id
      from w0_shi.jda008t10
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
