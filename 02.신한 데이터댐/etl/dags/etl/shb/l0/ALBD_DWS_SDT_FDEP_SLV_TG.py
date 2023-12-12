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
pgm_id = 'ALBD_DWS_SDT_FDEP_SLV_TG'

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
s3_file_prefix = f'abd_dws_sdt_fdep_slv_/abd_dws_sdt_fdep_slv_{execution_kst}'

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
        delete from l0_shb.dws_sdt_fdep_slv
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dws_sdt_fdep_slv
                (
                  aws_ls_dt                               -- aws적재일시
                , acno                                    -- 계좌번호
                , cur_c                                   -- 통화code
                , trn                                     -- 회차
                , trxno                                   -- 거래번호
                , kwa_c                                   -- 과목code
                , acser                                   -- 계좌일련번호
                , upmu_g                                  -- 업무구분
                , dep_trx_g                               -- 수신거래구분
                , fdep_hji_g                              -- 외화예금해지구분
                , crt_can_g                               -- 정정취소구분
                , orgn_trxno                              -- 원거래번호
                , orgn_trxdt                              -- 원거래일자
                , gsdt_tryn                               -- 기산일거래여부
                , hsong_yn                                -- 후송여부
                , trxdt                                   -- 거래일자
                , acmt_dt                                 -- 계리일자
                , trx_oprt_dt                             -- 거래조작일자
                , chan_u                                  -- 채널유형
                , ipji_g                                  -- 입지구분
                , ip_trn                                  -- 입금회차
                , fcur_samt                               -- 외화합계금액
                , fcsh_amt                                -- 외화현찰금액
                , fdch_amt                                -- 외화대체금액
                , pstn_amt                                -- position금액
                , csh_amt                                 -- 현금금액
                , dch_amt                                 -- 대체금액
                , trx_tbnkchk_c1                          -- 거래타점권code1
                , trx_tjum_amt1                           -- 거래타점금액1
                , trx_tjum_amt2                           -- 거래타점금액2
                , trxbf_jan                               -- 거래전잔액
                , trxaf_jan                               -- 거래후잔액
                , trn_apnt_offset_amt                     -- 회차지정차감금액
                , for_upmu_b_c                            -- 외환업무분류code
                , for_udae_g                              -- 외환우대구분
                , spec_exrt_aprvno                        -- 특별환율승인번호
                , sprd_rt                                 -- spread율
                , apl_exrt                                -- 적용환율
                , trd_bas_rt                              -- 매매기준율
                , int_trd_bas_rt                          -- 이자매매기준율
                , cent_exrt                               -- 집중환율
                , note_buy_rt                             -- 지폐매입율
                , crcur_cvt_yn                            -- 이종통화전환여부
                , nts_noti_yn                             -- 국세청통보여부
                , udamt_prn_yn                            -- 우대금액인자여부
                , mutongj_ip_yn                           -- 무통장입금여부
                , handy_trt_g                             -- 편의취급구분
                , suik_div_yn                             -- 수익분할여부
                , silmul_k                                -- 실물종류
                , for_exim_ref_no1                        -- 외환수출입참조번호1
                , cms_no                                  -- cms번호
                , fcur_ynd_acno                           -- 외화연동계좌번호
                , fcur_ynd_amt                            -- 외화연동금액
                , fcur_ynd_trxno                          -- 외화연동거래번호
                , wcur_ynd_acno                           -- 원화연동계좌번호
                , wcur_ynd_amt                            -- 원화연동금액
                , wcur_ynd_trxno                          -- 원화연동거래번호
                , fcsh_ynd_acno                           -- 외화현찰연동계좌번호
                , fcsh_nat_c                              -- 외화현찰국가code
                , fcsh_juche_c                            -- 외화현찰주체code
                , fcsh_fnc_rsn_c                          -- 외화현찰무역외사유code
                , cbuy_rt_apl_amt                         -- 현찰매입율적용금액
                , fwex_ynd_acno                           -- 선물환연동계좌번호
                , fwex_aprvno                             -- 선물환승인번호
                , mgaf_yn                                 -- 마감후여부
                , fee_trx_ser                             -- 수수료거래일련번호
                , feejs_g                                 -- 수수료징수구분
                , spiyul_jkyn                             -- 특별이율적용여부
                , hchb_iyul_aprvno                        -- 회차별이율승인번호
                , mrkt_in_iyul                            -- 시장내부이율
                , fnd_ar_adj_iyul                         -- 자금부조정이율
                , plcy_adj_iyul                           -- 정책조정이율
                , saupb_adj_iyul                          -- 사업부조정이율
                , sdd_prmm_iyul                           -- 유동성프리미엄이율
                , spar_iyul                               -- 예비이율
                , hchb_due_iyul                           -- 회차별만기이율
                , hchb_apl_iyul                           -- 회차별적용이율
                , ji_ds_trn                               -- 지급대상회차
                , fdep_int_ji_b                           -- 외화예금이자지급분류
                , intgs_sjdt                              -- 이자계산시작일자
                , intgs_endt                              -- 이자계산종료일자
                , taxbf_fcur_int                          -- 세전외화이자
                , taxbf_wcur_int                          -- 세전원화이자
                , taxaf_cus_ji_fcur_int                   -- 세후고객지급외화이자
                , taxaf_cus_ji_wcur_int                   -- 세후고객지급원화이자
                , hwanip_fcur_int                         -- 환입외화이자
                , hwanip_int                              -- 환입이자
                , mrkt_in_int_amt                         -- 시장내부이자금액
                , fnd_ar_adj_int_amt                      -- 자금부조정이자금액
                , plcy_adj_int_amt                        -- 정책조정이자금액
                , saupb_adj_int_amt                       -- 사업부조정이자금액
                , sdd_prmm_int_amt                        -- 유동성프리미엄이자금액
                , spar_iyul_amt                           -- 예비이율금액
                , jingsu_cotax_amt                        -- 징수법인세금액
                , jingsu_sdtax_amt                        -- 징수소득세금액
                , jingsu_rtax_amt                         -- 징수주민세금액
                , hwanchl_cotax_amt                       -- 환출법인세금액
                , hwanchl_sdtax_amt                       -- 환출소득세금액
                , hwanchl_rtax_amt                        -- 환출주민세금액
                , bok_21_sub_rsn_c                        -- 한국은행21sub사유code
                , bok_ipji_rsn_c                          -- 한국은행입지사유code
                , fdep_useofc_c                           -- 외화예금사용처code
                , taact_silno_g                           -- 대외계정실명번호구분
                , taact_rsdt_g                            -- 대외계정거주자구분
                , taact_nat_c                             -- 대외계정국가code
                , taact_fnc_rsn_c                         -- 대외계정무역외사유code
                , ip_rqstr_silnm_g                        -- 입금의뢰인실명구분
                , daeriin_cusno                           -- 대리인고객번호
                , fnc_yom_k                               -- 무역외사후관리종류
                , fnc_yom_g                               -- 무역외사후관리구분
                , fnc_yom_no                              -- 무역외사후관리번호
                , fdep_fnc_yom_ctnt                       -- 외화예금무역외사후관리내용
                , singosr_g                               -- 신고수리구분
                , singosr_hmk_c                           -- 신고수리항목code
                , singosr_ser                             -- 신고수리일련번호
                , singosr_tf_bnkbr_no                     -- 신고수리외국환은행점번호
                , trxbrno                                 -- 거래점번호
                , chk_amtty_g1                            -- 수표권종구분1
                , chk_scnt1                               -- 수표매수1
                , chk_stt_no1                             -- 수표시작번호1
                , chk_end_no1                             -- 수표종료번호1
                , ilbang_amt1                             -- 일반권금액1
                , chk_amtty_g2                            -- 수표권종구분2
                , ilbang_amt3                             -- 일반권금액3
                , exrt_udae_amt                           -- 환율우대금액
                , int_ji_yn                               -- 이자지급여부
                , lst_cdt                                 -- 최종변경일자
                , lst_chg_brno                            -- 최종변경점번호
                , trx_oprt_time                           -- 거래시간
                , fil_30_ctnt2                            -- 환전계좌번호
                , taact_silno_cid                         -- 대외계정실명번호고객id
                , daeriin_silno_cid                       -- 대리인실명번호고객id
                , fcsh_new_fnc_rsn_c                      -- 외화현철신무역외사유코드
                , taact_new_fnc_rsn_c                     -- 대외계정신무역외사유코드
                , trx_jkyo                                -- 거래적요
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , acno                                    -- 계좌번호
          , cur_c                                   -- 통화code
          , trn                                     -- 회차
          , trxno                                   -- 거래번호
          , kwa_c                                   -- 과목code
          , acser                                   -- 계좌일련번호
          , upmu_g                                  -- 업무구분
          , dep_trx_g                               -- 수신거래구분
          , fdep_hji_g                              -- 외화예금해지구분
          , crt_can_g                               -- 정정취소구분
          , orgn_trxno                              -- 원거래번호
          , orgn_trxdt                              -- 원거래일자
          , gsdt_tryn                               -- 기산일거래여부
          , hsong_yn                                -- 후송여부
          , trxdt                                   -- 거래일자
          , acmt_dt                                 -- 계리일자
          , trx_oprt_dt                             -- 거래조작일자
          , chan_u                                  -- 채널유형
          , ipji_g                                  -- 입지구분
          , ip_trn                                  -- 입금회차
          , fcur_samt                               -- 외화합계금액
          , fcsh_amt                                -- 외화현찰금액
          , fdch_amt                                -- 외화대체금액
          , pstn_amt                                -- position금액
          , csh_amt                                 -- 현금금액
          , dch_amt                                 -- 대체금액
          , trx_tbnkchk_c1                          -- 거래타점권code1
          , trx_tjum_amt1                           -- 거래타점금액1
          , trx_tjum_amt2                           -- 거래타점금액2
          , trxbf_jan                               -- 거래전잔액
          , trxaf_jan                               -- 거래후잔액
          , trn_apnt_offset_amt                     -- 회차지정차감금액
          , for_upmu_b_c                            -- 외환업무분류code
          , for_udae_g                              -- 외환우대구분
          , spec_exrt_aprvno                        -- 특별환율승인번호
          , sprd_rt                                 -- spread율
          , apl_exrt                                -- 적용환율
          , trd_bas_rt                              -- 매매기준율
          , int_trd_bas_rt                          -- 이자매매기준율
          , cent_exrt                               -- 집중환율
          , note_buy_rt                             -- 지폐매입율
          , crcur_cvt_yn                            -- 이종통화전환여부
          , nts_noti_yn                             -- 국세청통보여부
          , udamt_prn_yn                            -- 우대금액인자여부
          , mutongj_ip_yn                           -- 무통장입금여부
          , handy_trt_g                             -- 편의취급구분
          , suik_div_yn                             -- 수익분할여부
          , silmul_k                                -- 실물종류
          , for_exim_ref_no1                        -- 외환수출입참조번호1
          , cms_no                                  -- cms번호
          , fcur_ynd_acno                           -- 외화연동계좌번호
          , fcur_ynd_amt                            -- 외화연동금액
          , fcur_ynd_trxno                          -- 외화연동거래번호
          , wcur_ynd_acno                           -- 원화연동계좌번호
          , wcur_ynd_amt                            -- 원화연동금액
          , wcur_ynd_trxno                          -- 원화연동거래번호
          , fcsh_ynd_acno                           -- 외화현찰연동계좌번호
          , fcsh_nat_c                              -- 외화현찰국가code
          , fcsh_juche_c                            -- 외화현찰주체code
          , fcsh_fnc_rsn_c                          -- 외화현찰무역외사유code
          , cbuy_rt_apl_amt                         -- 현찰매입율적용금액
          , fwex_ynd_acno                           -- 선물환연동계좌번호
          , fwex_aprvno                             -- 선물환승인번호
          , mgaf_yn                                 -- 마감후여부
          , fee_trx_ser                             -- 수수료거래일련번호
          , feejs_g                                 -- 수수료징수구분
          , spiyul_jkyn                             -- 특별이율적용여부
          , hchb_iyul_aprvno                        -- 회차별이율승인번호
          , mrkt_in_iyul                            -- 시장내부이율
          , fnd_ar_adj_iyul                         -- 자금부조정이율
          , plcy_adj_iyul                           -- 정책조정이율
          , saupb_adj_iyul                          -- 사업부조정이율
          , sdd_prmm_iyul                           -- 유동성프리미엄이율
          , spar_iyul                               -- 예비이율
          , hchb_due_iyul                           -- 회차별만기이율
          , hchb_apl_iyul                           -- 회차별적용이율
          , ji_ds_trn                               -- 지급대상회차
          , fdep_int_ji_b                           -- 외화예금이자지급분류
          , intgs_sjdt                              -- 이자계산시작일자
          , intgs_endt                              -- 이자계산종료일자
          , taxbf_fcur_int                          -- 세전외화이자
          , taxbf_wcur_int                          -- 세전원화이자
          , taxaf_cus_ji_fcur_int                   -- 세후고객지급외화이자
          , taxaf_cus_ji_wcur_int                   -- 세후고객지급원화이자
          , hwanip_fcur_int                         -- 환입외화이자
          , hwanip_int                              -- 환입이자
          , mrkt_in_int_amt                         -- 시장내부이자금액
          , fnd_ar_adj_int_amt                      -- 자금부조정이자금액
          , plcy_adj_int_amt                        -- 정책조정이자금액
          , saupb_adj_int_amt                       -- 사업부조정이자금액
          , sdd_prmm_int_amt                        -- 유동성프리미엄이자금액
          , spar_iyul_amt                           -- 예비이율금액
          , jingsu_cotax_amt                        -- 징수법인세금액
          , jingsu_sdtax_amt                        -- 징수소득세금액
          , jingsu_rtax_amt                         -- 징수주민세금액
          , hwanchl_cotax_amt                       -- 환출법인세금액
          , hwanchl_sdtax_amt                       -- 환출소득세금액
          , hwanchl_rtax_amt                        -- 환출주민세금액
          , bok_21_sub_rsn_c                        -- 한국은행21sub사유code
          , bok_ipji_rsn_c                          -- 한국은행입지사유code
          , fdep_useofc_c                           -- 외화예금사용처code
          , taact_silno_g                           -- 대외계정실명번호구분
          , taact_rsdt_g                            -- 대외계정거주자구분
          , taact_nat_c                             -- 대외계정국가code
          , taact_fnc_rsn_c                         -- 대외계정무역외사유code
          , ip_rqstr_silnm_g                        -- 입금의뢰인실명구분
          , daeriin_cusno                           -- 대리인고객번호
          , fnc_yom_k                               -- 무역외사후관리종류
          , fnc_yom_g                               -- 무역외사후관리구분
          , fnc_yom_no                              -- 무역외사후관리번호
          , fdep_fnc_yom_ctnt                       -- 외화예금무역외사후관리내용
          , singosr_g                               -- 신고수리구분
          , singosr_hmk_c                           -- 신고수리항목code
          , singosr_ser                             -- 신고수리일련번호
          , singosr_tf_bnkbr_no                     -- 신고수리외국환은행점번호
          , trxbrno                                 -- 거래점번호
          , chk_amtty_g1                            -- 수표권종구분1
          , chk_scnt1                               -- 수표매수1
          , chk_stt_no1                             -- 수표시작번호1
          , chk_end_no1                             -- 수표종료번호1
          , ilbang_amt1                             -- 일반권금액1
          , chk_amtty_g2                            -- 수표권종구분2
          , ilbang_amt3                             -- 일반권금액3
          , exrt_udae_amt                           -- 환율우대금액
          , int_ji_yn                               -- 이자지급여부
          , lst_cdt                                 -- 최종변경일자
          , lst_chg_brno                            -- 최종변경점번호
          , trx_oprt_time                           -- 거래시간
          , fil_30_ctnt2                            -- 환전계좌번호
          , taact_silno_cid                         -- 대외계정실명번호고객id
          , daeriin_silno_cid                       -- 대리인실명번호고객id
          , fcsh_new_fnc_rsn_c                      -- 외화현철신무역외사유코드
          , taact_new_fnc_rsn_c                     -- 대외계정신무역외사유코드
          , trx_jkyo                                -- 거래적요
      from w0_shb.dws_sdt_fdep_slv
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
