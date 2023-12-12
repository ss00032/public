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
pgm_id = 'ILBD_DWS_SDA_FDEP_MAS_TG'

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
s3_file_prefix = f'ibd_dws_sda_fdep_mas_/ibd_dws_sda_fdep_mas_{execution_kst}'

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
        delete from l0_shb.dws_sda_fdep_mas
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dws_sda_fdep_mas
                (
                  aws_ls_dt                               -- aws적재일시
                , acno                                    -- 계좌번호
                , cur_c                                   -- 통화code
                , trn                                     -- 회차
                , kwa_c                                   -- 과목code
                , acser                                   -- 계좌일련번호
                , now_dep_ac_s                            -- 현수신계좌상태
                , bf_dep_ac_s                             -- 전수신계좌상태
                , act_c                                   -- 계정code
                , smok_c                                  -- 세목code
                , busmok_c                                -- 부세목code
                , mkdt                                    -- 신규일자
                , dudt                                    -- 만기일자
                , cus_trxdt                               -- 고객거래일자
                , lst_jan_chg_dt                          -- 최종잔액변동일자
                , hji_dt                                  -- 해지일자
                , mkamt                                   -- 신규금액
                , ac_jan                                  -- 계좌잔액
                , jibul_psb_jan                           -- 지불가능잔액
                , lst_trxno                               -- 최종거래번호
                , grbrno                                  -- 관리점번호
                , hji_brno                                -- 해지점번호
                , ji_limt_amt                             -- 지급제한금액
                , japjwa_entr_amt                         -- 잡좌편입금액
                , lst_intgs_endt                          -- 최종이자계산종료일자
                , tbnkchk_ip_dt                           -- 타점권입금일자
                , rsave_jawon_g                           -- 재예치재원구분
                , trx_chan_u                              -- 거래채널유형
                , ac_fst_mkdt                             -- 계좌최초신규일자
                , rsave_psb_cnt                           -- 재예치가능횟수
                , ynj_re_mk_cnt                           -- 연장재신규횟수
                , rsave_drdt                              -- 재예치등록일자
                , rsave_tot_dep_conttm_g                  -- 재예치총수신계약기간구분
                , rsave_tterm_mcnt                        -- 재예치총기간월수
                , rsave_tot_dcnt                          -- 재예치총기간일수
                , rsave_dep_conttm_g                      -- 재예치수신계약기간구분
                , rsave_conttm_mcnt                       -- 재예치계약기간월수
                , rsave_cont_dcnt                         -- 재예치계약기간일수
                , rsave_addiyul_g                         -- 재예치가산이율구분
                , rsave_addiyul                           -- 재예치가산이율
                , auto_hji_sinc_yn                        -- 자동해지신청여부
                , auto_hji_link_acno                      -- 자동해지연결계좌번호
                , auto_hji_linkac_kwa_c                   -- 자동해지연결계좌과목code
                , auto_hji_link_acser                     -- 자동해지연결계좌일련번호
                , pldg_cret_tamt                          -- 질권설정총금액
                , trx_limt_lst_drdt                       -- 거래제한최종등록일자
                , law_ji_limt_cnt                         -- 법적지급제한건수
                , jan_doc_lst_bal_dt                      -- 잔액증명서최종발급일자
                , bbk_nus_g                               -- 통장미사용구분
                , bbk_char_c                              -- 통장성격code
                , bbk_profd_natv_no                       -- 통장증서고유번호
                , bbk_imptdoc_c                           -- 통장중요증서code
                , now_bbk_fst_trxno                       -- 현통장최초거래번호
                , gijang_stt_trxno                        -- 기장시작거래번호
                , rissu_cnt                               -- 재발행횟수
                , iwol_cnt                                -- 이월횟수
                , lst_baldt                               -- 최종발행일자
                , lst_gijang_line_cnt                     -- 최종기장line수
                , lst_gijang_jan                          -- 최종기장잔액
                , lst_gijang_dt                           -- 최종기장일자
                , lst_gijang_brno                         -- 최종기장점번호
                , cmm_mem_lst_gijang_no                   -- 공통메모최종기장번호
                , ttax_gijang_yr                          -- 종합과세기장년
                , apchuk_mgr_apl_g                        -- 압축기장적용구분
                , cusgd_prn_dt                            -- 고객등급인자일자
                , gijang_resrc_g                          -- 기장자원구분
                , jongtong_sosok_wichi_line_cnt           -- 종통소속위치line수
                , apchuk_mgr_lst_dt                       -- 압축기장최종일자
                , linkac_dryn                             -- 연결계좌등록여부
                , japik_excpt_dryn                        -- 잡익제외등록여부
                , police_rqst_sago_yn                     -- 경찰청의뢰사고여부
                , tamt_apryu_cnt                          -- 전액압류건수
                , ilbu_apryu_cnt                          -- 일부압류건수
                , ilbu_apryu_amt                          -- 일부압류금액
                , ji_stop_cnt                             -- 지급정지건수
                , cc_oi_jistop_yn                         -- cc외지급정지여부
                , dj_trx_hji_yn                           -- 당좌거래해지여부
                , ip_jistop_yn                            -- 입금지급정지여부
                , crime_sago_yn                           -- 범죄사고여부
                , obnk_pldg_dr_yn                         -- 당행질권등록여부
                , torg_pldg_dr_yn                         -- 타기관질권등록여부
                , int_ji_stop_yn                          -- 이자지급정지여부
                , ingam_loss_yn                           -- 인감분실여부
                , ingam_loss_wire_um                      -- 인감분실유선유무
                , bbk_loss_yn                             -- 통장분실여부
                , bbk_loss_wire_um                        -- 통장분실유선유무
                , jeonbumr_yn                             -- 전부명령여부
                , comm_nm_ac_yn                           -- 공동명의계좌여부
                , sign_mk_ac_yn                           -- 서명신규계좌여부
                , dep_dc_ac_yn                            -- 수신대출계좌여부
                , dep_adint_jeyn                          -- 예금원가제외여부
                , jongtong_dmb_jegong_yn                  -- 종통담보제공여부
                , bkrp_sungo_hjae_dt                      -- 파산선고해제일자
                , psn_dbtr_revv_hjae_dt                   -- 개인채무자회생해제일자
                , cnsr_bkrp_hjae_dt                       -- 소비자파산해제일자
                , psn_wkout_hjae_dt                       -- 개인워크아웃해제일자
                , ip_ji_stop_hjae_dt                      -- 입금지급정지해제일자
                , cc_proc_dryn                            -- 센터컷처리등록여부
                , tel_iche_svc_sinc_yn                    -- 전화이체서비스신청여부
                , cash_fee_atcal_jeyn                     -- 현찰수수료자동계산제외여부
                , spiyul_jkyn                             -- 특별이율적용여부
                , fcur_invs_obj_c                         -- 외화투자목적code
                , for_upmu_b_c                            -- 외환업무분류code
                , bf_cur_c                                -- 전통화code
                , mnd_rel_yn                              -- 국방부관련여부
                , mnd_lc_no                               -- 국방부신용장번호
                , mnd_lc_amt                              -- 국방부신용장금액
                , mnd_iyul_sprd_rt                        -- 국방부이율spread율
                , mnd_libor_irt                           -- 국방부libor금리
                , subo_dmb_yn                             -- 수입보증금담보여부
                , subo_dmb_amt                            -- 수입보증금담보금액
                , gtcur_mk_iyul                           -- 기타통화신규이율
                , bas_iyul                                -- 기본이율
                , addiyul                                 -- 가산이율
                , inirt_lst_intgs_endt                    -- 내부금리최종이자계산종료일자
                , mrkt_in_int_samt                        -- 시장내부이자합계금액
                , fnd_ar_adj_int_samt                     -- 자금부조정이자합계금액
                , plcy_adj_int_samt                       -- 정책조정이자합계금액
                , saupb_adj_int_samt                      -- 사업부조정이자합계금액
                , sdd_prmm_int_samt                       -- 유동성프리미엄이자합계금액
                , spar_iyul_samt                          -- 예비이율합계금액
                , iyul_aprvno                             -- 이율승인번호
                , fdep_jks                                -- 외화예금적수
                , fdep_jks2                               -- 외화예금적수2
                , lst_ksdt                                -- 최종결산일자
                , ji_int_samt                             -- 지급이자합계금액
                , fji_int_samt                            -- 외화지급이자합계금액
                , inet_recv_no                            -- internet수신번호
                , fdep_lst_jkrip_no                       -- 외화예금최종적립번호
                , conttm_mcnt                             -- 계약기간월수
                , cont_dcnt                               -- 계약기간일수
                , int_ji_wcur_fcur_g                      -- 이자지급원화외화구분
                , trn1_fcur_jiamt                         -- 1회차외화지급금액
                , int_ji_cnt                              -- 이자지급횟수
                , ilbu_hji_cnt                            -- 일부해지건수
                , sdtax_samt                              -- 소득세합계금액
                , rtax_samt                               -- 주민세합계금액
                , cotax_samt                              -- 법인세합계금액
                , act_proc_dt                             -- 계정처리일자
                , bfday_mgbf_dep_act_amt                  -- 전일마감전예금계정금액
                , bfday_mgbf_dc_act_amt                   -- 전일마감전대출계정금액
                , bfday_mgbf_un_jks_tjum_amt              -- 전일마감전비적수타점금액
                , bfday_mgaf_dep_act_amt                  -- 전일마감후예금계정금액
                , bfday_mgaf_dc_act_amt                   -- 전일마감후대출계정금액
                , bfday_mgaf_un_jks_tjum_amt              -- 전일마감후비적수타점금액
                , tday_mgbf_dep_act_amt                   -- 금일마감전예금계정금액
                , tday_mgbf_dc_act_amt                    -- 금일마감전대출계정금액
                , tday_mgbf_un_jks_tjum_amt               -- 금일마감전비적수타점금액
                , tday_mgaf_dep_act_amt                   -- 금일마감후예금계정금액
                , tday_mgaf_dc_act_amt                    -- 금일마감후대출계정금액
                , tday_mgaf_un_jks_tjum_amt               -- 금일마감후비적수타점금액
                , offset_amt                              -- 차감금액
                , inirt_lst_intgs_endt1                   -- 내부금리최종이자계산종료일자1
                , lst_chg_brno                            -- 최종변경점번호
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , acno                                    -- 계좌번호
          , cur_c                                   -- 통화code
          , trn                                     -- 회차
          , kwa_c                                   -- 과목code
          , acser                                   -- 계좌일련번호
          , now_dep_ac_s                            -- 현수신계좌상태
          , bf_dep_ac_s                             -- 전수신계좌상태
          , act_c                                   -- 계정code
          , smok_c                                  -- 세목code
          , busmok_c                                -- 부세목code
          , mkdt                                    -- 신규일자
          , dudt                                    -- 만기일자
          , cus_trxdt                               -- 고객거래일자
          , lst_jan_chg_dt                          -- 최종잔액변동일자
          , hji_dt                                  -- 해지일자
          , mkamt                                   -- 신규금액
          , ac_jan                                  -- 계좌잔액
          , jibul_psb_jan                           -- 지불가능잔액
          , lst_trxno                               -- 최종거래번호
          , grbrno                                  -- 관리점번호
          , hji_brno                                -- 해지점번호
          , ji_limt_amt                             -- 지급제한금액
          , japjwa_entr_amt                         -- 잡좌편입금액
          , lst_intgs_endt                          -- 최종이자계산종료일자
          , tbnkchk_ip_dt                           -- 타점권입금일자
          , rsave_jawon_g                           -- 재예치재원구분
          , trx_chan_u                              -- 거래채널유형
          , ac_fst_mkdt                             -- 계좌최초신규일자
          , rsave_psb_cnt                           -- 재예치가능횟수
          , ynj_re_mk_cnt                           -- 연장재신규횟수
          , rsave_drdt                              -- 재예치등록일자
          , rsave_tot_dep_conttm_g                  -- 재예치총수신계약기간구분
          , rsave_tterm_mcnt                        -- 재예치총기간월수
          , rsave_tot_dcnt                          -- 재예치총기간일수
          , rsave_dep_conttm_g                      -- 재예치수신계약기간구분
          , rsave_conttm_mcnt                       -- 재예치계약기간월수
          , rsave_cont_dcnt                         -- 재예치계약기간일수
          , rsave_addiyul_g                         -- 재예치가산이율구분
          , rsave_addiyul                           -- 재예치가산이율
          , auto_hji_sinc_yn                        -- 자동해지신청여부
          , auto_hji_link_acno                      -- 자동해지연결계좌번호
          , auto_hji_linkac_kwa_c                   -- 자동해지연결계좌과목code
          , auto_hji_link_acser                     -- 자동해지연결계좌일련번호
          , pldg_cret_tamt                          -- 질권설정총금액
          , trx_limt_lst_drdt                       -- 거래제한최종등록일자
          , law_ji_limt_cnt                         -- 법적지급제한건수
          , jan_doc_lst_bal_dt                      -- 잔액증명서최종발급일자
          , bbk_nus_g                               -- 통장미사용구분
          , bbk_char_c                              -- 통장성격code
          , bbk_profd_natv_no                       -- 통장증서고유번호
          , bbk_imptdoc_c                           -- 통장중요증서code
          , now_bbk_fst_trxno                       -- 현통장최초거래번호
          , gijang_stt_trxno                        -- 기장시작거래번호
          , rissu_cnt                               -- 재발행횟수
          , iwol_cnt                                -- 이월횟수
          , lst_baldt                               -- 최종발행일자
          , lst_gijang_line_cnt                     -- 최종기장line수
          , lst_gijang_jan                          -- 최종기장잔액
          , lst_gijang_dt                           -- 최종기장일자
          , lst_gijang_brno                         -- 최종기장점번호
          , cmm_mem_lst_gijang_no                   -- 공통메모최종기장번호
          , ttax_gijang_yr                          -- 종합과세기장년
          , apchuk_mgr_apl_g                        -- 압축기장적용구분
          , cusgd_prn_dt                            -- 고객등급인자일자
          , gijang_resrc_g                          -- 기장자원구분
          , jongtong_sosok_wichi_line_cnt           -- 종통소속위치line수
          , apchuk_mgr_lst_dt                       -- 압축기장최종일자
          , linkac_dryn                             -- 연결계좌등록여부
          , japik_excpt_dryn                        -- 잡익제외등록여부
          , police_rqst_sago_yn                     -- 경찰청의뢰사고여부
          , tamt_apryu_cnt                          -- 전액압류건수
          , ilbu_apryu_cnt                          -- 일부압류건수
          , ilbu_apryu_amt                          -- 일부압류금액
          , ji_stop_cnt                             -- 지급정지건수
          , cc_oi_jistop_yn                         -- cc외지급정지여부
          , dj_trx_hji_yn                           -- 당좌거래해지여부
          , ip_jistop_yn                            -- 입금지급정지여부
          , crime_sago_yn                           -- 범죄사고여부
          , obnk_pldg_dr_yn                         -- 당행질권등록여부
          , torg_pldg_dr_yn                         -- 타기관질권등록여부
          , int_ji_stop_yn                          -- 이자지급정지여부
          , ingam_loss_yn                           -- 인감분실여부
          , ingam_loss_wire_um                      -- 인감분실유선유무
          , bbk_loss_yn                             -- 통장분실여부
          , bbk_loss_wire_um                        -- 통장분실유선유무
          , jeonbumr_yn                             -- 전부명령여부
          , comm_nm_ac_yn                           -- 공동명의계좌여부
          , sign_mk_ac_yn                           -- 서명신규계좌여부
          , dep_dc_ac_yn                            -- 수신대출계좌여부
          , dep_adint_jeyn                          -- 예금원가제외여부
          , jongtong_dmb_jegong_yn                  -- 종통담보제공여부
          , bkrp_sungo_hjae_dt                      -- 파산선고해제일자
          , psn_dbtr_revv_hjae_dt                   -- 개인채무자회생해제일자
          , cnsr_bkrp_hjae_dt                       -- 소비자파산해제일자
          , psn_wkout_hjae_dt                       -- 개인워크아웃해제일자
          , ip_ji_stop_hjae_dt                      -- 입금지급정지해제일자
          , cc_proc_dryn                            -- 센터컷처리등록여부
          , tel_iche_svc_sinc_yn                    -- 전화이체서비스신청여부
          , cash_fee_atcal_jeyn                     -- 현찰수수료자동계산제외여부
          , spiyul_jkyn                             -- 특별이율적용여부
          , fcur_invs_obj_c                         -- 외화투자목적code
          , for_upmu_b_c                            -- 외환업무분류code
          , bf_cur_c                                -- 전통화code
          , mnd_rel_yn                              -- 국방부관련여부
          , mnd_lc_no                               -- 국방부신용장번호
          , mnd_lc_amt                              -- 국방부신용장금액
          , mnd_iyul_sprd_rt                        -- 국방부이율spread율
          , mnd_libor_irt                           -- 국방부libor금리
          , subo_dmb_yn                             -- 수입보증금담보여부
          , subo_dmb_amt                            -- 수입보증금담보금액
          , gtcur_mk_iyul                           -- 기타통화신규이율
          , bas_iyul                                -- 기본이율
          , addiyul                                 -- 가산이율
          , inirt_lst_intgs_endt                    -- 내부금리최종이자계산종료일자
          , mrkt_in_int_samt                        -- 시장내부이자합계금액
          , fnd_ar_adj_int_samt                     -- 자금부조정이자합계금액
          , plcy_adj_int_samt                       -- 정책조정이자합계금액
          , saupb_adj_int_samt                      -- 사업부조정이자합계금액
          , sdd_prmm_int_samt                       -- 유동성프리미엄이자합계금액
          , spar_iyul_samt                          -- 예비이율합계금액
          , iyul_aprvno                             -- 이율승인번호
          , fdep_jks                                -- 외화예금적수
          , fdep_jks2                               -- 외화예금적수2
          , lst_ksdt                                -- 최종결산일자
          , ji_int_samt                             -- 지급이자합계금액
          , fji_int_samt                            -- 외화지급이자합계금액
          , inet_recv_no                            -- internet수신번호
          , fdep_lst_jkrip_no                       -- 외화예금최종적립번호
          , conttm_mcnt                             -- 계약기간월수
          , cont_dcnt                               -- 계약기간일수
          , int_ji_wcur_fcur_g                      -- 이자지급원화외화구분
          , trn1_fcur_jiamt                         -- 1회차외화지급금액
          , int_ji_cnt                              -- 이자지급횟수
          , ilbu_hji_cnt                            -- 일부해지건수
          , sdtax_samt                              -- 소득세합계금액
          , rtax_samt                               -- 주민세합계금액
          , cotax_samt                              -- 법인세합계금액
          , act_proc_dt                             -- 계정처리일자
          , bfday_mgbf_dep_act_amt                  -- 전일마감전예금계정금액
          , bfday_mgbf_dc_act_amt                   -- 전일마감전대출계정금액
          , bfday_mgbf_un_jks_tjum_amt              -- 전일마감전비적수타점금액
          , bfday_mgaf_dep_act_amt                  -- 전일마감후예금계정금액
          , bfday_mgaf_dc_act_amt                   -- 전일마감후대출계정금액
          , bfday_mgaf_un_jks_tjum_amt              -- 전일마감후비적수타점금액
          , tday_mgbf_dep_act_amt                   -- 금일마감전예금계정금액
          , tday_mgbf_dc_act_amt                    -- 금일마감전대출계정금액
          , tday_mgbf_un_jks_tjum_amt               -- 금일마감전비적수타점금액
          , tday_mgaf_dep_act_amt                   -- 금일마감후예금계정금액
          , tday_mgaf_dc_act_amt                    -- 금일마감후대출계정금액
          , tday_mgaf_un_jks_tjum_amt               -- 금일마감후비적수타점금액
          , offset_amt                              -- 차감금액
          , inirt_lst_intgs_endt1                   -- 내부금리최종이자계산종료일자1
          , lst_chg_brno                            -- 최종변경점번호
      from w0_shb.dws_sda_fdep_mas
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
