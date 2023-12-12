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
pgm_id = 'ILBD_DMA_여신계좌정보_TG'

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
s3_file_prefix = f'ibd_dma_여신계좌정보_/ibd_dma_여신계좌정보_{execution_kst}'

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
        delete from l0_shb.dma_여신계좌정보
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dma_여신계좌정보
                (
                  aws_ls_dt                               -- aws적재일시
                , 고객번호_ci                                 -- 고객번호
                , 고객번호                                    -- 고객번호
                , 계좌번호                                    -- 계좌번호
                , 계정code                                  -- 계정code
                , 세목code                                  -- 세목code
                , 부세목code                                 -- 부세목code
                , 통화code                                  -- 통화code
                , 실행번호                                    -- 실행번호
                , 계좌관리점번호                                 -- 계좌관리점번호
                , 계정구분                                    -- 계정구분
                , 계정구분명                                   -- 계정구분명
                , 상품code                                  -- 상품code
                , 상품code명                                 -- 상품code명
                , 정책code                                  -- 정책code
                , 정책code명                                 -- 정책code명
                , 과목code                                  -- 과목code
                , 과목code명                                 -- 과목code명
                , 계좌상태                                    -- 계좌상태
                , 계좌상태명                                   -- 계좌상태명
                , 구관리은행구분                                 -- 구관리은행구분
                , 구계좌번호                                   -- 구계좌번호
                , 약정시작일자                                  -- 약정시작일자
                , 약정만기일자                                  -- 약정만기일자
                , 약정해제일자                                  -- 약정해제일자
                , 최초실행일자                                  -- 최초실행일자
                , 최종거래일자                                  -- 최종거래일자
                , 대출실행일자                                  -- 대출실행일자
                , 대출만기일자                                  -- 대출만기일자
                , 승인일자                                    -- 승인일자
                , 원금완제일자                                  -- 원금완제일자
                , 운용구분                                    -- 운용구분
                , 운용구분명                                   -- 운용구분명
                , 만기도래잔존개월수                               -- 만기도래잔존개월수
                , 금리구분                                    -- 금리구분
                , 금리구분명                                   -- 금리구분명
                , 여신금리code                                -- 여신금리code
                , 여신금리code명                               -- 여신금리code명
                , 여신금리기간code                              -- 여신금리기간code
                , 여신금리기간code명                             -- 여신금리기간code명
                , 여신채권보전code                              -- 여신채권보전code
                , 여신채권보전code명                             -- 여신채권보전code명
                , 집단대출종류                                  -- 집단대출종류
                , 집단대출종류명                                 -- 집단대출종류명
                , 전결구분                                    -- 전결구분
                , 전결구분명                                   -- 전결구분명
                , 상환방법                                    -- 상환방법
                , 상환방법명                                   -- 상환방법명
                , 합동code                                  -- 합동code
                , 합동code명                                 -- 합동code명
                , 대출유형                                    -- 대출유형
                , 대출유형명                                   -- 대출유형명
                , 자금용도code                                -- 자금용도code
                , 자금용도code명                               -- 자금용도code명
                , 담보종류                                    -- 담보종류
                , 담보종류명                                   -- 담보종류명
                , 매각여부                                    -- 매각여부
                , 승인번호                                    -- 승인번호
                , 승인일련번호                                  -- 승인일련번호
                , 대환여부                                    -- 대환여부
                , 기준이율                                    -- 기준이율
                , 가산이율                                    -- 가산이율
                , 자금부조정이율                                 -- 자금부조정이율
                , 정책조정금이율                                 -- 정책조정금이율
                , 시장본지점이율                                 -- 시장본지점이율
                , 사업부본지점이율                                -- 사업부본지점이율
                , 유동성프리미엄이율                               -- 유동성프리미엄이율
                , 최종적용이율                                  -- 최종적용이율
                , abs원관리점번호                               -- abs원관리점번호
                , abs매각여부                                 -- abs매각여부
                , nplabs매각여부                              -- nplabs매각여부
                , 공사모기지론매각여부                              -- 공사모기지론매각여부
                , 학자금유동화여부                                -- 학자금유동화여부
                , 원리금연체기준일자                               -- 원리금연체기준일자
                , 원금연체기준일자                                -- 원금연체기준일자
                , 전일계좌잔액                                  -- 전일계좌잔액
                , 계좌잔액                                    -- 계좌잔액
                , 원화환산잔액                                  -- 원화환산잔액
                , 미화환산잔액                                  -- 미화환산잔액
                , 한도금액                                    -- 한도금액
                , 원화환산한도금액                                -- 원화환산한도금액
                , 전월평잔                                    -- 전월평잔
                , 전월잔액                                    -- 전월잔액
                , 월중평잔                                    -- 월중평잔
                , 연체구분                                    -- 연체구분
                , 원리금연체일수                                 -- 원리금연체일수
                , 원리금연체월수                                 -- 원리금연체월수
                , 원리금연체횟수                                 -- 원리금연체횟수
                , 원금연체일수                                  -- 원금연체일수
                , 원금연체월수                                  -- 원금연체월수
                , 연체금액                                    -- 연체금액
                , ead기준담보배부금액                             -- ead기준담보배부금액
                , ead기준재무회계대손충당금금액                        -- ead기준재무회계대손충당금금액
                , ead기준관리회계대손충당금금액                        -- ead기준관리회계대손충당금금액
                , 집단대출관리번호                                -- 집단대출관리번호
                , 집단대출관리여부                                -- 집단대출관리여부
                , 펀드번호                                    -- 펀드번호
                , 전자방식외담대web약정여부                          -- 전자방식외담대web약정여부
                , 제휴업체번호                                  -- 제휴업체번호
                , 제휴업체명                                   -- 제휴업체명
                , 제휴업체관리점번호                               -- 제휴업체관리점번호
                , 한도미사용수수료최종계산일자                          -- 한도미사용수수료최종계산일자
                , 약정수수료금액                                 -- 약정수수료금액
                , 대출모집인번호                                 -- 대출모집인번호
                , dm최종적재일자                                -- dm최종적재일자
                , 분할상환개월수                                 -- 분할상환개월수
                , 신청번호                                    -- 신청번호
                , 만기일자지정상환금액                              -- 만기일자지정상환금액
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , 고객번호_ci                                 -- 고객번호
          , 고객번호                                    -- 고객번호
          , 계좌번호                                    -- 계좌번호
          , 계정code                                  -- 계정code
          , 세목code                                  -- 세목code
          , 부세목code                                 -- 부세목code
          , 통화code                                  -- 통화code
          , 실행번호                                    -- 실행번호
          , 계좌관리점번호                                 -- 계좌관리점번호
          , 계정구분                                    -- 계정구분
          , 계정구분명                                   -- 계정구분명
          , 상품code                                  -- 상품code
          , 상품code명                                 -- 상품code명
          , 정책code                                  -- 정책code
          , 정책code명                                 -- 정책code명
          , 과목code                                  -- 과목code
          , 과목code명                                 -- 과목code명
          , 계좌상태                                    -- 계좌상태
          , 계좌상태명                                   -- 계좌상태명
          , 구관리은행구분                                 -- 구관리은행구분
          , 구계좌번호                                   -- 구계좌번호
          , 약정시작일자                                  -- 약정시작일자
          , 약정만기일자                                  -- 약정만기일자
          , 약정해제일자                                  -- 약정해제일자
          , 최초실행일자                                  -- 최초실행일자
          , 최종거래일자                                  -- 최종거래일자
          , 대출실행일자                                  -- 대출실행일자
          , 대출만기일자                                  -- 대출만기일자
          , 승인일자                                    -- 승인일자
          , 원금완제일자                                  -- 원금완제일자
          , 운용구분                                    -- 운용구분
          , 운용구분명                                   -- 운용구분명
          , 만기도래잔존개월수                               -- 만기도래잔존개월수
          , 금리구분                                    -- 금리구분
          , 금리구분명                                   -- 금리구분명
          , 여신금리code                                -- 여신금리code
          , 여신금리code명                               -- 여신금리code명
          , 여신금리기간code                              -- 여신금리기간code
          , 여신금리기간code명                             -- 여신금리기간code명
          , 여신채권보전code                              -- 여신채권보전code
          , 여신채권보전code명                             -- 여신채권보전code명
          , 집단대출종류                                  -- 집단대출종류
          , 집단대출종류명                                 -- 집단대출종류명
          , 전결구분                                    -- 전결구분
          , 전결구분명                                   -- 전결구분명
          , 상환방법                                    -- 상환방법
          , 상환방법명                                   -- 상환방법명
          , 합동code                                  -- 합동code
          , 합동code명                                 -- 합동code명
          , 대출유형                                    -- 대출유형
          , 대출유형명                                   -- 대출유형명
          , 자금용도code                                -- 자금용도code
          , 자금용도code명                               -- 자금용도code명
          , 담보종류                                    -- 담보종류
          , 담보종류명                                   -- 담보종류명
          , 매각여부                                    -- 매각여부
          , 승인번호                                    -- 승인번호
          , 승인일련번호                                  -- 승인일련번호
          , 대환여부                                    -- 대환여부
          , 기준이율                                    -- 기준이율
          , 가산이율                                    -- 가산이율
          , 자금부조정이율                                 -- 자금부조정이율
          , 정책조정금이율                                 -- 정책조정금이율
          , 시장본지점이율                                 -- 시장본지점이율
          , 사업부본지점이율                                -- 사업부본지점이율
          , 유동성프리미엄이율                               -- 유동성프리미엄이율
          , 최종적용이율                                  -- 최종적용이율
          , abs원관리점번호                               -- abs원관리점번호
          , abs매각여부                                 -- abs매각여부
          , nplabs매각여부                              -- nplabs매각여부
          , 공사모기지론매각여부                              -- 공사모기지론매각여부
          , 학자금유동화여부                                -- 학자금유동화여부
          , 원리금연체기준일자                               -- 원리금연체기준일자
          , 원금연체기준일자                                -- 원금연체기준일자
          , 전일계좌잔액                                  -- 전일계좌잔액
          , 계좌잔액                                    -- 계좌잔액
          , 원화환산잔액                                  -- 원화환산잔액
          , 미화환산잔액                                  -- 미화환산잔액
          , 한도금액                                    -- 한도금액
          , 원화환산한도금액                                -- 원화환산한도금액
          , 전월평잔                                    -- 전월평잔
          , 전월잔액                                    -- 전월잔액
          , 월중평잔                                    -- 월중평잔
          , 연체구분                                    -- 연체구분
          , 원리금연체일수                                 -- 원리금연체일수
          , 원리금연체월수                                 -- 원리금연체월수
          , 원리금연체횟수                                 -- 원리금연체횟수
          , 원금연체일수                                  -- 원금연체일수
          , 원금연체월수                                  -- 원금연체월수
          , 연체금액                                    -- 연체금액
          , ead기준담보배부금액                             -- ead기준담보배부금액
          , ead기준재무회계대손충당금금액                        -- ead기준재무회계대손충당금금액
          , ead기준관리회계대손충당금금액                        -- ead기준관리회계대손충당금금액
          , 집단대출관리번호                                -- 집단대출관리번호
          , 집단대출관리여부                                -- 집단대출관리여부
          , 펀드번호                                    -- 펀드번호
          , 전자방식외담대web약정여부                          -- 전자방식외담대web약정여부
          , 제휴업체번호                                  -- 제휴업체번호
          , 제휴업체명                                   -- 제휴업체명
          , 제휴업체관리점번호                               -- 제휴업체관리점번호
          , 한도미사용수수료최종계산일자                          -- 한도미사용수수료최종계산일자
          , 약정수수료금액                                 -- 약정수수료금액
          , 대출모집인번호                                 -- 대출모집인번호
          , dm최종적재일자                                -- dm최종적재일자
          , 분할상환개월수                                 -- 분할상환개월수
          , 신청번호                                    -- 신청번호
          , 만기일자지정상환금액                              -- 만기일자지정상환금액
      from w0_shb.dma_여신계좌정보
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
