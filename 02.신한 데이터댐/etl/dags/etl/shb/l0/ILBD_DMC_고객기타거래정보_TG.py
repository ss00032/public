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
pgm_id = 'ILBD_DMC_고객기타거래정보_TG'

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
s3_file_prefix = f'ibd_dmc_고객기타거래정보_/ibd_dmc_고객기타거래정보_{execution_kst}'

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
        delete from l0_shb.dmc_고객기타거래정보
              where 1 = 1 
"""

""" 
(@) INSERT용 Working 테이블 조회 쿼리 (선택적)
"""
select_sql_for_insert = f"""
    insert into l0_shb.dmc_고객기타거래정보
                (
                  aws_ls_dt                               -- aws적재일시
                , 고객번호_ci                                 -- 고객번호
                , 고객번호                                    -- 고객번호
                , 급여이체여부                                  -- 급여이체여부
                , 신용카드보유여부                                -- 신용카드보유여부
                , 체크카드보유여부                                -- 체크카드보유여부
                , 인터넷뱅킹가입여부                               -- 인터넷뱅킹가입여부
                , 폰뱅킹가입여부                                 -- 폰뱅킹가입여부
                , ez뱅킹가입여부                                -- ez뱅킹가입여부
                , fna가입여부                                 -- fna가입여부
                , apt관리비이체여부                              -- apt관리비이체여부
                , cms모계좌보유여부                              -- cms모계좌보유여부
                , 연금신탁보유여부                                -- 연금신탁보유여부
                , 거치식펀드보유여부                               -- 거치식펀드보유여부
                , 적립식펀드보유여부                               -- 적립식펀드보유여부
                , 보험보유여부                                  -- 보험보유여부
                , 기업카드여부                                  -- 기업카드여부
                , 여신보유여부                                  -- 여신보유여부
                , 유동성대출보유여부                               -- 유동성대출보유여부
                , 거치식예금보유여부                               -- 거치식예금보유여부
                , 적립식예금보유여부                               -- 적립식예금보유여부
                , 나라사랑카드보유여부                              -- 나라사랑카드보유여부
                , 자동이체여부                                  -- 자동이체여부
                , 인터넷뱅킹활동여부                               -- 인터넷뱅킹활동여부
                , 폰뱅킹활동여부                                 -- 폰뱅킹활동여부
                , 체크카드활동여부                                -- 체크카드활동여부
                , smore통장보유여부                             -- smore통장보유여부
                , 신한카드활동여부                                -- 신한카드활동여부
                , 신한카드결제계좌보유여부                            -- 신한카드결제계좌보유여부
                , mmda보유여부                                -- mmda보유여부
                , mmf보유여부                                 -- mmf보유여부
                , eld상품보유여부                               -- eld상품보유여부
                , 외화예금보유여부                                -- 외화예금보유여부
                , 연금보험보유여부                                -- 연금보험보유여부
                , 적립식상품보유여부                               -- 적립식상품보유여부
                , 생계형예금가입여부                               -- 생계형예금가입여부
                , 장기주택마련저축보유여부                            -- 장기주택마련저축보유여부
                , 청약상품보유여부                                -- 청약상품보유여부
                , 마이너스통장보유여부                              -- 마이너스통장보유여부
                , 신용대출보유여부                                -- 신용대출보유여부
                , 모기지론보유여부                                -- 모기지론보유여부
                , 세금우대한도사용여부                              -- 세금우대한도사용여부
                , 기준일자                                    -- 기준일자
                , 최근3개월간전자금융수수료발생여부                       -- 최근3개월간전자금융수수료발생여부
                , 연금신탁보유여부290                             -- 연금신탁보유여부290
                , 수익증권보유여부                                -- 수익증권보유여부
                , 기업대출보유여부                                -- 기업대출보유여부
                , dm최종적재일자                                -- dm최종적재일자
                , 현금카드보유여부                                -- 현금카드보유여부
                , dm최종적재일자1                               -- dm최종적재일자1
                , vm뱅킹가입여부                                -- vm뱅킹가입여부
                , 보험이체여부                                  -- 보험이체여부
                , 전기요금이체여부                                -- 전기요금이체여부
                , 핸드폰이체여부                                 -- 핸드폰이체여부
                , 베이직팩가입여부                                -- 베이직팩가입여부
                , 베이직팩가입일자                                -- 베이직팩가입일자
                , 베이직팩등록점번호                               -- 베이직팩등록점번호
                , 신한월복리보유여부                               -- 신한월복리보유여부
                , mycar대출여부                               -- mycar대출여부
                , s뱅크가입여부                                 -- s뱅크가입여부
                , myshop케어가입여부                            -- myshop케어가입여부
                , 신한카드가맹점여부                               -- 신한카드가맹점여부
                , 가맹점결제계좌당행여부                             -- 가맹점결제계좌당행여부
                , 퇴직연금가입여부                                -- 퇴직연금가입여부
                , 회사설립일                                   -- 회사설립일
                , 미래설계통장가입여부                              -- 미래설계통장가입여부
                , s뱅킹미로그인여부                               -- s뱅킹미로그인여부
                , 주택담보대출보유여부                              -- 주택담보대출보유여부
                , 일임형isa가입여부                              -- 일임형isa가입여부
                , isa가입여부                                 -- isa가입여부
                , 최근6개월신한카드대금출금여부                         -- 최근6개월신한카드대금출금여부
                , 판클럽가입여부                                 -- 판클럽가입여부
                , s알리미가입여부                                -- s알리미가입여부
                , 신한플러스가입여부                               -- 신한플러스가입여부
                )
    select  current_timestamp AT TIME ZONE 'Asia/Seoul' 
          , 고객번호_ci                                 -- 고객번호
          , 고객번호                                    -- 고객번호
          , 급여이체여부                                  -- 급여이체여부
          , 신용카드보유여부                                -- 신용카드보유여부
          , 체크카드보유여부                                -- 체크카드보유여부
          , 인터넷뱅킹가입여부                               -- 인터넷뱅킹가입여부
          , 폰뱅킹가입여부                                 -- 폰뱅킹가입여부
          , ez뱅킹가입여부                                -- ez뱅킹가입여부
          , fna가입여부                                 -- fna가입여부
          , apt관리비이체여부                              -- apt관리비이체여부
          , cms모계좌보유여부                              -- cms모계좌보유여부
          , 연금신탁보유여부                                -- 연금신탁보유여부
          , 거치식펀드보유여부                               -- 거치식펀드보유여부
          , 적립식펀드보유여부                               -- 적립식펀드보유여부
          , 보험보유여부                                  -- 보험보유여부
          , 기업카드여부                                  -- 기업카드여부
          , 여신보유여부                                  -- 여신보유여부
          , 유동성대출보유여부                               -- 유동성대출보유여부
          , 거치식예금보유여부                               -- 거치식예금보유여부
          , 적립식예금보유여부                               -- 적립식예금보유여부
          , 나라사랑카드보유여부                              -- 나라사랑카드보유여부
          , 자동이체여부                                  -- 자동이체여부
          , 인터넷뱅킹활동여부                               -- 인터넷뱅킹활동여부
          , 폰뱅킹활동여부                                 -- 폰뱅킹활동여부
          , 체크카드활동여부                                -- 체크카드활동여부
          , smore통장보유여부                             -- smore통장보유여부
          , 신한카드활동여부                                -- 신한카드활동여부
          , 신한카드결제계좌보유여부                            -- 신한카드결제계좌보유여부
          , mmda보유여부                                -- mmda보유여부
          , mmf보유여부                                 -- mmf보유여부
          , eld상품보유여부                               -- eld상품보유여부
          , 외화예금보유여부                                -- 외화예금보유여부
          , 연금보험보유여부                                -- 연금보험보유여부
          , 적립식상품보유여부                               -- 적립식상품보유여부
          , 생계형예금가입여부                               -- 생계형예금가입여부
          , 장기주택마련저축보유여부                            -- 장기주택마련저축보유여부
          , 청약상품보유여부                                -- 청약상품보유여부
          , 마이너스통장보유여부                              -- 마이너스통장보유여부
          , 신용대출보유여부                                -- 신용대출보유여부
          , 모기지론보유여부                                -- 모기지론보유여부
          , 세금우대한도사용여부                              -- 세금우대한도사용여부
          , 기준일자                                    -- 기준일자
          , 최근3개월간전자금융수수료발생여부                       -- 최근3개월간전자금융수수료발생여부
          , 연금신탁보유여부290                             -- 연금신탁보유여부290
          , 수익증권보유여부                                -- 수익증권보유여부
          , 기업대출보유여부                                -- 기업대출보유여부
          , dm최종적재일자                                -- dm최종적재일자
          , 현금카드보유여부                                -- 현금카드보유여부
          , dm최종적재일자1                               -- dm최종적재일자1
          , vm뱅킹가입여부                                -- vm뱅킹가입여부
          , 보험이체여부                                  -- 보험이체여부
          , 전기요금이체여부                                -- 전기요금이체여부
          , 핸드폰이체여부                                 -- 핸드폰이체여부
          , 베이직팩가입여부                                -- 베이직팩가입여부
          , 베이직팩가입일자                                -- 베이직팩가입일자
          , 베이직팩등록점번호                               -- 베이직팩등록점번호
          , 신한월복리보유여부                               -- 신한월복리보유여부
          , mycar대출여부                               -- mycar대출여부
          , s뱅크가입여부                                 -- s뱅크가입여부
          , myshop케어가입여부                            -- myshop케어가입여부
          , 신한카드가맹점여부                               -- 신한카드가맹점여부
          , 가맹점결제계좌당행여부                             -- 가맹점결제계좌당행여부
          , 퇴직연금가입여부                                -- 퇴직연금가입여부
          , 회사설립일                                   -- 회사설립일
          , 미래설계통장가입여부                              -- 미래설계통장가입여부
          , s뱅킹미로그인여부                               -- s뱅킹미로그인여부
          , 주택담보대출보유여부                              -- 주택담보대출보유여부
          , 일임형isa가입여부                              -- 일임형isa가입여부
          , isa가입여부                                 -- isa가입여부
          , 최근6개월신한카드대금출금여부                         -- 최근6개월신한카드대금출금여부
          , 판클럽가입여부                                 -- 판클럽가입여부
          , s알리미가입여부                                -- s알리미가입여부
          , 신한플러스가입여부                               -- 신한플러스가입여부
      from w0_shb.dmc_고객기타거래정보
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
