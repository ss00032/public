# -*- coding: utf-8 -*-

from datetime import datetime
import pendulum

from airflow import DAG
from sgd import config
from sgd.operators.l1_load_operator import L1LoadOperator
from sgd.log_util import *
from sgd.utils import *


__author__     = "유수형"
__copyright__  = "Copyright 2021, Shinhan Datadam"
__credits__    = ["유수형"]
__version__    = "1.0"
__maintainer__ = "유수형"
__email__      = "april@lgcns.com"
__status__     = "Production"


"""
L0 데이터를 L1 으로 적재하는 DAG 템플릿

[ 적용 방법 ]

제공된 ETL 개발 템플릿에서
아래 수정 대상 '(@)' 부분만 변경해서 바로 실행 가능

(@) 변경 대상 :
  - 프로그램 ID
  - INSERT 쿼리

"""

################################################################################
### Start of Batch Configuration

""" 
(@) 프로그램 ID
"""
pgm_id = 'IBBD_L1_APPEND_TABLE_TG'


# 적재 Layer
target_layer = 'l1'

# pgm_id 파싱하여 변수 세팅
# 사용목적코드, 프로그램적재구분, 그룹사코드, 적재시점코드, 테이블명, TG, DAG TAGS
(up_cd, pt_cd, cp_cd, tm_cd, target_table, tg_cd, tags) = parse_pgm_id(pgm_id)

use_purpose = tags[0]
company_code = tags[2]

# 적재 스키마명
target_schema = f'{target_layer}_{company_code}'


""" 
(@) INSERT용 L0 테이블 조회 쿼리
"""
select_sql_for_insert = f"""
    insert into {target_schema}.{target_table} select *, sysdate from l0_schema.l0_table
"""

### End of Batch Configuration
################################################################################

""" DAG 공통 파라미터 """
args = {
    'owner': config.sgd_env['dag_owner'],
    'retries': config.sgd_env['retries'],
    'retry_delay': config.sgd_env['retry_delay'],
    'provide_context': True,
    'on_failure_callback': handle_etl_error,
    'company_code' : company_code,
    'use_purpose' : use_purpose
}

# DAG ID 는 프로그램명과 동일
dag_id = pgm_id

# 실프로그램은 이 부분 제거
tags.append('sample')
#######################

with DAG(
    dag_id=dag_id,
    description=f'{dag_id} DAG',
    start_date=config.sgd_env['start_date'],
    schedule_interval=None,
    default_args=args,
    tags=tags,
    catchup=False) as dag :

    l1_load_task = L1LoadOperator(
        task_id='l1_load_task',
        target_schema=target_schema,
        target_table=target_table,
        select_sql_for_insert=select_sql_for_insert,
    )
