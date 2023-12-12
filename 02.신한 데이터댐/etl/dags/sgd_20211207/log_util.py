# -*- coding: utf-8 -*-
import logging
import json

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context

log = logging.getLogger(__name__)


def _get_sgd_etl_history_conn():
    hook = PostgresHook(
        postgres_conn_id='sgd_dl_etl_history'
    )
    return hook.get_conn()


def handle_etl_error(context):
    dag_id = ''
    try:
        ti = context.get("task_instance")
        dag_run = context.get('dag_run')
        dag_id = ti.dag_id
        task_id = ti.task_id
        run_id = dag_run.run_id
        run_type = dag_run.run_type
        try_number = ti.try_number - 1
        dagrunconf_json = None
        if len(dag_run.conf) > 0:
            dagrunconf_json = json.dumps(dag_run.conf)

        log.info(f'handle_etl_error={dag_id}')
        conn = _get_sgd_etl_history_conn()
        with conn.cursor() as cursor:
            error_msg = str(context.get("exception")).replace("'", "\"")

            sql = """
                insert into sgdhistory.tbl_etl_job_history (
                dag_id, task_id, try_number, run_id, run_type, 
                start_dt, end_dt, execution_dt, status, error_msg,
                dagrun_conf)
                values 
                (%s, %s, %s, %s, %s,
                %s, %s, %s, 'F', %s, %s)
            """
            params = (dag_id, task_id, try_number, run_id, run_type,
                      ti.start_date, ti.end_date, context.get('execution_date'), error_msg,
                      dagrunconf_json)
            cursor.execute(sql, params)
            conn.commit()
            log.info(f'handle etl error logging => {params}')
        conn.close()
    except Exception as e:
        log.error(f'handle etl error logging fail [{dag_id}]: {e}')
        raise


def handle_etl_history(data_count=None, data_size=None):
    dag_id = ''
    logging_id = None
    try:
        context = get_current_context()
        ti = context.get('task_instance')
        dag_run = context.get('dag_run')
        dag_id = ti.dag_id
        task_id = ti.task_id
        run_id = dag_run.run_id
        run_type = dag_run.run_type
        try_number = ti.try_number - 1
        dagrunconf_json = None
        if len(dag_run.conf) > 0:
            dagrunconf_json = json.dumps(dag_run.conf)

        log.info(f'handle etl logging={dag_id}')
        conn = _get_sgd_etl_history_conn()
        with conn.cursor() as cursor:
            sql = """
                insert into sgdhistory.tbl_etl_job_history (
                dag_id, task_id, try_number, run_id, run_type,  
                start_dt, end_dt, execution_dt, status, data_count, data_size, 
                dagrun_conf)
                VALUES 
                (%s, %s, %s, %s, %s, 
                %s, current_timestamp, %s, %s, %s, %s, 
                %s) RETURNING id
            """
            params = (dag_id, task_id, try_number, run_id, run_type,
                      ti.start_date, context.get('execution_date'), 'S', data_count, data_size,
                      dagrunconf_json)
            cursor.execute(sql, params)
            logging_id = cursor.fetchone()[0]
            conn.commit()
            log.info(f'handle etl logging [id:{logging_id}] => {params}')
        conn.close()
    except Exception as e:
        log.error(f'handle etl logging fail [{dag_id}]: {e}')
        raise
    return logging_id

