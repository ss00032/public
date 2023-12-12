# -*- coding: utf-8 -*-

from datetime import datetime
from datetime import timedelta
import pendulum

SGD_TZ = pendulum.timezone('Asia/Seoul')
DEV_FLAG = 'dev'                             # prd/dev 구분

sgd_env = {
    'dag_owner': 'airflow',
    'wk_layer': 'w0',                                            # Working Layer
    'suffix_view': '_vw',                                        # View Suffix
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2021, 1, 1, tzinfo=SGD_TZ),           # DAG 시작일
}

conn_id = {
    'dl_s3': 'sgd_dl_s3_conn_id',                                # S3 connect
    'dl_redshift': {
        'igd': 'sgd_dl_igd_redshift_conn_id',                    # Redshift IGD cluster
        'acd': 'sgd_dl_acd_redshift_conn_id',                    # Redshift ACD cluster
    },
}

s3_env = {
    'stg_bucket_prefix': f'shcw-an2-datadam-{DEV_FLAG}-s3-dl-stg',
    'bck_bucket_prefix': f'shcw-an2-datadam-{DEV_FLAG}-s3-dl-bck',
}

redshift_env = {
    'copy_options': {
        'shb': ["csv ignoreheader 1 delimiter ','"],
        'shc': ["csv delimiter '|'"],
        'shi': ["csv ignoreheader 1 delimiter '~'"],
        'shl': ["csv delimiter '|'"],
    },
    'unload_options': {
        'shb': ("csv","DELIMITER AS ','"),
        'shc': ("csv","DELIMITER AS '|'"),
        'shi': ("csv","DELIMITER AS '~'"),
        'shl': ("csv","DELIMITER AS '|'"),
    },
    'include_header': {
        'shb': 'HEADER',
        'shc': '',
        'shi': 'HEADER',
        'shl': '',
    },
    'unload_parallel': {
        'on': 'PARALLEL ON',
        'off': 'PARALLEL OFF',
    },
    'iam_role': {
        'igd': 'arn:aws:iam::796123547122:role/SHCW-DataDam-DEV-ROL-REDSHIFT-IGD-1',
        'acd': 'arn:aws:iam::796123547122:role/SHCW-DataDam-DEV-ROL-REDSHIFT-ACD-1',
    }
}
