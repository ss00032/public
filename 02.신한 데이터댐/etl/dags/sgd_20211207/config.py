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
    'dl_ezgator': 'sgd_dl_ezgator_conn_id',
}

s3_env = {
    'stg_bucket_prefix': f'shcw-an2-datadam-{DEV_FLAG}-s3-dl-stg',
    'bck_bucket_prefix': f'shcw-an2-datadam-{DEV_FLAG}-s3-dl-bck',
}

redshift_env = {
    'copy_options': {
        'shb': ["csv delimiter '\037'"],
        'shc': ["csv delimiter '|'"],
        'shi': ["csv ignoreheader 1 delimiter '~'"],
        'shl': ["csv delimiter '|'"],
        'pbc': ["csv delimiter ','"],
    },
    'unload_options': {
        'shb': ("csv","DELIMITER AS '\037'"),
        'shc': ("csv","DELIMITER AS '|'"),
        'shi': ("csv","DELIMITER AS '~'"),
        'shl': ("csv","DELIMITER AS '|'"),
        'pbc': ("csv", "DELIMITER AS ','"),
    },
    'include_header': {
        'shb': '',
        'shc': '',
        'shi': 'HEADER',
        'shl': '',
        'pbc': '',
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

ezgator_env = {
    # 데이터댐 수신
    'recv': {
        'shell_path': '/shcsw/ezgator/client/ezgatorsend.sh',
        'if_nm_fmt': 'BIR{cp_cd}_{ez_seq}',
        # /로 끝나게 설정 (상위 경로는 IF에서 설정)
        'com_path' : {
            'shb_igd': '',
            'shb_acd': '',
            'shc_igd': '',
            'shc_acd': '',
            'shl_igd': '',
            'shl_acd': '',
        },
        # /로 끝나게 설정 (상위 경로는 IF에서 설정)
        'sgd_path' : {
            'shb_igd': '',
            'shb_acd': '',
            'shc_igd': '',
            'shc_acd': '',
            'shl_igd': '',
            'shl_acd': '',
        },
    },
    # 데이터댐 송신
    'send': {
        'shell_path': '/shcsw/ezgator/client/ezgatorsend.sh',
        'if_nm_fmt': 'BIS{cp_cd}_{ez_seq}',
        # /로 끝나게 설정
        'com_path': {
            'shb_igd': '',
            'shb_acd': '',
            'shc_igd': '',
            'shc_acd': '',
            'shl_igd': '',
            'shl_acd': '',
        },
        # /로 끝나게 설정
        'sgd_path' : {
            'shb_igd': 'shb/',
            'shb_acd': 'shb/',
            'shc_igd': 'shc/',
            'shc_acd': 'shc/',
            'shl_igd': 'shl/',
            'shl_acd': 'shl/',
        },
    },
    'list': {
        'shell_path': '/shcsw/ezgator/client/ezgatorlist.sh',
        'com_path': {
            'shb_igd': '',
            'shb_acd': '',
            'shc_igd': '/etl_dat/data/eai/snd/data/sgd',
            'shc_acd': '/etl_dat/data/eai/snd/data/sgd',
            'shl_igd': '/shcsw/ezgator/ftphome/snd',
            'shl_acd': '/shcsw/ezgator/ftphome/snd',
        },
        'system_nm': {
            'shb': 'SHINHAN_BANK',
            'shc': 'SHINHAN_CARD',
            'shl': 'TEMP_SHINHAN_DATADAM',
        },
    },
}
