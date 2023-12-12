# -*- coding: utf-8 -*-


from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from typing import Optional, Union
from sgd import config


class StgToBckOperator(BaseOperator):
    """
    S3 file backup custom operator :

    1. stg 버킷 -> bck 버킷 file cp
    2. stg 버킷에서 file rm
    """
    template_fields = ()
    template_ext = ()
    ui_color = '#99e699'

    @apply_defaults
    def __init__(self,
            company_code,
            use_purpose,
            s3_file_prefix,
            source_version_id=None,
            verify=None,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.company_code=company_code
        self.use_purpose=use_purpose
        self.s3_file_prefix=s3_file_prefix
        self.source_bucket_name = f"{config.s3_env['stg_bucket_prefix']}-{self.company_code}-{self.use_purpose}-1"
        self.dest_bucket_name = f"{config.s3_env['bck_bucket_prefix']}-{self.company_code}-{self.use_purpose}-1"
        self.source_version_id = source_version_id
        self.aws_conn_id = config.conn_id['dl_s3']
        self.verify = verify

    def execute(self, context):

        self.log.info(f"""
            ### FILE COPY ###
            source_bucket_name={self.source_bucket_name},
            prefix={self.s3_file_prefix},
            dest_bucket_name={self.dest_bucket_name}
            """)

        s3_hook = S3Hook(self.aws_conn_id)
        s3_key = self.s3_file_prefix
        keys = s3_hook.list_keys(bucket_name=self.source_bucket_name, prefix=s3_key)

        if keys:
            s3_hook = S3Hook(self.aws_conn_id, verify=self.verify)
            for k in keys:
                s3_hook.copy_object(source_bucket_key=k, dest_bucket_key=k,
                                     source_bucket_name=self.source_bucket_name, dest_bucket_name=self.dest_bucket_name)
                self.log.info(f"Key Copied : {k}")

        self.log.info(f"""### FILE COPY DONE ###""")

        self.log.info(f"""
            ### FILE DELETE ###
            bucket={self.source_bucket_name},
            prefix={self.s3_file_prefix},
            """)

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        keys = s3_hook.list_keys(bucket_name=self.source_bucket_name, prefix=self.s3_file_prefix)
        s3_hook.delete_objects(bucket=self.source_bucket_name, keys=keys)

        self.log.info(f"""### FILE DELETE DONE###""")
