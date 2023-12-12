# -*- coding: utf-8 -*-

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from sgd import config
from sgd import utils
from sgd import log_util
from sgd import date_util


class StgBackupOperator(BaseOperator):
    """
    S3 file backup custom operator :

    1. stg 버킷 -> bck 버킷 file cp
      (실행일자 디렉토리 생성)
    2. stg 버킷에서 file rm
    """
    template_fields = ('execution_kst','s3_key',)
    template_ext = ()
    ui_color = '#99e699'

    @apply_defaults
    def __init__(self,
                 company_code,
                 use_purpose,
                 execution_kst,
                 s3_key,
                 verify=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.company_code = company_code
        self.use_purpose = use_purpose
        self.execution_kst = execution_kst
        self.s3_key = s3_key
        self.source_bucket_name = f"{config.s3_env['stg_bucket_prefix']}-{self.company_code}-{self.use_purpose}-1"
        self.dest_bucket_name = f"{config.s3_env['bck_bucket_prefix']}-{self.company_code}-{self.use_purpose}-1"
        self.aws_conn_id = config.conn_id['dl_s3']
        self.verify = verify

    def execute(self, context):

        self.log.info(f"""
        
            ### FILE COPY ###
            source_bucket_name: {self.source_bucket_name}
            prefix: {self.s3_key}
            dest_bucket_name: {self.dest_bucket_name}
            
            ### EXECUTION DATE ###
            execution_kst: {self.execution_kst}
            
            """)
        self._check_validation()

        s3_hook = S3Hook(self.aws_conn_id)
        keys = s3_hook.list_keys(bucket_name=self.source_bucket_name, prefix=self.s3_key)

        dir_date = date_util.get_current_dt()  # 실행일자 (백업 디렉토리명)

        if keys:
            self.log.info("directory date: {dir_date}")
            for k in keys:
                s3_hook.copy_object(source_bucket_key=k, dest_bucket_key=f"{dir_date}/{k}",
                                    source_bucket_name=self.source_bucket_name, dest_bucket_name=self.dest_bucket_name)
                self.log.info(f"Key Copied : {k}")

            self.log.info(f"""
                ### FILE COPY DONE ###
                """)
            self.log.info(f"""
            
                ### FILE DELETE ###
                bucket: {self.source_bucket_name}
                prefix: {self.s3_key}
                
                """)

            s3_hook.delete_objects(bucket=self.source_bucket_name, keys=keys)
            self.log.info(f"""
                ### FILE DELETE DONE###
                """)

        # logging etl job
        log_util.handle_etl_history()

    def _check_validation(self):
        if not (utils.valid_company_code(self.company_code)
                and utils.valid_use_purpose(self.use_purpose)):
            raise AirflowException('Either company_code or use_purpose is invalid.')
