# -*- coding: utf-8 -*-

import re
from typing import Callable, List, Optional, Union
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator
from airflow.exceptions import AirflowException
from sgd import config
from sgd import utils
from sgd import log_util


class S3SensorOperator(BaseSensorOperator):
    """
    S3 object sensing operator
    """
    template_fields = ('s3_key',)
    template_ext = ()
    ui_color = '#99e699'

    @apply_defaults
    def __init__(self,
                 company_code,
                 use_purpose,
                 s3_key,
                 mode='reschedule',  # 센서 모드 reschedule
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.company_code = company_code
        self.use_purpose = use_purpose
        self.s3_key = s3_key
        self.bucket_name = f"{config.s3_env['stg_bucket_prefix']}-{self.company_code}-{self.use_purpose}-1"
        self.wildcard_match = True
        self.aws_conn_id = config.conn_id['dl_s3']
        self.verify = None
        self.mode = mode
        self.poke_interval = 60*2  # sensing 간격 2분
        self.timeout = 60*10  # task timeout 10분

    def poke(self, context):

        self.log.info(f"""
        
            ### S3 Sensor ###
            company_code: {self.company_code}
            use_purpose: {self.use_purpose}
            bucket: {self.bucket_name}
            s3_key: {self.s3_key}
            
            wildcard_match : {self.wildcard_match}
            mode : {self.mode}
            
            """)
        self._check_validation()
        self.log.info('>> Poking for key : s3://%s/%s', self.bucket_name, self.s3_key)

        s3hook = self.get_hook()

        # object 하나 센싱
        if not self.wildcard_match:
            return s3hook.check_for_key(self.s3_key, self.bucket_name)

        # prefix 로 시작하는 objects 센싱
        elif self.wildcard_match:

            if s3hook.check_for_wildcard_key(f"{self.s3_key}*", self.bucket_name):
                keys = s3hook.list_keys(bucket_name=self.bucket_name, prefix=self.s3_key)
                self.log.info("### Keys ###")
                for k in keys:
                    self.log.info(f"Key : {k}")
                return True

        # logging etl job
        log_util.handle_etl_history()

    def get_hook(self) -> S3Hook:
        """Create and return an S3Hook"""
        hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        return hook

    def _check_validation(self):
        if not (utils.valid_company_code(self.company_code)
                and utils.valid_use_purpose(self.use_purpose)):
            raise AirflowException('Either company_code or use_purpose is invalid.')
