# -*- coding: utf-8 -*-

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from sgd import config_2
from sgd import utils
from sgd import log_util


class W0UnloadOperator(BaseOperator):
    """
    Redshift to S3 unload custom operator :

    DAG 소스코드에서 작성한 select_sql_for_unload 실행 결과를 S3에 파일로 내린다.
    타겟 경로에 파일이 있을 경우 OVERWRITE 한다.
    """
    template_fields = ('execution_kst','s3_key','select_query','unload_command',)
    template_ext = ()
    ui_color = '#99e699'

    @apply_defaults
    def __init__(self,
                 company_code,
                 use_purpose,
                 execution_kst,
                 schema,
                 table,
                 select_query,
                 s3_key,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.company_code = company_code
        self.use_purpose = use_purpose
        self.execution_kst = execution_kst
        self.schema = schema
        self.table = table
        self.select_query = select_query.strip()
        self.unload_command = ''
        self.s3_bucket = f"{config_2.s3_env['stg_bucket_prefix']}-{self.company_code}-{utils.SGD_UP_CODES['a']}-1"
        self.s3_key = s3_key
        self.unload_options = config_2.redshift_env['unload_options'][self.company_code]
        self.include_header = config_2.redshift_env['include_header'][self.company_code]
        self.unload_parallel = 'PARALLEL ON'  # default option
        self.allow_overwrite = 'allowoverwrite'  # 디폴트 옵션에서는 overwirte 하지 않고 중단됨
        self.verify = None
        self.autocommit = False

    def execute(self, context):

        self.log.info(f"""
        
            ### Data Unload ###
            schema: {self.schema}
            table: {self.table}
            company_code: {self.company_code}
            use_purpose: {self.use_purpose}
            s3_bucket: {self.s3_bucket}
            prefix: {self.s3_key}
            
            unload_parallel: {self.unload_parallel}
            allow_overwrite: {self.allow_overwrite}
            
            select_query: {self.select_query}

            ### Execution Date ###
            execution_kst: {self.execution_kst}
            
            """)
        self._check_validation()

        redshift_conn_id = config_2.conn_id['dl_redshift'][self.use_purpose]
        hook = PostgresHook(postgres_conn_id=redshift_conn_id)

        unload_options = ' '.join(self.unload_options)
        unload_command = """
            UNLOAD ('{select_query}')
            TO 's3://{s3_bucket}/{s3_key}'
            iam_role '{iam_role}'
            {unload_options}
            {include_header}
            {allow_overwrite}
            {unload_parallel};
            """.format(select_query=self.select_query,
                       s3_bucket=self.s3_bucket,
                       s3_key=self.s3_key,
                       table=self.table,
                       iam_role=config_2.redshift_env['iam_role']['igd'],
                       unload_options=unload_options,
                       include_header=self.include_header,
                       allow_overwrite=self.allow_overwrite,
                       unload_parallel=self.unload_parallel)

        self.log.info(f"""
            ### Unload Command ###
            {unload_command}
            """)
        hook.run(unload_command, self.autocommit)
        self.log.info("### UNLOAD complete ###")

        # logging etl job
        log_util.handle_etl_history()

    def _check_validation(self):
        if not (utils.valid_company_code(self.company_code)
                and utils.valid_use_purpose(self.use_purpose)):
            raise AirflowException('Either company_code or use_purpose is invalid.')
