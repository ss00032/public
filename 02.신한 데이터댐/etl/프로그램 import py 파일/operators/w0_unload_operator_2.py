# -*- coding: utf-8 -*-


from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from sgd import config
from sgd import utils
from sgd import log_util


class W0UnloadOperator(BaseOperator):
    """
    Redshift to S3 unload custom operator :

    DAG 소스코드에서 작성한 select_sql_for_unload 실행 결과를 S3에 파일로 내린다.
    """
    template_fields = ()
    template_ext = ()
    ui_color = '#99e699'

    @apply_defaults
    def __init__(self,
                company_code,
                use_purpose,
                schema,
                table,
                select_query,
                s3_key,
                verify = None,
                autocommit = False,
                unload_parallel = False,
                *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.company_code = company_code
        self.use_purpose = use_purpose
        self.schema = schema
        self.table = table
        self.select_query = select_query.strip()
        self.s3_bucket = f"{config.s3_env['stg_bucket_prefix']}-{self.company_code}-{utils.SGD_UP_CODES['a']}-1"
        self.s3_key = s3_key
        self.verify = verify
        self.unload_options = config.redshift_env['unload_options'][self.company_code]
        self.autocommit = autocommit
        self.include_header = config.redshift_env['include_header'][self.company_code]
        self.unload_parallel = unload_parallel


    def execute(self, context):

        self.log.info(f"""
            ### Data Unload ###
            schema={self.schema}, table={self.table},
            company_code={self.company_code}, use_purpose={self.use_purpose},
            select_query={self.select_query}
            s3_bucket={self.s3_bucket}, prefix={self.s3_key}
            """)

        redshift_conn_id = config.conn_id['dl_redshift'][self.use_purpose]
        self.hook = PostgresHook(postgres_conn_id=redshift_conn_id)
        aws_conn_id = config.conn_id['dl_s3']
        self.s3 = S3Hook(aws_conn_id=aws_conn_id, verify=self.verify)

        credentials = self.s3.get_credentials()
        unload_options = ' '.join(self.unload_options)

        select_query = self.select_query

        unload_command = """
                    UNLOAD ('{select_query}')
                    TO 's3://{s3_bucket}/{s3_key}/{table}_'
                    with credentials 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {unload_options}
                    {include_header}
                    {unload_parallel};
                    """.format(select_query=select_query,
                               table=self.table,
                               s3_bucket=self.s3_bucket,
                               s3_key=self.s3_key,
                               access_key=credentials.access_key,
                               secret_key=credentials.secret_key,
                               unload_options=unload_options,
                               include_header=self.include_header,
                               unload_parallel=self.unload_parallel
                               ),


        self.log.info(f"""
            ### Select Query For Unload ###
            {self.select_query}
            """)
        self.log.info('Executing UNLOAD command...')
        self.log.info(f"""
            ### Unload Command ###
            {self.unload_command}
            """)
        self.hook.run(unload_command, self.autocommit)
        self.log.info("UNLOAD command complete...")

        # logging etl job
        log_util.handle_etl_history(self.affected_rows)


    def _check_validation(self):
        self.log.info(f"""
            ### Check Validation ###
            company_code={self.company_code}, use_purpose={self.use_purpose}
            """)
        if not (utils.valid_company_code(self.company_code)
                or utils.valid_use_purpose(self.use_purpose)):
            raise AirflowException('Either company_code or use_purpose is invalid.')
