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
                unload_options = ("CSV","DELIMITER AS '|'"),  # 파일 확장자, delimeter
                autocommit = False,
                include_header = False,
                *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.company_code = company_code
        self.use_purpose = use_purpose
        self.schema = schema
        self.table = table
        self.select_query = select_query.strip()
        self.s3_bucket = f"{config.s3_env['stg_bucket_prefix']}-{self.company_code}-acd-1"
        self.s3_key = s3_key
        self.verify = verify
        self.unload_options = unload_options
        self.include_header = include_header
        self.autocommit = autocommit
        self.include_header = config.redshift_env['include_header'][self.company_code]

        if self.include_header and \
           'PARALLEL OFF' not in [uo.upper().strip() for uo in unload_options]:
            self.unload_options = list(unload_options) + ['PARALLEL OFF', ]

    def execute(self, context):

        self.log.info(f"""
            ### Data Unload ###
            schema={self.schema}, table={self.table},
            company_code={self.company_code}, use_purpose={self.use_purpose},
            select_query={self.select_query}
            s3_bucket={self.s3_bucket}, s3_key={self.s3_key}
            """)

        redshift_conn_id = config.conn_id['dl_redshift'][self.use_purpose]
        self.hook = PostgresHook(postgres_conn_id=redshift_conn_id)
        aws_conn_id = config.conn_id['dl_s3']
        self.s3 = S3Hook(aws_conn_id=aws_conn_id, verify=self.verify)

        credentials = self.s3.get_credentials()
        unload_options = ' '.join(self.unload_options)

        if self.include_header:  # 헤더 포함
            self.log.info("Retrieving headers from %s.%s...",
                          self.schema, self.table)

            columns_query = """SELECT column_name
                                        FROM information_schema.columns
                                        WHERE table_schema = '{schema}'
                                        AND   table_name = '{table}'
                                        ORDER BY ordinal_position
                            """.format(schema=self.schema,
                                       table=self.table)

            cursor = self.hook.get_conn().cursor()
            cursor.execute(columns_query)
            rows = cursor.fetchall()
            columns = [row[0] for row in rows]
            column_names = ', '.join("{0}".format(c) for c in columns)
            column_headers = ', '.join("\\'{0}\\'".format(c) for c in columns)
            column_castings = ', '.join("CAST({0} AS text) AS {0}".format(c)
                                        for c in columns)

            select_query = """SELECT {column_names} FROM
                                    (SELECT 2 sort_order, {column_castings}
                                     FROM {schema}.{table}
                                    UNION ALL
                                    SELECT 1 sort_order, {column_headers})
                                 ORDER BY sort_order"""\
                            .format(column_names=column_names,
                                    column_castings=column_castings,
                                    column_headers=column_headers,
                                    schema=self.schema,
                                    table=self.table)
        else:  # 헤더 미포함
            select_query = self.select_query

        unload_query = """
                    UNLOAD ('{select_query}')
                    TO 's3://{s3_bucket}/{s3_key}/{table}_'
                    with credentials
                    'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {unload_options};
                    """.format(select_query=select_query,
                               table=self.table,
                               s3_bucket=self.s3_bucket,
                               s3_key=self.s3_key,
                               access_key=credentials.access_key,
                               secret_key=credentials.secret_key,
                               unload_options=unload_options)

        self.log.info(f"""
            ### Select Query For Unload ###
            {self.select_query}
            """)
        self.log.info('Executing UNLOAD command...')
        self.hook.run(unload_query, self.autocommit)
        self.log.info("UNLOAD command complete...")

    def _check_validation(self):
        self.log.info(f"""
            ### Check Validation ###
            company_code={self.company_code}, use_purpose={self.use_purpose}
            """)
        if not (utils.valid_company_code(self.company_code)
                or utils.valid_use_purpose(self.use_purpose)):
            raise AirflowException('Either company_code or use_purpose is invalid.')
