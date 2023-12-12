# -*- coding: utf-8 -*-

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from sgd import config
from sgd import utils
from sgd import log_util
from datetime import timedelta


class L0LoadOperator_test(BaseOperator):
    """
    S3 file to redshift custom operator

    1. Working 테이블 delete
    2. Working 테이블 load (S3 -> Redshift COPY)
    3. Target 변경 데이터 delete (Optional: Merge, D-Append type)
    4. Working 테이블 -> Target 테이블 insert
    """
    template_fields = ('execution_kst',
                       's3_key',
                       'delete_sql_for_merge',
                       'select_sql_for_insert',
                       'delete_sql_for_append',
                       'insert_query', )
    template_ext = ()
    ui_color = '#99e699'

    @apply_defaults
    def __init__(self,
                 company_code,
                 use_purpose,
                 execution_kst,
                 target_schema,
                 target_table,
                 table_load_type,
                 s3_key,
                 delete_sql_for_merge,
                 select_sql_for_insert,
                 delete_sql_for_append='',
                 copy_options=tuple(),
                 autocommit=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.company_code = company_code
        self.use_purpose = use_purpose
        self.execution_kst = execution_kst
        self.target_schema = target_schema
        self.target_table = target_table
        self.table_load_type = table_load_type
        self.s3_key = s3_key
        self.delete_sql_for_append = ''
        if self.table_load_type == 'a':
            self.delete_sql_for_append = delete_sql_for_append.strip()
        self.delete_sql_for_merge = ''
        if self.table_load_type == 'm':
            self.delete_sql_for_merge = delete_sql_for_merge.strip()
        self.select_sql_for_insert = select_sql_for_insert.strip()
        self.insert_query = ''
        self.copy_options = copy_options
        if len(self.copy_options) == 0:
            self.copy_options = config.redshift_env['copy_options'][self.company_code]
        self.autocommit = autocommit
        self.working_schema = f"{config.sgd_env['wk_layer']}_{company_code}"
        self.s3_stg_bucket = f"{config.s3_env['stg_bucket_prefix']}-{self.company_code}-{self.use_purpose}-1"
        self.affected_rows = 0

    def execute(self, context):

        self.log.info(f"""
        
            ### Data Load ###
            target_schema = {self.target_schema}
            target_table = {self.target_table}
            s3_key = {self.s3_key}
            table_load_type = {self.table_load_type}
            company_code = {self.company_code}
            use_purpose = {self.use_purpose}
            
            delete_sql_for_append = \n{self.delete_sql_for_append}
            
            delete_sql_for_merge = \n\t{self.delete_sql_for_merge}
            
            select_sql_for_insert = \n\t{self.select_sql_for_insert}
                        
            ### Execution Date ###
            execution_kst = {self.execution_kst}
            
            """)
        self._check_validation()

        redshift_conn_id = config.conn_id['dl_redshift'][self.use_purpose]
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)

        with redshift_hook.get_conn() as conn:
            #  truncate working(or target) table
            self._init_tbl(conn, self.working_schema, self.target_table)

            # copy (s3 -> working(or target))
            self._copy_to_redshift_from_s3(conn, self.working_schema, self.target_table)

            # append : l0 부분 delete, w0 -> target insert
            if self.table_load_type == 'a':
                if self.delete_sql_for_append:
                    self._delete_tgt_tbl_for_append(conn)
                    self._insert_tgt_tbl_from_wk_tbl(conn)
                else:
                    self.log.info('Append without delete.')
                    self._insert_tgt_tbl_from_wk_tbl(conn)

            # overwrite : target truncate, w0 -> target insert
            elif self.table_load_type == 'o':
                self._init_tbl(conn, self.target_schema, self.target_table)
                self._insert_tgt_tbl_from_wk_tbl(conn)

            # merge : target 변경분 delete, w0 -> target insert
            elif self.table_load_type == 'm':
                self._delete_tgt_tbl_for_merge(conn)
                self._insert_tgt_tbl_from_wk_tbl(conn)

            if not self.autocommit:
                conn.commit()

        # logging etl job
        log_util.handle_etl_history(self.affected_rows)

        self.log.info(f"""
        
            ### Data Load done ###
            target_schema = {self.target_schema}
            target_table = {self.target_table}
            s3_key = {self.s3_key}
            table_load_type = {self.table_load_type}
            
            """)

    def _init_tbl(self, conn, schema, table):
        truncate_query = f"""
            truncate table {schema}.{table}
        """
        with conn.cursor() as cursor:
            cursor.execute(truncate_query)
        self.log.info(f"Table truncated: {truncate_query}")

    def _copy_to_redshift_from_s3(self, conn, schema, table):
        copy_options = '\n\t\t\t'.join(self.copy_options)
        copy_query = f"""
            COPY {schema}.{table}
            FROM 's3://{self.s3_stg_bucket}/{self.s3_key}'
            iam_role '{config.redshift_env['iam_role'][self.use_purpose]}'
            csv quote as '\036' delimiter '|';
        """
        self.log.info(f"Copy: {copy_query}")
        with conn.cursor() as cursor:
            cursor.execute(copy_query)
        self.log.info(f"Copy done")

    def _delete_tgt_tbl_for_append(self, conn):
        affected_rows = 0
        with conn.cursor() as cursor:
            cursor.execute(self.delete_sql_for_append)
            affected_rows = cursor.rowcount
        self.log.info(f"""
            Target table deleted for append: {affected_rows} deleted. \n\n{self.delete_sql_for_append}
            """)

    def _delete_tgt_tbl_for_merge(self, conn):
        affected_rows = 0
        with conn.cursor() as cursor:
            cursor.execute(self.delete_sql_for_merge)
            affected_rows = cursor.rowcount
        self.log.info(f"""
            Target table deleted for merge: {affected_rows} deleted. \n\n{self.delete_sql_for_merge}
            """)

    def _insert_tgt_tbl_from_wk_tbl(self, conn):
        if not self.select_sql_for_insert:
            insert_query = f"""
            insert into {self.target_schema}.{self.target_table}
            select * from {self.working_schema}.{self.target_table}
            """
        else:
            insert_query = f"""
            {self.select_sql_for_insert}
            """
        self.log.info(f"Insert start. \n{insert_query}")
        with conn.cursor() as cursor:
            cursor.execute(insert_query)
            self.affected_rows = cursor.rowcount

        self.log.info(f"Insert done: {self.affected_rows} inserted.")

    def _check_validation(self):
        if not (utils.valid_table_load_type(self.table_load_type)
                and utils.valid_company_code(self.company_code)
                and utils.valid_use_purpose(self.use_purpose)):
            raise AirflowException('Either table_load_type or company_code or use_purpose is invalid.')
