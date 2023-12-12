# -*- coding: utf-8 -*-

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from sgd import config
from sgd import utils
from sgd import log_util


class L1LoadOperator(BaseOperator):
    """
    Redshift L0 to L1 operator :

    L0 데이터를 조회하여 L1 으로 적재
    """
    template_fields = ('select_sql_for_insert',)
    template_ext = ()
    ui_color = '#99e699'

    @apply_defaults
    def __init__(self,
                 company_code,
                 use_purpose,
                 target_schema,
                 target_table,
                 select_sql_for_insert,
                 autocommit=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.company_code = company_code
        self.use_purpose = use_purpose
        self.target_schema = target_schema
        self.target_table = target_table
        self.select_sql_for_insert = select_sql_for_insert.strip()
        self.autocommit = autocommit
        self.affected_rows = 0

    def execute(self, context):

        self.log.info(f"""
            ### Data Load ###
            target_schema={self.target_schema}, target_table={self.target_table},
            company_code={self.company_code}, use_purpose={self.use_purpose}
            """)
        self._check_validation()

        redshift_conn_id = config.conn_id['dl_redshift'][self.use_purpose]
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)

        with redshift_hook.get_conn() as conn:

            if not self.select_sql_for_insert:
                raise AirflowException('select_sql_for_insert is invalid.')

            else:
                insert_query = f"""
                    {self.select_sql_for_insert}
                """
            with conn.cursor() as cursor:
                cursor.execute(insert_query)
                self.affected_rows = cursor.rowcount

            self.log.info(f"""
                Insert done: {self.affected_rows} inserted. \n{insert_query}
                """)

            if not self.autocommit:
                conn.commit()

        # logging etl job
        log_util.handle_etl_history(self.affected_rows)

        self.log.info(f"""
            ### Data Load done ### 
            """)

    def _check_validation(self):
        if not (utils.valid_company_code(self.company_code)
                and utils.valid_use_purpose(self.use_purpose)):
            raise AirflowException('Either company_code or use_purpose is invalid.')
