# -*- coding: utf-8 -*-

from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

from sgd import config
from sgd import utils
from sgd_t import log_util
from sgd import ssh_util
import json


class EzgatorSendOperator(BaseOperator):
    """
    Ez-GATOR file send custom operator

    /shcsw/ezgator/client/ezgatorsend.sh로 파일 전송 명령 수행
    """
    template_fields = ('s3_key',)
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 company_code,
                 use_purpose,
                 s3_key,
                 ssh_timeout: int = 10,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.company_code = company_code
        self.use_purpose = use_purpose
        self.s3_key = s3_key
        self.ssh_timeout = ssh_timeout

    def execute(self, context):
        try:
            self._check_validation()
            # 파일 전송 변수 설정
            env_key = f"{self.company_code}_{self.use_purpose}"
            # 타겟 시스템명
            t_system = f"{config.ezgator_env['list']['system_nm'][self.company_code]}"
            # 송신 파일명(경로포함)
            snd_path = f"{config.ezgator_env['list']['com_path'][env_key]}"

            list_command = f"{config.ezgator_env['list']['shell_path']} -path {snd_path} -t_system {t_system} -condition '^(?i){self.s3_key}.*$'"
            self.log.info(f"EZ-SEND: list_cmd={list_command}")

            ssh_hook = SSHHook(ssh_conn_id=config.conn_id['dl_ezgator'])
            (list_exit_status, list_output_msg, list_error_msg) = ssh_util.ssh_execute(ssh_hook, list_command, self.ssh_timeout)

            list_output_json = json.loads(list_output_msg)
            if list_exit_status != 0:
                raise AirflowException(f"EZ-SEND FAIL: [{list_output_json['error_code']}] {list_output_json['error_msg']}")

            self.log.info(f"EZ-SEND: {list_output_json['file_count']} files found.")

            # Ez-GATOR 인터페이스명
            if_name = config.ezgator_env['recv']['if_nm_fmt'].format(
                cp_cd=utils.get_cp_cd(self.company_code).upper(),
                ez_seq='0001',
            )

            idx = 0
            send_bytes = 0
            for file_info in list_output_json['files']:
                idx = idx + 1
                self.log.info(f"EZ-SEND: [{idx}]={file_info['filename']}")

                # 송신측 파일명(루트경로 제외)
                snd_file = f"{config.ezgator_env['recv']['com_path'][env_key]}{file_info['filename']}"
                # 수신측 파일명(루트경로 제외)
                rcv_file = f"{config.ezgator_env['recv']['sgd_path'][env_key]}{file_info['filename']}"

                send_command = f"{config.ezgator_env['recv']['shell_path']} -i {if_name} -f {snd_file} -t {rcv_file}"
                self.log.info(f"EZ-SEND: [{idx}]={send_command}")

                (send_exit_status, send_output_msg, send_error_msg) \
                    = ssh_util.ssh_execute(ssh_hook, send_command, self.ssh_timeout)
                send_output_json = json.loads(send_output_msg)
                self.log.info(f"EZ-SEND: [{idx}]={file_info['filename']}, "
                              f"result=[{send_output_json['error_code']}]{send_output_json['error_msg']}")
                if send_exit_status != 0:
                    raise AirflowException(
                        f"EZ-SEND FAIL: [{send_output_json['error_code']}] {send_output_json['error_msg']}")
                send_bytes = send_bytes + send_output_json['data_size']

            log_util.handle_etl_history(data_size=send_bytes)
            self.log.info(f"EZ-SEND DONE: {self.s3_key}={idx} files, {send_bytes} bytes")
        except AirflowException as e:
            raise e
        except Exception as e:
            raise AirflowException(f"EZ-SEND FAIL: {str(e)}")

    def _check_validation(self):
        if not (utils.valid_company_code(self.company_code)
                and utils.valid_use_purpose(self.use_purpose)):
            raise AirflowException('Either company_code or use_purpose is invalid.')
