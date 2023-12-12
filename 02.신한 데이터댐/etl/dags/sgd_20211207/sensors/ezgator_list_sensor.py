# -*- coding: utf-8 -*-

from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.sensors.base import BaseSensorOperator
from airflow.exceptions import AirflowException

from sgd import config
from sgd import utils
from sgd_t import log_util
from sgd import ssh_util
import json


class EzgatorListSensor(BaseSensorOperator):
    """
    Ez-GATOR file list custom sensor

    SRC 시스템에 파일이 존재하는지 확인하는 Custom Sensor
    /shcsw/ezgator/client/ezgatorlist.sh 로 파일 목록 확인
    """
    template_fields = ('s3_key',)

    def __init__(
            self,
            company_code,
            use_purpose,
            s3_key,
            ssh_timeout: int = 10,
            poke_timeout: float = 60 * 10,
            soft_fail: bool = False,
            mode: str = 'reschedule',
            poke_interval: float = 60,
            **kwargs,
    ):
        super().__init__(**kwargs)
        self.company_code = company_code
        self.use_purpose = use_purpose
        self.s3_key = s3_key
        self.ssh_timeout = ssh_timeout
        self.timeout = poke_timeout
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.mode = mode

    def poke(self, context):
        self.log.info(f"EZ-LIST: mode={self.mode},poke_interval={self.poke_interval},poke_timeout={self.timeout}")
        exit_status = -1
        try:
            self._check_validation()
            # 파일 전송 변수 설정
            env_key = f"{self.company_code}_{self.use_purpose}"
            # 송신 파일명(경로포함)
            snd_path = f"{config.ezgator_env['list']['com_path'][env_key]}"
            # 타겟 시스템명
            t_system = f"{config.ezgator_env['list']['system_nm'][self.company_code]}"

            ssh_command = f"{config.ezgator_env['list']['shell_path']} -path {snd_path} -t_system {t_system} -condition '^(?i){self.s3_key}.*$'"
            self.log.info(f"EZ-LIST: cmd={ssh_command}")
            file_key = f"[{t_system}] {snd_path}/{self.s3_key}"

            ssh_hook = SSHHook(ssh_conn_id=config.conn_id['dl_ezgator'])
            (exit_status, output_msg, error_msg) = ssh_util.ssh_execute(ssh_hook, ssh_command, self.ssh_timeout)
            output_json = json.loads(output_msg)

            if exit_status == 0:
                log_util.handle_etl_history()
                self.log.info(f"""EZ-LIST DONE: file_count={output_json['file_count']}
                {output_json['files']}""")
                return True

            error_code = output_json['error_code']
            if error_code != '404' and error_code != '00000':
                raise AirflowException(f"EZ-LIST FAIL: [{output_json['error_code']}] {output_json['error_msg']}")

            self.log.info(f"EZ-LIST DONE: FileNotFound='{file_key}'")
            log_util.handle_etl_running()
            return False
        except AirflowException as e:
            raise e
        except Exception as e:
            raise AirflowException(f"ezgator list sensor fail: {str(e)}")

    def _check_validation(self):
        if not (utils.valid_company_code(self.company_code)
                and utils.valid_use_purpose(self.use_purpose)):
            raise AirflowException('Either company_code or use_purpose is invalid.')
