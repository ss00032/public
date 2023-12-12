# -*- coding: utf-8 -*-

from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from select import select

from sgd import config
from sgd import utils
from sgd import log_util


class EzgatorSendOperator(BaseOperator):
    """
    Ez-GATOR file send custom operator

    1. Working 테이블 delete
    2. Working 테이블 load (S3 -> Redshift COPY)
    3. Target 변경 데이터 delete (Optional: Merge type)
    4. Working 테이블 -> Target 테이블 insert
    """
    template_fields = ('s3_key',)
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 company_code,
                 use_purpose,
                 s3_key,
                 timeout: int = 10,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.company_code = company_code
        self.use_purpose = use_purpose
        self.s3_key = s3_key
        self.file_bytes = 0
        self.timeout = timeout

    def execute(self, context):
        try:
            self._check_validation()
            # 파일 전송 변수 설정
            env_key = f"{self.company_code}_{self.use_purpose}"

            # Ez-GATOR 인터페이스명
            ez_if_nm = config.ezgator_env['send']['if_nm_fmt'].format(company_code=self.company_code).upper()
            # 송신측 파일명(루트경로 제외)
            snd_path = f"{config.ezgator_env['send']['com_path'][env_key]}{self.s3_key}"
            # 수신측 파일명(루트경로 제외)
            rcv_path = f"{config.ezgator_env['send']['sgd_path'][env_key]}{self.s3_key}"

            command = f"{config.ezgator_env['send']['shell_path']} -i {ez_if_nm} -f {snd_path} -t {rcv_path}"
            self.log.info(f"EZ-SEND: {command}")

            ssh_hook = SSHHook(ssh_conn_id=config.conn_id['dl_ezgator'])
            exit_status = self._ssh_execute(ssh_hook, command)
            log_util.handle_etl_history(data_size=self.file_bytes)
            self.log.info(f"EZ-SEND DONE: '{command}'=[{exit_status}]={self.file_bytes} bytes")
        except AirflowException as e:
            raise e
        except Exception as e:
            raise AirflowException(f"EZ-SEND FAIL: {str(e)}")

    def _ssh_execute(self, ssh_hook, command):
        exit_status = -1
        # paramiko.client.SSHClient
        with ssh_hook.get_conn() as ssh_client:
            stdin, stdout, stderr = ssh_client.exec_command(
                command=command,
            )
            # get channels
            channel = stdout.channel
            # closing stdin
            stdin.close()
            channel.shutdown_write()
            agg_stdout = b''
            agg_stderr = b''

            # capture any initial output in case channel is closed already
            stdout_buffer_length = len(stdout.channel.in_buffer)

            if stdout_buffer_length > 0:
                agg_stdout += stdout.channel.recv(stdout_buffer_length)

            # read from both stdout and stderr
            while not channel.closed or channel.recv_ready() or channel.recv_stderr_ready():
                readq, _, _ = select([channel], [], [], self.timeout)
                for recv in readq:
                    if recv.recv_ready():
                        line = stdout.channel.recv(len(recv.in_buffer))
                        agg_stdout += line
                        self.log.info(line.decode('utf-8', 'replace').strip('\n'))
                    if recv.recv_stderr_ready():
                        line = stderr.channel.recv_stderr(len(recv.in_stderr_buffer))
                        agg_stderr += line
                        self.log.warning(line.decode('utf-8', 'replace').strip('\n'))
                if (
                        stdout.channel.exit_status_ready()
                        and not stderr.channel.recv_stderr_ready()
                        and not stdout.channel.recv_ready()
                ):
                    stdout.channel.shutdown_read()
                    stdout.channel.close()
                    break

            stdout.close()
            stderr.close()

            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                error_msg = agg_stderr.decode('utf-8')
                raise AirflowException(f"EZ-SEND FAIL: '{command}'=[{exit_status}] {error_msg}")
            output_msg = agg_stdout.decode('utf-8')
            # TODO: parse output_msg

            self.log.info(f"EZ-SEND OUTPUT: {output_msg}")

        return exit_status

    def _check_validation(self):
        if not (utils.valid_company_code(self.company_code)
                and utils.valid_use_purpose(self.use_purpose)):
            raise AirflowException('Either company_code or use_purpose is invalid.')
