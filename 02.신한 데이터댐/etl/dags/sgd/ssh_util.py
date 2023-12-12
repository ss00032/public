# -*- coding: utf-8 -*-

from select import select


def ssh_execute(ssh_hook, command, ssh_timeout):
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
            readq, _, _ = select([channel], [], [], ssh_timeout)
            for recv in readq:
                if recv.recv_ready():
                    line = stdout.channel.recv(len(recv.in_buffer))
                    agg_stdout += line
                if recv.recv_stderr_ready():
                    line = stderr.channel.recv_stderr(len(recv.in_stderr_buffer))
                    agg_stderr += line
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
        error_msg = ''
        if exit_status != 0:
            error_msg = agg_stderr.decode('utf-8')
        output_msg = agg_stdout.decode('utf-8')

    return exit_status, output_msg, error_msg
