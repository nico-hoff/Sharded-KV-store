from subprocess import Popen
import tempfile
import sys
import subprocess
import time
import psutil

from testsupport import (
    run_project_executable,
    info,
    warn,
    find_project_executable,
)

def is_server_listening(port: int) -> bool:
    for conn in psutil.net_connections():
        if conn.laddr.port == port and conn.status == "LISTEN":
            return True
    return False

def run_client(port: int, operation: str, key: int, value: int, master_port: int, direct: int) -> int:
    info(
        f"Running client."
    )

    with tempfile.TemporaryFile(mode="w+") as stdout:
        proc = run_project_executable(
            "clt",
            args=[
                "-p", str(port),
                "-o", operation,
                "-k", str(key),
                "-v", str(value),
                "-m", str(master_port),
                "-d", str(direct),
            ],
            stdout=stdout,
            check=False
        )

        ret = proc.returncode

        return ret

def run_master(port: int) -> Popen:
    # master always run with port number 1025
    try:
        # make sure port 1025 is not bound anymore
        while is_server_listening(port):
            info("Waiting for port to be unbound")
            time.sleep(5)

        master = [find_project_executable("master-svr"), "-p", str(port)]

        info(f"Run master")

        print(" ".join(master))
        info(f"{master}")
        proc = subprocess.Popen(master, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,)
        return proc
    except OSError as e:
        warn(f"Failed to run command: {e}")
        sys.exit(1)

def run_server(port: int, master_port: int) -> Popen:
    # master always run with port number 1025
    try:
        # make sure port 1025 is not bound anymore
        while is_server_listening(port):
            info("Waiting for port to be unbound")
            time.sleep(5)

        server = [find_project_executable("svr"), "-p", str(port), "-m", str(master_port)]

        info(f"Run server")

        print(" ".join(server))
        info(f"{server}")
        proc = subprocess.Popen(server, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,)
        return proc
    except OSError as e:
        warn(f"Failed to run command: {e}")
        sys.exit(1)
