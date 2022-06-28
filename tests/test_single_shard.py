#!/usr/bin/env python3

import sys
from time import sleep
from testsupport import subtest, info, run
from socketsupport import run_client, run_master, run_server


def main() -> None:
    with subtest("Testing single shard for put and get requests"):
        master_proc = run_master(1025)
        sleep(5)
        server_proc = run_server(1026, 1025)
        sleep(5)
        client_ret = run_client(1026, "PUT", 1, 1000, 1025, 0)
        if client_ret !=0:
            master_proc.terminate()
            server_proc.terminate()
            sys.exit(1)
        sleep(5)
        client_ret = run_client(1026, "PUT", 2, 1000, 1025, 0)
        if client_ret !=0:
            master_proc.terminate()
            server_proc.terminate()
            sys.exit(1)
        sleep(5)
        client_ret = run_client(1026, "GET", 1, 1000, 1025, 0)
        if client_ret !=0:
            master_proc.terminate()
            server_proc.terminate()
            sys.exit(1)
        sleep(5)
        client_ret = run_client(1026, "GET", 2, 1000, 1025, 0)
        if client_ret !=0:
            master_proc.terminate()
            server_proc.terminate()
            sys.exit(1)
        sleep(5)
        client_ret = run_client(1026, "GET", 3, 1000, 1025, 0)
        if client_ret !=2:
            master_proc.terminate()
            server_proc.terminate()
            sys.exit(1)
        sleep(5)
        info(f"ran all clients successfully")
        # run(["kill", "-9", str(master_proc.pid)])
        # run(["kill", "-9", str(server_proc.pid)])
        # run(["docker-compose", "kill"])
        master_proc.terminate()
        server_proc.terminate()


if __name__ == "__main__":
    main()
