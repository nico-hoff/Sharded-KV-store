#!/usr/bin/env python3

import sys
from time import sleep
from testsupport import subtest, info, run
from socketsupport import run_client, run_master, run_server


def main() -> None:
    with subtest("Testing reconfiguration when a new shard joins"):
        master_proc = run_master(1025)
        sleep(5)
        server_proc_one = run_server(1026, 1025)
        sleep(5)
        server_proc_two = run_server(1027, 1025)
        sleep(5)

        for i in range(1, 6):
            client_ret = run_client(1026, "PUT", i, 1000, 1025, 0)
            if client_ret !=0:
                master_proc.terminate()
                server_proc_one.terminate()
                server_proc_two.terminate()
                sys.exit(1)
            sleep(3)
        
        server_proc_three = run_server(1028, 1025)
        sleep(10)

        info(f"Testing key redistribution")
        at_least_one_get = False
        for i in range(1, 6):
            client_ret = run_client(1028, "GET", i, 1000, 1025, 1)
            if client_ret ==0:
                at_least_one_get = True
            sleep(3)
        
        if not at_least_one_get:
            master_proc.terminate()
            server_proc_one.terminate()
            server_proc_two.terminate()
            server_proc_three.terminate()
            sys.exit(-1)

        master_proc.terminate()
        server_proc_one.terminate()
        server_proc_two.terminate()
        server_proc_three.terminate()


if __name__ == "__main__":
    main()
