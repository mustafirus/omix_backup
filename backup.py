import json
import os
import subprocess
from subprocess import call, PIPE, STDOUT, check_call, check_output, CalledProcessError
import threading
from time import sleep

backup_config = list()
omix_cloud_dest = 'pm1.ssc.bla:rpool/misc/omix-backup'

def _log_error(msg):
    print "error: ", msg, "\n"

def _log_info(msg):
    print "info: " + msg


def loadconfig():
    with open("omix_backup.json", 'r') as conffile:
        global backup_config
        backup_config = json.load(conffile)
    pass


def client_backup_vm(vm):
    _log_info("start backup: %s:%s" % (vm['host'],vm['vmid']))
    sleep(1)

def client_backup(client):
    _log_info("start backup: " + client['client'])
    for vm in client["vms"]:
        sleep(1)
        client_backup_vm(vm)

    pass

def client_sheduller(client):
    _log_info("start sheduller: " + client['client'])
    dest = client['dest'] if client['dest'] != 'omix_cloud' else omix_cloud_dest

    while True:
        sleep(5)
        client_backup(client)

    pass


def run():
    pass

def check_fs(host, fs):
    ret = call(['ssh', "root@" + host, 'zfs', 'list', fs], stdout=PIPE, stderr=STDOUT)
    return ret == 0

def check_and_create_fs(host, fs):
    if not check_fs(host, fs):
        try:
            out = check_output(['ssh', "root@" + host, 'zfs', 'create', fs], stderr=STDOUT)
        except CalledProcessError as e:
            _log_error("check_and_create_fs: " + e.output)
            raise

def start():
    loadconfig()
    check_and_create_fs(*omix_cloud_dest.split(':'))

    return
    backup_client(backup_config[0])
    return
    client_threads = list()
    for client in backup_config:
        t = threading.Thread(target=client_sheduller, name=client['client'], args=(client,))
        t.start()
        client_threads.append(t)
        pass

    while len(client_threads):
        sleep(3)
        for t in client_threads[:]:
            if t.is_alive():
                continue
            client_threads.remove(t)
    pass

if __name__ == "__main__":
    start()

    # import argparse
    #
    # parser = argparse.ArgumentParser(description='Process some integers.')
    # parser.add_argument('integers', metavar='N', type=int, nargs='+',
    #                     help='an integer for the accumulator')
    # parser.add_argument('--sum', dest='zzz', action='store_const',
    #                     const=sum,
    #                     default=max,
    #                     help='sum the integers (default: find the max)')
    #
    # args = parser.parse_args()
    # print(args.zzz(args.integers))
    #
    # from configobj import ConfigObj
    #
    # config = ConfigObj('omix_backup.conf')
    # zzz = config.get('base_backup_path')
    # pass
