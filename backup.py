import json
import os
import subprocess
from subprocess import call, PIPE, STDOUT, check_call, check_output, CalledProcessError
import threading
from threading import Event
from time import sleep, strftime,localtime

backup_config = list()
omix_cloud_dest = 'pm1.ssc.bla:rpool/misc/omix-backup'
confdir = '.conf'
logdir = '.log'
shutdown=Event('zzz')
devnull = open(os.devnull, 'w')
dryrun = True

def logfilename(client):
    return "%s/%s_%s.log" % (logdir, client, strftime("%Y-%m-%d_%H:%M:%S", localtime()))

def remote_command_call(host=None, cmd=None, log=None):
    cmd = "ssh root@%s '%s' 3>&2 2>&1 1>&3 3>&- | tee >(cat 1>&2)" % (host, cmd)
    try:
        check_call(cmd, stderr=log, shell=True, executable='/bin/bash')
    except CalledProcessError as e:
        _log_error("check_and_create_fs: " + str(e.output))
    pass

def _log_error(msg):
    print "error: ", msg, "\n"

def _log_info(msg):
    print "info: " + msg


def loadconfig():
    with open(confdir+"/omix_backup.json", 'r') as conffile:
        global backup_config
        backup_config = json.load(conffile)
    pass


def client_backup_vm(vm):
    _log_info("start backup vm: %s:%s" % (vm['host'],vm['vmid']))
    sleep(1)

def client_backup(client):
    _log_info("start backup client: " + client['client'])

    for vm in client["vms"]:
        if shutdown.is_set():
            return
        sleep(1)
        client_backup_vm(vm)

    pass

def client_sheduller(client):
    _log_info("start sheduller: " + client['client'])
    dest = client['dest'] if client['dest'] != 'omix_cloud' else omix_cloud_dest



    while not shutdown.is_set():
        sleep(5)
        client_backup(client)
    pass


def check_fs(host, fs):
    ret = call(['ssh', "root@" + host, 'zfs', 'list', fs], stdout=PIPE, stderr=STDOUT)
    return ret == 0

def check_and_create_fs(host, fs):
    if not check_fs(host, fs):
        try:
            check_call(['ssh', "root@" + host, 'zfs', 'create', fs], stderr=STDOUT)
        except CalledProcessError as e:
            _log_error("check_and_create_fs: " + e.output)
            raise

def start():
    loadconfig()
    check_and_create_fs(*omix_cloud_dest.split(':'))

    # return
    # backup_client(backup_config[0])
    # return
    client_threads = list()
    for client in backup_config:
        t = threading.Thread(target=client_sheduller, name=client['client'], args=(client,))
        t.start()
        client_threads.append(t)
        pass

    # while len(client_threads):
    #     sleep(3)
    #     for t in client_threads[:]:
    #         if t.is_alive():
    #             continue
    #         client_threads.remove(t)
    pass


import signal
import sys
def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    shutdown.set()
#    sys.exit(0)

if __name__ == "__main__":
    # with open(logfilename('zzz'), 'w') as lll:
    #     remote_command_call(host='localhost', cmd='/home/golubev/dev/omix_backup/tmp/z', log=lll)
    # exit(0)
    #
    signal.signal(signal.SIGINT, signal_handler)
    start()
    signal.pause()
    pass
