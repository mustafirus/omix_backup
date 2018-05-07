#!/usr/bin/python3
import json

import subprocess
from subprocess import Popen, run, PIPE

omix_cloud_dest = 'pm1.ssc.bla:rpool/misc/omixbackup'


def loadconfig():
    with open("/etc/omix_replicate/omix_backup.json", 'r') as conffile:
        return json.load(conffile)

def remote_script(host=None, script=None, args=None):
    shell = Popen(["ssh", "root@" + host, "bash", "-s", "--"] + (args or []),
                  stdin=PIPE, universal_newlines=True)
    shell.communicate(script)
    return shell.returncode == 0



def start():
    backup_config = loadconfig()
    dests = list(d['dest'] if d['dest'] != 'omix_cloud' else omix_cloud_dest for d in backup_config)
    hosts = list(v1['host']+"."+v2["domain"] for v2 in backup_config for v1 in v2['src'])
    hosts += list(d.split(':')[0] for d in dests)
    hosts = list(set(hosts))
    for host in hosts:
        if host == 'localhost':
            continue
        print('Adding key to: ', host)
        run(['ssh-copy-id', 'root@%s' % host])
        if not remote_script(host, 'which nc'):
            remote_script(host, 'apt-get -y install nc')
        if not remote_script(host, 'which gawk'):
            remote_script(host, 'apt-get -y install gawk')
        if not remote_script(host, 'which mbuffer'):
            remote_script(host, 'apt-get -y install mbuffer')


if __name__ == "__main__":
    start()

