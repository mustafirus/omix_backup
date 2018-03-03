#!/usr/bin/python
import json
import subprocess

omix_cloud_dest = 'pm1.ssc.bla:rpool/misc/omixbackup'


def loadconfig():
    with open(".conf/omix_backup.json", 'r') as conffile:
        return json.load(conffile)



def start():
    backup_config = loadconfig()
    dests = list(d['dest'] if d['dest'] != 'omix_cloud' else omix_cloud_dest for d in backup_config)
    hosts = list(v1['host']+"."+v2["domain"] for v2 in backup_config for v1 in v2['src'])
    hosts += list(d.split(':')[0] for d in dests)
    hosts = list(set(hosts))
    for host in hosts:
        if host == 'localhost':
            continue
        print 'Adding key to: ', host
        subprocess.call(['ssh-copy-id', 'root@%s' % host])


if __name__ == "__main__":
    start()

