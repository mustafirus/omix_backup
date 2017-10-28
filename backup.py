from datetime import datetime
import json
import os
from subprocess import call, PIPE, STDOUT, check_call, check_output, CalledProcessError, Popen, DEVNULL, run
import threading
from time import sleep, strftime, localtime

import sys

backup_config = list()
omix_cloud_dest = 'pm1.ssc.bla:rpool/misc/omix-backup'
confdir = '.conf'
logdir = '.log'
devnull = open(os.devnull, 'w')
dryrun = True
port_is_free = 'ss -tln | grep -q ":9000 "; echo $?'


class Dummy(object):
    pass


class Dataset(object):
    def __init__(self, client, host, path, dest):
        self.client = client['client']
        self.src_host = host
        self.src_path = path
        (self.dest_host, self.dest_path) = dest.split(':')
        self.dest_host = host if self.dest_host == 'localhost' else self.dest_host
        self._update()

    def _update(self):
        self.dest_exists = check_fs(self.dest_host, self.dest_path)
        if not self.dest_exists:
            self._del_snap_src()
        params = remote_script(host=self.src_host, script=get_dataset_params, args=[self.src_path])
        params = json.loads(params)  # exception on wrong params
        self.snap = params["snap_first"]  # from snap
        self.start = datetime.strptime(params["omix_sync_start"], "%Y-%m-%d %H:%M").timestamp()\
            if params["omix_sync_start"] else 0
        self.last = params["omix_sync_time"]
        self.interval = self._interval_to_timestamp(params["omix_sync_interval"] or '99999d')
        self.next = self.last + self.interval
        if self.start > self.next:
            self.next = self.start
        pass

    @staticmethod
    def _interval_to_timestamp(interval):
        m = {'d': 86400, 'h': 3600, 'm': 60}
        q = int(interval[:-1] or 0)
        k = m.get(interval[-1]) or 0
        return q*k

    def _test(self):
        cmd_test_send = "nc -zw5 {} 9000".format(self.dest_host)
        cmd_test_recv = "nc -w5 -lp 9000"

        return remote_sync(
                send_host=self.src_host,
                send_cmd=cmd_test_send,
                recv_host=self.dest_host,
                recv_cmd=cmd_test_recv,
                log=None)

    def _snap(self):
        if not check_fs(self.src_host, '{}@omix_send'.format(self.src_path)):
            run(['ssh', "root@" + self.src_host, 'zfs snap {}@omix_send'.format(self.src_path)], check=True)

    def _del_snap_src(self):
        old_sync = '{}@omix_sync'.format(self.src_path)
        if check_fs(self.src_host, old_sync):
            run(['ssh', "root@" + self.src_host, 'zfs destroy {}'.format(old_sync)], check=True)

    def _del_snap_dest(self):
        old_sync = '{}@omix_sync'.format(self.dest_path)
        if check_fs(self.dest_host, old_sync):
            run(['ssh', "root@" + self.dest_host, 'zfs destroy {}'.format(old_sync)], check=True)

    def _rename_snap(self):
        self._del_snap_src()
        self._del_snap_dest()
        zfs_rename = 'zfs rename {fs}@omix_send {fs}@omix_sync'
        run(['ssh', "root@" + self.dest_host, zfs_rename.format(fs=self.dest_path)], check=True)
        run(['ssh', "root@" + self.src_host, zfs_rename.format(fs=self.src_path)], check=True)

    def _sync(self, cmd_send, cmd_recv, log):
        mbuf_send = "| mbuffer -q -s 128k -m 1G -O {}:9000 -W 5".format(self.dest_host)  # -W 30
        mbuf_recv = "mbuffer -q -s 128k -m 1G -I 9000 | "  # -W 30
        mbuf_loc = "| mbuffer -q -s 128k -m 1G -W 30 |"
        if self.src_host != self.dest_host:
            return remote_sync(send_host=self.src_host, send_cmd=cmd_send + mbuf_send,
                               recv_host=self.dest_host, recv_cmd=mbuf_recv + cmd_recv,
                               log=log)
        else:
            return remote_sync(send_host=None, send_cmd=None, recv_host=self.dest_host,
                               recv_cmd=cmd_send + mbuf_loc + cmd_recv,
                               log=log)
        pass

    def run(self):
        if self.next > datetime.now().timestamp():
            return False

        if not self.dest_exists:
            parent = os.path.dirname(self.dest_path)
            check_and_create_fs(self.dest_host, parent)

        # test connection
        if self.src_host != self.dest_host:
            if not self._test():
                return

        # cmd_send = "zfs snap {}@omix_send\n".format(self.src_path) if self.snap == "@omix_sync" else ""
        # mbuf_send = "| mbuffer -q -s 128k -m 1G -O {}:9000 -W 5".format(self.dest_host)  # -W 30
        # mbuf_recv = "mbuffer -q -s 128k -m 1G -I 9000 | "  # -W 30
        # mbuf_loc = "| mbuffer -q -s 128k -m 1G -W 30 |"
        with open(logfilename((self.client, os.path.basename(self.src_path))), 'w') as logfile:
            cmd_recv = "zfs recv -uvF {}".format(self.dest_path)
            if not self.dest_exists:
                cmd_send = "zfs send -pv {}{} ".format(self.src_path, self.snap)
                self._sync(cmd_send=cmd_send, cmd_recv=cmd_recv, log=None)
                self._update()

            self._snap()
            cmd_send = "zfs send -pv {}@omix_send -I {} ".format(self.src_path, self.snap)
            self._sync(cmd_send=cmd_send, cmd_recv=cmd_recv, log=None)
            self._rename_snap()
        self._update()


def logfilename(parts):
    return "%s/%s_%s.log" % (logdir, '_'.join(parts), strftime("%Y-%m-%d_%H:%M:%S", localtime()))


def check_fs(host, fs):
    ps = run(['ssh', "root@" + host, 'zfs list', fs], stdout=DEVNULL, stderr=STDOUT)
    return ps.returncode == 0


def check_and_create_fs(host, fs):
    if not check_fs(host, fs):
        run(['ssh', "root@" + host, 'zfs create -po mountpoint=none', fs], check=True)


def remote_script(host=None, script=None, args=None):
    shell = Popen(["ssh", "root@" + host, "bash", "-s", "--"] + (args or []),
                  stdin=PIPE, stdout=PIPE, stderr=PIPE, universal_newlines=True)
    # if shutdown.is_set():
    #     return
    (out, err) = shell.communicate(script)
    if shell.returncode != 0:
        _log_error("remote script: %s: %s!" % (host, err))
        return ''
    return out


def remote_sync(send_host, send_cmd, recv_host, recv_cmd, log):
    _log_info("remote sync send: from {} cmd: {}...".format(send_host, send_cmd))
    _log_info("remote sync recv: to   {} cmd: {}...".format(recv_host, recv_cmd))
    if recv_host and recv_cmd:
        recv = Popen(["ssh", "root@" + recv_host, "bash", "-s", "--"],
                     stdin=PIPE, stdout=log, stderr=STDOUT, universal_newlines=True)
        recv._stdin_write(recv_cmd)
        sleep(1)
        recv.poll()
        if recv.returncode is not None:
            _log_error("remote_sync: recv something wrong see log")
            return
    else:
        recv = Dummy()
        recv.returncode = 0
    send = Popen(["ssh", "root@" + send_host, "bash", "-s", "--"],
                 stdin=PIPE, stdout=log, stderr=STDOUT, universal_newlines=True)
    send.communicate(send_cmd)  # exception
    if recv_host and recv_cmd:
        recv.wait()

    if send.returncode == 0 and recv.returncode == 0:
        return True
    if send.returncode > 0 or recv.returncode > 0:
        _log_error("remote_sync: something wrong see log")
    if send.returncode < 0:
        _log_error("remote_sync: send killed by signal: {}".format(send.returncode))
    if recv.returncode < 0:
        _log_error("remote_sync: recv killed by signal: {}".format(recv.returncode))

    pass
    return False


def _log_error(msg):
    print("error: ", msg.strip())


def _log_info(msg):
    print("info: ", msg.strip())


def loadconfig():
    with open(confdir+"/omix_backup.json", 'r') as conffile:
        global backup_config
        backup_config = json.load(conffile)
    pass


def client_backup_vm(vm):
    _log_info("start backup vm: %s:%s" % (vm['src_host'], vm['vmid']))
    sleep(1)


def client_backup(client):
    _log_info("start backup client: " + client['client'])

    dest = client['dest'] if client['dest'] != 'omix_cloud' else omix_cloud_dest
    datasets = []

    for src in client["src"]:
        path = src.get("path")
        vmid = src.get("vmid")
        host = ".".join((src['host'], client['domain']))
        if path:
            datasets.append(Dataset(
                client=client,
                host=host,
                path=path,
                dest='/'.join((dest, client['client'], path))
            ))
            pass
        elif vmid:
            paths = remote_script(host=host, script=get_vm_disks_script, args=[str(vmid)]).strip()
            paths = paths.split('\n') if paths else []
            for path in paths:
                datasets.append(Dataset(
                    client=client,
                    host=host,
                    path=path,
                    dest='/'.join((dest, client['client'], vmid, os.path.basename(path)))
                ))
                pass
        else:
            _log_error("unknown backup src: " + str(src))

    for dataset in datasets:
        dataset.run()
        pass
    pass


def start():
    loadconfig()
    check_and_create_fs(*omix_cloud_dest.split(':'))

    for client in backup_config:
        client_backup(client)
    return
    client_threads = list()
    for client in backup_config:
        t = threading.Thread(target=client_backup, name=client['client'], args=(client,))
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


get_vm_disks_script = r'''
config=`pct config $1 2>/dev/null`
bak_skip='/^mp[[:digit:]]\+:/{/.*backup=\(1\|yes\|on\|true\)/I!d}'
if [ -z "$config" ]; then
    config=`qm config $1 2>/dev/null`
    bak_skip='/backup=\(0\|no\|off\|false\)/Id'
fi
if [ -z "$config" ]; then
    (>&2 echo -n "unknown VMID: $1")
    exit 1
fi
set -e
set -x
echo "$config" | sed "
/^\(\(\(virtio\|ide\|scsi\|sata\|mp\)[[:digit:]]\+\)\|rootfs\): .*$/b next
d
: next
/cdrom\|none/d
$bak_skip
s/^[^:]\+: /,/
s/file=\|volume=//
s/^.*,\([A-Za-z0-9]\+:\)\([/.A-Za-z0-9\-]\+\),\?.*$/\1\2/
s/\(.*\)/pvesm path \1 /
" | bash - | sed -ne's,^/\(dev/zvol/\)\?,zfs list -Ho name ,p' | bash - 2>/dev/null
'''

get_dataset_params = '''
dataset=$1
snaps=$(zfs list -rHptsnap -o name -s creation $dataset)
origin=$(zfs get -Hpo value origin $dataset)
#set -x
omix_sync=$(echo "$snaps" | sed -ne 's/^.*@omix_sync.*$/@omix_sync/p')
snap_first=$(echo "$snaps" | sed -ne '1 s/^.*@/@/p')
[ $origin = - ] || snap_first=$origin
[ -z $omix_sync ] || snap_first=$omix_sync

[ -n "$omix_sync" ] && omix_sync_time=$(zfs get -Hpo value creation $dataset@omix_sync) || omix_sync_time=0

omix_sync_start=$(zfs get -Hpo value "ua.com.omix:sync_start" $dataset)
[ "$omix_sync_start" = "-" ] && omix_sync_start=
omix_sync_interval=$(zfs get -Hpo value "ua.com.omix:sync_interval" $dataset)
[ "$omix_sync_interval" = "-" ] && omix_sync_interval=

cat <<EOF
{
    "dataset": "$dataset",
    "snap_first": "$snap_first",
    "omix_sync_time": $omix_sync_time,
    "omix_sync_start": "$omix_sync_start",
    "omix_sync_interval": "$omix_sync_interval"
}
EOF
'''
# import signal
# def signal_handler(signal, frame):
#     print('You pressed Ctrl+C!')
#     shutdown.set()
#    sys.exit(0)

if __name__ == "__main__":
    # with open(logfilename('zzz'), 'w') as lll:
    #     remote_command_call(src_host='localhost', cmd='/home/golubev/dev/omix_backup/tmp/z', log=lll)
    # print remote_script(src_host='pm2.mebel.bla', script=get_vm_disks_script, args=['11004'])
    # exit(0)
    #
    # signal.signal(signal.SIGINT, signal_handler)
    # a = remote_sync(send_host="pm1.mebel.bla",
    #                 send_cmd="ls",
    #                 recv_host=None,
    #                 recv_cmd=None,
    #                 log=None)
    # exit(0)

    try:
        start()
    except KeyboardInterrupt:
        sys.exit(0)
#    signal.pause()
    pass


    # while not shutdown.is_set():
    #     sleep(3)
    #     for client in backup_config:
    #         client_backup(client)

    # return
    # backup_client(backup_config[0])
    # return
# def get_vm_disks(host, vmid):
#     cmd = "ssh root@%s '%s' " % (host, get_vm_disks_script + vmid)
#     try:
#         check_output(cmd, shell=True, executable='/bin/bash')
#     except CalledProcessError as e:
#         _log_error("get_vm_disks: " + str(e.output))
#     pass
#     pass


# def remote_command_call(host=None, cmd=None, log=None):
#     cmd = "ssh root@%s '%s' 3>&2 2>&1 1>&3 3>&- | tee >(cat 1>&2)" % (host, cmd)
#     try:
#         check_call(cmd, stderr=log, shell=True, executable='/bin/bash')
#     except CalledProcessError as e:
#         _log_error("check_and_create_fs: " + str(e.output))
#     pass
