#!/usr/bin/python3
from datetime import datetime
import json
import os
from signal import signal, SIGINT, pause
from subprocess import PIPE, STDOUT, Popen, DEVNULL, run
import threading
from threading import Lock, Event
from time import sleep

backup_config = list()
omix_cloud_dest = 'pm1.ssc.bla:rpool/misc/omix-backup'
confdir = '.conf'
logdir = '.log'
sync_lock = Lock()
shutdown = Event()


class Shutdown(Exception):
    pass


class Dummy(object):
    def __init__(self):
        self.returncode = 0

    @staticmethod
    def wait():
        return 0


def _log_error(msg):
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "error: ", msg.strip())


def _log_info(msg):
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "info: ", msg.strip())


def _log_returncode(code):
    if code == 255:
        return
    if code > 0:
        _log_error("remote_sync: something wrong see log")
    if code < 0:
        _log_error("killed by signal: {}".format(code))


def loadconfig():
    with open(confdir+"/omix_backup.json", 'r') as conffile:
        global backup_config
        backup_config = json.load(conffile)
    pass


def check_shutdown():
    if shutdown.is_set():
        raise Shutdown()


def logfilename(parts):
    return "{}/{}_{}.log".format(logdir, '_'.join(parts), datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))


def check_cmd_is_running(host, cmd):
    ps = run(['ssh', "root@" + host, 'pgrep -fx "{}"'.format(cmd)], stdout=DEVNULL, stderr=STDOUT)
    return ps.returncode == 0


def check_fs(host, fs):
    ps = run(['ssh', "root@" + host, 'zfs list', fs], stdout=DEVNULL, stderr=STDOUT)
    return ps.returncode == 0


def check_and_create_fs(host, fs):
    if not check_fs(host, fs):
        run(['ssh', "root@" + host, 'zfs create -po mountpoint=none', fs], check=True)


def remote_script(host=None, script=None, args=None):
    shell = Popen(["ssh", "root@" + host, "bash", "-s", "--"] + (args or []),
                  stdin=PIPE, stdout=PIPE, stderr=PIPE, universal_newlines=True)
    (out, err) = shell.communicate(script)
    if shell.returncode != 0:
        _log_error("remote script: %s: %s!" % (host, err))
        return ''
    return out


def remote_sync_cmd(host, cmd, log):
    if host and cmd:
        proc = Popen(["ssh", "root@" + host, "bash -s --"],
                     stdin=PIPE, stdout=log, stderr=STDOUT, universal_newlines=True)
        proc.stdin.write(cmd)
        proc.stdin.close()
        sleep(1)
        proc.poll()
    else:
        proc = Dummy()
        proc.returncode = 0
    return proc


def remote_sync(send_host, send_cmd, recv_host, recv_cmd, log):
    _log_info("remote sync send: from {} cmd: {}...".format(send_host, send_cmd))
    _log_info("remote sync recv: to   {} cmd: {}...".format(recv_host, recv_cmd))

    with sync_lock:
        recv = remote_sync_cmd(recv_host, recv_cmd, log)
        if recv.returncode is not None:
            _log_error("remote_sync: recv cant start see log")
            return False
        send = remote_sync_cmd(send_host, send_cmd, log)
        _log_info("remote sync wait for transfer: {}".format(recv_host))
        remote_script(host=recv_host, script=timeout_port + free_up_port, args=["30"])
        _log_info("remote sync transfer begun: {}".format(recv_host))

        # TODO test close logfile at this point
    send.wait()
    recv.wait()

    _log_returncode(send.returncode)
    _log_returncode(recv.returncode)

    return send.returncode == 0 and recv.returncode == 0


class Dataset(object):
    def __init__(self, client, host, path, dest):
        self.client = client['client']
        self.src_host = host
        self.src_path = path
        (self.dest_host, self.dest_path) = dest.split(':')
        self.dest_host = host if self.dest_host == 'localhost' else self.dest_host
        self.dest_exists = None
        self.snap = None
        self.start = None
        self.last = None
        self.interval = None
        self.next = None
        self.next_update = None
        self.update()

    def update(self):
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
        self.next = self.last + self.interval  # TODO make daily at night
        if self.start > self.next:
            self.next = self.start
        self.next_update = datetime.now().timestamp() + 3600
        pass

    @staticmethod
    def _interval_to_timestamp(interval):
        m = {'d': 86400, 'h': 3600, 'm': 60}
        q = int(interval[:-1] or 0)
        k = m.get(interval[-1]) or 0
        return q*k

    def _test(self):
        cmd_test_send = "nc -zw10 {} 9000".format(self.dest_host)
        cmd_test_recv = free_up_port + "nc -w10 -lp 9000"

        if self.src_host == self.dest_host:
            return True

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
        mbuf_send = "| mbuffer -q -s 128k -m 1G -O {}:9000 -W 300".format(self.dest_host)
        mbuf_recv = "mbuffer -q -s 128k -m 1G -I 9000 -W 300 | "
        mbuf_loc = "| mbuffer -q -s 128k -m 1G -W 300 |"
        if log:
            self._log_cmd(log, cmd_send, cmd_recv)
        if self.src_host != self.dest_host:
            return remote_sync(send_host=self.src_host, send_cmd=cmd_send + mbuf_send,
                               recv_host=self.dest_host, recv_cmd=mbuf_recv + cmd_recv,
                               log=log)
        else:
            return remote_sync(send_host=None, send_cmd=None, recv_host=self.dest_host,
                               recv_cmd=cmd_send + mbuf_loc + cmd_recv,
                               log=log)
        pass

    def _find_last_snap(self):
        # TODO rewite for checking omix_sync on src and dest
        dest_snaps = remote_script(host=self.dest_host,
                                   script="zfs list -rHtsnap -oname -Screation $1 | sed -e's/^.*@/@/'",
                                   args=[self.dest_path])
        dest_snaps = dest_snaps.strip().split('\n')
        for snap in dest_snaps:
            if check_fs(self.src_host, self.src_path + snap):
                self.snap = snap
                break
        pass

    def _log_cmd(self, logfile, c1, c2):
        logfile.write("begin sync\n")
        logfile.write(c1 + "\n")
        logfile.write(c2 + "\n")
        logfile.flush()

    def run(self):
        if self.next > datetime.now().timestamp():
            return

        _log_info("sync client: {} fs: {}".format(self.client, self.src_path))
        if not self.dest_exists:
            parent = os.path.dirname(self.dest_path)
            check_and_create_fs(self.dest_host, parent)

        # test connection
        if not self._test():
            return

        with open(logfilename((self.client, os.path.basename(self.src_path))), 'w', buffering=1) as logfile:
            cmd_recv = "zfs recv -uvF {}".format(self.dest_path)
            if check_cmd_is_running(self.dest_host, cmd_recv):
                _log_info("sync client: {} fs: {} already runing: next try in 10 min"
                          .format(self.client, self.src_path))
                self.next = datetime.now().timestamp() + 600
                return
            if not self.dest_exists:
                cmd_send = "zfs send -pv {}{} ".format(self.src_path, self.snap)
                if not self._sync(cmd_send=cmd_send, cmd_recv=cmd_recv, log=logfile):
                    return
                self.update()

            if not self.last:
                self._find_last_snap()

            self._snap()
            cmd_send = "zfs send -pv {}@omix_send -I {} ".format(self.src_path, self.snap)
            if self._sync(cmd_send=cmd_send, cmd_recv=cmd_recv, log=logfile):
                self._rename_snap()
        self.update()


def client_backup(client):
    _log_info("start backup client: " + client['client'])

    dest = client['dest'] if client['dest'] != 'omix_cloud' else omix_cloud_dest
    datasets = []
# TODO check nc,mbuffer installed
# TODO backup <vmid>.conf
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
    if len(datasets) == 0:
        _log_error("nothing to do: " + str(client['client']))
        return
    while not shutdown.is_set():
        next_run = None
        _log_info("next run for client: " + client['client'])
        for dataset in datasets:
            dataset.run()  # TODO periodicaly rerun _update for changes of interval
            # TODO catch CalledProcessError retry in 10? min or try ping before run
            next_run = min(next_run, dataset.next) if next_run else dataset.next
            if shutdown.is_set():
                return

        for dataset in datasets:
            if datetime.now().timestamp() > dataset.next_update:
                dataset.update()

        s = next_run - datetime.now().timestamp()
        while s > 0:
            sleep(1)
            if shutdown.is_set():
                return
            s -= 1
        sleep(1)


def start():
    loadconfig()
    check_and_create_fs(*omix_cloud_dest.split(':'))

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

get_dataset_params = r'''
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
timeout_port = r'''T=$1+1; while ((T-=1)); do ss -tlnp | grep -q ' *:9000 ' || break; sleep 1; done; '''
free_up_port = r'''ss -tlnp | awk '$4~/\*:9000/ { print gensub(/^.*,pid=([0-9]*),.*$/,"\\1","g",$6)}' \
| xargs -r kill;'''


def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    shutdown.set()


if __name__ == "__main__":
    # zzz = remote_script(host="pm1.ssc.bla", script=free_up_port)
    # exit(0)
    signal(SIGINT, signal_handler)

    start()
    # try:
    #     while True:
    #         sleep(5)
    # except KeyboardInterrupt:
    #     sys.exit(0)
    pause()
    pass
    # while not shutdown.is_set():
    #     sleep(3)
    #     for client in backup_config:
    #         client_backup(client)