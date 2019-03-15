#!/usr/bin/python3 -uO
import re
from _signal import SIGTERM
from datetime import datetime
import json
import os
from json import JSONDecodeError
from signal import signal, SIGINT, pause
from subprocess import PIPE, STDOUT, Popen, DEVNULL, run, CalledProcessError, TimeoutExpired
import threading
from threading import Lock, Event
from time import sleep

# backup_config = list()
omix_cloud_dest = 'pm1.ssc.bla:rpool/misc/omix-backup'
confdir = '.conf'
logdir = '.log'
# confdir = '/etc/omix_replicate'
# logdir = '/var/log/omix_replicate'
sync_lock = Lock()
client_threads = list()
DEFAULT_INTERVAL = '1d'  # '99999d'
FAIL_INTERVAL = 600  # seconds
# DONE: backup <vmid>.conf
# TODO: check time sync with target hosts max delta ~5 sec notify if not refuse to repl if >1h
# TODO: check zfs version: modinfo zfs | sed -n 's/^version: *//p' > 0.7.3
# TODO:   packaging.version.parse ("2.3.1") < packaging.version.parse("10.1.2")
# TODO: add datetime to logfile: make logfie filelike object which is add timestamp to each string
# TODO: check free mem 1G


# class Shutdown(Exception):
#     pass
#

class Dummy(object):
    def __init__(self):
        self.returncode = 0

    @staticmethod
    def wait():
        return 0


# datetime.now().strftime("%Y-%m-%d %H:%M:%S"), not need
# syslog add timestamp
TOPICPREFIX='omix/rep/'
def set_state(topic, result, msg=None):
    topic = TOPICPREFIX + topic
    if msg:
        if result == 'error':
            _log_error(msg)
        else:
            _log_info(msg)

def _log_error(msg):
    print("error: ", str(msg).strip())


def _log_info(msg):
    print("info: ", str(msg).strip())


def _log_returncode(code, fname, logname):
    if not code:
        return
    if code == 255:
        return
    if code > 0:
        _log_error("{}: something wrong; code: {}; log: {}".format(fname, code, logname))
    if code < 0:
        _log_error("{}: killed by signal: {}; log: {}".format(fname, code, logname))


def loadconfig():
    with open(confdir+"/omix_backup.json", 'r') as conffile:
        # global backup_config
        try:
            config = json.load(conffile)
            set_state('config', 'ok')
        except JSONDecodeError as e:
            config = None
            set_state('config','error', "incorrect config: " + str(e))
        return config


# def check_shutdown():
#     if shutdown.is_set():
#         raise Shutdown()
#

def logfilename(parts):
    return "{}/{}_{}.log".format(logdir, '_'.join(parts), datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))


def check_cmd_is_running(host, cmd):
    ps = run(['ssh', "root@" + host, 'pgrep -fx "{}"'.format(cmd)], stdout=DEVNULL, stderr=STDOUT)
    return ps.returncode == 0


def check_fs(host, fs):
    ps = run(['ssh', "root@" + host, 'zfs list', fs], stdout=DEVNULL, stderr=STDOUT)
    return ps.returncode == 0


def put_config(host, fs, config):
    run(['ssh', "root@" + host, 'cat > /'+fs], input=config, check=True, universal_newlines=True)
    return


def ping(host):  # ping -q -c3 -W10
    ps = run(['nc', '-z', host, '22'], stdout=DEVNULL, stderr=DEVNULL)
    # ps = run(['ping', '-c3', '-W10', host], stdout=DEVNULL, stderr=DEVNULL)
    return ps.returncode == 0


def reachable(host):
    reachable = True
    if not ping(host):
        _log_error("host unreachable: " + host)
        reachable = False
    if reachable:
        if not check_ssh(host):
            _log_error("source ssh key login failed: " + host)
            reachable = False
    return reachable


# def reachable2(src_host, dest_host):
#     reachable = True
#     if not ping(src_host):
#         _log_error("source unreachable: " + src_host)
#         reachable = False
#     if not ping(dest_host):
#         _log_error("destination unreachable: " + dest_host)
#         reachable = False
#     if reachable:
#         if not check_ssh(src_host):
#             _log_error("source ssh key login failed: " + src_host)
#             reachable = False
#         if not check_ssh(dest_host):
#             _log_error("destination ssh key login failed: " + dest_host)
#             reachable = False
#     return reachable


def check_ssh(host):
    try:
        run(['ssh', "root@" + host, 'exit'], timeout=10)  #  , stdout=DEVNULL, stderr=DEVNULL
    except TimeoutExpired:
        return False
    return True


def check_and_create_fs(host, fs):
    if not check_fs(host, fs):
        run(['ssh', "root@" + host, 'zfs create -p', fs], check=True)
        # TODO: instead of CalledProcessError check stderr on eroor and raise own exception


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
    if log:
        _log_info("remote sync send: from {} cmd: {}...".format(send_host, send_cmd))
        _log_info("remote sync recv: to   {} cmd: {}...".format(recv_host, recv_cmd))
    # if send_host and send_cmd:
    # if recv_host and recv_cmd:

    with sync_lock:
        recv = remote_sync_cmd(recv_host, recv_cmd, log)
        sleep(2)
        if recv.returncode is not None:
            log and _log_error("remote_sync: recv cant start see log {}; retcode: {}".format(log.name, recv.returncode))
            return False
        send = remote_sync_cmd(send_host, send_cmd, log)
        log and _log_info("remote sync wait for transfer: {} -> {}".format(send_host, recv_host))
        ret = remote_script(host=recv_host, script=timeout_port + free_up_port + exit_code,
                            args=["30"]).strip()
        if log:
            if ret == 'ok':
                _log_info("remote sync transfer begun: {} -> {}".format(send_host, recv_host))
            else:
                _log_error("remote sync transfer failed: {} -> {}".format(send_host, recv_host))

    send.wait()
    recv.wait()

    _log_returncode(send.returncode, "remote_sync send", log.name if log else None)
    _log_returncode(recv.returncode, "remote_sync recv", log.name if log else None)

    return send.returncode == 0 and recv.returncode == 0

def addremovelist(old, new):
    toadd = [i for i in new if i.name not in [s.name for s in old]]
    toremove = [i for i in old if i.name not in [s.name for s in new]]
    for r in toremove:
        old.remove(r)
    old.extend(toadd)


class Dataset:
    def __str__(self):
        s = "client: {}; src_host: {}; src_path: {}; dest_host: {}; " \
            "dest_path: {}; dest_exists: {}; start: {}; last: {}; " \
            "interval: {}; next: {}; next_update: {};"
        return s.format(self.client, self.src_host, self.src_path, self.dest_host,
                        self.dest_path, self.dest_exists, self.start, self.last,
                        self.interval, self.next, self.last_update)

    def __init__(self, client, src_host, src_path, dest_host, dest_path, origins_path):
        self.client = client
        self.src_host, self.src_path = src_host, src_path
        self.dest_host, self.dest_path = dest_host, dest_path
        self.origins = []
        self.origins_path = origins_path
        # self.dest_host = host if self.dest_host == 'localhost' else self.dest_host
        self.dest_exists = None
        # self.snap = None
        self.start = None
        self.last = None
        self.interval = None
        self.next = None
        self.last_update = 0
        self.logfile = None
        self.shutdown = None

        # self.update()  # srchost may be unreachable on service start

    def _log_next_sync(self):
        _log_info("Next sync for: {}:{} on: {}".
                  format(self.src_host, self.src_path,
                         datetime.fromtimestamp(self.next).strftime('%Y-%m-%d %H:%M:%S')))

    def set_orphan_origins(self):
        cmd = get_orphan_origins_cmd.format(fs=self.src_path, re=re.escape(self.src_path))
        orphan_origins_text = remote_script(host=self.src_host, script=cmd).strip()
        if not orphan_origins_text:
            return
        orphan_origins = {}
        for line in orphan_origins_text.splitlines():
            name, guid = line.split()
            orphan_origins.update({guid: name})
        existing_text = remote_script(host=self.dest_host,
                                      script="zfs get guid -Hovalue | egrep '$1'",
                                      args=['|'.join(orphan_origins.keys())]).strip()

        existing = existing_text.splitlines()
        self.origins.clear()
        for src_path in [ v for k,v in orphan_origins if k not in existing]:
            self.origins.append(
                Origin(src_path, self.origins_path + '/' + os.path.basename(src_path)))

    def update(self):
        # if not force and datetime.now().timestamp() < self.next_update:
        #     return
        self.dest_exists = check_fs(self.dest_host, self.dest_path)
        if not self.dest_exists:
            self._del_sync_src()
            self.set_orphan_origins()
        params = remote_script(host=self.src_host, script=get_dataset_params, args=[self.src_path])
        if params:
            params = json.loads(params)  # exception on wrong params
            # self.origin = params["origin"]
            # self.snap = params["snap_first"]
            self.start = datetime.strptime(params["omix_sync_start"], "%Y-%m-%d %H:%M").timestamp()\
                if params["omix_sync_start"] else 0
            self.last = params["omix_sync_time"]
            # if self.last == 0:
            #     self._del_sync_dest()
            # elif not check_fs(self.dest_host, '{}@omix_sync'.format(self.dest_path)):
            #     self.last = 0
            self.interval = self._interval_to_timestamp(params["omix_sync_interval"])
            self.next = self.last + self.interval  # TODO make daily at night
            if self.start > self.next:
                self.next = self.start
            # self.next_update = datetime.now().timestamp() + 3600
            self.last_update = datetime.now().timestamp()
        else:
            pass

            # connection error
            # self.next = datetime.now().timestamp() + FAIL_INTERVAL
            # self.next_update = datetime.now().timestamp() + FAIL_INTERVAL

        # self._log_next_sync()

        # log next sync time
        # self._log_next_sync()
        # _log_info("Next sync for: {}:{} on: {}".
        #           format(self.src_host, self.src_path,
        #                  datetime.fromtimestamp(self.next).strftime('%Y-%m-%d %H:%M:%S')))
        pass

    @staticmethod
    def _interval_to_timestamp(interval):
        if not interval:
            interval = DEFAULT_INTERVAL
        m = {'d': 86400, 'h': 3600, 'm': 60}
        q = int(interval[:-1] or 0)
        k = m.get(interval[-1]) or 0
        return q*k

    def _test(self):
        # try:
        #     run(['nc', '-z', self.src_host, '22'], check=True)
        #     run(['nc', '-z', self.dest_host, '22'], check=True)
        # except CalledProcessError as e:
        #     _log_error("Host unreachable: {}".format(e.cmd[2]))
        #     return False

        if self.src_host == self.dest_host:
            return True

        _log_info("run test connection: {} -> {}".format(self.src_host, self.dest_host))
        cmd_test_send = "nc -zw10 {} 9000".format(self.dest_host)
        cmd_test_recv = free_up_port + "nc -w10 -lp 9000"
        ret = remote_sync(
                send_host=self.src_host,
                send_cmd=cmd_test_send,
                recv_host=self.dest_host,
                recv_cmd=cmd_test_recv,
                log=None)
        if ret:
            _log_info("run connection passed: {} -> {}".format(self.src_host, self.dest_host))
        else:
            _log_error("run connection failed: {} -> {}".format(self.src_host, self.dest_host))
        return ret

    def _snap(self):
        zfs_delete = 'zfs destroy -r {fs}@omix_send 2>/dev/null\n'
        run(['ssh', "root@" + self.dest_host, zfs_delete.format(fs=self.dest_path)])
        run(['ssh', "root@" + self.src_host, zfs_delete.format(fs=self.src_path)])
        run(['ssh', "root@" + self.src_host, 'zfs snap -r {}@omix_send'.format(self.src_path)], check=True)
        return "@omix_send"

    def _del_sync_src(self):
        old_sync = '{}@omix_sync'.format(self.src_path)
        if check_fs(self.src_host, old_sync):
            run(['ssh', "root@" + self.src_host, 'zfs destroy -r {}'.format(old_sync)], check=True)

    def _del_sync_dest(self):
        old_sync = '{}@omix_sync'.format(self.dest_path)
        if check_fs(self.dest_host, old_sync):
            run(['ssh', "root@" + self.dest_host, 'zfs destroy -r {}'.format(old_sync)], check=True)

    def _rename_snap_one(self, src_path, dest_path):
        zfs_rename = 'set -e\n'\
            'if zfs list {fs}@omix_prev >/dev/null 2>&1; then zfs destroy {fs}@omix_prev; fi\n' \
            'if zfs list {fs}@omix_sync >/dev/null 2>&1; then zfs rename {fs}@omix_sync {fs}@omix_prev; fi\n' \
            'zfs rename {fs}@omix_send {fs}@omix_sync\n'
        run(['ssh', "root@" + self.dest_host, zfs_rename.format(fs=dest_path)], check=True)
        run(['ssh', "root@" + self.src_host, zfs_rename.format(fs=src_path)], check=True)

    def _rename_snap_all(self):
        cmd = "zfs get guid -r -Hovalue,name $1  | sed -ne 's/@omix_send//p'"
        srcsnaps = remote_script(host=self.src_host, script=cmd, args=[self.src_path]).strip()
        dstsnaps = remote_script(host=self.dest_host, script=cmd, args=[self.dest_path]).strip()
        send = {}
        for line in srcsnaps.splitlines():
            snap = line.split()
            if not snap[0] in send:
                send[snap[0]] = dict()
            send[snap[0]].update({'src': snap[1]})
        for line in dstsnaps.splitlines():
            snap = line.split()
            if not snap[0] in send:
                send[snap[0]] = dict()
            send[snap[0]].update({'dst': snap[1]})
        for v in send.values():
            if 'src' in v and 'dst' in v:
                self._rename_snap_one(v['src'], v['dst'])
        _log_info("Renamed omix_send to omix_sync for: {}:{}, {}:{}, and descendants".format(
            self.src_host, self.src_path,
            self.dest_host, self.dest_path))

    # def _rename_snap_all(self):
    #     self._rename_snap_all(self.src_path, self.dest_path)

    def _sync(self, cmd_send, cmd_recv):
        log = self.logfile if self.logfile and not self.logfile.closed else None
        mbuf_send = "| mbuffer -q -s 128k -m 256M -O {}:9000 -W 300".format(self.dest_host)
        mbuf_recv = "mbuffer -q -s 128k -m 256M -I 9000 -W 300 | "
        mbuf_loc = "| mbuffer -q -s 128k -m 256M -W 300 |"
        if log:
            self._log_cmd(log, cmd_send, cmd_recv)
        if self.src_host != self.dest_host:
            ret = remote_sync(send_host=self.src_host, send_cmd=cmd_send + mbuf_send,
                              recv_host=self.dest_host, recv_cmd=mbuf_recv + cmd_recv,
                              log=log)
        else:
            ret = remote_sync(send_host=None, send_cmd=None, recv_host=self.dest_host,
                              recv_cmd=cmd_send + mbuf_loc + cmd_recv,
                              log=log)
        if log:
            self._log_result(log, ret)
        if not ret:
            self.next = datetime.now().timestamp() + 60
            self._log_next_sync()
            # _log_info("Next sync for: {}:{} on: {}".
            #           format(self.src_host, self.src_path,
            #                  datetime.fromtimestamp(self.next).strftime('%Y-%m-%d %H:%M:%S')))
        return ret

    def _get_resume_token(self, dest_path):
        resume_token = remote_script(host=self.dest_host,
                   script="zfs get -r -Honame,value receive_resume_token $1 | sed '/-$/d' | sed '1!d' ",
                   args=[dest_path]).strip()
        #
        return resume_token.split() if resume_token else None
        # return "" if resume_token == "-" else resume_token

#     def _find_last_snap(self):
#         dest_snaps = remote_script(host=self.dest_host,
#                                    script="zfs list -rd1 -Htsnap -oname -Screation $1 | sed -e's/^.*@/@/'",
#                                    args=[self.dest_path])
#         dest_snaps = dest_snaps.strip().split('\n')
# #        _log_info("dest_snaps: {}".format(dest_snaps))
#         self.snap = None
#         for snap in dest_snaps:
#             if snap and check_fs(self.src_host, self.src_path + snap):
#                 self.snap = snap
#                 break
#         # if not self.snap:
#         #     self.snap = self.origin if self.origin else None
#         # must destroy cloned fs
#         pass

    @staticmethod
    def _log_cmd(logfile, c1, c2):
        dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logfile.write("begin sync: {}\n".format(dt))
        logfile.write(c1 + "\n")
        logfile.write(c2 + "\n")
        logfile.flush()

    @staticmethod
    def _log_result(logfile, ret):
        dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if ret:
            logfile.write("end sync - OK: {}\n".format(dt))
        else:
            logfile.write("ERROR sync: {}\n".format(dt))
        logfile.flush()

    def resume_sync(self, dest_path):
        while True:
            if self.shutdown.is_set():
                return False
            resume_token = self._get_resume_token(dest_path)
            if not resume_token:
                break
            cmd_send = "zfs send -v -t {}".format(resume_token[1])
            cmd_recv = "zfs recv -suvF {}".format(resume_token[0])
            if self._sync(cmd_send=cmd_send, cmd_recv=cmd_recv):
                break
        return True

    def sync(self, full = False):
        if self.shutdown.is_set():
            return False
        inc = "" if full else " -I @omix_sync"
        cmd_send = "zfs send -RLecv {}@omix_send" + inc.format(self.src_path)
        cmd_recv = "zfs recv -suvF {}".format(self.dest_path)
        return self._sync(cmd_send=cmd_send, cmd_recv=cmd_recv)

    def run(self):

        if not reachable(self.src_host) or not reachable(self.dest_host):
            self.next = datetime.now().timestamp() + FAIL_INTERVAL
            self._log_next_sync()
            return

        # self.update()
        if self.next > datetime.now().timestamp():
            return

        _log_info("sync client: {} fs: {}".format(self.client, self.src_path))
        if not self.dest_exists:
            parent = os.path.dirname(self.dest_path)
            check_and_create_fs(self.dest_host, parent)

        # test connection
        if not self._test():
            self.next = datetime.now().timestamp() + FAIL_INTERVAL
            self._log_next_sync()
            return
        # TODO: check_cmd_is_running must not generate logfile - move it out of "with open"
        if self.dest_exists and not self.last:
            _log_error("lost snap: {} fs: {} @omix_sync"
                       .format(self.client, self.src_path))
            self.next = datetime.now().timestamp() + 3600
            self._log_next_sync()
            return

        # if check_cmd_is_running(self.dest_host, cmd_recv):
        #     _log_info("sync client: {} fs: {} already runing: next try in 1 hour"
        #               .format(self.client, self.src_path))
        #     self.next = datetime.now().timestamp() + 3600
        #     self._log_next_sync()
        #     return

        with open(logfilename((self.client, os.path.basename(self.src_path))), 'w', buffering=1) as self.logfile:
            _log_info("run begin transfer: {}:{} -> {}".format(self.src_host, self.src_path, self.dest_host))
            for origin in self.origins:
                cmd_send = "zfs send -RLecv {}".format(origin.src_path)
                cmd_recv = "zfs recv -suvF {}".format(origin.dst_path)
                if not self._sync(cmd_send=cmd_send, cmd_recv=cmd_recv):
                    if not self.resume_sync(self.dest_path):
                        return

            if self.shutdown.is_set():
                return
            self.update()
            if self.origins:
                return
            if not self.dest_exists:
                # fromorigin = "-I {}".format(self.origin) if self.origin else ""
                self._snap()
                if not self.sync(full=True):
                    if not self.resume_sync(self.dest_path):
                        return
                self._rename_snap_all()
                self.update()

            self._snap()
            if not self.sync():
                if not self.resume_sync(self.dest_path):
                    return
            self._rename_snap_all()
            self.update()
            return

class Path:
    def __init__(self, host, path):
        self.host = host
        self.path = path
        self.dataset = None

    @property
    def name(self):
        return self.host + ':' + self.path

class VM:
    def __init__(self, host, vmid):
        self.host = host
        self.vmid = vmid
        self.paths = []

    @property
    def name(self):
        return self.host + ':' + self.vmid

class Origin:
    def __init__(self, src_path, dst_path):
        self.src_path = src_path
        self.dst_path = dst_path

class DSHolder:
    def __init__(self, client):
        self.client = client
        self._paths = []
        self._vms = []
        self._tpls = []
        self._dest_host = None
        self._dest_path = None

    @property
    def dest(self, dest):
        pass

    @dest.setter
    def dest(self, dest):
        dest = dest if dest != 'omix_cloud' else omix_cloud_dest
        (dest_host, dest_path) = dest.split(':')
        if self._dest_host == None and self._dest_path == None:
            self._dest_host, self._dest_path = dest_host, dest_path
            return
        if self._dest_host != dest_host or self._dest_path != dest_path:
            self._paths.clear()
            self._vms.clear()

    @property
    def paths(self):
        pass

    @paths.setter
    def paths(self, lst):
        addremovelist(self._paths, lst)
        pass

    @property
    def vms(self):
        pass

    @vms.setter
    def vms(self, lst):
        addremovelist(self._vms, lst)
        pass

    def get_all_paths(self):
        all = list(self._paths)
        for vm in self._vms:
            all.extend(vm.paths)
        return all

    def next2run(self):
        rdy2run = {}
        for path in self.get_all_paths():
            if path.dataset and path.dataset.next < datetime.now().timestamp():
                rdy2run.update({path.dataset.next: path.dataset})
        if rdy2run:
            return sorted(rdy2run.items())[0][1]



    def create_dataset(self, path, dest_path, origins_path):
        if not path.dataset:
            if reachable(path.host):
                check_and_create_fs(self._dest_host, dest_path)
                path.dataset = Dataset(self.client,
                                       path.host, path.path,
                                       self._dest_host, dest_path + '/' + os.path.basename(path.path),
                                       origins_path)

    def need_prepare_datasets(self):
        for path in self._paths:
            if not path.dataset:
                return True
        for vm in self._vms:
            if not vm.paths:
                return True


    def prepare_datasets(self):
        if not reachable(self._dest_host):
            return
        check_and_create_fs(self._dest_host, self._dest_path)
        origins_path = self._dest_path + '/' + 'origins'
        check_and_create_fs(self._dest_host, origins_path)
        for path in self._paths:
            self.create_dataset(path, self._dest_path, origins_path)
        self.vm2paths()
        for vm in self._vms:
            dest_path = self._dest_path + '/' + vm.vmid
            for path in vm.paths:
                self.create_dataset(path, dest_path, origins_path)

    def update_datasets(self):
        for path in self.get_all_paths():
            if path.dataset and path.dataset.last_update + 3600 < datetime.now().timestamp():
                path.dataset.update()


    def vm2paths(self):
        paths= []
        for vm in self._vms:
            if not reachable(vm.host):
                continue
            params = remote_script(host=vm.host, script=get_vm_disks_script, args=[str(vm.vmid)]).strip()
            confpaths, conf = params.split('-----\n')
            check_and_create_fs(self._dest_host, self._dest_path)
            put_config(self._dest_host, self._dest_path + '/' + vm.vmid + '.conf', conf)
            confpaths = confpaths.splitlines() if confpaths else []
            for path in confpaths:
                paths.append(Path(vm.host, path))
        addremovelist(vm.paths, paths)

def client_backup(client):
    _log_info("start backup client: " + client['client'])

    dest = client['dest'] if client['dest'] != 'omix_cloud' else omix_cloud_dest
    (dest_host1, dest_path) = dest.split(':')

    datasets = []

    for src in client["src"]:
        paths = src.get("paths")
        vmids = src.get("vmids")
        host = ".".join((src['host'], client['domain']))
        dest_host = host if dest_host1 == 'localhost' else dest_host1

        while not reachable(host, dest_host):
            s = 300
            while s:
                s -= 1
                sleep(1)
                if shutdown.is_set():
                    return

        if paths:
            for path in paths:
                datasets.append(Dataset(
                    client=client,
                    host=host,
                    path=path,
                    dest_host=dest_host,
                    dest_path='/'.join((dest_path, client['client'], path))
                ))
            pass
        elif vmids:
            for vmid in vmids:
                params = remote_script(host=host, script=get_vm_disks_script, args=[str(vmid)]).strip()
                paths,conf = params.split('-----\n')
                config_path = '/'.join((dest_path, client['client']))
                check_and_create_fs(dest_host, config_path)
                put_config(dest_host, config_path + '/' + vmid + '.conf', conf)
                paths = paths.splitlines() if paths else []
                for path in paths:
                    datasets.append(Dataset(
                        client=client,
                        host=host,
                        path=path,
                        dest_host=dest_host,
                        dest='/'.join((dest_path, client['client'], vmid, os.path.basename(path)))
                    ))
            pass
        else:
            _log_error("unknown backup src: " + str(src))
    if len(datasets) == 0:
        _log_error("nothing to do: " + str(client['client']))
        return
    while not shutdown.is_set():
        next_run = datetime.now().timestamp() + 3600 # need for update in run
        _log_info("next run for client: " + client['client'])
        for dataset in datasets:
            try:
                dataset.run()
            except CalledProcessError as e:
                _log_error("Command error: {}".format(e.cmd))
                dataset.next = datetime.now().timestamp() + FAIL_INTERVAL
                dataset._log_next_sync()

            # TODO catch CalledProcessError retry in 10? min or try ping before run
            next_run = min(next_run, dataset.next)  # if next_run else dataset.next
            if shutdown.is_set():
                return

        # for dataset in datasets:   # periodicaly rerun _update for changes of interval
        #     dataset.update()

        # s = next_run - datetime.now().timestamp()
        while next_run > datetime.now().timestamp():
            sleep(2)
            if shutdown.is_set():
                return
            # s -= 1
        sleep(1)

# [
#     {
#         "client" : "client name"
#         "shutdown": True or False,
#         "datasets": [ds1, ds2]
#     }
# ]

class ClientThread(threading.Thread):

    clients=dict()

    def __init__(self, name):
        super().__init__()
        self.client = name
        self.shutdown = Event()
        self.conf = None
        self.dsholder = DSHolder(name)
        ClientThread.clients.update({name: self})

    @staticmethod
    def setconf(confs):
        if not confs:
            return
        must = []
        for conf in confs:
            name = conf["client"]
            must.append(name)
            ct = ClientThread.clients.get(name)
            if not ct:
                ct = ClientThread(name)
                # ClientThread.clients.update({name: ct})
                ct.start()
            ct.conf = conf
        for name, ct in list(ClientThread.clients.items()):
            if name not in must:
                ct.remove()
                # del ClientThread.clients[name]
        pass

    @staticmethod
    def shutdown():
        for ct in ClientThread.clients.values():
            # ct = ClientThread.clients[name]
            ct.shutdown.set()
        sleep(1)
        while len(ClientThread.clients):
            for name, ct in list(ClientThread.clients.items()):
                if not ct.is_alive():
                    del ClientThread.clients[name]

    def remove(self):
        self.shutdown.set()
        set_state(self.client, "remove")
        while self.is_alive():
            sleep(1)
        del ClientThread.clients[self.client]

    def parseconf(self):
        self.dsholder.dest = self.conf['dest'] + '/' + self.client
        paths = []
        vms = []

        for src in list(self.conf["src"]):
            host = ".".join((src['host'], self.conf['domain']))

            confpaths = src.get("paths", [])
            for path in confpaths:
                paths.append(Path(host, path))

            confvmids = src.get("vmids", [])
            for vmid in confvmids:
                vms.append(VM(host, vmid))

        self.dsholder.paths = paths
        self.dsholder.vms = vms
        self.conf = None

    def run(self):
        try:
            self._run()
        except Exception as e:
            _log_error(e)

    def _run(self):
        # threading.local
        zzz = threading.local()
        zzz.ClientThread = self
        prepare_datasets_last = 0
        update_datasets_last = 0
        while not self.shutdown.is_set():
            if self.conf:
                print(self.client, self.conf)
                self.parseconf()
                prepare_datasets_last = 0
                update_datasets_last = 0
            if prepare_datasets_last + 600 < datetime.now().timestamp()\
                    and self.dsholder.need_prepare_datasets():
                self.dsholder.prepare_datasets()
                prepare_datasets_last = datetime.now().timestamp()
            if update_datasets_last + 600 < datetime.now().timestamp():
                self.dsholder.update_datasets()
                update_datasets_last = datetime.now().timestamp()

            dataset = self.dsholder.next2run()
            if dataset:
                dataset.shutdown = self.shutdown
                dataset.run()
            sleep(1)




    # @staticmethod
    # def create(conf):
    #     name = conf["client"]
    #     ct = ClientThread.clients.get(name)
    #     if not ct:
    #         ct = ClientThread()
    #         ClientThread.clients.update({ name: ct })
    #         ct.start()
    #     ct.setconf(conf)
    #     return ct
    #
    # @staticmethod
    # def destroyz(name):
    #     ct = ClientThread.clients.get(name)
    #     if ct:
    #         del ClientThread.clients[name]
    #         ct.__destroy__()


CONFIG_FREQ=20

def start():
    # loadconfig()
    # TODO: verify that omix_cloud_dest creates on rep
    # check_and_create_fs(*omix_cloud_dest.split(':'))

    next_run = 0
    while not shutdown.is_set():
        if next_run < datetime.now().timestamp():
            next_run = datetime.now().timestamp() + CONFIG_FREQ
            print("read conf")
            t = datetime.now().timestamp()
            ClientThread.setconf(loadconfig())
            print("end conf ", datetime.now().timestamp() - t)
        sleep(1)
    ClientThread.shutdown()
    pass
    #     return

    # for clientconf in loadconfig():
    #     ClientThread.get(clientconf)
    #     pass

    # for client in backup_config:
    #     t = threading.Thread(target=client_backup, name=client['client'], args=(client,))
    #     t.start()
    #     client_threads.append(t)
    #     pass

    # while len(client_threads):
    #     sleep(3)
    #     for t in client_threads[:]:
    #         if t.is_alive():
    #             continue
    #         client_threads.remove(t)


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
echo -----
echo "$config"
'''

get_dataset_origin_cmd = "zfs get -Hpo value origin {fs}"
get_orphan_origins_cmd = '''origins=$(zfs list -r {fs} -Hoorigin | sed -e '/-/d' -e '/^{re}/d')
[ -z $origins ] || zfs get guid -Honame,value $origins
'''
#    "zfs get guid -Honame,value $(zfs list -r {fs} -Hoorigin | sed -e '/-/d' -e '/^{re}/d')"
#    "zfs list -r {fs} -Hoorigin | sed -e '/-/d' -e '/^{re}/d'"


get_dataset_params = r'''
dataset=$1

#origin=$(zfs get -Hpo value origin $dataset)
#[ $origin = - ] && origin=

zfs list $dataset@omix_sync >/dev/null 2>&1 \
    && omix_sync_time=$(zfs get -Hpo value creation $dataset@omix_sync) \
    || omix_sync_time=0 

omix_sync_start=$(zfs get -Hpo value "ua.com.omix:sync_start" $dataset)
[ "$omix_sync_start" = "-" ] && omix_sync_start=

omix_sync_interval=$(zfs get -Hpo value "ua.com.omix:sync_interval" $dataset)
[ "$omix_sync_interval" = "-" ] && omix_sync_interval=

cat <<EOF
{
    "dataset": "$dataset",
    "omix_sync_time": $omix_sync_time,
    "omix_sync_start": "$omix_sync_start",
    "omix_sync_interval": "$omix_sync_interval"
}
EOF
'''
#    "origin": "$origin",
timeout_port = r'''T=$1+1; while ((T-=1)); do ss -tlnp | grep -q ' *:9000 ' || break; sleep 1; done; '''
free_up_port = r'''ss -tlnp | awk '$4~/\*:9000/ { print gensub(/^.*,pid=([0-9]*),.*$/,"\\1","g",$6)}' \
| xargs -r kill; '''
exit_code = r'''[ "$T" -ne "0" ] && echo ok'''


shutdown = Event()
def signal_handler(sig, frame):
    if sig == SIGTERM:
        print('Signal TERM!')
    elif sig == SIGINT:
        print('You pressed Ctrl+C!')
    else:
        print('Unknown signal!')
    shutdown.set()

# class Test:
#     def __init__(self):
#         self.logfile = None
#
#     def open(self):
#         self.testlogfile()
#         with open("zzz", 'w', buffering=1) as self.logfile:
#             self.testlogfile()
#
#     def testlogfile(self):
#         if self.logfile and not self.logfile.closed:
#             print("opened")
#         else:
#             print("closed")




if __name__ == "__main__":
    # zzz = remote_script(host="pm1.ssc.bla", script=free_up_port)
    # t = Test()
    # t.open()
    # t.open()
    # t.open()
    # exit(0)
    signal(SIGINT, signal_handler)
    signal(SIGTERM, signal_handler)

    start()
    # try:
    #     while True:
    #         sleep(5)
    # except KeyboardInterrupt:
    #     sys.exit(0)
    # pause()
    pass
    # while not shutdown.is_set():
    #     sleep(3)
    #     for client in backup_config:
    #         client_backup(client)
