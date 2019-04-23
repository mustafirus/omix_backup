#!/usr/bin/python3 -uO
import re
from _signal import SIGTERM, SIG_BLOCK
from datetime import datetime
import json
import os
from json import JSONDecodeError
from signal import signal, SIGINT, pthread_sigmask
from subprocess import PIPE, STDOUT, Popen, DEVNULL, run, CalledProcessError, TimeoutExpired
import threading
from threading import Lock, Event
from time import sleep

# backup_config = list()
import sys

omix_cloud_dest = 'pm1.ssc.bla:rpool/misc/omix-backup'
confdir = '.conf'
logdir = '.log'
# confdir = '/etc/omix_replicate'
# logdir = '/var/log/omix_replicate'
sync_begin_lock = Lock()
client_threads = list()
DEFAULT_INTERVAL = '1d'  # '99999d'
FAIL_INTERVAL = 3600  # seconds
PREPARE_DATASETS_FREQ = 200
TRY_UPDATE_DATASETS_FREQ = 300
UPDATE_DATASETS_FREQ = 3600
CHECK_RUNNING_ORPHAN_SYNC = 3600
TOPICPREFIX = 'omix/rep/'
CONFIG_FREQ = 20
shutdown = Event()
mystate = {}
running_syncs = []
REMOTEPROCBASHCMD = "bash -s -- omixrepl561BAA1"
# DONE: backup <vmid>.conf
# TODO: check time sync with target hosts max delta ~5 sec notify if not refuse to repl if >1h
# TODO: check free mem 1G
# TODO: wait for usb dest pool

# FOR DEBUG
DEFAULT_INTERVAL = '1h'  # '99999d'
FAIL_INTERVAL = 60  # seconds
PREPARE_DATASETS_FREQ = 200
TRY_UPDATE_DATASETS_FREQ = 300
UPDATE_DATASETS_FREQ = 600
CHECK_RUNNING_ORPHAN_SYNC = 20


class BadClient(Exception):
    pass


class SyncError(Exception):
    pass

class SyncUnreachable(Exception):
    pass


class Shutdown(Exception):
    pass


class Dummy(object):
    def __init__(self):
        self.returncode = 0

    def wait(self):
        return 0

    def terminate(self):
        pass

# def preexec_ignore_sigint():
#     signal.signal(signal.SIGINT, signal.SIG_IGN)
#

def set_state(topic, result, msg=None):
    topic = TOPICPREFIX + topic
    mystate.update({topic: result})
    if result == 'remove':
        for k in list(mystate):
            if k.startswith(topic):
                del mystate[k]
    if msg:
        if result == 'error':
            _log_error(msg)
        else:
            _log_info(msg)

def save_state():
    with open(logdir + "/omix_rep.state", 'w') as file:
        # global backup_config
        for k, v in sorted(mystate.items()):
            print(k + "\t" + v, file=file)

def _log_error(msg):
    print("error: ", str(msg).strip())


def _log_info(msg):
    print("info: ", str(msg).strip())

def _log_debug(msg):
    print("debug: ", str(msg).strip())


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
            set_state('config', 'error', "incorrect config: " + str(e))
        save_state()
        return config


# def check_shutdown():
#     if shutdown.is_set():
#         raise Shutdown()
#

def logfilename(parts):
    return "{}/{}_{}.log".format(logdir, '_'.join(parts), datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))

needed_soft = [
    ('mbuffer', 'mbuffer')
]
good_hosts = set()
def check_prerequisites(host):
    if host in good_hosts:
        return True
    if check_and_install_soft(host) and check_zfs_version(host):
        good_hosts.add(host)
        return True
    return False

def check_and_install_soft(host):
    pkgs = []
    for prog, pkg in needed_soft:
        ps = run(['ssh', "root@" + host, 'which {}'.format(prog)], stdout=DEVNULL)
        if ps.returncode != 0:
            pkgs.append(pkg)
    if not pkgs:
        return True
    pkgs = ' '.join(pkgs)
    _log_info("{}: need to install {}".format(host, pkgs))
    run(['ssh', "root@" + host, 'apt-get install -y ' + pkgs], stdout=DEVNULL)
    if ps.returncode == 0:
        _log_info("{}: install OK".format(host))
        return True
    else:
        _log_error("{}: install FAILED".format(host))
        return False


def check_zfs_version(host):
    # ALT  packaging.version.parse ("2.3.1") < packaging.version.parse("10.1.2")
    # this works only in debian
    cmd = "dpkg --compare-versions $(modinfo zfs | sed -n 's/^version: *//p') gt 0.7"
    ps = run(['ssh', "root@" + host, cmd], stdout=DEVNULL)
    if ps.returncode == 0:
        # _log_info("{}: zfs version OK".format(host))
        return True
    else:
        _log_error("{}: zfs version OLDER 0.7".format(host))
        return False


def check_is_running_sync(host):
    ps = run(['ssh', "root@" + host, 'pgrep -fx "{}"'.format(REMOTEPROCBASHCMD)], stdout=DEVNULL)
    if ps.returncode == 0:
        _log_error(host + ": previous sync already running")
        return True
    return False


def check_fs(host, fs):
    # pool = fs.split('/', 1)[0]
    pool = re.split('[/@]', fs, maxsplit=1)[0]
    ps = run(['ssh', "root@" + host, 'zfs list', pool], stdout=DEVNULL, stderr=STDOUT)
    if ps.returncode != 0:
        raise BadClient(" no such pool '%s'" % pool)
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
    ok = True
    if not ping(host):
        _log_error("host unreachable: " + host)
        ok = False
    if ok:
        if not check_ssh(host):
            _log_error("source ssh key login failed: " + host)
            ok = False
    return ok


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
        run(['ssh', "root@" + host, 'exit'], timeout=10, stdout=DEVNULL, stderr=DEVNULL)
    except TimeoutExpired:
        return False
    return True


def check_and_create_fs(host, fs):
    if not check_fs(host, fs):
        run(['ssh', "root@" + host, 'zfs create -p', fs], stderr=PIPE, check=True)

def remote_script(host=None, script=None, args=None):
    shell = Popen(["ssh", "root@" + host, "bash", "-s", "--"] + (args or []),
                  stdin=PIPE, stdout=PIPE, stderr=PIPE, universal_newlines=True)
    (out, err) = shell.communicate(script)
    if shell.returncode != 0:
        _log_error("remote script: %s: %s!" % (host, err))
        return ''
    return out

#xargs
def remote_cmd_xargs(host, cmd, argstr):
    proc = Popen(["ssh", "root@" + host, "xargs " + cmd],
                 stdin=PIPE, stdout=PIPE, stderr=DEVNULL, universal_newlines=True)
    (out, err) = proc.communicate(argstr)
    return out

def remote_sync_cmd(host, cmd, log):
    if host and cmd:
        proc = Popen(["ssh", "root@" + host, REMOTEPROCBASHCMD],
                     stdin=PIPE, stdout=log, stderr=STDOUT, universal_newlines=True)
        # SIGINT SIGTERM SIGHUP SIGPIPE SIGQUIT
        # "set -e\n"
        pre = "trap '' SIGPIPE\n"
        proc.stdin.write(pre)
        proc.stdin.write(cmd)
        proc.stdin.close()
        sleep(1)
        proc.poll()
    else:
        proc = Dummy()
        proc.returncode = 0
    return proc


def remote_sync_begin(send_host, send_cmd, recv_host, recv_cmd, log=None):
    if log:
        _log_info("remote sync send: from {} cmd: {}...".format(send_host, send_cmd))
        _log_info("remote sync recv: to   {} cmd: {}...".format(recv_host, recv_cmd))
    # if send_host and send_cmd:
    # if recv_host and recv_cmd:

    if recv_host and recv_cmd:
        recv = remote_sync_cmd(recv_host, recv_cmd, log)
        sleep(2)
        if recv.returncode is not None:
            log and _log_error("remote_sync: recv cant start: see log {}; retcode: {}".format(log.name, recv.returncode))
            return Dummy(), recv
    else:
        recv = Dummy()
    send = remote_sync_cmd(send_host, send_cmd, log)
    log and _log_info("remote sync wait for transfer: {} -> {}".format(send_host, recv_host))
    if recv_host and recv_cmd:
        ret = remote_script(host=recv_host, script=timeout_port + free_up_port + exit_code,
                            args=["30"]).strip()
    else:
        ret = 'ok'
    if log:  # dont log result from _test_sync_connection (slightly ugly)
        if ret == 'ok':
            _log_info("remote sync transfer begun: {} -> {}".format(send_host, recv_host))
        else:
            _log_error("remote sync transfer failed: {} -> {}".format(send_host, recv_host))

    return send, recv


def remote_sync_wait(sr, log=None):
    running_syncs.append(sr)
    send, recv = sr
    send.wait()
    recv.wait()
    running_syncs.remove(sr)

    # if send.returncode == -9:
    #     raise Shutdown
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
    # def __str__(self):
    #     s = "client: {}; src_host: {}; src_path: {}; dest_host: {}; " \
    #         "dest_path: {}; dest_exists: {}; start: {}; last: {}; " \
    #         "interval: {}; next: {}; next_update: {};"
    #     return s.format(self.client, self.src_host, self.src_path, self.dest_host,
    #                     self.dest_path, self.dest_exists, self.start, self.last,
    #                     self.interval, self.next, self.last_update)

    def __init__(self, client, src_host, src_path, dest_host, dest_path, origins_path):
        self.client = client
        self.src_host, self.src_path = src_host, src_path
        self.dest_host, self.dest_path = dest_host, dest_path
        # self.origins = []
        self.origins_path = origins_path
        # self.dest_host = host if self.dest_host == 'localhost' else self.dest_host
        # self.dest_exists = None
        # self.snap = None
        self.start = None
        self.last = None
        self.interval = None
        self.next = None
        self.last_update = 0
        self.logfile = None
        self.shutdown = None

        # self.update()  # srchost may be unreachable on service start
    def check_shutdown(self):
        if self.shutdown.is_set():
            raise Shutdown

    def dest_exists(self):
        return check_fs(self.dest_host, self.dest_path)

    def get_orphan_origins(self):
        cmd = get_orphan_origins_cmd.format(fs=self.src_path, re=re.escape(self.src_path))
        orphan_origins_text = remote_script(host=self.src_host, script=cmd).strip()
        if not orphan_origins_text:
            return []
        orphan_origins = {}
        for line in orphan_origins_text.splitlines():
            name, guid = line.split()
            orphan_origins.update({guid: name})
        existing_text = remote_script(host=self.dest_host,
                                      script="zfs get guid -Hovalue | egrep '$1'",
                                      args=['|'.join(orphan_origins.keys())]).strip()

        existing = existing_text.splitlines()
        origins = []
        for src_path in [v for k, v in orphan_origins.items() if k not in existing]:
            origins.append(
                Origin(src_path, self.origins_path + '/' + os.path.basename(src_path)))

    def _log_next_sync(self):
        _log_info("Next sync for: {}:{} on: {}".
                  format(self.src_host, self.src_path,
                         datetime.fromtimestamp(self.next).strftime('%Y-%m-%d %H:%M:%S')))

    def _set_next_sync(self, interval):
        self.next = datetime.now().timestamp() + interval
        self._log_next_sync()

    def set_next_planed_sync(self):
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
            self._log_next_sync()
        else:
            raise Exception('Fuck!!!')


        # if not self.dest_exists:
        #     self._del_sync_src()
        # if not self.last:
        #     self.set_orphan_origins()

    @staticmethod
    def _interval_to_timestamp(interval):
        if not interval:
            interval = DEFAULT_INTERVAL
        m = {'d': 86400, 'h': 3600, 'm': 60}
        q = int(interval[:-1] or 0)
        k = m.get(interval[-1]) or 0
        return q*k

    def _test_sync_connection(self):
        if self.src_host == self.dest_host:
            return True

        _log_info("run test connection: {} -> {}".format(self.src_host, self.dest_host))
        cmd_test_send = "nc -zw10 {} 9000".format(self.dest_host)
        cmd_test_recv = free_up_port + "nc -w10 -lp 9000"
        with sync_begin_lock:
            sr = remote_sync_begin(
                send_host=self.src_host, send_cmd=cmd_test_send,
                recv_host=self.dest_host, recv_cmd=cmd_test_recv)
            ret = remote_sync_wait(sr)
        if ret:
            _log_info("test connection passed: {} -> {}".format(self.src_host, self.dest_host))
        else:
            _log_error("test connection failed: {} -> {}".format(self.src_host, self.dest_host))
        return ret

    def _snap(self):
        zfs_delete = 'zfs destroy -r {fs}@omix_send 2>/dev/null\n'
        run(['ssh', "root@" + self.dest_host, zfs_delete.format(fs=self.dest_path)])
        run(['ssh', "root@" + self.src_host, zfs_delete.format(fs=self.src_path)])
        run(['ssh', "root@" + self.src_host, 'zfs snap -r {}@omix_send'.format(self.src_path)], check=True)
        return "@omix_send"

    def _del_sync_src(self):
        for snap in ['send', 'sync', 'prev']:
            self._del_snap_src(snap)

    def _del_snap_src(self, snap):
        old_sync = '{}@omix_{}'.format(self.src_path, snap)
        if check_fs(self.src_host, old_sync):
            run(['ssh', "root@" + self.src_host, 'zfs destroy -r ' + old_sync], check=True)

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

    def rename_snap_all(self):
        self.restore_original_snaps()
        self._rename_snap_all()

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
        renamed = False
        for v in send.values():
            if 'src' in v and 'dst' in v:
                renamed = True
                self._rename_snap_one(v['src'], v['dst'])
        if renamed:
            _log_info("Renamed omix_send to omix_sync for: {}:{}, {}:{}, and descendants".format(
                self.src_host, self.src_path,
                self.dest_host, self.dest_path))

    def fix_last_common_snap(self, src_path, dest_path):
        lastsnapname = ''
        allsrcsnaps = remote_script(self.src_host, "zfs list -rd1 -tsnap -Honame -Screation $1", [src_path])
        if allsrcsnaps:
            alldstsnaps = re.sub(r'^' + src_path, dest_path, allsrcsnaps, flags=re.MULTILINE)
            lastsnapname = remote_cmd_xargs(self.dest_host,
                                               "zfs list -Honame 2>/dev/null | sed -e '1!d' -e 's/^.*@//'", alldstsnaps)
        if not lastsnapname:
            remote_script(self.dest_host, destroy_all_snaps_in_dataset, [dest_path])
            return
        remote_script(self.src_host, last_snap_make_sync_cmd, [src_path, lastsnapname])

    def fix_incomplete_sync(self):
        dms = remote_script(self.src_host, datasets_missed_snap_script, [self.src_path, "omix_sync"])
        if not dms: return
        dms = re.sub(r'^' + self.src_path, self.dest_path, dms, flags=re.MULTILINE)
        dms = remote_cmd_xargs(self.dest_host, "zfs list -Honame", dms)
        datasets_dst = dms.splitlines()
        dms = re.sub(r'^' + self.dest_path, self.src_path, dms, flags=re.MULTILINE)
        datasets_src = dms.splitlines()
        for n in range(0,len(datasets_src)):
            self.fix_last_common_snap(datasets_src[n], datasets_dst[n])
        pass

    def restore_original_snaps(self):
        remote_script(self.src_host, restore_original_snaps_cmd, [self.src_path])

    def _get_resume_token(self, dest_path):
        resume_token = remote_script(host=self.dest_host,
                   script="zfs get -r -Honame,value receive_resume_token $1 | sed '/-$/d' | sed '1!d' ",
                   args=[dest_path]).strip()
        return resume_token.split() if resume_token else None

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

    def _sync(self, cmd_send, cmd_recv):
        self.check_shutdown()
        mbuf_send = "| mbuffer -q -s 128k -m 256M -O {}:9000 -W 300".format(self.dest_host)
        mbuf_recv = "mbuffer -q -s 128k -m 256M -I 9000 -W 300 | "
        mbuf_loc = "| mbuffer -q -s 128k -m 256M -W 300 |"

        log = self.logfile if self.logfile and not self.logfile.closed else None
        with sync_begin_lock:
            self._log_cmd(log, cmd_send, cmd_recv)
            if self.src_host != self.dest_host:
                sr = remote_sync_begin(send_host=self.src_host, send_cmd=cmd_send + mbuf_send,
                                  recv_host=self.dest_host, recv_cmd=mbuf_recv + cmd_recv,
                                  log=log)
            else:
                sr = remote_sync_begin(send_host=self.src_host, send_cmd=cmd_send + mbuf_loc + cmd_recv,
                                  recv_host=None, recv_cmd=None,
                                  log=log)
        ret = remote_sync_wait(sr, log)
        self._log_result(log, ret)
        if not ret:
            raise SyncError
        # if not ret:
        #     self._set_next_sync(FAIL_INTERVAL)

    def sync(self, full=False):
        self.check_shutdown()
        if self.dest_exists():
            inc = " -I @omix_sync"
            _log_info("begin incremental transfer: {}:{} -> {}".format(self.src_host, self.src_path, self.dest_host))
        else:
            inc = ""
            _log_info("begin full transfer: {}:{} -> {}".format(self.src_host, self.src_path, self.dest_host))
        cmd_send = "zfs send -RLecv {}@omix_send".format(self.src_path) + inc
        cmd_recv = "zfs recv -suvF {}".format(self.dest_path)
        self._snap()
        self._sync(cmd_send=cmd_send, cmd_recv=cmd_recv)

    def resume_sync(self, dest_path):
        while True:
            resume_token = self._get_resume_token(dest_path)
            if not resume_token:
                break
            token_dest_path, token = resume_token
            cmd_send = "zfs send -v -t {}".format(token)
            cmd_recv = "zfs recv -suvF {}".format(token_dest_path)
            _log_info("checking: {} -> {}:{}".format(self.src_host, self.dest_host, token_dest_path))
            self._sync(cmd_send=cmd_send, cmd_recv=cmd_recv)

    def check_and_sync_origins(self):
        origins = self.get_orphan_origins()
        if origins:
            _log_info("transfer origins of: {}:{} -> {}".format(self.src_host, self.src_path, self.dest_host))
            for origin in origins:
                self.resume_sync(origin.dst_path)
                _log_info("transfer origins of: {}:{} -> {}".format(self.src_host, self.src_path, self.dest_host))
                cmd_send = "zfs send -RLecv {}".format(origin.src_path)
                cmd_recv = "zfs recv -suvF {}".format(origin.dst_path)
                self._sync(cmd_send=cmd_send, cmd_recv=cmd_recv)

        if self.get_orphan_origins():
            _log_error("origins must be in sync at this point")
            raise SyncError

    def run(self):
        # self.fix_incomplete_sync()
        # return
        try:
            if self._run():
                self.set_state('ok')
            else:
                self._set_next_sync(FAIL_INTERVAL)
                self.set_state('error')
        except SyncError:
            self._set_next_sync(FAIL_INTERVAL)
            self.set_state('error')
        except SyncUnreachable:
            self._set_next_sync(FAIL_INTERVAL)
            self.set_state('error')

    def _run(self):

        if self.next > datetime.now().timestamp():
            _log_info("dont reach this point")
            return

        if not reachable(self.src_host) or not reachable(self.dest_host):
            raise SyncUnreachable
        if not self._test_sync_connection():
            raise SyncUnreachable

        _log_info("sync client: {} fs: {}".format(self.client, self.src_path))
        if not self.dest_exists():
            check_and_create_fs(self.dest_host, os.path.dirname(self.dest_path))
            self._del_sync_src()

        incomplete = check_fs(self.src_host, self.src_path + "@omix_send")

        with open(logfilename((self.client, os.path.basename(self.src_path))), 'w', buffering=1) as self.logfile:
            self.check_and_sync_origins()

            if incomplete:
                self.resume_sync(self.dest_path)
                self.rename_snap_all()
                self.fix_incomplete_sync()

            self.sync()
            self.rename_snap_all()
            self.set_next_planed_sync()

            return True

    def set_state(self, res, msg=None):
        set_state("/".join((self.client, self.src_host, self.src_path)), res, msg)


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
    def dest(self):
        return None

    @property
    def dest_host(self):
        return self._dest_host

    @dest.setter
    def dest(self, dest):
        dest = dest if dest != 'omix_cloud' else omix_cloud_dest
        (dest_host, dest_path) = dest.split(':')
        if self._dest_host is None and self._dest_path is None:
            self._dest_host, self._dest_path = dest_host, dest_path
            return
        if self._dest_host != dest_host or self._dest_path != dest_path:
            self._paths.clear()
            self._vms.clear()

    @property
    def paths(self):
        return None

    @paths.setter
    def paths(self, lst):
        addremovelist(self._paths, lst)
        pass

    @property
    def vms(self):
        return None

    @vms.setter
    def vms(self, lst):
        addremovelist(self._vms, lst)
        pass

    def get_all_paths(self):
        allpaths = list(self._paths)
        for vm in self._vms:
            allpaths.extend(vm.paths)
        return allpaths

    def lognextsync(self):
        for path in self.get_all_paths():
            path.dataset._log_next_sync()

    def next2run(self):
        runseq = {}
        for path in self.get_all_paths():
            runseq.update({path.dataset.next: path.dataset})
        torun = sorted(runseq.items())
        if not torun:
            _log_debug("{}: nothing to run".format(self.client))
            return None
        next_run, dataset = torun[0]
        if next_run > datetime.now().timestamp():
            # _log_info("{}: next run in {}s".format(self.client, next_run - datetime.now().timestamp()))
            return None
        return dataset
    # def next2run(self):
    #     rdy2run = {}
    #     for path in self.get_all_paths():
    #         if path.dataset and path.dataset.next < datetime.now().timestamp():
    #             rdy2run.update({path.dataset.next: path.dataset})
    #     if rdy2run:
    #         return sorted(rdy2run.items())[0][1]

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
        if not self.need_prepare_datasets():
            return
        if not reachable(self._dest_host):
            return
        check_and_create_fs(self._dest_host, self._dest_path)
        origins_path = self._dest_path + '/' + 'origins'
        check_and_create_fs(self._dest_host, origins_path)
        for path in self._paths:
            self.create_dataset(path, self._dest_path + '/' + path.host, origins_path)
        self.vm2paths()
        for vm in self._vms:
            dest_path = self._dest_path + '/' + vm.vmid
            for path in vm.paths:
                self.create_dataset(path, dest_path, origins_path)

    def update_datasets(self):
        for path in self.get_all_paths():
            if path.dataset and path.dataset.last_update + 3600 < datetime.now().timestamp():
                path.dataset.set_next_planed_sync()

    def vm2paths(self):
        for vm in self._vms:
            if not reachable(vm.host):
                continue
            paths = []
            params = remote_script(host=vm.host, script=get_vm_disks_script, args=[str(vm.vmid)]).strip()
            confpaths, conf = params.split('-----\n')
            check_and_create_fs(self._dest_host, self._dest_path)
            put_config(self._dest_host, self._dest_path + '/' + vm.vmid + '.conf', conf)
            confpaths = confpaths.splitlines() if confpaths else []
            for path in confpaths:
                paths.append(Path(vm.host, path))
            addremovelist(vm.paths, paths)


class ClientThread(threading.Thread):

    clients = dict()

    def __init__(self, name):
        super().__init__()
        self.client = name
        self.shutdown = Event()
        self.conf = None
        self.seq = None
        self.dsholder = DSHolder(name)
        ClientThread.clients.update({name: self})

    @staticmethod
    def setconf():
        confs = loadconfig()
        if not confs:
            return
        must = []
        for conf in confs:
            name = conf["client"]
            seq = conf["seq"]
            must.append(name)
            ct = ClientThread.clients.get(name)
            if ct and ct.seq == seq:
                continue
            if not ct or not ct.is_alive():
                ct = ClientThread(name)
                # ClientThread.clients.update({name: ct})
                ct.start()
            ct.conf = conf
            ct.seq = seq
        for name, ct in list(ClientThread.clients.items()):
            if name not in must:
                ct.remove()
                # del ClientThread.clients[name]
        pass

    @staticmethod
    def shutdown_all():
        for ct in ClientThread.clients.values():
            # ct = ClientThread.clients[name]
            ct.shutdown.set()
        sleep(1)
        for _ in range(1, 3):
            if not running_syncs:
                break
            sleep(1)
        for s, r in running_syncs:
            r.terminate()
            s.terminate()

        while len(ClientThread.clients):
            for name, ct in list(ClientThread.clients.items()):
                if not ct.is_alive():
                    del ClientThread.clients[name]

    def remove(self):
        self.shutdown.set()
        set_state(self.client, "remove", "client '{client}' no longer exists in conf".format(client=self.client))
        while self.is_alive():
            sleep(1)
        del ClientThread.clients[self.client]

    def parseconf(self):
        self.dsholder.dest = self.conf['dest'] + '/' + self.client
        host = self.dsholder.dest_host
        if not check_prerequisites(host):
            raise BadClient("check prerequisites!")

        paths = []
        vms = []

        for src in list(self.conf["src"]):
            host = ".".join((src['host'], self.conf['domain']))
            if not check_prerequisites(host):
                set_state(self.client + "/" + host, "error")
                continue

            confpaths = src.get("paths", [])
            for path in confpaths:
                paths.append(Path(host, path))

            confvmids = src.get("vmids", [])
            for vmid in confvmids:
                vms.append(VM(host, vmid))

        if not paths and not vms:
            raise BadClient("bad / nothing to do")

        self.dsholder.paths = paths
        self.dsholder.vms = vms
        self.conf = None

    def set_state(self, res, msg=None):
        set_state(self.client, res, self.client + ": " + msg)

    def run(self):
        try:
            self._run()
        except CalledProcessError as e:
            self.set_state('error', e.stderr.decode("utf-8") if e.stderr else str(e))
        except BadClient as e:
            self.set_state('error', str(e))
        except Shutdown:
            self.set_state('ok', 'shutting down')
        # except Exception as e:
        #     _log_error(self.client + ": unknown:" + str(e))
        #     raise e.with_traceback(sys.exc_info()[2])
        finally:
            _log_info(self.client + ": exit thread")
            save_state()

    def _run(self):
        # threading.local
        threading.local().ClientThread = self
        # prepare_datasets_last = 0
        # update_datasets_last = 0
        nr_prepare_datasets = NextRunner(self.dsholder.prepare_datasets, PREPARE_DATASETS_FREQ)
        nr_update_datasets = NextRunner(self.dsholder.update_datasets, TRY_UPDATE_DATASETS_FREQ)

        self.dsholder.lognextsync()

        while not self.shutdown.is_set():
            if self.conf:
                # print(self.client, self.conf)
                self.parseconf()
                nr_prepare_datasets.reset()
                nr_update_datasets.reset()
            nr_prepare_datasets.run()
            nr_update_datasets.run()

            dataset = self.dsholder.next2run()
            if dataset:
                if check_is_running_sync(dataset.src_host):
                    dataset._set_next_sync(CHECK_RUNNING_ORPHAN_SYNC)
                else:
                    dataset.shutdown = self.shutdown
                    dataset.run()
            sleep(1)


class NextRunner:
    def __init__(self, fn, freq):
        self.fn = fn
        self.freq = freq
        self.next_run = 0

    def reset(self):
        self.next_run = 0

    def run(self):
        if self.next_run < datetime.now().timestamp():
            self.fn()
            self.next_run = datetime.now().timestamp() + self.freq


# def start():
#     nr_setconf = NextRunner(ClientThread.setconf, CONFIG_FREQ)
#     while not shutdown.is_set():
#         nr_setconf.run()
#         sleep(1)
#     ClientThread.shutdown_all()


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

datasets_missed_snap_script = '''
ds=$1
snap=$2
for fs in $(zfs list -r -Honame $ds); do
  zfs list $fs@$snap >/dev/null 2>&1 || echo $fs
done
'''
destroy_all_snaps_in_dataset='''
for snap in `zfs list -rd1 -tsnap -Honame $1`
do
zfs destroy $snap
done
'''
last_snap_make_sync_cmd='''
ds=$1
snap=$2
set -e
zfs set ua.omix.bla:omix_old=$snap $ds@$snap
zfs rename $ds@$snap $ds@omix_sync
'''
restore_original_snaps_cmd='''
ds=$1
set -e
script='{ print "zfs rename " $2"@"$3 " " $2"@"$1 "; zfs inherit ua.omix.bla:omix_old " $2"@"$1 "\\n" }
'
commands=$(zfs get -r -Hovalue,name ua.omix.bla:omix_old $ds | sed -e '/^-/d' -e 's/@/ /' |  awk "$script")
eval "$commands"
'''


def signal_handler(sig, frame):
    if sig == SIGTERM:
        print('Signal TERM!')
    elif sig == SIGINT:
        print('You pressed Ctrl+C!')
    else:
        print('Unknown signal!')
    shutdown.set()



if __name__ == "__main__":
    # exit(0)
    # signal(SIGINT, signal_handler)
    # signal(SIGTERM, signal_handler)
    # pthread_sigmask(SIG_BLOCK, {SIGINT})
    # with open("zzzaaa.txt", 'w', buffering=1) as logfile:
    #     cmd = "while true; do\n" \
    #           "sleep 1\n" \
    #           "done"
    #     proc = remote_sync_cmd("deb.home.nt.bla", cmd, logfile)
    #     ret = proc.poll()
    #     while not shutdown.is_set():
    #         ret = proc.poll()
    #         if ret is not None:
    #             break
    #         sleep(1)
    #
    #     ret = proc.poll()
    #     proc.terminate()
    #     while True:
    #         ret = proc.poll()
    #         if ret is not None:
    #             break
    #         sleep(1)
    #
    # # check_and_install_soft("deb.home.nt.bla")
    signal(SIGINT, signal_handler)
    signal(SIGTERM, signal_handler)
    pthread_sigmask(SIG_BLOCK, {SIGINT})
    _log_info("service started")
    nr_setconf = NextRunner(ClientThread.setconf, CONFIG_FREQ)
    while not shutdown.is_set():
        nr_setconf.run()
        sleep(1)
    ClientThread.shutdown_all()
    _log_info("service stopped")
