#!/usr/bin/python3
# apt-get install python3-dateutil
import re
import shlex
import subprocess
from datetime import datetime, date, timedelta
from subprocess import PIPE
from time import sleep

import itertools
from dateutil.relativedelta import relativedelta
state_file = "/var/run/ozas.state"
run_next = 0
run_next_hour = 0
run_next_day = 0
run_next_week = 0
run_next_month = 0
zpools = ""


def _log_error(msg):
#    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "error: ", str(msg).strip(), flush=True)
    print("error: ", str(msg).strip(), flush=True)


def _log_info(msg):
#    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "info: ", str(msg).strip(), flush=True)
    print("info: ", str(msg).strip(), flush=True)


def next_hour():
    return datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)


def next_day():
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)


def next_week():
    td = date.today()
    return datetime.combine(td + timedelta(days=(6-td.weekday())), datetime.min.time())


def next_month():
    return datetime.combine(date.today().replace(day=1) + relativedelta(months=1), datetime.min.time())


def subrun(cmd):
    _log_info("cmd: {}".format(cmd))
    cp = subprocess.run(shlex.split(cmd),
                        stdout=PIPE, stderr=PIPE, timeout=600, universal_newlines=True)
    if cp.returncode != 0:
        _log_error("stderr: {}".format(cp.stderr))
    return cp.stdout


def loadstate():
    try:
        with open(state_file) as file:
            global run_next, run_next_hour, run_next_day, run_next_week, run_next_month
            (v, run_next_hour, run_next_day, run_next_week, run_next_month) = file.read().split(' ')
            if v != "v1":
                raise OSError(strerror="Incorrect state file")
            run_next_hour, run_next_day, run_next_week, run_next_month =\
                float(run_next_hour), float(run_next_day), float(run_next_week), float(run_next_month)
            run_next = min((run_next_hour, run_next_day, run_next_week, run_next_month))
    except (OSError, ValueError) as err:
        _log_error(err)
        _log_info("create new statefile")
        shedule()
    global zpools
    zpools = []
    for zpool in subrun("/sbin/zpool list -Ho name").strip().splitlines():
        zpools.append(zpool)


def shedule():
    global run_next, run_next_hour, run_next_day, run_next_week, run_next_month
    run_next_month = next_month().timestamp()
    run_next_week = next_week().timestamp()
    run_next_day = next_day().timestamp()
    run_next_hour = next_hour().timestamp()
    run_next = min((run_next_hour, run_next_day, run_next_week, run_next_month))
    with open(state_file, mode='w') as file:
        file.write('{} {} {} {} {}'.format("v1", run_next_hour, run_next_day, run_next_week, run_next_month))


def snapit():
    if datetime.now().timestamp() > run_next_month:
        period = "monthly"
    elif datetime.now().timestamp() > run_next_week:
        period = "weekly"
    elif datetime.now().timestamp() > run_next_day:
        period = "dayly"
    elif datetime.now().timestamp() > run_next_hour:
        period = "hourly"
    else:
        period = "-"
    snapname = "omix_{}-{}".format(period, datetime.now().strftime("%Y-%m-%d-%H%M"))
    for zpool in zpools:
        val = subrun("/sbin/zfs get ua.com.omix:autosnap -Ho value {}".format(zpool)).strip().lower()
        periods = periods_from_attribute(val)
        r = periods.get(period[0])
        if r and r > 0:
            subrun("/sbin/zfs snap -r {}@{}".format(zpool, snapname))
    shedule()


def periods_from_attribute(val):
    re_dD = re.compile("(\d+)(\D)")
    periods = {}
    if val == "-":
        return periods
    for val in val.split():
        try:
            (count, period) = re_dD.split(val)[1:3]
            if period in ("h", "d", "w", "m"):
                periods[period] = int(count)
        except ValueError:
            continue
    return periods


def delit():
    re_snap_type = re.compile("@omix_(monthly|weekly|dayly|hourly)-")
    for zpool in zpools:
        datasets = subrun("/sbin/zfs list -rHo name,ua.com.omix:autosnap {}".format(zpool)).strip().splitlines()
        for dataset in datasets:
            (dataset, val) = dataset.split("\t")
            periods = periods_from_attribute(val)
            mwh = {"m": [], "w": [], "d": [], "h": []}
            for snap in subrun("/sbin/zfs list -rd1 -Ho name -t snap -S creation {}".format(dataset)).splitlines():
                k = re_snap_type.findall(snap)
                if k:
                    mwh[k[0][0]].append(snap)
            for k in mwh.keys():
                if k in periods:
                    mwh[k] = mwh[k][periods[k]:]
            for snap in list(itertools.chain.from_iterable(mwh.values())):
                subrun("/sbin/zfs destroy {}".format(snap))
            pass


def run():
    loadstate()
    while True:
        if datetime.now().timestamp() > run_next:
            snapit()
            delit()
        sleep(1)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
    exit(0)
