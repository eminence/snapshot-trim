#!/usr/bin/python

import subprocess
import re
import time
from collections import namedtuple
import time

dataset = "storage/home/achin"

def get_snapshots(root):
    snaps = []
    p = subprocess.Popen(["/sbin/zfs", "list", "-r", "-t", "snapshot", "-o", "name", root], stdout=subprocess.PIPE)
    while (True):
        line = p.stdout.readline().strip().decode()
        #if exp.match(line):
        try:
            snap_time = time.mktime(time.strptime(line,root+"@%Y%m%d-%H%M"))
            yield snap_time
        except ValueError:
            pass
        if line is None or line == '':
            break
    #return snaps

def get_num_snapshots(snaps, start, end):
    counter=0
    for snap in snaps:
        if snap >= start and snap <= end:
            #print(time.asctime(time.localtime(snap)))
            counter += 1
    return counter


snaps = list(get_snapshots(dataset))
now = int(time.time())

oldest_snap = int(min(snaps))

Density = namedtuple('Density', ['snaps_per_period', 'snaps_per_hour', 'hours_per_snap'])
def get_density(start, period=604800):
    snaps_this_week = get_num_snapshots(snaps, start - period, start)
    if snaps_this_week == 0:
        return (0,0,0)
    # return snaps this period, snaps per hour, and hours per snap
    return Density(snaps_per_period=snaps_this_week,
            snaps_per_hour=(snaps_this_week / period) * 3600,
            hours_per_snap=period / (snaps_this_week * 3600))

def target_density(ts):
    age = (now - ts) / 3600.0
    if age > 8700:
        return (1/720.0)
    elif age > 5840:
        return (1/168.0)
    elif age > 1460:
        return (1/24.0)
    elif age > 336:
        return (1/12.0)
    else:
        return 1/4.0

def get_nearest_snap(ts):
    best_val = now
    best = None
    for snap in snaps:
        if abs(ts - snap) < best_val:
            best_val = abs(ts - snap)
            best = snap
    return int(best)

for x in list(range(oldest_snap, now, 3600)) + [now]:
    target = target_density(x)
    actual = get_density(x, period=(1/target)*3*3600)[1]
    print ("%s: target:%f  actual:%f" % (time.asctime(time.localtime(x)), target, actual))
    if actual > target and now - x > 129600:
        print (time.strftime("%Y%m%d-%H%M", time.localtime(get_nearest_snap(x))))
