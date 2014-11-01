import datetime
import time
import os
import re

def unix_time(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()

def sec_from_epoch2datetime(seconds):
    return datetime.datetime.fromtimestamp(seconds)

current_processes = {}

intervals = []

while True:
    first_line = True
    chunk2index = {}
    now = unix_time(datetime.datetime.now())
    current_processes_updated = []
    for line in os.popen("ps aux"):
        chunks = [item for item in re.findall("[^\s]+", line)]
        #print chunks
        if first_line:
            for index in xrange(len(chunks)):
                chunk2index[chunks[index]] = index
            first_line = False
            continue
        chunks = chunks[:len(chunk2index) - 1] + [" ".join(chunks[len(chunk2index) - 1:])]
        key = "-*-".join([chunks[chunk2index["USER"]], chunks[chunk2index["PID"]], chunks[chunk2index["COMMAND"]]])
        #memory = chunks[chunk2index["%"]]
        if "ps aux" in key:
            continue
        current_processes_updated += [key]
    current_processes_updated = set(current_processes_updated)
    to_remove = []
    updated = False
    for key, start_time in current_processes.items():
        if not key in current_processes_updated:
            intervals += [(start_time, now, key)]
            updated = True
            to_remove += [key]
    for key in current_processes_updated:
        if not key in current_processes:
            current_processes[key] = now
    for key in to_remove:
        del current_processes[key]
    if updated:
        print len(intervals)
    time.sleep(1)
    

    