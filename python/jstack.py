import datetime
import time
import os
import re
import socket

host_name = socket.gethostname()

def unix_time(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()

def sec_from_epoch2datetime(seconds):
    return datetime.datetime.fromtimestamp(seconds)

def get_java_processes():
    first_line = True
    processes = []
    fields_count = 0
    for line in os.popen("ps aux"):
        chunks = [item for item in re.findall("[^\s]+", line)]
        #print chunks
        if first_line:
            fields_count = len(chunks)
            first_line = False
            continue
        chunks = chunks[:fields_count - 1] + [" ".join(chunks[fields_count - 1:])]
        command = chunks[-1]
        if "ps aux" in command:
            continue
        if not "java" in command:
            continue
        pid = chunks[1]
        processes += [pid]
    return processes



def parse_log(input):
    stacks = {}
    stack = []
    thread_id = ""
    for line in input:
        if line.startswith("----"):
            if stack:
                stacks[thread_id] = stack
            stack = []
            thread_id = line.split(" ")[1]
            continue
        if line.startswith(" - "):
            method = line.split(") @")[0][3:] + ")"
            stack = [method] + stack
            continue
    if stack:
        stacks[thread_id] = stack
    return stacks        


current_processes = {}
out = open("intervals.txt", "w")

while True:
    pids = get_java_processes()
    full_log = []
    for pid in pids:
        now = unix_time(datetime.datetime.now())
        #log = [line for line in os.popen("/usr/lib/jvm/java-7-oracle-cloudera/bin/jstack -F " + pid)]
        log = [line for line in os.popen("jstack -m " + pid)]
        full_log += [(now, log)]
    
    current_processes_updated = []
    for now, log in full_log:
        stacks = parse_log(log)
        for thread_id, stack in stacks.items():
            for depth in xrange(len(stack)):
                key = "%s:%d:%s" % (thread_id, depth, stack[depth])
                current_processes_updated += [key]
    current_processes_updated = set(current_processes_updated)
    to_remove = []
    updated = False
    for key, start_time in current_processes.items():
        if not key in current_processes_updated:
            out.write("%s\t%s\t%s\t%s\n" % (host_name, str(start_time), str(now), key))
            out.flush()
            updated = True
            to_remove += [key]
    for key in current_processes_updated:
        if not key in current_processes:
            current_processes[key] = now
    for key in to_remove:
        del current_processes[key]
    time.sleep(1)    
    
    


    