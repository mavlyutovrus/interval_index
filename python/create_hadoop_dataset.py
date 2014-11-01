import random
import matplotlib.pyplot as plt
from numpy.random import normal, uniform
import numpy as np
import math
import random
from heapq import heapify, heappush, heappop
import sys

MIN = 0
MAX = 10000000
POINTS_COUNT = 1000000
QUERIES_COUNT = 100000

def save_dataset(filename, intervals, queries):
    out = open(filename, "w")
    out.write(str(len(intervals)) + "\n")
    for index in xrange(len(intervals)):
        start, length = intervals[index]
        out.write(str(start) + "\t" + str(start + length) + "\t" + str(index + 1) + "\n")
    out.write(str(len(queries)) + "\n")
    for start, length in queries:
        out.write(str(start) + "\t" + str(start + length) + "\n")
    out.close()


if 1:
    # query_len
    min_len         = 1
    max_len         = 2000
    min_start       = 0
    max_start       = 10000000000
    intervals_count = 10000000000
    intervals = []
    for index in xrange(intervals_count):
        if index % 1000000 == 0:
            sys.stderr.write("..processed %d\n" % index)
            sys.stderr.flush()
        start = random.random()  * (max_start - min_start) + min_start
        length = random.random() * (max_len - min_len) + min_len
        print start, length, index + 1
    """
    intervals.sort()
    overlappings = []
    started = []
    for start, length in intervals:
        while started:
            right_border = heappop(started)
            if right_border >= start:
                heappush(started, right_border)
                break
        overlappings += [len(started)]
        heappush(started, start + length)
    avg_overlapping = sum(overlappings) / float(len(overlappings))
    print avg_overlapping
    """