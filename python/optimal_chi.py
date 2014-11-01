import random
import matplotlib.pyplot as plt
from numpy.random import normal
import numpy as np
import math
import random

MIN = 0
MAX = 100000
MAX_LENGTH = random.randint(MIN + 10, MAX / 10)
POINTS_COUNT = random.randint(100, MAX)
QUERIES_COUNT = 1000


intervals = []
for point_index in xrange(POINTS_COUNT):
    start = random.randint(MIN, MAX)
    end = start + random.randint(1, MAX_LENGTH)
    intervals += [(start, end)]
intervals.sort()

queries = []
for _ in xrange(QUERIES_COUNT):
    start = random.randint(MIN, MAX)
    length = random.randint(1, MAX_LENGTH)
    queries += [(start, start + length)]
queries.sort()

queries_start_indices = []
if not queries_start_indices:
    interval_index = 0
    for query_index in xrange(QUERIES_COUNT):
        query_start = queries[query_index][0]
        added = False
        while True:
            if interval_index == POINTS_COUNT:
                break
            if query_start <= intervals[interval_index][0]:
                queries_start_indices += [interval_index]
                added = True
                break
            else:
                interval_index += 1
        if not added:
            queries_start_indices += [POINTS_COUNT]

chipseqs = []
total_size = 0
checkpoint_intervals_values = [[] for _ in xrange(len(intervals))]
for chi_value in xrange(1, len(intervals)):
    offset = 0
    while offset < len(intervals):
        checkpoint_intervals_values[offset] += [chi_value]
        offset += chi_value
        total_size += 1
import math
print POINTS_COUNT, math.log(POINTS_COUNT) * POINTS_COUNT, total_size

steps = 0
overheads = [0 for _ in xrange(POINTS_COUNT)]
for query_index in xrange(len(queries)):
    query_start, query_end = queries[query_index]
    query_start_index = queries_start_indices[query_index]
    overhead = 0
    for checkpoint_position in xrange(query_start_index - 1, query_start_index / 2 - 1, -1):
        if intervals[checkpoint_position][1] <= query_start:
            overhead += 1
        for chi_value_index in xrange(len(checkpoint_intervals_values[checkpoint_position]) - 1, -1, -1):
            checkpoint_value = checkpoint_intervals_values[checkpoint_position][chi_value_index]
            if checkpoint_position + checkpoint_value < query_start_index:
                break
            overheads[checkpoint_value] += overhead
            steps += 1

print steps, POINTS_COUNT * POINTS_COUNT

print overheads[:100]
            
overheads_other = [0]
if 1:
    for checkpoint_interval in xrange(1, 100):
        checkpoint_overheads = []
        for query_index in xrange(QUERIES_COUNT):
            overhead = 0
            query_start = queries[query_index][0]
            query_start_index = queries_start_indices[query_index]
            if not query_start_index:
                checkpoint_overheads += [0.0]
                continue
            closest_checkpoint = (query_start_index - 1) - ((query_start_index - 1) % checkpoint_interval)
            for interval_index in xrange(closest_checkpoint, query_start_index):
                if intervals[interval_index][1] <= query_start:
                    overhead += 1
            path_size = query_start_index - closest_checkpoint
            
            if path_size:
                checkpoint_overheads += [float(overhead)] 
            else:
                checkpoint_overheads += [0.0]
        overheads_other += [int(sum(checkpoint_overheads))]

print overheads_other


    
    