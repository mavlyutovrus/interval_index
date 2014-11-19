import random
import matplotlib.pyplot as plt
from numpy.random import normal, uniform
import numpy as np
import math
import random
from heapq import heapify, heappush, heappop

MIN = 0
MAX = 10000000
POINTS_COUNT = 1000000
QUERIES_COUNT = 200000


def save_dataset(filename, intervals, queries):
    intervals_copy = [value for value in intervals]
    queries_copy = [value for value in queries]
    random.shuffle(intervals_copy)
    random.shuffle(queries_copy)
    out = open(filename, "w")
    out.write(str(len(intervals_copy)) + "\n")
    for index in xrange(len(intervals_copy)):
        start, length = intervals_copy[index]
        out.write(str(start) + "\t" + str(start + length) + "\t" + str(index + 1) + "\n")
    out.write(str(len(queries_copy)) + "\n")
    for start, length in queries_copy:
        out.write(str(start) + "\t" + str(start + length) + "\n")
    out.close()

if 0:
    # chi_time_mem
    len_mean = 100
    len_stdev = 10
    intervals = []
    queries = []
    lengths = [length >=0 and length or 0.0 for length in normal(len_mean, len_stdev, POINTS_COUNT)]
    for point_index in xrange(POINTS_COUNT):
        start = random.random() * (MAX - MIN) + MIN
        length = lengths[point_index]
        intervals += [(start, length)]
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
    print "avg overlapping", avg_overlapping
    QUERIES_COUNT_SPEC = 1000000
    query_len_mean = 100
    lengths = normal(query_len_mean, len_stdev, QUERIES_COUNT_SPEC)
    queries = []
    for point_index in xrange(QUERIES_COUNT_SPEC):
        start = random.random() * (MAX - MIN) + MIN
        queries += [(start, lengths[point_index])]
    save_dataset("../datasets/chi_time_mem_1M_100_1M_100.txt", intervals, queries)


if 0:
    # query_len
    len_mean = 100
    len_stdev = 10
    query_len = 1
    intervals = []
    queries = []
    lengths = [length >=0 and length or 0.0 for length in normal(len_mean, len_stdev, POINTS_COUNT)]
    for point_index in xrange(POINTS_COUNT):
        start = random.random() * (MAX - MIN) + MIN
        length = lengths[point_index]
        intervals += [(start, length)]
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
    
    lengths = normal(100, 10, QUERIES_COUNT)
    DATASETS_COUNT = 30
    query_length = 10
    factor = math.exp(math.log(10000 / float(query_length) ) / (DATASETS_COUNT - 1))
    
    for length_factor in xrange(DATASETS_COUNT):
        queries = []
        for point_index in xrange(QUERIES_COUNT):
            start = random.random() * (MAX - MIN) + MIN
            queries += [(start, query_length)]
        save_dataset("../datasets/query_len/dataset_query_len_%d.txt" % (query_length), intervals, queries)
        print query_length
        query_length =  math.ceil(query_length * factor)

if 0:
    # avg_overlapping
    queries = []
    for query_index in xrange(QUERIES_COUNT):
        start = random.random() * (MAX - MIN) + MIN
        length = 100
        queries += [(start, length)]
    
    len_mean = 1
    max_len = 100000
    DATASETS_COUNT = 30
    factor = math.exp(math.log(max_len / float(len_mean) ) / (DATASETS_COUNT - 1))
    while len_mean <= 100000:
        print "mean len:", len_mean
        if 1:
            intervals = []
            lengths = [length >=0 and length or 0.0 for length in normal(len_mean, len_mean / 20.0, POINTS_COUNT)]
            if len_mean == 1: #here we want overlapping to be zero
                lengths = [0 for l in lengths]
            for interval_index in xrange(POINTS_COUNT):
                start = random.random() * (MAX - MIN) + MIN
                length = lengths[interval_index]
                intervals += [(start, length)]
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
            print sum(overlappings)
            print "avg. overlapping", avg_overlapping
            save_dataset("../datasets/avg_overlapping/%f.txt" % (avg_overlapping), intervals, queries)
        len_mean = math.ceil(len_mean * factor)


if 0:
    # avg_overlapping standard deviation
    queries = []
    for query_index in xrange(QUERIES_COUNT):
        start = random.random() * (MAX - MIN) + MIN
        length = 100
        queries += [(start, length)]
    
    len_mean = 1000
    DATASETS_COUNT = 30
    radius = 0
    max_radius = len_mean
    delta = (max_radius - radius) / (float(DATASETS_COUNT - 1))
    for _ in xrange(20):
        print "radius:", radius
        if 1:
            intervals = []
            lengths = [length >=0 and length or 0.0 for length in uniform(len_mean - radius, len_mean + radius, POINTS_COUNT)]
            print min(lengths), lengths[:15]
            for interval_index in xrange(POINTS_COUNT):
                start = random.random() * (MAX - MIN) + MIN
                length = lengths[interval_index]
                intervals += [(start, length)]
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
            print sum(overlappings)
            print "avg. overlapping", avg_overlapping
            save_dataset("../datasets/avg_overlapping_stdev/%f.txt" % (radius), intervals, queries)
        radius += delta


if 1:
    # different number of intervals
    intervals_counts = [10000]
    for _ in xrange(50):
        intervals_counts += [int(1.15 * intervals_counts[-1])]
    max_values = [counts for counts in intervals_counts]
    interval_length = 10
    
    for dataset_index in xrange(len(intervals_counts)):
        intervals_count = intervals_counts[dataset_index]
        MAX = max_values[dataset_index]
        intervals = []
        for _ in xrange(intervals_count):
            start = random.random() * MAX
            intervals += [(start, interval_length)]
        if intervals_count < 10000000:
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
            print sum(overlappings)
            print "avg. overlapping", avg_overlapping
            
        queries = []
        for query_index in xrange(QUERIES_COUNT):
            start = random.random() * MAX
            length = 1000
            queries += [(start, length)]
        print "intervals_count", intervals_count
        save_dataset("../datasets/intervals_count/%d.txt" % (intervals_count), intervals, queries)

if 0:
    #real: exome
    queries = []
    for line in open("../datasets/exome_alignement/20130108.exome.targets.bed"):
        _, start, end = [float(item) for item in line.split("\t")]
        queries += [(start, end - start)]
    queries.sort()
    intervals = []
    for line in open("../datasets/exome_alignement/exome.bed"):
        start, end = [float(item) for item in line.split("\t")[1:3]]
        intervals += [(start, end - start)]
    intervals.sort()
    if 1:
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
        print sum(overlappings)
        print "avg. overlapping", avg_overlapping
    save_dataset("../datasets/exome_alignement/dataset.txt", intervals, queries)

if 0:
    #real: time intervals
    import os
    path = "../datasets/time_intervals/"
    files = [fname for fname in os.listdir(path) if fname.startswith("all_intervals")]
    id = 0
    intervals = []
    
    for file in files:
        file = path + file
        for line in open(file):
           start, end = [float(item) for item in line.split("\t")[1:-1]]
           intervals += [(start, end - start)]
    min_left = min(left for left, _ in intervals )
    intervals = [(start - min_left, end) for start, end in intervals]
           
    print len(intervals)
    intervals.sort()
    out = open("t", "w")
    for start, end in intervals:
        out.write(str(start) + "\t" + str(end) + "\n")
    out.close()
    if 1:
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
        print sum(overlappings)
        print "avg. overlapping", avg_overlapping
    #save_dataset("../datasets/exome_alignement/dataset.txt", intervals, queries)



exit()

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
