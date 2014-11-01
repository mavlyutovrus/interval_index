import random
import matplotlib.pyplot as plt
from numpy.random import normal
import numpy as np
import math
import random

"""
k = 1024
values = [(chi, k / chi, k % chi) for chi in xrange(1, 100)]
for index in xrange(1, len(values)):
    prev_chi, prev_d, prev_r = values[index - 1]
    chi, d, r = values[index]
    delta_d = prev_d - d
    calced_delta_d = float(prev_d - prev_r) / (prev_chi + 1)
    if calced_delta_d < 0:
        calced_delta_d = 0
    else:
        calced_delta_d = int(math.ceil(calced_delta_d))
    print delta_d, calced_delta_d, d

exit()
"""




MIN = 0
MAX = 10000
MAX_LENGTH = random.randint(MIN + 10, MAX / 10)
POINTS_COUNT = random.randint(100, MAX / 10)
QUERIES_COUNT = 1000






def draw(intervals, queries, name):
    intervals.sort()
    queries.sort()
    queries_start_indices = []
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
    
    avg_overheads = []
    rough_overheads = []
    std_devs = []
    path_sizes =[]
    for checkpoint_interval in xrange(1, 100):
        overheads = []
        path_size = []
        for query_index in xrange(QUERIES_COUNT):
            overhead = 0
            query_start = queries[query_index][0]
            query_start_index = queries_start_indices[query_index]
            if not query_start_index:
                overheads += [0.0]
                continue
            closest_checkpoint = (query_start_index - 1) - ((query_start_index - 1) % checkpoint_interval)
            for interval_index in xrange(closest_checkpoint, query_start_index):
                if intervals[interval_index][1] <= query_start:
                    overhead += 1
            overheads += [overhead]
            path_size += [query_start_index - closest_checkpoint]
        avg_overheads += [max(overheads)]
        path_sizes += [path_size]
        std_devs += [np.std(overheads)] 
        
        overheads = []
        for query_index in xrange(QUERIES_COUNT):
            overhead = 0
            query_start = queries[query_index][0]
            query_start_index = queries_start_indices[query_index]
            if not query_start_index:
                overheads += [0.0]
                continue
            closest_checkpoint = max(0, query_start_index - checkpoint_interval)
            for interval_index in xrange(closest_checkpoint, query_start_index):
                if intervals[interval_index][1] <= query_start:
                    overhead += 1
            overheads += [overhead]
        rough_overheads += [max(overheads)]          
        
    local_minimas = []
    local_minimax_x = []
    for index in xrange(1, len(avg_overheads) - 1):
        if avg_overheads[index] < avg_overheads[index - 1] and avg_overheads[index] < avg_overheads[index + 1]:
            local_minimas += [avg_overheads[index]]
            local_minimax_x += [index]
    print std_devs
    plt.scatter(range(len(avg_overheads)), avg_overheads)
    plt.scatter(range(len(std_devs)), std_devs, color='red')
    plt.scatter(range(len(rough_overheads)), rough_overheads, color='green')
    
    plt.plot(local_minimax_x, local_minimas)
    plt.savefig(name + "_EQO.png")
    plt.show()


start_points = np.random.normal( MAX / 2 , 100, POINTS_COUNT)
intervals = []
for point_index in xrange(POINTS_COUNT):
    start = random.randint(MIN, MAX)
    end = start + random.randint(1, MAX_LENGTH)
    intervals += [(start, end)]
    """
    mid = start_points[point_index]
    length = random.randint(2, MAX_LENGTH)
    start = mid - length / 2
    intervals += [(start, start + length)]
    """
    

queries = []
for _ in xrange(QUERIES_COUNT):
    start = random.randint(MIN, MAX)
    length = random.randint(1, MAX_LENGTH)
    queries += [(start, start + length)]
    
draw(intervals, queries, "uniform")
"""


start_points = np.random.normal( MAX / 2 , 500, QUERIES_COUNT)
queries = []
for index in xrange(QUERIES_COUNT):
    mid = start_points[index]
    length = 300
    queries += [(mid - length / 2, mid + length / 2)]
  
draw(intervals, queries, "norm")
    
"""

"""
queries = []
for index in xrange(QUERIES_COUNT):
    queries += [(MAX + 10, MAX + 20)]
draw(intervals, queries, "queries_right")
"""