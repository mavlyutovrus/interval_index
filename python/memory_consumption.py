import random
import matplotlib.pyplot as plt
from numpy.random import normal
import numpy as np
import math
import random

def draw_graphs(intervals, name):
    inside = []
    overlappings = []
    for interval_index in xrange(POINTS_COUNT):
        filtered = []
        for end in inside:
            if end > intervals[interval_index][0]:
                filtered += [end]
        filtered += [intervals[interval_index][1]]
        inside = filtered
        overlappings += [len(inside)]
    
    plt.scatter(range(len(overlappings)), overlappings)
    plt.savefig(name + "_OP.png")
    plt.show() 
    
    checkpoint_intervals = []
    arrays_size = []
    
    for checkpoint_interval in xrange(1, 100):
        checkpoint_arrays_total_size = 0
        position = 0
        while position < POINTS_COUNT:
            checkpoint_arrays_total_size += overlappings[position]
            position += checkpoint_interval 
        checkpoint_intervals += [checkpoint_interval]
        arrays_size += [math.log(checkpoint_arrays_total_size)]
        #print checkpoint_interval, "\t", avg_overhead, "\t", checkpoint_arrays_total_size / float(POINTS_COUNT)
    
    plt.scatter(checkpoint_intervals, arrays_size)
    plt.savefig(name + "_TCS.png")
    plt.show()

MIN = 0
MAX = 10000
MAX_LENGTH = 100
POINTS_COUNT = 100

#ideal
intervals = []
for index in xrange(POINTS_COUNT):
    start = (MAX / POINTS_COUNT) * index
    end = start + (MAX / POINTS_COUNT) * 10
    intervals += [(start, end)]
draw_graphs(intervals, "ideal")

#equal
intervals = []
for index in xrange(POINTS_COUNT):
    start = random.randint(MIN, MAX)
    end = start + random.randint(1, MAX_LENGTH)
    intervals += [(start, end)]
draw_graphs(intervals, "equal")
"""
#normal
intervals = []
for index in xrange(POINTS_COUNT):
    start = random.randint(MIN, MAX)
    end = start + random.randint(1, MAX_LENGTH)
    intervals += [(start, end)]
draw_graphs(intervals, "ideal")
"""

"""
start_points = np.random.normal( MAX / 2 , 10000, POINTS_COUNT)

for index in xrange(POINTS_COUNT):
    middle = start_points[index]
    length = 1000
    start = max(middle - length, 0)
    end = start + length
    
    start = random.randint(MIN, MAX)
    end = random.randint(MIN, MAX)
    start, end = min(start, end), max(start, end) + 1
    
    #length = random.randint(1, MAX_LENGTH)
    intervals += [(start, start + length)]
intervals.sort()
"""



