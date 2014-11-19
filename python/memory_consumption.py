# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import matplotlib
from pylab import *
import numpy
from copy_reg import remove_extension
from heapq import heappush
from mpl_toolkits.axes_grid1 import host_subplot
import mpl_toolkits.axisartist as AA
import random





def draw_scatter(x_values, y):
    fig, ax1 = plt.subplots()
    #fig(figsize=(8, 6), dpi=80)
    grid(b=True, which='major', color='gray', axis="both", linestyle='--', zorder=-1)
    font_size = 20
    ax1.scatter(x_values, y, lw=1, color="black")
    
    #plt.yscale('log')
    #ax1.get_xaxis().set_ticks([1.0, 10.0])
    #ax1.get_yaxis().set_ticklabels([1.0, 10.0])
    
    ax1.set_xlim([0, 100])
    ax1.set_ylim([0, 6])
    xticks = [item for item in ax1.get_xaxis().get_majorticklocs()] + [1]
    xticks.sort()
    ax1.get_xaxis().set_ticks(xticks)
    xlabels  = [str(int(item)) for item in xticks]
    xlabels[0] = ""
    ax1.get_xaxis().set_ticklabels(xlabels) 
    
    ax1.set_ylabel("Space Factor", size = font_size)
    ax1.set_xlabel("Checkpoint Interval", size = font_size)
    
    #line.set_zorder(1) 
    """
    if y_log:
        
    if x_log:
        plt.xscale('log')
    ylabel = plt.ylabel(yaxis_name)
    ylabel.set_fontsize(font_size)
    xlabel = plt.xlabel(xaxis_name)
    xlabel.set_fontsize(font_size)
    for ytick in plt.yticks()[1]:
        ytick.set_fontsize(font_size / 1.5)
    xticks2show = x_values
    if not xticks_labels:
        xticks2show_labels = [str(value) for value in xticks2show]
    else:
        xticks2show_labels = xticks_labels
    
    plt.xlim([0, max(x_values)])
        
    plt.xticks(xticks2show, xticks2show_labels, fontsize=font_size / 1.5)
    #legend = plt.legend( (p1[0], p2[0]), ("shuffled", "sorted"), shadow=False, loc=1, fontsize=font_size)
    #legend.draw_frame(False) 
    fig.patch.set_visible(False)    
    #figtext(.94, 0.4, u"© http://exascale.info", rotation='vertical')
    
    title = plt.title(title)
    title.set_fontsize(font_size)
    """
    savefig("../paper/imgs/space_factor.eps", transparent="True", pad_inches=0)
    plt.show()
    #exit()    
    
    
    

MIN = 0
MAX = 10000
MAX_LENGTH = 1000
POINTS_COUNT = 100


#equal
intervals = []
for index in xrange(POINTS_COUNT):
    start = random.random() * (MAX - MIN) + MIN
    end = start + random.random() * MAX_LENGTH
    intervals += [(start, end)]

intervals.sort()
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
print sum(overlappings) / float(len(overlappings))
print np.std(overlappings)

checkpoint_intervals = []
arrays_size = []

for checkpoint_interval in xrange(1, 100):
    checkpoint_arrays_total_size = 0
    position = 0
    while position < POINTS_COUNT:
        checkpoint_arrays_total_size += overlappings[position]
        position += checkpoint_interval 
    checkpoint_intervals += [checkpoint_interval]
    arrays_size += [(checkpoint_arrays_total_size) / float(len(intervals))]
draw_scatter(checkpoint_intervals, arrays_size)
