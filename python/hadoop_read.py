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
    ax1.set_ylabel("Avg Query Time, [ms]", size = font_size)
    ax1.set_xlabel("Read Size, [bytes]", size = font_size)
    
    
    ax1.set_xlim([0, 100000])
    
    #plt.yscale('log')
    #ax1.get_xaxis().set_ticks([1.0, 10.0])
    #ax1.get_yaxis().set_ticklabels([1.0, 10.0])
    
    #ax1.set_xlim([0, 100])
    #ax1.set_ylim([0, 6])
    #xticks = [item for item in ax1.get_xaxis().get_majorticklocs()] + [1]
    #xticks.sort()
    #ax1.get_xaxis().set_ticks(xticks)
    #xlabels  = [str(int(item)) for item in xticks]
    #xlabels[0] = ""
    #ax1.get_xaxis().set_ticklabels(xlabels) 
    
    #ax1.set_ylabel("Space Factor", size = font_size)
    #ax1.set_xlabel("Checkpoint Interval", size = font_size)
    
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
    #savefig("../paper/imgs/space_factor.eps", transparent="True", pad_inches=0)
    plt.show()
    #exit()    
    

by_length = {}
for line in open("../test_results/hadoop_read_not_cashed.txt"):
    value = float(line.split("\t")[-1].split(";")[0])
    if value > 10000:
        value = value / 1000000
    length = float(line.split("\t")[0])
    by_length.setdefault(length, []).append(value)
    
data = []
boxplot_values = []
for key, values in by_length.items():
    values.sort()
    #values = values[2:-2]
    value = sum(values) / len(values)
    data += [(key, value)]

"""
if 1:
    fig, ax1 = plt.subplots()
    #fig(figsize=(8, 6), dpi=80)
    ax1.boxplot(boxplot_values)
    ax1.set_ylim([0, 10])
    #grid(b=True, which='major', color='gray', axis="both", linestyle='--', zorder=-1)
    #font_size = 20
    #ax1.scatter(x_values, y, lw=1, color="black")
    plt.show()
exit()
"""

data.sort()
x_vals1 = []
y_vals1 = []
for x, y in data:
    x_vals1 += [x]
    y_vals1 += [y]
data = []

by_length = {}
for line in open("../test_results/hadoop_read_cashed.txt"):
    values = []
    for value in line.split("\t")[-1].split(";")[:-1]:
        value = float(value)
        value = value / 1000000
        values += [value]
    length = float(line.split("\t")[0])
    by_length.setdefault(length, [])
    by_length[length] += values
    
data = []
boxplot_values = []
for key, values in by_length.items():
    values.sort()
    #values = values[2:-2]
    #print values
    value = sum(values) / len(values)
    data += [(key, value)]

data.sort()
x_vals2 = []
y_vals2 = []
for x, y in data:
    x_vals2 += [x]
    y_vals2 += [y]



if 1:
    fig, ax1 = plt.subplots()
    #fig(figsize=(8, 6), dpi=80)
    grid(b=True, which='major', color='gray', axis="both", linestyle='--', zorder=-1)
    font_size = 20
    first = ax1.scatter(x_vals1, y_vals1, lw=1, color="black", marker=".")
    trend1 = numpy.poly1d(numpy.polyfit(x_vals1, y_vals1, 1))(x_vals1)
    ax1.plot(x_vals1, trend1, "--", lw=2, color="blue")
    
    second = ax1.scatter(x_vals2, y_vals2, lw=1, color="black", marker="x")
    trend2 = numpy.poly1d(numpy.polyfit(x_vals2, y_vals2, 1))(x_vals2)
    ax1.plot(x_vals2, trend2, "--", lw=2, color="red")
    
    ax1.set_ylabel("Avg Query Time, [ms]", size = font_size)
    ax1.set_xlabel("Read Size, [bytes]", size = font_size)
    ax1.set_xlim([0, 100000])
    ax1.set_ylim([0, 20])
    ax1.legend([first, second], ["not-cached", "cached"], loc=2)
    savefig("../paper/imgs/hdfs_read.eps", transparent="True", pad_inches=0)
    plt.show()
