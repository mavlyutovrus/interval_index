# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import matplotlib
from pylab import *
import numpy
from copy_reg import remove_extension
from heapq import heappush
from mpl_toolkits.axes_grid1 import host_subplot
import mpl_toolkits.axisartist as AA

colors = ["red", "green", "blue", "orange", "brown", "black", "silver", "aqua", "purple"]
suffix = ""
file_type = ""

def draw_scatter_plot(trends_time, trends_reads_count, trends_chi_reads_count):
    fig = figure(figsize=(8, 6), dpi=80)
    grid(b=True, which='major', color='gray', axis="both", linestyle='--', zorder=-1)
    font_size = 20
    print colors
    
    x_values = [x for x, y, margin_err in trends_time]
    y_values = [y for x, y, margin_err in trends_time]
    err_values = [margin_err for x, y, margin_err in trends_time]
    line, = plt.plot(x_values, y_values, lw=3, color=colors[0])
    line.set_zorder(1)
    plt.errorbar(x_values, y_values, yerr=err_values, color="black")  
    
    plt.xlim([1024, 2**17])
    plt.xscale("log")   
    
    #plt.xlim([0, 1000])
    #plt.ylim([0, 100])
    """
    for algo_index in xrange(len(algos)):
        algo = algos[algo_index]
        x_values = [x for x,y in algo2results[algo]]
        y_values = [y for x,y in algo2results[algo]]
        line, = plt.plot(x_values, y_values, lw=3, color=colors[algo_index])
        line.set_zorder(1) 
    """
    
    """
    plt.xlim([10000, 10**7])
    plt.xscale('log')
    plt.ylim([0, 2])   
    
    ylabel = plt.ylabel("Time per 100K queries [s]")
    ylabel.set_fontsize(font_size)
    xlabel = plt.xlabel("Dataset size")
    xlabel.set_fontsize(font_size)

    for ytick in plt.yticks()[1]:
        ytick.set_fontsize(font_size)
    for xtick in plt.xticks()[1]:
        xtick.set_fontsize(font_size)
    plt.tight_layout()
    """
    plt.show()


def draw_legend():
    font_size= 20
    fig = figure(figsize=(8, 6), dpi=80)
    p1 = plt.bar(range(len(algo2index)), range(len(algo2index)), 1.0, color="#7FCA9F")
    for algo_index in xrange(len(algos)):
        p1[algo_index].set_color(colors[algo_index])
    fig = figure(figsize=(12, 6), dpi=80)
    desc = [algo for algo in algos]
    legend = plt.legend( p1, desc, shadow=False, loc=1, fontsize=font_size)
    legend.draw_frame(True) 
    savefig("../graphs/test_results/legend" + file_type, transparent="True", pad_inches=0)
    
def calc_avg_minus_extremes(values):
    values.sort()
    quartile = len(values) / 4
    values = values[3:-3]
    import numpy, math
    margin_err = 1.96 * numpy.std(values) / math.sqrt(len(values))
    return float(sum(values)) / len(values), margin_err


RESPONSE_SIZE_POS = 0
TIME_RESULT_POS = 3
MEM_CONSUMPTION_POS = 1
QUERIES_COUNT = 200000

if 1:
    trends_time = {}
    trends_reads_count = {}
    trends_chi_reads_count = {}
    
    for line in open("../test_results/8_hdfs_iindex_read_size.txt"):
        if not "queries" in line:
            continue
        chunks = line.split()
        algo = chunks[0]
        if chunks[1] == "read_size":
            algo += "_" + chunks[2].zfill(5)
        queries_count = int(chunks[-9])
        if queries_count < 9501:
            continue
        x = int(algo.split("_")[-1])
        read_count = int(chunks[-3])
        ch_read_count = int(chunks[-5])
        time = float(chunks[-1])
        
        trends_time.setdefault(x, []).append(time / queries_count)
        trends_reads_count.setdefault(x, []).append(float(read_count) / queries_count)
        trends_chi_reads_count.setdefault(x, []).append(float(ch_read_count) / queries_count)
    
    
    trends_time = [(x, sum(y_vals) / len(y_vals), numpy.std(y_vals)) for x, y_vals in trends_time.items()]
    trends_reads_count = [(x, sum(y_vals) / len(y_vals), numpy.std(y_vals)) for x, y_vals in trends_reads_count.items()]
    trends_chi_reads_count = [(x, sum(y_vals) / len(y_vals), 1.96 * numpy.std(y_vals) / math.sqrt(len(y_vals))) for x, y_vals in trends_chi_reads_count.items()]
    trends_time.sort()
    trends_reads_count.sort()
    trends_chi_reads_count.sort()

    draw_scatter_plot(trends_time, trends_reads_count, trends_chi_reads_count)    
    


