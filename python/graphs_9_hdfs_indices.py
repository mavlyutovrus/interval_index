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

def draw_scatter_plot(ii_trends, other_trends):
    fig = figure(figsize=(8, 6), dpi=80)
    grid(b=True, which='major', color='gray', axis="both", linestyle='--', zorder=-1)
    font_size = 20
    algos = [algo for algo in ii_trends.keys()]
    algos.sort()
    plt.ylim([0, 100])   
    for algo_index in xrange(len(algos)):
        algo = algos[algo_index]
        x_values = [x for x,y,y_err in ii_trends[algo]]
        y_values = [y for x,y,y_err in ii_trends[algo]]
        err_values = [y_err for x,y,y_err in ii_trends[algo]]
        line, = plt.plot(x_values, y_values, lw=3, color="red")
        y_values = [y_values[index] for index in xrange(0, len(x_values), 2)]
        err_values = [err_values[index] for index in xrange(0, len(x_values), 2)]
        x_values = [x_values[index] for index in xrange(0, len(x_values), 2)]
        bar, _, _ = plt.errorbar(x_values, y_values, yerr=err_values, lw=1, linestyle='-', color="black")
        line.set_zorder(1) 
        bar.set_zorder(2)
    plt.ylim([0, 80]) 
    plt.xlim([0, 80000])
    
    
    algos = [algo for algo in other_trends.keys()]
    algos.sort()
    for algo_index in xrange(len(algos)):
        algo = algos[algo_index]
        x_values = [x for x,y in other_trends[algo]]
        y_values = [y for x,y in other_trends[algo]]
        line_style = "-"
        marker = "x"
        if "mapred" in algo:
            line_style = "-"
            marker= "^"
        line, = plt.plot(x_values, y_values, lw=2, linestyle=line_style, marker=marker,  color="black")
        line.set_zorder(1) 
    
    
    ylabel = plt.ylabel("Time per one query [ms]")
    ylabel.set_fontsize(font_size)
    xlabel = plt.xlabel("Queries count x1000")
    xlabel.set_fontsize(font_size)
    for ytick in plt.yticks()[1]:
        ytick.set_fontsize(font_size)
        
        
        
    for xtick in plt.xticks()[1]:
        xtick.set_fontsize(font_size)
    if 1:
        print plt.xticks()
        xticks = [item for item in plt.xticks()[0]]
        xticks_labels = [str(int(item / 1000)) for item in xticks]
        plt.xticks(xticks, xticks_labels)
    
    plt.tight_layout()
    savefig("../paper/imgs/9_time_distrib_hdfs_indices.eps", transparent="True", pad_inches=0)
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
    ii_trends = {}
    for line in open("../test_results/9_hdfs_iindex_read_size.txt"):
        if not "read_size" in line:
            continue
        chunks = line.split()
        algo = chunks[0]
        if chunks[1] == "read_size":
            algo += "_" + chunks[2].zfill(5)
        x = int(chunks[-9])
        y = float(chunks[-1])
        ii_trends.setdefault(algo, []).append((x, y / x)) 
    
    trend = []
    for key in ii_trends.keys():
        ii_trends[key].sort()
        by_x = {}
        for x, y in ii_trends[key]:
            by_x.setdefault(x, []).append(y)
        trend = []
        for x, y_vals in by_x.items():
            y_vals.sort()
            initial = [item for item in y_vals]
            y_mean, y_err = 0, 0
            while True:
                y_mean, y_err = sum(y_vals) / len(y_vals), 1.96 * numpy.std(y_vals) / math.sqrt(len(y_vals))
                if y_err / y_mean < 0.2:
                    break
                if len(y_vals) < 6:
                    y_vals = initial
                    y_mean, y_err = sum(y_vals) / len(y_vals), 1.96 * numpy.std(y_vals) / math.sqrt(len(y_vals))
                    print x, y_err / y_mean, initial, y_vals 
                    break
                y_vals = y_vals[1:-1]
            print y_err/ y_mean
            trend += [(x, y_mean, y_err)]
        trend.sort()
        ii_trends[key] = trend
        
    other_trends = {}
    for line in open("../test_results/7_hdfs_indices.txt"):
        chunks = line.split()
        algo = chunks[0]
        if not "spatial" in algo and not "mapred" in algo:
            continue
        if chunks[1] == "read_size":
            algo += "_" + chunks[2].zfill(5)
        x = int(chunks[-5])
        y = float(chunks[-1])
        other_trends.setdefault(algo, []).append((x, y / x)) 
    
    for key in other_trends.keys():
        other_trends[key].sort()
        
    draw_scatter_plot(ii_trends, other_trends)    
    


