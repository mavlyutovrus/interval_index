# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import matplotlib
from pylab import *
import numpy
from copy_reg import remove_extension
from heapq import heappush
from mpl_toolkits.axes_grid1 import host_subplot
import mpl_toolkits.axisartist as AA

algo2index = []


def load_data(filename):
    all_results = {}
    data_set = ""
    mapper = ""
    data = {}
    mapper_results = []
    by_datasets = {}
    for line in open(filename):
        if not "\t" in line:
            continue
        chunks = line.split("\t")
        ds, algo = chunks[:2]
        if algo.startswith("Mavl"):
            algo = "1" + algo
        results = [float(chunk) for chunk in chunks[2:]]
        by_datasets.setdefault(ds, {})
        
        by_datasets[ds].setdefault(algo, []).append(results)
    return by_datasets


colors = ["red", "green", "blue", "orange", "brown", "black", "silver", "aqua", "purple"]
markers = [",", "x", "d", "v", "s", "p"] 
suffix = ""
file_type = ""

def draw_scatter_plot(x_values, algo2results):
    fig = figure(figsize=(8, 6), dpi=80)
    grid(b=True, which='major', color='gray', axis="both", linestyle='--', zorder=-1)
    font_size = 20
    algos = [algo for algo in algo2results.keys()]
    algos.sort()
    print algos
    for algo_index in xrange(len(algos)):
        algo = algos[algo_index]
        line, = plt.plot(x_values, algo2results[algo], lw=3, color=colors[algo_index], marker=markers[algo_index])
        line.set_zorder(1) 
    plt.xlim([0, 13000])
    #plt.ylim([0, 0.5])
    #plt.xscale('log')
    
    ylabel = plt.ylabel("Time per 100K queries [s]")
    ylabel.set_fontsize(font_size)
    xlabel = plt.xlabel("Interval length range")
    xlabel.set_fontsize(font_size)

    for ytick in plt.yticks()[1]:
        ytick.set_fontsize(font_size)
    for xtick in plt.xticks()[1]:
        xtick.set_fontsize(font_size)
    plt.tight_layout()
    savefig("../paper/imgs/5_time_overlapping_stdev.eps", transparent="True", pad_inches=0)
    
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
    #values = values[quartile + 1:-quartile]
    import numpy, math
    margin_err = 1.96 * numpy.std(values) / math.sqrt(len(values))
    return float(sum(values)) / len(values), margin_err


RESPONSE_SIZE_POS = 0
TIME_RESULT_POS = 3
MEM_CONSUMPTION_POS = 1
QUERIES_COUNT = 200000

if 1:
    query_len_results = load_data("../test_results/avg_overlapping_stdev.txt")
    ds_name2x = [(float(ds_name.split("/")[-1].split("_")[-1].replace(".txt", "")), ds_name) for ds_name in query_len_results.keys()]
    ds_name2x.sort()
    x_values = [value for value, _ in ds_name2x]
    print x_values
    ds_name2x_pos = {}
    for index in xrange(len(ds_name2x)):
        value, key = ds_name2x[index]
        ds_name2x_pos[key] = index
    trends = {}
    for ds_name, algos_results in query_len_results.items():
        for algo, results in algos_results.items():
            trends.setdefault(algo, []).append( (ds_name2x_pos[ds_name], [(result[RESPONSE_SIZE_POS], result[TIME_RESULT_POS]) for result in results]))
    
    avg_results_per_query_trend = []
    for algo in trends.keys():
        trends[algo].sort()
        trend = []
        for index, values in trends[algo]:
            response_size = values[0][0]
            values = [value for _, value in values]
            trend += [100000 * calc_avg_minus_extremes(values)[0] / float(QUERIES_COUNT)]
            if len(avg_results_per_query_trend) < len(x_values):
                avg_results_per_query_trend += [float(response_size) / QUERIES_COUNT]
        trends[algo] = trend
    for algo in trends.keys():
        if algo.startswith("R-Tree") and algo != "R-Tree64":
            print algo
            del trends[algo]
    print len(trends)
    #trends["avg_results_per_query"] = avg_results_per_query_trend
    results_per_query = {}
    results_per_query["1"] = avg_results_per_query_trend
    draw_scatter_plot(x_values, trends)    
    


