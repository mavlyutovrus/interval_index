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
        results = [float(chunk) for chunk in chunks[2:]]
        by_datasets.setdefault(ds, {})
        
        by_datasets[ds].setdefault(algo, []).append(results)
    return by_datasets


query_len_results = load_data("../test_results/query_len.txt")
avg_overlapping_results = load_data("../test_results/avg_overlapping.txt")
stdev_results = load_data("../test_results/avg_overlapping_stdev.txt")

algos =  [name for name in query_len_results.values()[0].keys()]
algos.sort()
#algos += ["avg_results_per_query"]


algo2index = {}
for index in xrange(len(algos)):
    algo2index[algos[index]] = index

colors =        ["yellow", "red", "blue", "green", "silver", "aqua", "purple",  "orange", "brown", "black"]
#colors =        ["#F26C4F", "#F68E55", "#FFF467", "#3BB878", "#00BFF3", "#605CA8", "#A763A8"]
#colors_darker = ["#9E0B0F", "#A0410D", "#ABA000", "#007236", "#0076A3", "#1B1464", "#630460"]
RESPONSE_SIZE_POS = 0
TIME_RESULT_POS = 3
MEM_CONSUMPTION_POS = 1

suffix = ""
file_type = ".eps"

def draw_scatter_plot(x_values, algo2results, title, filename, yaxis_name, xaxis_name, y_log=False, x_log=False, left_margin=0.2, xticks_labels = None):
    fig = figure(figsize=(8, 6), dpi=80)
    grid(b=True, which='major', color='gray', axis="both", linestyle='--', zorder=-1)
    font_size = 20
    #plt.subplot2grid((1,3), (0,0), colspan=2)
    for algo_index in xrange(len(algos)):
        algo = algos[algo_index]
        line, = plt.plot(x_values, algo2results[algo], lw=3, color=colors[algo_index])
        line.set_zorder(1) 
    if y_log:
        plt.yscale('log')
    if x_log:
        plt.xscale('log')
    ylabel = plt.ylabel(yaxis_name)
    ylabel.set_fontsize(font_size)
    xlabel = plt.xlabel(xaxis_name)
    xlabel.set_fontsize(font_size)
    for ytick in plt.yticks()[1]:
        ytick.set_fontsize(font_size / 1.5)
    """
    print x_values
    MAX_TICKS = 10
    step = 100000
    for step in [100000, 200000, 500000, 1000000, 2000000, 5000000, 10000000]:
        if MAX_TICKS * step >= max(x_values):
            break
    xticks2show = range(0, max(x_values) + 1, step)
    xticks2show_labels = [value and str(int(value)) or "" for value in xticks2show]
    """
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
    
    savefig(filename + file_type, transparent="True", pad_inches=0)
    plt.show()
    #exit()



def draw_bar_charts(trends, title, yaxis_name, filename, ylim=None, p2onTop=False):
    def get_means_and_deviatons(dataset_results):
        means = [(algo2index[algo_name], numpy.mean(aglo_results)) for algo_name, aglo_results in dataset_results.items()]
        means.sort()
        means = [mean for _, mean in means]
        deviations = [(algo2index[algo_name], numpy.std(aglo_results)) for algo_name, aglo_results in dataset_results.items()]
        deviations.sort()
        deviations = [deviation for _, deviation in deviations]
        return means, deviations
    
    fig = figure(figsize=(8, 6), dpi=80)
    grid(b=True, which='major', color='gray', axis="y", linestyle='--', zorder=-1)
    bar_width = 0.05
    font_size= 10
    experiments_count = len(trends.values()[0])
    algos_count = len(algos)
    
    margin = bar_width / 2
    total_width = bar_width * experiments_count * algos_count + margin * (experiments_count  + 1)
    
    offsets = [index * (bar_width * algos_count + margin) + margin  for index in xrange(experiments_count)]
    bars_sets = [] 
    for algo_index in xrange(len(algos)):
        algo_offsets = [offset + algo_index * bar_width for offset in offsets]  
        bars_sets.append(plt.bar(algo_offsets, trends[algos[algo_index]], bar_width, color=colors[algo_index]))
        #for bar in bars_sets[-1]:
        #    
    
    #plt.subplots_adjust(left=0.17, right=0.95, top=0.95, bottom=0.1)
    #xticks = second_offsets  
    #plt.xticks(xticks, [char for char in "ABCDE"], fontsize=font_size)     
    
    """
    fig.patch.set_visible(False)

    plt.yscale('log', nonposy='clip')
    
    title = plt.title("Shit dependence on shitty experiments")
    title.set_fontsize(font_size)
    ylabel = plt.ylabel("Important shit")
    ylabel.set_fontsize(font_size)
    for ytick in plt.yticks()[1]:
        ytick.set_fontsize(font_size)    
    xlabel = plt.xlabel("Experiments measuring shit")
    xlabel.set_fontsize(font_size)
    for xtick in plt.yticks()[1]:
        xtick.set_fontsize(font_size)
    if 1:
        for line in p1:
            line.set_zorder(3) 
        for line in p2:
            line.set_zorder(4) 
    else:
        for line in p1:
            line.set_zorder(4) 
        for line in p2:
            line.set_zorder(3)
    fig.patch.set_visible(False)
    for algo_index in xrange(len(algos)):
        p1[algo_index].set_color(colors[algo_index])
        p2[algo_index].set_color(colors_darker[algo_index])
    """
    savefig(filename, transparent="True", pad_inches=0)   
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

draw_legend()

### query_len

def calc_avg_minus_extremes(values):
    values.sort()
    values = values[1:-1]
    import numpy
    
    return float(sum(values)) / len(values)#, numpy.std(values)


#query len
if 1:
    for key in query_len_results.keys():
        query_size = float(key.split("/")[-1].split("_")[-1].replace(".txt", ""))
        #if query_size > 10000:
        #    del query_len_results[key]
    
    ds_name2x = [(float(ds_name.split("/")[-1].split("_")[-1].replace(".txt", "")), ds_name) for ds_name in query_len_results.keys()]
    ds_name2x.sort()
    x_values = [value for value, _ in ds_name2x]
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
            trend += [1000 * calc_avg_minus_extremes(values) / float(response_size)]
            if len(avg_results_per_query_trend) < len(x_values):
                avg_results_per_query_trend += [float(response_size) / 1000000]
        trends[algo] = trend
    #trends["avg_results_per_query"] = avg_results_per_query_trend
    
    draw_scatter_plot(x_values, trends, "Query execution time = f(query_len)", 
                                        "../paper/imgs/query_len",
                                        "Avg time per returned interval, ms", "Query length", 
                                        y_log=False, x_log=True, left_margin=0.2)

### average overlapping - query time
if 0:
    
    for key in query_len_results.keys():
        query_size = float(key.split("/")[-1].split("_")[-1].replace(".txt", ""))
        if query_size > 150:
            del query_len_results[key]
    
    ds_name2x = [(float(ds_name.split("/")[-1].split("_")[-1].replace(".txt", "")), ds_name) for ds_name in avg_overlapping_results.keys()]
    ds_name2x.sort()
    
    x_ticks_labels = [str(int(value * 10) / 10.0) for value, _ in ds_name2x]
    for index in xrange(len(x_ticks_labels)):
        if index % 2:
            x_ticks_labels[index] = ""
    x_values = [index for index in xrange(len(ds_name2x))]
    ds_name2x_pos = {}
    for index in xrange(len(ds_name2x)):
        value, key = ds_name2x[index]
        ds_name2x_pos[key] = index
    
    trends = {}
    for ds_name, algos_results in avg_overlapping_results.items():
        for algo, results in algos_results.items():
            trends.setdefault(algo, []).append( (ds_name2x_pos[ds_name], [(result[RESPONSE_SIZE_POS], result[TIME_RESULT_POS]) for result in results]))
    
    for algo in trends.keys():
        trends[algo].sort()
        trend = []
        for index, values in trends[algo]:
            response_size = values[0][0]
            values = [value for _, value in values]
            trend += [1000 * calc_avg_minus_extremes(values) / float(response_size)] 
        trends[algo] = trend
    draw_scatter_plot(x_values, trends, "Query time = f(avg_overlapping)", 
                                        "../graphs/test_results/avg_overlapping",
                                        "Avg time per returned interval, ms", "Avg. overlapping", 
                                        y_log=False, x_log=False, left_margin=0.2, xticks_labels= x_ticks_labels)
    



### average overlapping stdev - query time
if 0:
    ds_name2x = [(float(ds_name.split("/")[-1].split("_")[-1].replace(".txt", "")), ds_name) for ds_name in stdev_results.keys()]
    ds_name2x.sort()
    x_values = [value for value, _ in ds_name2x]
    ds_name2x_pos = {}
    for index in xrange(len(ds_name2x)):
        value, key = ds_name2x[index]
        ds_name2x_pos[key] = index
    

    trends = {}
    for ds_name, algos_results in stdev_results.items():
        for algo, results in algos_results.items():
            trends.setdefault(algo, []).append( (ds_name2x_pos[ds_name], [(result[RESPONSE_SIZE_POS], result[TIME_RESULT_POS]) for result in results]))
        
    for algo in trends.keys():
        trends[algo].sort()
        trend = []
        for index, values in trends[algo]:
            print set([rs for rs, _ in values])
            response_size = values[0][0]
            values = [value for _, value in values]
            trend += [1000 * calc_avg_minus_extremes(values) / float(response_size)] 
        trends[algo] = trend
    
    draw_scatter_plot(x_values, trends, "Query time = f(overlapping stdev)", 
                                        "../graphs/test_results/stddev",
                                        "Avg time per returned interval, ms", "Intervals length range / 2", 
                                        y_log=False, x_log=False, left_margin=0.2)    
    #draw_bar_charts(trends, "100K queries time = f(stdev)", "100K queries time, s", "../graphs/test_results/stddev")


### average overlapping - mem consumption
if 0:
    for key in query_len_results.keys():
        query_size = float(key.split("/")[-1].split("_")[-1].replace(".txt", ""))
        #if query_size > 150:
        #    del query_len_results[key]
    
    ds_name2x = [(float(ds_name.split("/")[-1].split("_")[-1].replace(".txt", "")), ds_name) for ds_name in avg_overlapping_results.keys()]
    ds_name2x.sort()
    
    x_ticks_labels = [str(int(value * 10) / 10.0) for value, _ in ds_name2x]
    for index in xrange(len(x_ticks_labels)):
        if index % 2:
            x_ticks_labels[index] = ""
    x_values = [index for index in xrange(len(ds_name2x))]
    ds_name2x_pos = {}
    for index in xrange(len(ds_name2x)):
        value, key = ds_name2x[index]
        ds_name2x_pos[key] = index
    
    trends = {}
    for ds_name, algos_results in avg_overlapping_results.items():
        for algo, results in algos_results.items():
            trends.setdefault(algo, []).append( (ds_name2x_pos[ds_name], [result[MEM_CONSUMPTION_POS] for result in results]))
    
    for algo in trends.keys():
        trends[algo].sort()
        trend = []
        for index, values in trends[algo]:
            trend += [calc_avg_minus_extremes(values)] 
        trends[algo] = trend
    draw_scatter_plot(x_values, trends, "Memory consumption = f(avg_overlapping)", 
                                        "../graphs/test_results/mem_consumption_avg_overlapping",
                                        "Memory consumption, kb", "Avg. overlapping", 
                                        y_log=False, x_log=False, left_margin=0.2, xticks_labels= x_ticks_labels)


### average overlapping stdev - mem consumption
if 0:
    for key in query_len_results.keys():
        query_size = float(key.split("/")[-1].split("_")[-1].replace(".txt", ""))
        #if query_size > 150:
        #    del query_len_results[key]
    
    ds_name2x = [(float(ds_name.split("/")[-1].split("_")[-1].replace(".txt", "")), ds_name) for ds_name in stdev_results.keys()]
    ds_name2x.sort()
    
    x_ticks_labels = [str(int(value * 10) / 10.0) for value, _ in ds_name2x]
    for index in xrange(len(x_ticks_labels)):
        if index % 2:
            x_ticks_labels[index] = ""
    x_values = [index for index in xrange(len(ds_name2x))]
    ds_name2x_pos = {}
    for index in xrange(len(ds_name2x)):
        value, key = ds_name2x[index]
        ds_name2x_pos[key] = index
    
    trends = {}
    for ds_name, algos_results in stdev_results.items():
        for algo, results in algos_results.items():
            trends.setdefault(algo, []).append( (ds_name2x_pos[ds_name], [result[MEM_CONSUMPTION_POS] for result in results]))
    
    for algo in trends.keys():
        trends[algo].sort()
        trend = []
        for index, values in trends[algo]:
            trend += [calc_avg_minus_extremes(values)] 
        trends[algo] = trend
    draw_scatter_plot(x_values, trends, "Memory consumption = f(avg_overlapping_stdev)", 
                                        "../graphs/test_results/mem_consumption_avg_overlapping_stdev",
                                        "Memory consumption, kb", "Avg. overlapping", 
                                        y_log=False, x_log=False, left_margin=0.2, xticks_labels= x_ticks_labels)


