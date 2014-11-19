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
        line, = plt.plot(x_values, algo2results[algo], lw=3, color=colors[algo_index])
        line.set_zorder(1) 
    plt.xlim([10, 10000])
    plt.xscale('log')
    
    
    ylabel = plt.ylabel("Time per returned interval [microsec]")
    ylabel.set_fontsize(font_size)
    xlabel = plt.xlabel("Query length")
    xlabel.set_fontsize(font_size)

    for ytick in plt.yticks()[1]:
        ytick.set_fontsize(font_size)
    for xtick in plt.xticks()[1]:
        xtick.set_fontsize(font_size)
    plt.tight_layout()
    savefig("../paper/imgs/3_time_query_len.eps", transparent="True", pad_inches=0)
    
    plt.show()
    exit()
    

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
    
    font_size = 20
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    
    
    
    ax1.grid(b=True, which='major', color='gray', axis="both", linestyle='--', zorder=-1)
    
    #ax1
    if 1:
        #yerr=left_algo2results["full_time"][1], 
        line, = ax1.plot(x_values, 
                         left_algo2results.values()[0], 
                         lw=1, color="red", 
                         linestyle="-")
        #ax2.fill(x_values[:1] + x_values + x_values[-1:], [0] + left_algo2results["only_binsearch"] + [0], "b")

    #ax2 
    if 1:
        
        algos = [algo for algo in right_algo2results.keys()]
        algos.sort()    
        for algo_index in xrange(len(algos)):
            algo = algos[algo_index]
            #ax1.scatter(x_values, right_algo2results[algo], lw=2, color="black", linestyle="-")
            linestyle = "--"
            if algo.startswith("_Mavlyutov"):
                linestyle = "-"
            elif algo.startswith("Segm"):
                linestyle = "-."
            line, = ax2.plot(x_values, right_algo2results[algo], lw=2, color="black", linestyle=linestyle)
            line.set_zorder(1)  
    
    ax1.set_xscale('log')
    ax2.set_xscale("log")
    
    plt.show()
    
    """
    if "ax1": 
        #ax1.set_ylim([1, 15])
        #ax1.set_yscale('log')
        ax1.set_ylabel('Avg response size per query', color='black', size=font_size)
        #ax1.get_yaxis().set_ticks([1.0, 2.0, 10.0])
        #ax1.get_yaxis().set_ticklabels([1.0, 2.0, 10.0])
        for tl in ax1.get_yticklabels():
            tl.set_color('r')

    if "ax2":
        ax2.set_ylabel('1OOK queries time, s', color='black', size=font_size)
    ax1.set_xlabel('Query length', color='black', size=font_size)
    
    ax1.set_xlim([0, 10000])
    
    fig.patch.set_visible(False)    

    #figtext(.94, 0.4, u"© http://exascale.info", rotation='vertical')
    """
    
    """
    
    t = np.arange(0.01, 10.0, 0.01)
    s1 = np.exp(t)
    ax1.plot(t, s1, 'b-')
    ax1.set_xlabel('time (s)')
    # Make the y-axis label and tick labels match the line color.
    ax1.set_ylabel('exp', color='b')
    for tl in ax1.get_yticklabels():
        tl.set_color('b')
    
    
    ax2 = ax1.twinx()
    s2 = np.sin(2*np.pi*t)
    ax2.plot(t, s2, 'r.')
    ax2.set_ylabel('sin', color='r')
    for tl in ax2.get_yticklabels():
        tl.set_color('r')
    plt.show()    
    """

    #plt.subplot2grid((1,3), (0,0), colspan=2)

        
    """
    ylabel = ax1.ylabel(yaxis_name)
    ylabel.set_fontsize(font_size)
    xlabel = ax1.xlabel(xaxis_name)
    xlabel.set_fontsize(font_size)
    for ytick in ax1.yticks()[1]:
        ytick.set_fontsize(font_size / 1.5)

    xticks2show = x_values
    if not xticks_labels:
        xticks2show_labels = [str(value) for value in xticks2show]
    else:
        xticks2show_labels = xticks_labels
    ax1.xlim([0, max(x_values)])
    
    max_y = 0
    for values in algo2results.values():
        print values
        max_y = max(max(values), max_y)
    ax1.ylim([0, max_y * 1.1])
        
    ax1.xticks(xticks2show, xticks2show_labels, fontsize=font_size / 1.5)
    #legend = plt.legend( (p1[0], p2[0]), ("shuffled", "sorted"), shadow=False, loc=1, fontsize=font_size)
    #legend.draw_frame(False) 
    """
    

    
    #savefig(filename + file_type, transparent="True", pad_inches=0)
    plt.show()
    #exit()

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
    query_len_results = load_data("../test_results/query_len.txt")
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
            trend += [1000000 * calc_avg_minus_extremes(values)[0] / float(response_size)]
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
    


