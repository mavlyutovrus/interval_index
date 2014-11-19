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


colors = ["red", "green", "blue", "black", "silver", "aqua", "purple",  "orange", "brown"]
suffix = ""
file_type = ""

def draw_scatter_plot(x_values, left_algo2results, right_algo2results, 
                         title, filename, yaxis_name, xaxis_name, 
                         y_log=False, x_log=False, left_margin=0.2, xticks_labels = None):
    
    #fig = figure(figsize=(8, 6), dpi=80)
    
    font_size = 20
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    
    ax1.grid(b=True, which='major', color='gray', axis="both", linestyle='--', zorder=-1)
    
    #ax1
    if 1:
        #yerr=left_algo2results["full_time"][1], 
        line, = ax2.plot(x_values, 
                         left_algo2results["full_time"][0], 
                         lw=1, color="black", 
                         linestyle="-")  
        #plt.errorbar(a,b,yerr=c, linestyle="None")
        #plt.errorbar(x_values, left_algo2results["full_time"][0], yerr=left_algo2results["full_time"][1], color="black")  
        line, = ax2.plot(x_values, left_algo2results["no_inside"][0], lw=1, color="black", linestyle="--")   
        line, = ax2.plot(x_values, left_algo2results["only_binsearch"][0], lw=1, color="black", linestyle="-.")            
        #ax2.fill(x_values[:1] + x_values + x_values[-1:], [0] + left_algo2results["only_binsearch"] + [0], "b")

    #ax2 
    if 1:
        algos = [algo for algo in right_algo2results.keys()]
        algos.sort()    
        for algo_index in xrange(len(algos)):
            algo = algos[algo_index]
            print right_algo2results[algo][0]
            line, = ax1.plot(x_values, right_algo2results[algo][0], lw=2, color="red", linestyle="-")
            line.set_zorder(1)         
    
    
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
    

    
    savefig(filename + file_type, transparent="True", pad_inches=0)
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
    import numpy, math
    margin_err = 1.96 * numpy.std(values) / math.sqrt(len(values))
    return float(sum(values)) / len(values), margin_err


RESPONSE_SIZE_POS = 0
TIME_RESULT_POS = 3
MEM_CONSUMPTION_POS = 1

if 1:
    time_qlen = load_data("../test_results/2_individ_time_query_len.txt")
    
    
    for key in time_qlen.keys():
        time_qlen[key] = time_qlen[key].values()[0]
    
    algos_results = time_qlen
    
    algos = [algo for algo in algos_results.keys()]
    algos = [(int(algo.replace(".txt", "").split("_")[-1]), algo) for algo in algos]
    algos.sort()
    x_values = [x for x, algo in algos]
    print x_values
    QUERIES_COUNT = 200000
    
    time_trends = {}
    response_trends = {}
    for algo_index in xrange(len(algos)):
        algo = algos[algo_index][1]
        all_algo_results = algos_results[algo]
        algo_result, err_margin = calc_avg_minus_extremes([results[-1] for results in all_algo_results])
        algo_result = algo_result * 100000 / QUERIES_COUNT
        err_margin = err_margin * 100000 / QUERIES_COUNT
        time_trends.setdefault("full_time", []).append((algo_index, algo_result, err_margin))
        algo_result, err_margin = calc_avg_minus_extremes([results[-2] for results in all_algo_results])
        algo_result = algo_result * 100000 / QUERIES_COUNT
        err_margin = err_margin * 100000 / QUERIES_COUNT
        time_trends.setdefault("no_inside", []).append((algo_index, algo_result, err_margin))
        algo_result, err_margin = calc_avg_minus_extremes([results[-4] for results in all_algo_results])
        algo_result = algo_result * 100000 / QUERIES_COUNT
        err_margin = err_margin * 100000 / QUERIES_COUNT
        time_trends.setdefault("only_binsearch", []).append((algo_index, algo_result, err_margin))
        
        
        algo_result, err_margin = calc_avg_minus_extremes([results[4] for results in all_algo_results])
        algo_result = algo_result / float(QUERIES_COUNT)
        response_trends.setdefault("total_response", []).append((algo_index, algo_result, err_margin))

        algo_result, err_margin = calc_avg_minus_extremes([results[3] for results in all_algo_results])
        algo_result = algo_result / float(QUERIES_COUNT)
        response_trends.setdefault("before_response", []).append((algo_index, algo_result, err_margin))   

    for trend in time_trends.keys():
        time_trends[trend].sort()
        time_trends[trend] = ([value for index, value, err_margin in time_trends[trend]], [err_margin for index, value, err_margin in time_trends[trend]])
    for trend in response_trends.keys():
        response_trends[trend].sort()
        response_trends[trend] = ([value for index, value, err_margin in response_trends[trend]], [err_margin for index, value, err_margin in response_trends[trend]])
    
    
    draw_scatter_plot(x_values, time_trends, response_trends, "Query execution time = f(query_len)", 
                                        "../paper/imgs/2_time_query_len.eps",
                                        "100K requests time, ms", "Checkpoint interval", 
                                        y_log=False, x_log=False, left_margin=0.2)


