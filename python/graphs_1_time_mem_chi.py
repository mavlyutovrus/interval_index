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
file_type = ".eps"

def draw_scatter_plot(x_values, left_algo2results, right_algo2results, 
                         title, filename, yaxis_name, xaxis_name, 
                         y_log=False, x_log=False, left_margin=0.2, xticks_labels = None):
    
    #fig = figure(figsize=(8, 6), dpi=80)
    
    font_size = 20
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    
    ax1.grid(b=True, which='major', color='gray', axis="x", linestyle='-.', zorder=-1)
    
    #ax1
    if 1:
        #yerr=left_algo2results["full_time"][1], 
        line, = ax2.plot(x_values, 
                         left_algo2results["full_time"][0], 
                         lw=1, color="black", 
                         linestyle="-")  
        #plt.errorbar(a,b,yerr=c, linestyle="None")
        plt.errorbar(x_values, left_algo2results["full_time"][0], yerr=left_algo2results["full_time"][1], color="black")  
        line, = ax2.plot(x_values, left_algo2results["only_binsearch+and_walk+inside"][0], lw=1, color="black", linestyle="--", marker="d")   
        line, = ax2.plot(x_values, left_algo2results["only_binsearch+and_walk"][0], lw=1, color="black", linestyle="--", marker="x")   
        line, = ax2.plot(x_values, left_algo2results["only_binsearch"][0], lw=1, color="black", linestyle="--")            
        #ax2.fill(x_values[:1] + x_values + x_values[-1:], [0] + left_algo2results["only_binsearch"] + [0], "b")

    #ax2 
    if 1:
        algos = [algo for algo in right_algo2results.keys()]
        algos.sort()    
        for algo_index in xrange(len(algos)):
            algo = algos[algo_index]
            line, = ax1.plot(x_values, right_algo2results[algo][0], lw=2, color="red", linestyle="-")
            line.set_zorder(1)         
    
    ax2.set_ylim([0, 0.09])
    
    
    if "ax1": 
        
        ax1.set_ylim([1, 15])
        ax1.set_yscale('log')
        
        ax1.get_yaxis().set_ticks([1.0, 2.0, 10.0])
        ax1.get_yaxis().set_ticklabels([0, 1.0, 9.0])
        ax1.spines['left'].set_color('red')
        for tl in ax1.get_yticklabels():
            tl.set_color('r')
        
        ax1.set_xlim([0, 150])
        
        if "x":
            xticks = [item for item in ax1.get_xaxis().get_majorticklocs()] + [11]
            xticks.sort()
            ax1.get_xaxis().set_ticks(xticks)
            xticks_labels = [str(int(item)) for item in xticks]
            xticks_labels[0] = ""
            xticks_labels[-1] = ""
            ax1.get_xaxis().set_ticklabels(xticks_labels)        
            ax1.get_xticklabels()[1].set_color('b')

        
    for tl in ax2.get_yticklabels():
        tl.set_size(font_size * 0.9)
    for tl in ax1.get_yticklabels():
        tl.set_size(font_size * 0.9)    
    for tl in ax1.get_xticklabels():
        tl.set_size(font_size * 0.9)   
    ax1.set_xlabel('Checkpoint interval', color='black', size=font_size)
    ax1.set_ylabel('Space Factor', color='black', size=font_size)
    ax2.set_ylabel('100K queries time, s', color='black', size=font_size)
    
    fig.patch.set_visible(False)    
    plt.tight_layout()
    
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
    time_mem_chi = load_data("../test_results/1_time_mem_chi.txt")
    algos_results = time_mem_chi.values()[0]
    del algos_results["free"]
    algos = [algo for algo in algos_results.keys()]
    algos.sort()
    x_values = [int(algo[1:]) for algo in algos]
    print x_values
    
    QUERIES_COUNT = 1000000
    time_trends = {}
    mem_trends = {}
    for algo_index in xrange(len(algos)):
        algo = algos[algo_index]
        all_algo_results = algos_results[algo]
        algo_result, err_margin = calc_avg_minus_extremes([results[-1] for results in all_algo_results])
        algo_result = algo_result * 100000 / QUERIES_COUNT
        err_margin = err_margin * 100000 / QUERIES_COUNT
        time_trends.setdefault("full_time", []).append((algo_index, algo_result, err_margin))      
        
        algo_result, err_margin = calc_avg_minus_extremes([results[-4] for results in all_algo_results])
        algo_result = algo_result * 100000 / QUERIES_COUNT
        err_margin = err_margin * 100000 / QUERIES_COUNT
        time_trends.setdefault("only_binsearch", []).append((algo_index, algo_result, err_margin))
          
        algo_result, err_margin = calc_avg_minus_extremes([results[-3] for results in all_algo_results])
        algo_result = algo_result * 100000 / QUERIES_COUNT
        err_margin = err_margin * 100000 / QUERIES_COUNT
        time_trends.setdefault("only_binsearch+and_walk", []).append((algo_index, algo_result, err_margin))
        
        algo_result, err_margin = calc_avg_minus_extremes([results[-1] - results[-2] + results[-3] for results in all_algo_results])
        algo_result = algo_result * 100000 / QUERIES_COUNT
        err_margin = err_margin * 100000 / QUERIES_COUNT
        time_trends.setdefault("only_binsearch+and_walk+inside", []).append((algo_index, algo_result, err_margin))        
        

        

        

        
        algo_result, err_margin = calc_avg_minus_extremes([results[1] / float(results[0]) for results in all_algo_results])
        #algo_result, err_margin = calc_avg_minus_extremes([float(results[3] - results[2]) / results[4] for results in all_algo_results])
        mem_trends.setdefault("mem_cosumption", []).append((algo_index, algo_result, err_margin))
    

    for algo in time_trends.keys():
        time_trends[algo].sort()
        time_trends[algo] = ([value for index, value, err_margin in time_trends[algo]], [err_margin for index, value, err_margin in time_trends[algo]])
        print algo, time_trends[algo][0]
    for algo in mem_trends.keys():
        mem_trends[algo].sort()
        mem_trends[algo] = ([value for index, value, err_margin in mem_trends[algo]], [err_margin for index, value, err_margin in mem_trends[algo]])
    
    
    draw_scatter_plot(x_values, time_trends, mem_trends, "Query execution time = f(query_len)", 
                                        "../paper/imgs/1_time_mem_chi",
                                        "100K requests time, ms", "Checkpoint interval", 
                                        y_log=False, x_log=False, left_margin=0.2)


