# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
import matplotlib
from pylab import *
import numpy
from copy_reg import remove_extension
from heapq import heappush

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
        by_datasets[ds][algo] = results
    return by_datasets


query_len_results = load_data("../test_results/query_length.txt")
avg_overlapping_results = load_data("../test_results/avg_overlapping.txt")
stdev_results = load_data("../test_results/avg_overlapping_stdev.txt")

algos =  [name for name in query_len_results.values()[0].keys()]
algos.sort()
algo2index = {}
for index in xrange(len(algos)):
    algo2index[algos[index]] = index

colors =        ["green", "red", "blue", "yellow", "silver", "aqua", "purple",  "orange"]
#colors =        ["#F26C4F", "#F68E55", "#FFF467", "#3BB878", "#00BFF3", "#605CA8", "#A763A8"]
#colors_darker = ["#9E0B0F", "#A0410D", "#ABA000", "#007236", "#0076A3", "#1B1464", "#630460"]
RESPONSE_SIZE_POS = 0
TIME_RESULT_POS = 1

suffix = ""
file_type = ".png"

def draw_scatter_plot(x_values, algo2results, title, filename, yaxis_name, xaxis_name, y_log=False, x_log=False, left_margin=0.2):
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
        ytick.set_fontsize(font_size)
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
    xticks2show_labels = [str(value) for value in xticks2show]
    
    plt.xlim([0, max(x_values)])
        
    plt.xticks(xticks2show, xticks2show_labels, fontsize=font_size)     
    plt.subplots_adjust(left_margin, right=0.95, top=0.95, bottom=0.13)
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
    p1 = plt.bar(range(7), range(7), 1.0, color="#7FCA9F")
    for algo_index in xrange(len(algos)):
        p1[algo_index].set_color(colors[algo_index])
    fig = figure(figsize=(12, 6), dpi=80)
    desc = [algo for algo in algos]
    legend = plt.legend( p1, desc, shadow=False, loc=1, fontsize=font_size)
    legend.draw_frame(True) 
    savefig("../graphs/test_results/legend" + file_type, transparent="True", pad_inches=0)

#draw_legend()

### query_len
if 0:
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
            trends.setdefault(algo, []).append( (ds_name2x_pos[ds_name], results[TIME_RESULT_POS]))
    
    for algo in trends.keys():
        trends[algo].sort()
        trends[algo] = [value for index, value in trends[algo]]
    
    draw_scatter_plot(x_values, trends, "100K queries time = f(query_len)", 
                                        "../graphs/test_results/query_len.svg",
                                        "100K queries time, s", "Query length", 
                                        y_log=False, x_log=True, left_margin=0.2)

### query_len
if 0:
    ds_name2x = [(float(ds_name.split("/")[-1].split("_")[-1].replace(".txt", "")), ds_name) for ds_name in avg_overlapping_results.keys()]
    ds_name2x.sort()
    x_values = [value for value, _ in ds_name2x]
    ds_name2x_pos = {}
    for index in xrange(len(ds_name2x)):
        value, key = ds_name2x[index]
        ds_name2x_pos[key] = index
    
    trends = {}
    for ds_name, algos_results in avg_overlapping_results.items():
        for algo, results in algos_results.items():
            trends.setdefault(algo, []).append( (ds_name2x_pos[ds_name], results[TIME_RESULT_POS] ))
    
    for algo in trends.keys():
        trends[algo].sort()
        trends[algo] = [value for index, value in trends[algo]]
    
    draw_scatter_plot(x_values, trends, "100K queries time = f(avg_overlapping)", 
                                        "../graphs/test_results/avg_overlapping",
                                        "100K queries time, s", "Avg. overlapping", 
                                        y_log=False, x_log=False, left_margin=0.2)


### average overlapping
### query_len
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
            trends.setdefault(algo, []).append( (ds_name2x_pos[ds_name], results[TIME_RESULT_POS] ))
    
    for algo in trends.keys():
        trends[algo].sort()
        trends[algo] = [value for index, value in trends[algo]]
    
    draw_scatter_plot(x_values, trends, "100K queries time = f(stdev)", 
                                        "../graphs/test_results/stddev",
                                        "100K queries time, s", "Intervals length range / 2", 
                                        y_log=False, x_log=False, left_margin=0.2)    
    #draw_bar_charts(trends, "100K queries time = f(stdev)", "100K queries time, s", "../graphs/test_results/stddev")







exit()
"""
=====================================
=====================================
=====================================
"""



def remove_extremes(variable_values, to_remove = 1):
    median = numpy.median(variable_values)
    distances = [(abs(variable_values[index] - median), index) for index in xrange(len(variable_values))]
    distances.sort()
    for _ in xrange(to_remove):
        if distances and distances[-1][0] > 0:
            distances = distances[:-1]
    filtered = [variable_values[index] for _, index in distances]
    return filtered



def draw_legend2():
    font_size= 20
    fig = figure(figsize=(8, 6), dpi=80)
    p1 = plt.bar(range(7), range(7), 1.0, color="#7FCA9F")
    for algo_index in xrange(len(algos)):
        p1[algo_index].set_color(colors[algo_index])
    fig = figure(figsize=(12.5, 3), dpi=80)
    legend = plt.legend( p1, algos, shadow=False, loc=1, ncol=2, fontsize=font_size)
    legend.draw_frame(True) 
    savefig("graphs/legend2" + file_type, transparent="True", pad_inches=0)

def draw_bar_charts(sorted, shuffled, title, yaxis_name, filename, ylim=None, p2onTop=False):
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
    
    bar_width = 0.1
    margin = bar_width / 2
    font_size= 20
    total_width = bar_width * len(algos) * 2 + margin * (len(algos) * 2  + 1)
    
    offsets = [index * (bar_width * 2 + margin) + margin  for index in xrange(len(algos))]
    second_offsets = [offset + bar_width for offset in offsets]
    xticks = second_offsets
    
    "shuffled"
    means, deviations = get_means_and_deviatons(shuffled)
    #p1 = plt.bar(offsets, means, 2 * bar_width, color="#7FCA9F", yerr=deviations)
    p1 = plt.bar(offsets, means, bar_width, color="#7FCA9F", yerr=deviations)
    
    width = 0.1       # the width of the bars: can also be len(x) sequence
    #plt.subplot2grid((1,3), (0,0), colspan=2)
    
    
    "sorted"
    means, deviations = get_means_and_deviatons(sorted)
    ind = numpy.arange(len(deviations))    # the x locations for the groups
    width = 0.1       # the width of the bars: can also be len(x) sequence
    #plt.subplot2grid((1,3), (0,0), colspan=2)
    #p2 = plt.bar(second_offsets, means,  bar_width, color="#E96D63", yerr=deviations)  
    
    
    #p2 = plt.bar(offsets, means, 2 * bar_width, color="#E96D63", yerr=deviations) 
    p2 = plt.bar(second_offsets, means, bar_width, color="#E96D63", yerr=deviations) 
    
    for algo_index in xrange(len(algos)):
        p1[algo_index].set_color(colors[algo_index])
        p2[algo_index].set_color(colors_darker[algo_index])
    
    if not p2onTop:
        for line in p1:
            line.set_zorder(3) 
        for line in p2:
            line.set_zorder(4) 
    else:
        for line in p1:
            line.set_zorder(4) 
        for line in p2:
            line.set_zorder(3)         
           
    for bar in p2 + p1:
        #print bar.__dict__.keys() 
        bar._linewidth = 0
    
    ylabel = plt.ylabel(yaxis_name)
    ylabel.set_fontsize(font_size)
    for ytick in plt.yticks()[1]:
        ytick.set_fontsize(font_size)
    
    if ylim:
        plt.ylim([0, ylim])
    plt.xticks(xticks, ["" for char in "ABCDEFG"], fontsize=font_size)     
    plt.subplots_adjust(left=0.17, right=0.95, top=0.95, bottom=0.05)
    #legend = plt.legend( (p1[0], p2[0]), ("shuffled", "sorted"), shadow=False, loc=1, fontsize=font_size)
    #legend.draw_frame(False) 
    fig.patch.set_visible(False)    
    
    title = plt.title(title)
    #figtext(.955, 0.3, u"© http://exascale.info", rotation='vertical')
    title.set_fontsize(font_size)
    savefig(filename, transparent="True", pad_inches=0)
    #plt.show()
    #exit()


def draw_scatter_plot(x_values, algo2results, title, filename, yaxis_name, xaxis_name, y_log=False, left_margin=0.2):
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
    ylabel = plt.ylabel(yaxis_name)
    ylabel.set_fontsize(font_size)
    xlabel = plt.xlabel(xaxis_name)
    xlabel.set_fontsize(font_size)
    for ytick in plt.yticks()[1]:
        ytick.set_fontsize(font_size)
    
    print x_values
    MAX_TICKS = 10
    step = 100000
    for step in [100000, 200000, 500000, 1000000, 2000000, 5000000, 10000000]:
        if MAX_TICKS * step >= max(x_values):
            break
    xticks2show = range(0, max(x_values) + 1, step)
    xticks2show_labels = [value and str(int(value / 100000)) or "" for value in xticks2show]
    
    plt.xlim([0, max(x_values)])
        
    plt.xticks(xticks2show, xticks2show_labels, fontsize=font_size)     
    plt.subplots_adjust(left_margin, right=0.95, top=0.95, bottom=0.13)
    #legend = plt.legend( (p1[0], p2[0]), ("shuffled", "sorted"), shadow=False, loc=1, fontsize=font_size)
    #legend.draw_frame(False) 
    fig.patch.set_visible(False)    
    #figtext(.94, 0.4, u"© http://exascale.info", rotation='vertical')
    
    title = plt.title(title)
    title.set_fontsize(font_size)
    
    savefig(filename, transparent="True", pad_inches=0)
    #plt.show()
    #exit()

def draw_total_upload_time(results, remove_extrems = False):
    datasets = [key for key in results[0].keys()]
    dataset2upload_time = {}
    for dataset_name in datasets:
        dataset_results = {}
        for algo in algos:
            dataset_results[algo] = []
            for launch in results:
                algo_results = launch[dataset_name][algo]
                total_load_time = sum([row[-2] for row in algo_results])
                dataset_results[algo] += [total_load_time]
        dataset2upload_time[dataset_name] = dataset_results
    if remove_extrems:
        for ds_name in dataset2upload_time.keys():
            for algo in dataset2upload_time[ds_name].keys():
                dataset2upload_time[ds_name][algo] = remove_extremes(dataset2upload_time[ds_name][algo])
    by_dataset = {}
    for ds_name in datasets:
        no_suffix = ds_name.replace("_shuff", "")
        position = "_shuff" in ds_name and 1 or 0
        by_dataset.setdefault(no_suffix, ["", ""])
        by_dataset[no_suffix][position] = dataset2upload_time[ds_name]
    for ds_name in datasets:
        if "_shuff" in ds_name:
            continue
        graph_id = "load_time"
        filename = "graphs/" + graph_id + "-" + ds_name + suffix + file_type
        sorted, shuffled = by_dataset[ds_name]
        title = ds_name.split("_")[0]
        draw_bar_charts(sorted, shuffled, title, "Time [s]", filename)


def draw_total_mem_consumption(results, remove_extrems = False):
    datasets = [key for key in results[0].keys()]
    dataset2upload_time = {}
    for dataset_name in datasets:
        dataset_results = {}
        for algo in algos:
            dataset_results[algo] = []
            for launch in results:
                algo_results = launch[dataset_name][algo]
                total_mem_consumption =  max(algo_results[-1][2], algo_results[-1][3])
                dataset_results[algo] += [total_mem_consumption]
        dataset2upload_time[dataset_name] = dataset_results
    if remove_extrems:
        for ds_name in dataset2upload_time.keys():
            for algo in dataset2upload_time[ds_name].keys():
                dataset2upload_time[ds_name][algo] = remove_extremes(dataset2upload_time[ds_name][algo])
    by_dataset = {}
    for ds_name in datasets:
        no_suffix = ds_name.replace("_shuff", "")
        position = "_shuff" in ds_name and 1 or 0
        by_dataset.setdefault(no_suffix, ["", ""])
        by_dataset[no_suffix][position] = dataset2upload_time[ds_name]
    
    for ds_name in datasets:
        if "_shuff" in ds_name:
            continue
        graph_id = "memory_consumption"
        filename = "graphs/" + graph_id + "-" + ds_name + suffix + file_type
        sorted, shuffled = by_dataset[ds_name]
        title = ds_name.split("_")[0]
        draw_bar_charts(sorted, shuffled, title, 'Memory [MB]', filename, p2onTop = True)

def draw_num_keys2mem_consumption(results, remove_extreme_points = False):
    datasets = [key for key in results[0].keys()]
    dataset2algos_results = {}
    x_values = {}
    for dataset_name in datasets:
        dataset_results = {}
        for algo in algos:
            dataset_results[algo] = []
            for launch in results:
                algo_results = launch[dataset_name][algo]
                memory_consumptions = [max(row[2], row[3]) for row in algo_results]
                dataset_results[algo] += [memory_consumptions]
                if not dataset_name in x_values:
                    x_values[dataset_name] = [row[0] for row in algo_results]                
                #print memory_consumptions
            if remove_extreme_points:
                dataset_results[algo] = [numpy.mean(remove_extremes(numpy.array(row).flatten())) for row in np.matrix(dataset_results[algo]).transpose()]
            else:
                dataset_results[algo] = [numpy.mean(row) for row in np.matrix(dataset_results[algo]).transpose()]
        dataset2algos_results[dataset_name] = dataset_results
    
    for ds_name in datasets:
        graph_id = "num_keys-memory"
        filename = "graphs/" + graph_id + "-" + ds_name + suffix + file_type
        title = ds_name.split("_")[0].replace("_shuff", "").split("_")[0] + ("shuff" in ds_name and " unordered" or " ordered")
        draw_scatter_plot(x_values[ds_name], dataset2algos_results[ds_name], title, filename, 'Memory [MB]', "#keys inserted [10^5]", left_margin=0.15)

def draw_num_keys2insert_time(results, remove_extreme_points = False):
    datasets = [key for key in results[0].keys()]
    dataset2algos_results = {}
    x_values = {}
    for dataset_name in datasets:
        dataset_results = {}
        for algo in algos:
            dataset_results[algo] = []
            for launch in results:
                algo_results = launch[dataset_name][algo]
                insert_times = [row[-2] for row in algo_results]
                dataset_results[algo] += [insert_times]
                if not dataset_name in x_values:
                    x_values[dataset_name] = [row[0] for row in algo_results]
            if remove_extreme_points:
                dataset_results[algo] = [numpy.mean(remove_extremes(numpy.array(row).flatten())) for row in np.matrix(dataset_results[algo]).transpose()]
            else:
                dataset_results[algo] = [numpy.mean(row) for row in np.matrix(dataset_results[algo]).transpose()]
        dataset2algos_results[dataset_name] = dataset_results
    
    for ds_name in datasets:
        graph_id = "num_keys-insert_time100K"
        filename = "graphs/" + graph_id + "-" + ds_name + suffix + file_type
        title = ds_name.replace("_shuff", "").split("_")[0] + ("shuff" in ds_name and " unordered" or " ordered")
        draw_scatter_plot(x_values[ds_name], dataset2algos_results[ds_name], title, filename, 'Time in seconds (log)', "#keys inserted [10^5]", y_log=True, left_margin=0.13)

def draw_num_keys2query_time(results, remove_extreme_points = False):
    datasets = [key for key in results[0].keys()]
    dataset2algos_results = {}
    x_values = {}
    for dataset_name in datasets:
        dataset_results = {}
        for algo in algos:
            dataset_results[algo] = []
            for launch in results:
                algo_results = launch[dataset_name][algo]
                insert_times = [row[-1] for row in algo_results]
                dataset_results[algo] += [insert_times]
                if not dataset_name in x_values:
                    x_values[dataset_name] = [row[0] for row in algo_results]
            if remove_extreme_points:
                dataset_results[algo] = [numpy.mean(remove_extremes(numpy.array(row).flatten())) for row in np.matrix(dataset_results[algo]).transpose()]
            else:
                dataset_results[algo] = [numpy.mean(row) for row in np.matrix(dataset_results[algo]).transpose()]
        dataset2algos_results[dataset_name] = dataset_results
    
    for ds_name in datasets:
        graph_id = "num_keys-query_time100K"
        filename = "graphs/" + graph_id + "-" + ds_name + suffix + file_type
        title = ds_name.replace("_shuff", "").split("_")[0] + ("shuff" in ds_name and " unordered" or " ordered")
        draw_scatter_plot(x_values[ds_name], dataset2algos_results[ds_name], title, filename, 'Time [s]', "#keys inserted [10^5]", y_log=False, left_margin=0.15)


if 1:
    experiments = ((results, True), (ds6_results, True), (other_ds_results, True))
    #experiments = [(other_ds_results, True)]
    for experiment_results, need_filtration in experiments:
        draw_total_upload_time(experiment_results, need_filtration)
        draw_total_mem_consumption(experiment_results, need_filtration)    
        draw_num_keys2mem_consumption(experiment_results, need_filtration)
        draw_num_keys2insert_time(experiment_results, need_filtration)
        draw_num_keys2query_time(experiment_results, need_filtration)

draw_legend()

