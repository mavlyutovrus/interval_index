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
    results = load_data("../test_results/local_real.txt")
    by_algo = {}
    for ds_name, algos_results in results.items():
        print ds_name
        for algo, results in algos_results.items():
            query_times = [result[-1] for result in results]
            query_time, margin = calc_avg_minus_extremes(query_times)
            by_algo.setdefault(algo, []).append((ds_name, query_time, margin))
    algos = [algo for algo in by_algo.keys()]
    algos.sort()
    for algo in algos:
        results = by_algo[algo]
        results.sort()
        print algo, 
        for ds_name, time, margin in results:
            print  "& $%.4f\pm%.2f$ " % (time, margin),
        print "\\\\ \hline"


