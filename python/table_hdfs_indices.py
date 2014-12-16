
import numpy, math
ds_name = ""

results = {}

for line in open("../test_results/spathad_mr.txt"):
    if not "RESPONSE" in line:
        continue
    if "for" in line:
        ds_name = line.strip().split()[-1]
        continue
    chunks = line[:-1].split()
    algo = chunks[1]
    qlen = chunks[3]
    response_size = float(chunks[5])
    time = float(chunks[7])
    results.setdefault(ds_name + " " + qlen, {}).setdefault(algo, []).append( (response_size, time ) )
    
for line in open("../test_results/distributed_ii.txt"):
    if not "RESPONSE" in line:
        continue
    if "for" in line:
        ds_name = line.strip().split()[-1]
        continue
    chunks = line[:-1].split()
    algo = chunks[1]
    algo = "interval_index_warm"
    qlen = chunks[5]
    read_count = chunks[3]
    response_size = float(chunks[7])
    time = float(chunks[9])
    results.setdefault(ds_name + " " + qlen, {}).setdefault(algo, []).append( (response_size, time, read_count) )

for line in open("../test_results/distributed_ii_cold"):
    if not "RESPONSE" in line:
        continue
    if "for" in line:
        ds_name = line.strip().split()[-1]
        continue
    chunks = line[:-1].split()
    algo = chunks[1]
    algo = "interval_index_cold"
    qlen = chunks[5]
    read_count = chunks[3]
    response_size = float(chunks[7])
    time = float(chunks[9])
    results.setdefault(ds_name + " " + qlen, {}).setdefault(algo, []).append( (response_size, time, read_count) )


ds_name = "Exome dataset";
for file in ["../test_results/exome_distr_ii_cold.txt", "../test_results/exome_distr_ii.txt", "../test_results/distributed_exome.txt"]:
    for line in open(file):
        if not "RESPONSE" in line:
            continue
        chunks = line[:-1].split()
        algo = chunks[1]
        if "interval_index" in algo:
            qlen = chunks[5]
            read_count = chunks[3]
            response_size = float(chunks[7])
            time = float(chunks[9])
            results.setdefault(ds_name + " " + qlen, {}).setdefault(algo, []).append( (response_size, time, read_count) )
        else:
            qlen = chunks[3]
            response_size = float(chunks[5])
            time = float(chunks[7])
            results.setdefault(ds_name + " " + qlen, {}).setdefault(algo, []).append( (response_size, time ) )        


joined_results = {}
for dataset, algo_results in results.items():
    mean_results = []
    for algo, values in algo_results.items():
        sizes = []
        times = []
        read_counts = []
        if "interval_index" in algo:
            for response_size, time, read_count in values:
                sizes += [response_size]
                times += [time]
                read_counts += [float(read_count)]
            #print dataset, numpy.mean(times), numpy.std(times), times
            time_per_response = [1000 * float(time)/read_count for time, read_count in zip(times, read_counts)]   
            time_per_response_mean, time_per_response_std = numpy.mean(time_per_response), numpy.std(time_per_response)
            time_per_response_margin =  1.96 * time_per_response_std / math.sqrt(len(times)) 
            time_per_response = "$%.3f\pm%.3f$" % (time_per_response_mean, time_per_response_margin)
             
            #print time_per_returned_interval      
            time_mean, time_std = numpy.mean(times), numpy.std(times)
            time_margin =  1.96 * time_std / math.sqrt(len(times))
            size_mean, size_std = numpy.mean(sizes), numpy.std(sizes)    
            size_margin =  1.96 * size_std / math.sqrt(len(sizes))  
            read_count_mean = numpy.mean(read_counts) 
            algo = "warm" in algo and  "4_ii" or "3_ii"
            mean_results += [(algo, "$%.2f\pm%.2f$" % (time_mean, time_margin), "$%.1f$" % (read_count_mean),  "$%.1f$" % (size_mean), time_per_response)]
        else:
            for response_size, time in values:
                sizes += [response_size]
                times += [time]
            time_mean, time_std = numpy.mean(times), numpy.std(times)
            time_margin = 1.96 * time_std / math.sqrt(len(times))
            size_mean, size_std = numpy.mean(sizes), numpy.std(sizes) 
            size_margin =  1.96 * size_std / math.sqrt(len(sizes))  
            if "spat" in algo:
                algo = "2_spat"
            elif "map_reduce" in algo:
                algo = "1_mapred"
            mean_results += [(algo, "$%d\pm%d$" % (int(time_mean), int(time_margin)), "$%.1f$" % (size_mean) )]
    joined_results[dataset] = mean_results



datasets = [dataset for dataset in joined_results.keys()]
datasetsets_order = []
for key in datasets:
    if not "xome" in key:
        name, qlen = key.split(" ")
        size, overlapping = name.replace(".txt", "").split("_")[-2:]
        qlen, size, overlapping = float(qlen), float(size), float(overlapping)
        sort_key = (size, overlapping, qlen)
        size_desc = size == 10**7 and "10M" or size == 10**8 and "100M" or size == 10**9 and "1000M" or "" 
        description = "%s, %.1f, %.1f" % (size_desc, overlapping, qlen)
        datasetsets_order += [(sort_key, description, key)]
    else:
        datasetsets_order += [(("a", 0, 0), "Exome alignement dataset", key)]

datasetsets_order.sort()


for _, desc, key in datasetsets_order:
    mean_results = joined_results[key]
    mean_results.sort()
    print desc, "&",
    for algo_results in mean_results:
        algo = algo_results[0]
        if algo.startswith("1_"):
            print algo_results[-1], "&",
        print algo_results[1], "&",
        if "4_" in algo:
            print algo_results[2],           
    print "\\\\", "\\hline"
    




    