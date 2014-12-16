set -e
rm -f index_run
g++-4.8 -O3 src/main.cpp src/nclist/intervaldb.c -Isrc/ -std=gnu++11 -o bin/index_run
echo "BUILD SUCCESSFUL"

sh run_scripts/run.sh datasets/query_len/ > test_results/query_len1.txt 
sh run_scripts/run.sh datasets/avg_overlapping/ > test_results/avg_overlapping1.txt 
sh run_scripts/run.sh datasets/avg_overlapping_stdev/ > test_results/avg_overlapping_stdev1.txt 
