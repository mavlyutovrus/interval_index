rm index_run
g++-4.8 -O3 main.cpp nclist/intervaldb.c -std=gnu++11 -o index_run

algos_count=`./index_run -wcount`
last_algo=`expr $algos_count - 1` 
for run_index in 0 1 2 3 4 5 6 7 8 9 
do 
  echo run$run_index
  for path in `ls $1/*.txt`
  do
    for algo_index in `seq 0 $last_algo`
    do
      ./index_run $path $algo_index 
    done
  done
done

