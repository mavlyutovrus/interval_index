rm index_run
g++ -O3 main.cpp nclist/intervaldb.c ../tools/papi-5.3.2/src/libpapi.a -I../tools/papi-5.3.2/src/ -std=gnu++11 -o index_run


for path in `ls $1/*.txt`
do
    echo $path
    ./index_run $path 
done

