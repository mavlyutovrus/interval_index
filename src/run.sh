rm index_run
g++ -O3 main.cpp nclist/intervaldb.c -o index_run -std=gnu++11
#g++ -g main.cpp -o index_run -std=gnu++11


for path in `ls $1/*.txt`
do
    echo $path
    ./index_run $path 
done

