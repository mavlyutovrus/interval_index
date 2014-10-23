g++ -O3 main.cpp -o index_run -std=gnu++11
#g++ -g main.cpp -o index_run -std=gnu++11

for path in `ls -r $1/*.txt`
do
    echo $path
    ./index_run $path 
done

