
for size in 128 256 512 1024 2048
do
sudo /home/jxx/Desktop/LAKV/LAKV/Straight-Key_separate_2023_03_20/out-static/db_bench --benchmarks="fillrandom,readrandom" --num=10000000 --value_size=${size}  > /home/jxx/Desktop/LAKV/My_test/db_bench_LAKV_test_${size}.txt 2>&1
done


