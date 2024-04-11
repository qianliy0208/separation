value_size=1024
write_buffer_size=67108864
num=10000000
block_size=4096
#use_existing_db
#histogram
#comparisons
#threads
#max_file_size
#key_prefix
#bloom_bits
#bloom_bits
#cache_size

sudo ./out-static/db_bench  --benchmarks="fillrandom,readrandom,stats"  --num=$num --value_size=$value_size \
--write_buffer_size=$write_buffer_size  \
--block_size=$block_size \
