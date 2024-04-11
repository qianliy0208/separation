//
// Created by yql on 5/11/23.
//

#ifndef LEVELDB_MY_ANALYSIS_H
#define LEVELDB_MY_ANALYSIS_H


#include <cstdint>
#include <iostream>
struct MyAnalysis{
  uint64_t read_key_from_block_cache_hit_num = 0;
  uint64_t read_key_from_block_cache_miss_num = 0;
  void ReSet();
  void Print();

  int read_key_from_table_cache_total_num = 0;
  int read_key_from_table_cache_miss_num = 0;

  int filter_valid_num_read = 0;
  int filter_invalid_num_read = 0;

  int read_block_from_file_system_num = 0;
};


#endif  // LEVELDB_MY_ANALYSIS_H
