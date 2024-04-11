//
// Created by yql on 5/11/23.
//
#include "my_analysis.h"



void MyAnalysis::ReSet() {
  read_key_from_block_cache_hit_num = 0;
  read_key_from_block_cache_miss_num = 0;

  filter_valid_num_read = 0;
  filter_invalid_num_read = 0;

  read_key_from_table_cache_total_num = 0;
  read_key_from_table_cache_miss_num = 0;
  read_block_from_file_system_num = 0;

}
void MyAnalysis::Print() {



  double block_read_cache_hit_rate = (double)read_key_from_block_cache_hit_num /(read_key_from_block_cache_hit_num + read_key_from_block_cache_miss_num);



  std::cout << "读key从文件缓存命中次数： \t" << read_key_from_table_cache_total_num - read_key_from_table_cache_miss_num << std::endl;
  std::cout << "读key从文件缓存丢失次数：\t" << read_key_from_table_cache_miss_num << std::endl;
  std::cout << "查找文件缓存命中率：\t"  << (double) (read_key_from_table_cache_total_num - read_key_from_table_cache_miss_num)/read_key_from_table_cache_total_num;

  std::cout << "文件缓存队列没找到则创建打开一个新文件，找到文件之后开始先过滤后读文件内部的块：" << std::endl;



  std::cout << "读block块缓存命中次数：\t" << read_key_from_block_cache_hit_num << std::endl;
  std::cout << "读block块缓存丢失次数：\t" << read_key_from_block_cache_miss_num << std::endl;
  std::cout << "读block块缓存命中率：\t" << block_read_cache_hit_rate << std::endl;


  std::cout << "读block块缓存之前文件过滤器失效次数：\t" << filter_invalid_num_read << std::endl;

  std::cout << "从文件系统读block块次数：\t" << read_block_from_file_system_num << std::endl;




}