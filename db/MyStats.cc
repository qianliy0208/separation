/*
 * MyStats.cpp
 *
 *  Created on: Apr 20, 2023
 *      Author: yql
 */

#include <iostream>

#include "MyStats.h"

namespace leveldb {




void MyStats::Init() {
   num_key_generate_ = 0;

   num_unique_key_generate_ = 0;

   num_repetite_key_generate_ = 0;

   map_generate_key_num_={};      ///存储负载生成的key数目


    ///dbimp analysis
    num_put_call_wr = 0;
    num_put__wr = 0;
    /**
    uint64_t num_put_call_wr;
    uint64_t num_put_call_;
    uint64_t num_put_call_;
    uint64_t num_put_call_;
    uint64_t num_put_call_;
    uint64_t num_put_call_;
    **/

    num_key_put_into_batch_ = 0;
    num_key_insert_into_batch_ = 0;
    num_memtable_created_ = 0;
    num_key_kTypeValue_be_put_into_mem_ = 0;

    num_key_kTypeDeletion_be_put_into_mem_ = 0;

    ////文件写入l0层数量统计
   num_flush_imm_into_below_ = 0;

    ///写人L0层的文件数目记录




    ///读LO的文件序号:block块访问信息指针:block块访问信息指针
    num_build_table_ = 0;
    files_access_with_below_record = {};

     vector_files_id_be_writen_into_L0 = {};
     vector_files_id_be_writen_into_L1 = {};
     vector_files_id_be_writen_into_L2 = {};
     vector_files_id_be_writen_into_L3 = {};
     vector_files_id_be_writen_into_L4 = {};
     vector_files_id_be_writen_into_L5 = {};
     vector_files_id_be_writen_into_L6 = {};

    ///读每层的文件数目记录
     vector_files_id_be_read_from_L0 = {};
     vector_files_id_be_read_from_L1 = {};
     vector_files_id_be_read_from_L2 = {};
     vector_files_id_be_read_from_L3 = {};
     vector_files_id_be_read_from_L4 = {};
     vector_files_id_be_read_from_L5 = {};
     vector_files_id_be_read_from_L6 = {};
      num_0_total_ = 0 ;
      num_1_total_ = 0 ;
      num_2_total_ = 0 ;
      num_3_total_ = 0 ;
      num_4_total_ = 0 ;
      num_5_total_ = 0 ;
      num_6_total_ = 0 ;

     ///写入文件信息
     files_write_into_L0 = {};
     files_write_into_L1 = {};
     files_write_into_L2 = {};
     files_write_into_L3 = {};
     files_write_into_L4 = {};
     files_write_into_L5 = {};
     files_write_into_L6 = {};
     num_build_table_failed_ = 0;
     num_key_get_ = 0;
     map_get_key_num_ = {};
     num_get_key_function_called_ = 0;

     num_get_key_hit_mem_ = 0;
     num_get_key_hit_imm_ = 0;
     num_get_key_hit_current_version_ = 0;
     num_call_background_compaction_ = 0;
     num_call_mamual_compaction_ = 0;
     num_call_backgound_auto_compaction_ = 0;
     file_fds_pick_every_background_compaction_ = {};
     num_compaction_ = 0;
     compacts_stats = {};
     files_IO_Info_ = {};
     aver_num_files_from_L0_when_get_key_ = 0;
     aver_num_files_from_L1_when_get_key_ = 0;
     aver_num_files_from_L2_when_get_key_ = 0;
     aver_num_files_from_L3_when_get_key_ = 0;
     aver_num_files_from_L4_when_get_key_ = 0;
     aver_num_files_from_L5_when_get_key_ = 0;
     aver_num_files_from_L6_when_get_key_ = 0;
     num_key_hit_from_table_cache_ = 0;
     num_key_hit_from_table_IO_ = 0;
     num_keys_not_found_from_filter_block_ = 0;
     num_keys_get_from_data_block_ = 0;
}

void  MyStats::UpdateAverageLevelFileNum(int new_files_num, int level){

  switch (level){
    case 0 : aver_num_files_from_L0_when_get_key_ = (aver_num_files_from_L0_when_get_key_ * num_0_total_ + new_files_num)/ (num_0_total_+1);num_0_total_++ ; break;
    case 1 : aver_num_files_from_L1_when_get_key_ = (aver_num_files_from_L1_when_get_key_ * num_1_total_ + new_files_num)/ (num_1_total_+1);num_1_total_++ ; break;
    case 2 : aver_num_files_from_L2_when_get_key_ = (aver_num_files_from_L2_when_get_key_ * num_2_total_ + new_files_num)/ (num_2_total_+1);num_2_total_++ ; break;
    case 3 : aver_num_files_from_L3_when_get_key_ = (aver_num_files_from_L3_when_get_key_ * num_3_total_ + new_files_num)/ (num_3_total_+1);num_3_total_++ ; break;
    case 4 : aver_num_files_from_L4_when_get_key_ = (aver_num_files_from_L4_when_get_key_ * num_4_total_ + new_files_num)/ (num_4_total_+1);num_4_total_++ ; break;
    case 5 : aver_num_files_from_L5_when_get_key_ = (aver_num_files_from_L5_when_get_key_ * num_5_total_ + new_files_num)/ (num_5_total_+1);num_5_total_++ ; break;
    case 6 : aver_num_files_from_L6_when_get_key_ = (aver_num_files_from_L6_when_get_key_ * num_6_total_ + new_files_num)/ (num_6_total_+1);num_6_total_++ ; break;
  }

}
void MyStats::PrintAllFilesOutputDuringCompaction(){
  int order = 0;

  if(compacts_stats.empty()){
    std::cout << "没有压缩输出过程记录" << std::endl;
  }

  for(auto tables = compacts_stats.begin();tables != compacts_stats.end(); tables++) {
    std::cout << "第"<< order++ <<"次背景压缩遍历生成文件统计如下：\t" <<std::endl;
    std::cout << "本次压缩总key数：\t" << (*tables)->num_total_keys << std::endl;
    std::cout << "本次压缩不同的key数：\t" << (*tables)->num_unique_keys << std::endl;
    std::cout << "本次压缩删除的key数：\t" << (*tables)->num_drop_keys << std::endl;
    if((*tables)==nullptr) {
      std::cout << "第"<< order++ <<"次背景压缩挑选文件空：\t" <<std::endl;
      continue;
    }
    std::cout << "本次压缩输出的文件数：\t" << (*tables)->num_output_files << std::endl;


    if((*tables)->output_files->empty()){
      std::cout << "本次压缩输出的文件数为0：\t" << std::endl;
      continue;
    }
    int order_file = 1;
    for(auto tableMeta = (*tables)->output_files->begin();tableMeta != (*tables)->output_files->end();tableMeta++)
    {

      if((*tableMeta) == nullptr) {
        std::cout << "第"<< order_file++ <<"个输出文件信息为空：\t" <<std::endl;
        continue;
      }
      std::cout << "第"<< order_file++ <<"个输出文件信息如下：\t" <<std::endl;
      std::cout << "文件号：\t" << (*tableMeta)->number << std::endl;
      std::cout << "文件最小key：\t" << (*tableMeta)->smallest << std::endl;
      std::cout << "文件最大key：\t" << (*tableMeta)->largest << std::endl;
      std::cout << "文件大小：\t" << (*tableMeta)->file_size << std::endl;
      if((*tableMeta)->blocks == nullptr){
        std::cout << "文件块指针为空" << std::endl;
        continue;
      }
      if((*tableMeta)->blocks->empty()) {
        std::cout << "文件block块是空" << std::endl;
        continue;
      }
      std::cout << "文件内块信息如下：\t"  << std::endl;
      for(auto blockInfo = (*tableMeta)->blocks->begin();blockInfo != (*tableMeta)->blocks->end(); blockInfo++) {
        ///////////////之后添加块信息；

      }

    }
  }
  std::cout << "压缩信息输出完毕....." << std::endl;
};

void MyStats::PrintAllFilesPickedByCompaction(){
    int order = 0;
    if(file_fds_pick_every_background_compaction_.empty()) {
      std::cout << "压缩挑选文件为空" << std::endl;
      return ;
    }
    for(auto files_picked = file_fds_pick_every_background_compaction_.begin();files_picked != file_fds_pick_every_background_compaction_.end();files_picked++)
  {
      if((*files_picked)->empty()){
      std::cout << "第"<< order++ <<"次背景压缩挑选文件空：\t" <<std::endl;
      continue ;
      }
      std::cout << "第"<< order++ <<"次背景压缩挑选文件如下：\t" <<std::endl;
      for(auto file = (*files_picked)->begin();file != (*files_picked)->end();file++){
        std::cout << "文件号：\t" << (*file)->number << "文件健范围：\t" << (*file)->smallest <<"_____" <<(*file)->largest << "文件大小：\t " << (*file)-> file_size <<std::endl;
      }
      std::cout << "\n******************" << std::endl;
    }
};

void MyStats::Reset() {
  ReleaseSpaceAlloctedForPickFilesDuringCompaction();
  ReleaseSpaceAllocated();
  Init();

}


MyStats::MyStats(){
    Init();
}

MyStats::~MyStats() {
  Reset();


}

void MyStats::OverWriteInsertKey(const std::string& s_key){
  num_key_generate_++;          ///总生成数++

  if(map_generate_key_num_.find(s_key)==map_generate_key_num_.end()){  ///找不到
//    map_generate_key_num_[s_key]= 1;
//    num_unique_key_generate_++;   ///新的key

  }
  else{///找到
    map_generate_key_num_[s_key]++;
  }

  return;

}
void MyStats::InsertKey(const std::string& s_key){
	num_key_generate_++;          ///总生成数++

	if(map_generate_key_num_.find(s_key)==map_generate_key_num_.end()){  ///找不到
		map_generate_key_num_[s_key]= 1;
		num_unique_key_generate_++;   ///新的key
	}
	else{///找到
		map_generate_key_num_[s_key]++;
	}

	return;

}

void MyStats::PrintKeyStatistic(){

	unsigned long int num_access[11] = {0};

	for(auto iter = map_generate_key_num_.begin();iter != map_generate_key_num_.end();iter++){
		if(iter->second>9) { ///超过9次
		   num_access[10]++;
		}
		else { ///低于10次
		   num_access[iter->second]++;
		}
		num_access[0]++;   ///保存总数

	}

	std::cout << "生成的不同key总数：" << num_access[0] << std::endl;
	for(int i = 1; i < 11; i++) {
		std::cout << "写入次数：\t" << i << "的key数目：\t" << num_access[i] << std::endl;
	}
        PrintGetKeyStatistic();
        std::cout << "*************压缩挑选文件信息******************" <<std::endl;
        //PrintAllFilesPickedByCompaction();
        std::cout << "*************压缩遍历key输出文件信息******************" <<std::endl;
        //PrintAllFilesOutputDuringCompaction();
        std::cout << "*************其他统计信息******************" <<std::endl;
        PrintOtherInfo();
	return ;

}
void MyStats::PrintGetKeyStatistic(){

  unsigned long int num_access[11] = {0};

  for(auto iter = map_get_key_num_.begin();iter != map_get_key_num_.end();iter++){
    if(iter->second>9) { ///超过9次
      num_access[10]++;
    }
    else { ///低于10次
      num_access[iter->second]++;
    }
    num_access[0]++;   ///保存总数

  }

  std::cout << "读取的不同key总数：" << num_access[0] << std::endl;
  for(int i = 1; i < 11; i++) {
    std::cout << "读取次数：\t" << i << "的key数目：\t" << num_access[i] << std::endl;
  }

  return ;

}

void MyStats::InsertGetKey(std::string s_key) {

  num_key_get_++;          ///总生成数++

  if(map_get_key_num_.find(s_key)==map_get_key_num_.end()){  ///找不到
    map_get_key_num_[s_key] = 1;

  }
  else{///找到
    map_get_key_num_[s_key]++;
  }
  return;




}
void MyStats::PrintCompactionInfo() {

  if(compacts_stats.empty()){
    std::cout << "压缩信息空" << std::endl;
    return;
  }

  for(auto files = compacts_stats.begin();files != compacts_stats.end(); files++) {

    std::cout << "第"<<(*files)->comp_id <<"次压缩：\t"<<  std::endl;
    std::cout << "共产生输出文件:\t " <<(*files)->num_output_files <<std::endl;
    std::cout << "本次压缩过程中总共遍历的key数目：\t " << (*files) -> num_total_keys << std::endl;
    std::cout << "其中不同的key数目：\t " << (*files)->num_unique_keys << std::endl;
    std::cout << "其中删除的key数目：\t " << (*files)->num_drop_keys << std::endl;
    std::cout << "其中因为标记删除类型删除的key数目：\t " << (*files)->num_drop_for_kTypeDeletion << std::endl;


    if((*files)->output_files->empty()) {
      std::cout << "文件写入信息为空" << std::endl;
      continue;
    }
    std::cout << "本次压缩生成输出文件信息如下：\t" << std::endl;
    for(auto file = (*files)->output_files->begin(); file != (*files)->output_files->end(); file++) {
      std::cout << "文件号：" << (*file)->number << std::endl;
      std::cout << "文件最小用户key：" << (*file)->smallest << std::endl;
      std::cout << "文件最大用户key：" << (*file)->largest << std::endl;
    }

  }





}

MyCompactionStat::MyCompactionStat(const MyCompactionStat& myCompactionStat): \
       comp_id(myCompactionStat.comp_id), \
       num_output_files(myCompactionStat.num_output_files), \
       num_drop_keys(myCompactionStat.num_drop_keys), \
       num_total_keys(myCompactionStat.num_total_keys), \
       num_unique_keys(myCompactionStat.num_unique_keys), \
       output_files(myCompactionStat.output_files),\
       num_drop_for_kTypeDeletion(0), \
       total_written_bytes(0)
{


}
MyCompactionStat::MyCompactionStat(): \
    comp_id(0), \
    num_output_files(0), \
    num_drop_keys(0), \
    num_total_keys(0), \
    num_unique_keys(0), \
    output_files(nullptr), \
    num_drop_for_kTypeDeletion(0), \
    total_written_bytes(0)
{


}
TableMeta::TableMeta():num_read(0),is_deleted(false),number(0),file_size(0),smallest(""),largest(""),blocks(nullptr) {

}
TableMeta* MyStats::AllocateATableMeta() { return new TableMeta; }
std::vector<TableMeta*>* MyStats::AllocateATableMetaVector() { return new std::vector<TableMeta*>; }
void MyStats::ReleaseSpaceAllocated() {



  if(compacts_stats.empty()){
    std::cout << "没有空间需要释放" << std::endl;
    return ;
  }

  for(auto tables = compacts_stats.begin();tables != compacts_stats.end(); tables++) {

    assert((*tables) != nullptr);  ////对于每一个压缩

    if((*tables)->output_files == nullptr ){
      delete *tables;
      continue;
    }
    if( (*tables)->output_files->empty()) {
      delete (*tables)->output_files;
      delete *tables;
      continue ;
    }

    assert(!(*tables)->output_files->empty());

    for(auto tableMeta = (*tables)->output_files->begin();tableMeta != (*tables)->output_files->end();tableMeta++) {

      assert((*tableMeta) != nullptr);

      if((*tableMeta)->blocks== nullptr) {
        delete (*tableMeta);
        continue;
      }
      if ((*tableMeta)->blocks->empty())
      {
        delete ((*tableMeta)->blocks);
        delete (*tableMeta);
        continue ;
      }

      for(auto blockInfo = (*tableMeta)->blocks->begin();blockInfo != (*tableMeta)->blocks->end(); blockInfo++) {
        delete (*blockInfo);
      }
       delete ((*tableMeta)->blocks);
       delete (*tableMeta);
    }
     delete *tables;
  }

}
BlockAccessInfo* MyStats::AllocateABlockAccessInfo() { return new BlockAccessInfo; }
BlockAccessInfo::BlockAccessInfo():accessType(AccessType::DEFAULT),id_block(0),num_be_read(0),num_key_total(0),num_key_read(0),num_key_writen(0),offset(0) {}
MyCompactionStat* MyStats::AllocateAMyCompactionStat() { return new MyCompactionStat; }
void MyStats::ReleaseSpaceAlloctedForPickFilesDuringCompaction() {
  if (file_fds_pick_every_background_compaction_.empty()) {
    return;
  }
  for (auto tables = file_fds_pick_every_background_compaction_.begin();tables != file_fds_pick_every_background_compaction_.end(); tables++) {
    if ((*tables)->empty()) {
      continue;
    }
    for (auto tableMeta = (*tables)->begin(); tableMeta != (*tables)->end();
         tableMeta++) {
      if ((*tableMeta) == nullptr) {
        continue;
      }
      if ((*tableMeta)->blocks == nullptr||(*tableMeta)->blocks->empty()) {
       // std::vector<std::vector<TableMeta*>*> file_fds_pick_every_background_compaction_;
        delete (*tableMeta);
        continue;
      }
      for (auto blockInfo = (*tableMeta)->blocks->begin();
           blockInfo != (*tableMeta)->blocks->end(); blockInfo++) {
        delete (*blockInfo);
      }
      delete (*tableMeta);
    }
       delete (*tables);
    // std::vector<std::vector<TableMeta*>*> file_fds_pick_every_background_compaction_;
    // std::vector<BlockAccessInfo*>* blocks;
  }
}


std::vector<BlockAccessInfo*>* MyStats::AllocateABlockAccessInfoVector() {
  return new std::vector<BlockAccessInfo*>;
}
void MyStats::PrintOtherInfo() {

  std::cout << "读key函数调用数：\t"<< num_get_key_function_called_ << "\n" \
            << "读key命中mem个数：\t"<< num_get_key_hit_mem_ << "\n" \
            << "读key命中immutable个数：\t"<< num_get_key_hit_imm_ << "\n" \
            << "读key命中当前版本个数：\t"<< num_get_key_hit_current_version_ << "\n" \

            << "读key从L0层文件的平均读取次数：\t"<< aver_num_files_from_L0_when_get_key_ << "\n" \
            << "读key从L1层文件的平均读取次数：\t"<< aver_num_files_from_L1_when_get_key_ << "\n" \
            << "读key从L2层文件的平均读取次数：\t"<< aver_num_files_from_L2_when_get_key_ << "\n" \
            << "读key从L3层文件的平均读取次数：\t"<< aver_num_files_from_L3_when_get_key_ << "\n" \
            << "读key从L4层文件的平均读取次数：\t"<< aver_num_files_from_L4_when_get_key_ << "\n" \
            << "读key从L5层文件的平均读取次数：\t"<< aver_num_files_from_L5_when_get_key_ << "\n" \
            << "读key从L6层文件的平均读取次数：\t"<< aver_num_files_from_L6_when_get_key_ << "\n" \
            << "以上读key从缓存的读取次数：\t"<<  num_key_hit_from_table_cache_  << "\n" \
            << "以上读key从文件IO读取次数：\t"<<  num_key_hit_from_table_IO_  << "\n" \
            << "读文件的过滤块没有找到，不需要继续读的key数目：\t"<<  num_keys_not_found_from_filter_block_  << "\n" \
            << "读数据块内容读取key的数目：\t"<<   num_keys_get_from_data_block_  << std::endl;


                std::cout<<"生成的key数目:\t" << num_key_generate_<<std::endl;
                std::cout<<"生成的不重复的key数目:\t" << num_unique_key_generate_<<std::endl;
                std::cout<<"重复key数目:\t" << num_repetite_key_generate_<<std::endl;
                std::cout<<"put函数调用次数:\t" << num_put_call_wr<<std::endl;
              //  std::cout<<":\t" << num_put__wr<<std::endl;
                std::cout<<"放入batch的key数目:\t" << num_key_put_into_batch_<<std::endl;
                std::cout<<"插入batch的key数目:\t" << num_key_insert_into_batch_<<std::endl;
                std::cout<<"创建的memtable:\t" << num_memtable_created_<<std::endl;
                std::cout<<"插入memetbale中类型为kTypeValue的key数目:\t" << num_key_kTypeValue_be_put_into_mem_<<std::endl;
                std::cout<<"插入memetbale中类型为kTypeDeletion的key数目:\t" << num_key_kTypeDeletion_be_put_into_mem_<<std::endl;
                std::cout<<"flush_imm的数目:\t" << num_flush_imm_into_below_<<std::endl;
                std::cout<<"SST表创建的数目:\t" << num_build_table_<<std::endl;
                std::cout<<"get的key数目:\t" << num_key_get_<<std::endl;
                std::cout<<":\t" << num_get_key_function_called_<<std::endl;







}


void MyStats::SetZeroToGenerateKey() {

  for(auto & iter : map_generate_key_num_) {
    iter.second = 0;
  }

}
} /* namespace leveldb */
