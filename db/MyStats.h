/*
 * MyStats.h
 *
 *  Created on: Apr 20, 2023
 *      Author: yql
 */

#ifndef DB_MYSTATS_H_
#define DB_MYSTATS_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "db/dbformat.h"

typedef unsigned long int uint64_t;



namespace leveldb {

enum AccessType {
  DEFAULT,
  WRITE,
  READ,
  COMPACTION_R,
  COMPACTION_W,
  DELETE
};
struct BlockAccessInfo;
class TableMeta {

 public:
  TableMeta();
   uint64_t num_read;
   bool is_deleted;

  uint64_t number;
  uint64_t file_size;    // File size in bytes
  std::string smallest;  // Smallest internal key served by table
  std::string largest;   // Largest internal key served by table
  std::vector<BlockAccessInfo*>* blocks;
};

///压缩单位的统计
class MyCompactionStat {
 public:
  MyCompactionStat(const MyCompactionStat& myCompactionStat);
  MyCompactionStat();
  uint64_t comp_id;
  uint64_t num_output_files;
  uint64_t  num_drop_keys;
  uint64_t  num_total_keys;
  uint64_t  num_unique_keys;
  std::vector<TableMeta*>* output_files;
  uint64_t num_drop_for_kTypeDeletion;
  uint64_t total_written_bytes;
};




struct BlockAccessInfo{
  BlockAccessInfo();
  AccessType accessType;
  int id_block;
  int num_be_read;
  int num_key_total;  ///key总数
  int num_key_read;   ///读的key次数
  int num_key_writen; ///写的key次数
  uint64_t offset;
};

struct FileInfo {
  uint64_t file_id;
  uint8_t level;
  AccessType accessType;
  std::string smallest_key;
  std::string largest_key;
  std::vector<BlockAccessInfo*> block_vector;

  FileInfo(uint64_t file_id):file_id(file_id){};

};



class  MyStats {

public:
	MyStats();

	~MyStats();
	/*
	* 把生成的key放入map
	*/

	void InsertKey(const std::string& s_key);
	/*
	* 打印统计信息到终端
	*/
	void PrintKeyStatistic();
        void Init();
        void Reset();
        void PrintGetKeyStatistic();
        void PrintAllFilesPickedByCompaction();
        void PrintAllFilesOutputDuringCompaction();
        TableMeta* AllocateATableMeta();
        BlockAccessInfo* AllocateABlockAccessInfo();

        std::vector<TableMeta*>* AllocateATableMetaVector();
        void PrintOtherInfo();
        void ReleaseSpaceAllocated();
        ///key analysis
        uint64_t num_key_generate_;

        uint64_t num_unique_key_generate_;

        uint64_t num_repetite_key_generate_;

        std::unordered_map<std::string,int>
            map_generate_key_num_{};      ///存储负载生成的key数目


        std::unordered_map<uint64_t ,std::vector<BlockAccessInfo*>*> files_IO_Info_;

         ///dbimp analysis
         uint64_t num_put_call_wr;
         uint64_t num_put__wr;
         /**
         uint64_t num_put_call_wr;
         uint64_t num_put_call_;
         uint64_t num_put_call_;
         uint64_t num_put_call_;
         uint64_t num_put_call_;
         uint64_t num_put_call_;
         **/


         ///block analysis

         uint64_t num_key_put_into_batch_;

         int num_key_insert_into_batch_;
         int num_memtable_created_;
         int num_key_kTypeValue_be_put_into_mem_;

         int num_key_kTypeDeletion_be_put_into_mem_;

         ////文件写入l0层数量统计
         int num_flush_imm_into_below_;



         ///写人每层的文件数目记录
         std::vector<int> vector_files_id_be_writen_into_L0;
         std::vector<int> vector_files_id_be_writen_into_L1;
         std::vector<int> vector_files_id_be_writen_into_L2;
         std::vector<int> vector_files_id_be_writen_into_L3;
         std::vector<int> vector_files_id_be_writen_into_L4;
         std::vector<int> vector_files_id_be_writen_into_L5;
         std::vector<int> vector_files_id_be_writen_into_L6;

         ///读每层的文件数目记录
         std::vector<int> vector_files_id_be_read_from_L0;

         std::vector<int> vector_files_id_be_read_from_L1;
         std::vector<int> vector_files_id_be_read_from_L2;
         std::vector<int> vector_files_id_be_read_from_L3;
         std::vector<int> vector_files_id_be_read_from_L4;
         std::vector<int> vector_files_id_be_read_from_L5;
         std::vector<int> vector_files_id_be_read_from_L6;
         uint64_t num_0_total_;
         uint64_t num_1_total_;
         uint64_t num_2_total_;
         uint64_t num_3_total_;
         uint64_t num_4_total_;
         uint64_t num_5_total_;
         uint64_t num_6_total_;




         ///写入文件信息
         std::vector<TableMeta*> files_write_into_L0;
         std::vector<TableMeta*> files_write_into_L1;
         std::vector<TableMeta*> files_write_into_L2;
         std::vector<TableMeta*> files_write_into_L3;
         std::vector<TableMeta*> files_write_into_L4;
         std::vector<TableMeta*> files_write_into_L5;
         std::vector<TableMeta*> files_write_into_L6;
         //std::unordered_map<uint64_t,std::vector<BlockAccessInfo>*> map_files_blocks_info_be_read_from_l0_for_compaction_;
         double aver_num_files_from_L0_when_get_key_;
         double aver_num_files_from_L1_when_get_key_ ;
         double aver_num_files_from_L2_when_get_key_ ;
         double aver_num_files_from_L3_when_get_key_ ;
         double aver_num_files_from_L4_when_get_key_ ;
         double aver_num_files_from_L5_when_get_key_ ;
         double aver_num_files_from_L6_when_get_key_ ;



         std::vector<TableMeta*> files_access_with_below_record;



         uint64_t num_build_table_;

         int num_build_table_failed_;
         int num_key_get_;
         std::unordered_map<std::string,uint64_t> map_get_key_num_;
         //////////////////读取的key统计
         void InsertGetKey(std::string s_key);
         int num_get_key_function_called_;
         int num_get_key_hit_mem_;
         int num_get_key_hit_imm_;
         int num_get_key_hit_current_version_;
         int num_call_background_compaction_;
         int num_call_mamual_compaction_;
         int num_call_backgound_auto_compaction_;
         std::vector<std::vector<TableMeta*>*> file_fds_pick_every_background_compaction_;
         void  UpdateAverageLevelFileNum(int new_files_num, int level);
         void  ReleaseSpaceAlloctedForPickFilesDuringCompaction();

         uint8_t num_compaction_;
         std::vector<MyCompactionStat*> compacts_stats;

         MyCompactionStat* AllocateAMyCompactionStat();
         std::vector<BlockAccessInfo*>* AllocateABlockAccessInfoVector();
         void PrintCompactionInfo();

         int num_key_hit_from_table_cache_;
         int num_key_hit_from_table_IO_;
         int num_keys_not_found_from_filter_block_;
         int num_keys_get_from_data_block_;


         void SetZeroToGenerateKey();
         void OverWriteInsertKey(const std::string& s_key);
};






} /* namespace leveldb */

#endif /* DB_MYSTATS_H_ */
