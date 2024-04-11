//
// Created by jxx on 2/21/24.
//

#include "ColdFiles.h"

namespace MyNameSpace{
#define MAX_FILE_SIZE (67108864)     // 默认64MB
#define OK 0;
char write_buff[8096] = {0,};
    ColdFiles::ColdFiles() {
        CreateNewFile();
    }

    bool ColdFiles::ExistKey(const std::string &key) {
        return keys_map_.find(key) != keys_map_.end();
    }
    void ColdFiles::PutIntoFiles(uint32_t file_num,ColdFileInfo& cfi) {
        files_[file_num] = cfi;
    }

   uint32_t ColdFiles::GetNextFileNumber() {
       return next_file_number.load();
   }
   uint32_t ColdFiles::GetNewFileNumber() {
        uint32_t new_fn = next_file_number.fetch_add(1);
        return new_fn;
    }
   void ColdFiles::CloseCurrentFile() {
       assert(current_file);
       fclose(current_file);
      // current_file_size = 0;
       files_[next_file_number - 1].total_size = current_file_size; //记录生成时的文件大小
       files_[next_file_number - 1].vaild_size = current_file_size; //记录生成时的文件大小
   }
   void ColdFiles::CreateNewFile() {
       current_file = fopen(file_path.append(std::to_string(GetNewFileNumber())).c_str(),"a+");
       current_file_size = 0;
       ColdFileInfo cfi;
       PutIntoFiles(GetNextFileNumber() - 1,cfi);  // 将新文件信息加入文件中
   }
    int ColdFiles::WriteRecord(const char* record,int size,uint32_t* offset) {
        assert(current_file != nullptr);
        if(current_file_size >= MAX_FILE_SIZE) { // 写满换新
            CloseCurrentFile();
            CreateNewFile();
        }
        // 开写 :从 大小 多少 到
        *offset = current_file_size;
        fwrite(record,1,size,current_file);
        current_file_size += size;
        return OK;
    }
int  ColdFiles::PutKey(const std::string& key,const std::string& value) {
    // 直接写，存储 key_size+value_size+key+value信息
    // 大小
    int k_s = key.size();
    int v_s = value.size();
    uint32_t t_s = 8 + k_s + v_s; // 总大小
    // 打包
    char size_buff[8];
    snprintf(size_buff,sizeof(size_buff),"%04d",k_s);
    memcpy(write_buff,&size_buff,4);
    snprintf(size_buff,sizeof(size_buff),"%04d",v_s);
    memcpy(write_buff + 4,&size_buff,4);
    memcpy(write_buff + 8,key.data(),k_s);
    memcpy(write_buff + 8 + k_s,value.data(),v_s);
    // 写入
    uint32_t offset;
    WriteRecord(write_buff,t_s,&offset);
    uint32_t cur_file = GetNextFileNumber() - 1;
    // 记入写入映射表
    keys_map_[key] = FilePos{cur_file,offset,t_s};

    return OK;
    }
    bool ColdFiles::GetKey(const std::string& key,std::string* value) {

        // 查找映射表
        auto iter = keys_map_.find(key);
        if(iter == keys_map_.end()) {
            return false; // 未找到
        }
        FilePos filePos = keys_map_[key]; // 获取位置

        // 读取 todo:加读取缓存
        FILE* file = fopen(file_path.append(std::to_string(filePos.file_number)).c_str(),"r");
        char read_buff[8092];
        fseek(file,filePos.file_offset,0);
        int read = fread(read_buff,1,filePos.file_length,file);
        assert(read == filePos.file_length);
        value->assign(read_buff,read);
        fclose(file);
        return true;
    }
}
