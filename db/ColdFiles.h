//
// Created by jxx on 2/21/24.
//

#ifndef STRAIGHT_KEY_SEPARATE_2023_03_20_COPY_COLDFILES_H
#define STRAIGHT_KEY_SEPARATE_2023_03_20_COPY_COLDFILES_H

#include <string.h>
#include <atomic>
#include <string>
#include <unordered_map>
#include <memory>
#include <stdio.h>
#include <assert.h>
namespace MyNameSpace {



// 文件元数据管理，垃圾回收
struct ColdFileInfo {
    uint64_t vaild_size;
    uint64_t total_size;
};

class ColdFiles {
public:
    ColdFiles();

    bool ExistKey(const std::string& key);

    // 写key 未存在写入，已经存在不写
    int PutKey(const std::string& key,const std::string& value);

    // 读key
    bool GetKey(const std::string& key,std::string* value);

private:
   void PutIntoFiles(uint32_t file_num,ColdFileInfo& cfi);
    void CloseCurrentFile();
    void CreateNewFile();
    uint32_t GetNextFileNumber();
    uint32_t GetNewFileNumber();

    // 写入文件
    int WriteRecord(const char* slice,int size,uint32_t* offset);


    std::string file_path = "/tmp/cold_files/";
    // 已有文件
    std::unordered_map<uint32_t/*filenumber*/,ColdFileInfo> files_;

    // 当前文件
    FILE* current_file = nullptr;
    // 当前文件大小
    uint64_t current_file_size = 0;

    // 文件序号
    std::atomic<uint32_t> next_file_number{0};   //todo:读文件获取
    struct FilePos {
        uint32_t file_number;
        uint32_t file_offset;
        uint32_t file_length;
    };
    // 存储键值对对应位置
    std::unordered_map<std::string,FilePos> keys_map_;

    // 获取键存在filepos中
    bool GetKeyPos(const std::string& key,FilePos& filepos);

};
}
#endif //STRAIGHT_KEY_SEPARATE_2023_03_20_COPY_COLDFILES_H
