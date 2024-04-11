//
// Created by jxx on 4/2/24.
//

#include <csignal>
#include <fstream>
#include <cstdint>
#include "PM_unordered_map.h"

PMUnorderedMap::PMUnorderedMap() {
    const char* pmem_file = "/mnt/pmemdir/mymemory"; // 替换为实际的 PMem 文件路径
    map_size =  1024 * 1024 * 1024; // 2GB，根据需求调整大小

// 创建或打开 PMem 文件
    fd = pmem_map_file(pmem_file, map_size, PMEM_FILE_CREATE, 0666, nullptr, nullptr);
    if (fd == nullptr) {
        std::cerr << "Failed to create or open PMem file" << std::endl;
    }

    // 在 PMem 上创建 unordered_map
    pm_map_ptr  = new(fd) std::unordered_map<std::string, uint64_t>;

}

void PMUnorderedMap::printPmMap() const {// 打印 unordered_map 中的数据
    for (const auto& entry : *pm_map_ptr) {
        std::cout << entry.first << ": " << entry.second << std::endl;
    }
}

PMUnorderedMap::~PMUnorderedMap() {
        // 解除映射并关闭 PMem 文件
        pmem_unmap(fd, map_size);
        // 销毁 unordered_map 对象
        pm_map_ptr->~unordered_map();
}

void PMUnorderedMap::insert(const std::string &key, uint64_t &value) {

        // 插入键值对
        (*pm_map_ptr)[key] = value;

}
void PMUnorderedMap::emplace(const std::string &key, uint64_t &value) {
    // 插入键值对
    (*pm_map_ptr)[key] = value;

}

void PMUnorderedMap::erase(const std::string &key) {

        // 删除键值对
        pm_map_ptr->erase(key);

}
uint64_t PMUnorderedMap::operator[](const std::string &key) {

        // 获取值
        auto it = pm_map_ptr->find(key);
        if (it != pm_map_ptr->end()) {
            return it->second;
        }
        return INT64_MAX;  // 返回一個最大，表示沒有

}
