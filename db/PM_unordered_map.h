//
// Created by jxx on 4/2/24.
//

#ifndef WISCKEY_SEPARATEKVSTORAGE_MASTER_PM_UNORDERED_MAP_H
#define WISCKEY_SEPARATEKVSTORAGE_MASTER_PM_UNORDERED_MAP_H

#include <iostream>
#include <libpmem.h>
#include <unordered_map>
#include <libpmemobj.h>
#include <libpmemobj/pool.h>
#include <libpmemobj/base.h>
// 定义在PMEM中存储的unordered_map类型

class PMUnorderedMap {

private :
    void * fd;
    size_t map_size;
    // PM中存储的unordered_map;
    std::unordered_map<std::string,uint64_t>* pm_map_ptr;

public:

    PMUnorderedMap() ;
    ~PMUnorderedMap() ;
    /*
     *
     *  插入函数 ：实现insert
     *
     */
    void insert(const std::string& key, uint64_t &zone) ;

    /*
     *
     *  删除函数：实现erase
     *
     */

    void emplace(const std::string& key, uint64_t &zone);

    void erase(const std::string& key);
    /*
     *
     *  获取函数：实现[]
     */
    uint64_t operator[](const std::string& key);

    void printPmMap() const;


};





#endif //WISCKEY_SEPARATEKVSTORAGE_MASTER_PM_UNORDERED_MAP_H
