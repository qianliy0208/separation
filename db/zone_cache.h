//
// Created by yql on 5/13/23.
//
/*

#ifndef STRAIGHT_KEY_SEPARATE_2023_03_20_ZONE_CACHE_H
#define STRAIGHT_KEY_SEPARATE_2023_03_20_ZONE_CACHE_H

#pragma once

#include "include/leveldb/cache.h"
#include "include/leveldb/slice.h"

namespace leveldb {




    struct ZoneInfo{
        uint32_t zone_num;
        uint8_t num_access;
    };

   */
/* void DeleteZoneInfo(const Slice& key, void* value){
        ZoneInfo* zi = reinterpret_cast<ZoneInfo*>(value);
        delete zi->num_access;
        delete zi->zone_num;
        delete zi;
    }*//*


    class ZoneCache {
    public:
        ZoneCache(uint32_t max_zone_num);
        ~ZoneCache();

        //插入缓存实现
        void Insert(uint32_t zone_num);

        //删除缓存实现
        void Delete(uint32_t zone_num);


        ZoneInfo* LookUp(uint32_t zone_num);


    private:

        Cache* cache_;
        int max_capacity;
        int cur_used;

    };


}  // namespace leveldb




#endif //STRAIGHT_KEY_SEPARATE_2023_03_20_ZONE_CACHE_H


*/



