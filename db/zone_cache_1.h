//
// Created by yql on 5/13/23.
//

/*
#ifndef STRAIGHT_KEY_SEPARATE_2023_03_20_ZONE_CACHE_1_H
#define STRAIGHT_KEY_SEPARATE_2023_03_20_ZONE_CACHE_1_H

#include <stdint.h>
#include <list>

namespace leveldb {


    ///实现队列：  热：100 101  102    冷编号：100 1

    struct ZoneInfo{
        ZoneInfo(uint64_t zone_num);
        uint64_t zone_num;
        uint64_t access_num;
    };


    class ZoneCache {
    public:
        ZoneCache();
        ~ZoneCache();


        ///插入：
        void Insert(uint64_t zone_num,int type);


        ///获得指针
        ZoneInfo* LookUp(uint64_t zone_num,int type);


        ///删除：
        void Delete(uint64_t zone_num,int type);









        std::list<ZoneInfo*> cold_zone_cache_;
        std::list<ZoneInfo*> hot_zone_cache_;

        static const uint64_t MAX_CAPCITY = 2500000;

        uint64_t current_capcity;





    }






}




#endif //STRAIGHT_KEY_SEPARATE_2023_03_20_ZONE_CACHE_1_H
*/
