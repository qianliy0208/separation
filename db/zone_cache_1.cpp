//
// Created by yql on 5/13/23.
//
//
//
/*
#include "zone_cache_1.h"

namespace leveldb{


    ZoneInfo::ZoneInfo(uint64_t zone_num){
        zone_num = zone_num;
        access_num = 1;
    }

    ZoneCache::ZoneCache():current_capcity(0) {

    }

    ZoneCache::~ZoneCache() {
        for(auto zi:zone_cache_) {
            delete zi;
        }
    }




    ZoneInfo* ZoneCache::LookUp(uint64_t num_zone,int type) {

        switch (type) {
            case 0:

                for (ZoneInfo *zi: cold_zone_cache_) {
                    if (zi->zone_num == num_zone) {
                        return zi;
                    }
                }

                return nullptr;


            case 1:

                for (ZoneInfo *zi: hot_zone_cache_) {
                    if (zi->zone_num == num_zone) {
                        return zi;
                    }
                }

                return nullptr;


        }

    }
    //当生成一个新key分配zone时，查找zoneinfo，查到将zone访问指针加一，查不到则生成一新的zoneinfo.f放入队列尾部
    void ZoneCache::Insert(uint64_t num_zone,int type){  ///i 等于 1 是 热  = 0 是 冷

        ///只实现热的，暂时不实现冷

        ZoneInfo* zi = LookUp(num_zone,1);
        if(zi){

            zi->access_num ++ ;

        }
        else{
            ZoneInfo* zi = new ZoneInfo(num_zone);

            hot_zone_cache_.push_back(zi);

        }

            current_capcity ++;

    }

     void ZoneCache::Delete(uint64_t zone_num, int type) {

        switch(type){
            case 0 :


            break;



            case 1:


                if(LookUp(zone_num,type)){


                }



                break;

        }

    }



}
*/

