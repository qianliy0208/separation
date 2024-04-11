//
// Created by yql on 5/13/23.
//


/*

#include "zone_cache.h"


namespace leveldb {

 ZoneCache::ZoneCache(uint32_t max_zone_num):cache_(NewLRUCache(max_zone_num)),cur_used(0),max_capacity(max_zone_num){
 }

 void ZoneCache::Insert(uint32_t zone_num) {

     Slice key(std::to_string(zone_num));
     ZoneInfo zoneInfo;
     zoneInfo.zone_num = zone_num;
     zoneInfo.num_access = 0;
     cache_->Insert(key,&zoneInfo,1,0);
     cur_used ++;
 }

 void ZoneCache::Delete(uint32_t zone_num) {
     Slice key(std::to_string(zone_num));
     auto handle = cache_->Lookup(key);
     if(handle){
         cache_->Release(handle);
         cur_used --;
     }

     else {
         fprintf(stdout,"zone缓存没有找到%d",zone_num);
     }

 }

 ZoneInfo* ZoneCache::LookUp(uint32_t zone_num){

     Slice key(std::to_string(zone_num));
     Cache::Handle* handle = cache_->Lookup(key);
     if(handle)
     {
         auto zi = cache_->Value(handle);
         return (ZoneInfo*)(zi);
     }
     else
        return nullptr;
 }



 ZoneCache::~ZoneCache() {



 }
}*/
