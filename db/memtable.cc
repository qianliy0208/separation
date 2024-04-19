// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.


#include <iostream>
#include <cinttypes>
#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include "PM_unordered_map.h"
namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

///////////////////////////////////////
/*MemTable::MemTable(const InternalKeyComparator& cmp,
                   ZoneNumber *z,
                   uint64_t *zs,
                   ZoneNumber *rz,
                   uint64_t *rzs,
                   std::unordered_map<std::string, ZoneNumber> *kzm)
    : comparator_(cmp),
      refs_(0),
      table_(comparator_, &arena_),
      current_zone_(z), 
      current_zone_size_(zs),
      current_reserved_zone_(rz),
      current_reserved_zone_size_(rzs),
      key_zone_map_(kzm) {
}*/
//////////////////////////////////////////////////////////////////////////////
MemTable::MemTable(const InternalKeyComparator& cmp,
                   ZoneNumber *z,
                   uint64_t *zs,
                   ZoneNumber *rz,
                   uint64_t *rzs,
                   PMUnorderedMap *kzm)
    : comparator_(cmp),
      refs_(0),
      table_(comparator_, &arena_),
      current_zone_(z),
      current_zone_size_(zs),
      current_reserved_zone_(rz),
      current_reserved_zone_size_(rzs),
      key_zone_map_(kzm) {
}
//////////////////////////////////////////////////////////////////////////////
MemTable::MemTable(const InternalKeyComparator& cmp,
                   ZoneNumber *z,
                   uint64_t *zs,
                   PMUnorderedMap *kzm)
    : comparator_(cmp),
      refs_(0),
      table_(comparator_, &arena_),
      current_zone_(z),
      current_zone_size_(zs),
      key_zone_map_(kzm) {
}
///////////////////////////////////////

MemTable::~MemTable() {
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator: public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) { }

  virtual bool Valid() const { return iter_.Valid(); }
  virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void Prev() { iter_.Prev(); }
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;       // For passing to EncodeKey

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

Iterator* MemTable::NewIterator() {
  return new MemTableIterator(&table_);
}

/*static std::string EncodeCharToBinary(char* c) {

    char bina[9];
    uint8_t * temp_num = reinterpret_cast<uint8_t*>(c);
    uint8_t place = 8;
    bina[place] = '\0';
    while(place>0){
        if((*temp_num)%2){
            bina[--place] ='1';
        }
        else {
            bina[--place] = '0';
        }
        *temp_num = (*temp_num)/2;
    }
    std::string result(bina);
    return result;
}

static std::string GetBinaryOfString(char *src,int len) {
    assert(len>0);
    std::string result;
    unsigned int has_done = 0;
    while(has_done<len){
        result.append(EncodeCharToBinary(src+has_done));
        has_done++;
    }
    return result;
}*/
/*
 *  热 0
 *  冷 1
 */
void MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& value,
                   const ZoneNumber zone,
                   HotType ht) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_zone_key.size()
  //  key bytes    : char[internal_zone_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 16;



  const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size;
  char* buf = arena_.Allocate(encoded_len);

  char *p = EncodeVarint32(buf, internal_key_size);
  //EncodeFixed64Big(p,zone);
  snprintf(p,9,"%08d",zone);
  p += 8;
  memcpy(p, key.data(), key_size);
  //std::cout << "16B key内容字符串：\t" << GetBinaryOfString(p,key_size) << std::endl;
 // std::cout << "追加zone序列号之后的字符串：\t" << GetBinaryOfString(buf,25) << std::endl;
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  //std::cout << "追加序列号（7B）+类型（1B）之后的字符串(即最终的key部分)：\t" << GetBinaryOfString(buf,33) << std::endl;

  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert((p + val_size) - buf == encoded_len);

  table_.Insert(buf);

  /*/// 冷熱佔比測試
  static uint64_t num_written_hot = 0;
  static uint64_t num_written_cold = 0;
  static uint64_t num_written_warm= 0;
  static uint64_t num_written = 0;
  num_written ++;
  switch (ht)
  {
      case kHotFirst: num_written_warm++;break;
      case kCold: num_written_cold++;break;
      case kHot:num_written_hot++;break;
      default:break;
  }

  if(num_written%1000000 == 0){
      num_written = 0;
      std::cout << "\nnum_written_hot " << num_written_hot << std::endl;
      std::cout << "num_written_cold " << num_written_cold << std::endl;
      std::cout << "num_written_warm " << num_written_warm << std::endl;
      std::cout << "(double)num_written_hot_mem/cold_mem " << (double)(num_written_warm+num_written_hot)/num_written_cold << std::endl;
  }*/
  switch (ht)
  {
  case kHot:
    break;
  case kCold:
    *current_zone_size_ += encoded_len;
    if (*current_zone_size_ > config::kMaxZoneSize) {
      *current_zone_ += 1;
      *current_zone_size_ = 0;
    }
    break;
  default:
    break;
  }
}

///////////////////////////////////////
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s, FG_Stats* fg_stats) {

  //// <fg_stats_>
  uint64_t mg_start = Env::Default()->NowMicros();
  fg_stats->mem_get_count += 1;

  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8),
            key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());

          //// <fg_stats_>
          fg_stats->mem_get_time += Env::Default()->NowMicros() - mg_start;

          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());

          //// <fg_stats_>
          fg_stats->mem_get_time += Env::Default()->NowMicros() - mg_start;

          return true;
      }
    }
  }

  //// <fg_stats_>
  fg_stats->mem_get_time += Env::Default()->NowMicros() - mg_start;
  
  return false;
}
///////////////////////////////////////

}  // namespace leveldb
