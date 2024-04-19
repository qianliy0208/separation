// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

#include <string>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/skiplist.h"
#include "util/arena.h"
#include "PM_unordered_map.h"

///////////////////////////////////////
#include "util/fg_stats.h"
#include <unordered_map>
///////////////////////////////////////

namespace leveldb {

class InternalKeyComparator;
class Mutex;
class MemTableIterator;

class MemTable {
 public:
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  ///////////////////////////////////////
  explicit MemTable(const InternalKeyComparator& comparator, ZoneNumber *zone, uint64_t *zone_size, 
                    ZoneNumber *reserved_zone, uint64_t *reserved_zone_size, PMUnorderedMap *zone_map);
  //////////////////////////////////////////////////////////////////////////////
 explicit MemTable(const InternalKeyComparator& comparator, ZoneNumber *zone, uint64_t *zone_size,
                    PMUnorderedMap *zone_map);
  //////////////////////////////////////////////////////////////////////////////
 /* explicit MemTable(const InternalKeyComparator &comparator, ZoneNumber *zone, uint64_t *zone_size,
                    ZoneNumber *reserved_zone, uint64_t *reserved_zone_size,
                    std::unordered_map <std::string, ZoneNumber> *zone_map);*/
  ///////////////////////////////////////

  // Increase reference count.
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTable is being modified.
  size_t ApproximateMemoryUsage();

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator();

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type,
           const Slice& key,
           const Slice& value,
           const ZoneNumber zone,
           HotType ht);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  ///////////////////////////////////////
  bool Get(const LookupKey& key, std::string* value, Status* s, FG_Stats* fg_stats);
  ///////////////////////////////////////

 private:
  ~MemTable();  // Private since only Unref() should be used to delete it

  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
    int operator()(const char* a, const char* b) const;
  };
  friend class MemTableIterator;
  friend class MemTableBackwardIterator;

  typedef SkipList<const char*, KeyComparator> Table;

  KeyComparator comparator_;
  int refs_;
  Arena arena_;
  Table table_;

  ///////////////////////////////////////
  ZoneNumber *current_zone_;
  uint64_t *current_zone_size_;
  ZoneNumber *current_reserved_zone_;
  uint64_t *current_reserved_zone_size_;
 // std::unordered_map<std::string, ZoneNumber> *key_zone_map_;
  PMUnorderedMap *key_zone_map_;
  ///////////////////////////////////////

  // No copying allowed
  MemTable(const MemTable&);
  void operator=(const MemTable&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
