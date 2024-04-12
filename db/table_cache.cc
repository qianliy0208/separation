// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"


namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       int entries,
                       FG_Stats* fg_stats)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)), 
      fg_stats_(fg_stats) {
}

TableCache::~TableCache() {
  delete cache_;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle,int type) {
  //// <fg_stats>
  uint64_t ft_start = env_->NowMicros();
  
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  fg_stats_->table_cache_search_num++;
    std::string fname;
  if (*handle == NULL) {                        // 缓存空 创建 打开新文件
    if(!type) {
        fname = TableFileName(dbname_, file_number);
    }
     else {
         fname = TableFileName("/mnt/pmemdir/logdir", file_number);    // PM
     }

    RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);  // 沒有。不用
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;            // 将 内存表 和 随机获取文件 指针 插入 文件缓存
      table->SetFGStats(fg_stats_);
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }

  //// <fg_stats>
  fg_stats_->find_table_time += env_->NowMicros() - ft_start;
  
  return s;
}
// 壓縮時創建迭代器
Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr,uint32_t type) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle,type);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       int level, 
                       uint64_t file_number,
                       uint64_t file_size,
                       std::vector<uint8_t>* fmd_stage, 
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&),int type) {
  
  //// <fg_stats_>
  fg_stats_->table_cache_get_count += 1;
  uint64_t tcg_start = Env::Default()->NowMicros();
  
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle,type);              // 打開放入緩存
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, level, fmd_stage, k, arg, saver);          // 內部讀取
    cache_->Release(handle);
  }

  //// <fg_stats_>
  fg_stats_->table_cache_get_time += Env::Default()->NowMicros() - tcg_start;
  
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
