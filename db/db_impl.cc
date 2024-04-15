// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include <iostream>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "PM_unordered_map.h"
namespace leveldb {


const int kNumNonTableCacheFiles = 10;

///////////////////////////////////////
  // 存储域
  ZoneNumber cur_zone_ = config::kMaxReservedZoneNumber + 1;
  uint64_t cur_zone_size_ = 0;
  ZoneNumber cur_reserved_zone_ = 1;
  uint64_t cur_reserved_zone_size_ = 0;
  //std::unordered_map<std::string, ZoneNumber> key_zone_map_;
  PMUnorderedMap key_zone_map_;
///////////////////////////////////////

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // 输出文件结构体
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
    uint32_t  type;
    std::vector<uint8_t> fmd_stage;

    Output() { 
      fmd_stage.push_back(0);
    }
    Output(const Output& outp) : number(outp.number), file_size(outp.file_size), 
            smallest(outp.smallest), largest(outp.largest),type(outp.type) {
      fmd_stage.assign(outp.fmd_stage.begin(), outp.fmd_stage.end());
    }
  };
  std::vector<Output> outputs;

  // 书写器和构造器
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  // 当前输出
  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(NULL),
        builder(NULL),
        total_bytes(0) {
  }
};

// Fix user-supplied options to be reasonable   【  | 】
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);   //限定属性范围
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.max_file_size,     1<<20,                       1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db           // 创建一个信息日志
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) {
    result.block_cache = NewLRUCache(8 << 20);    // 块缓存LRU缓存 8MB
  }
  return result;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      colddbname_("/tmp/cold"),            // 初始化db日誌文件路徑 爲PM
     //dblogdir_(dbname),            // 初始化db日誌文件路徑 爲PM
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      mem_(NULL),
      imm_(NULL),
      ///////////////////////////////////////
      hot_mem_(NULL),
      hot_imm_(NULL),
      ///////////////////////////////////////
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      seed_(0),
      // ///////////////////////////////////////
      // cur_zone_(config::kMaxReservedZoneNumber + 1),
      // cur_zone_size_(0),
      // cur_reserved_zone_(1),
      // cur_reserved_zone_size_(0),
      // ///////////////////////////////////////
      tmp_batch_(new WriteBatch),
      bg_compaction_scheduled_(false),
      manual_compaction_(NULL), 
      ////
      fg_stats_(new FG_Stats())
      ////
       {
  has_imm_.Release_Store(NULL);
  ///////////////////////////////////////
  // key_zone_map_.reserve(10000000);
  ///////////////////////////////////////

  // Reserve ten files or so for other uses and give the rest to TableCache.
  const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
  table_cache_ = new TableCache(dbname_, &options_, table_cache_size, fg_stats_);

  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             &internal_comparator_, fg_stats_);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  /////////////////////////////////////
  if (hot_mem_ != NULL) hot_mem_->Unref();
  if (hot_imm_ != NULL) hot_imm_->Unref();
  /////////////////////////////////////
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}
// 刪除過期文件
void DBImpl::DeleteObsoleteFiles() {
        if (!bg_error_.ok()) {
            // After a background error, we don't know whether a new version may
            // or may not have been committed, so we cannot safely garbage collect.
            return;
        }

        // Make a set of all of the live files
        std::set<uint64_t> live = pending_outputs_;           // 追加到版本
        versions_->AddLiveFiles(&live);

        std::vector<std::string> filenames;
        env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
        uint64_t number;
        FileType type;
        for (size_t i = 0; i < filenames.size(); i++) {
            if (ParseFileName(filenames[i], &number, &type)) {
                bool keep = true;
                switch (type) {
                    case kLogFile:
                        keep = ((number >= versions_->LogNumber()) ||
                                (number == versions_->PrevLogNumber()));
                        break;
                    case kDescriptorFile:
                        // Keep my manifest file, and any newer incarnations'
                        // (in case there is a race that allows other incarnations)
                        keep = (number >= versions_->ManifestFileNumber());
                        break;
                    case kTableFile:
                        keep = (live.find(number) != live.end());
                        break;
                    case kTempFile:
                        // Any temp files that are currently being written to must
                        // be recorded in pending_outputs_, which is inserted into "live"
                        keep = (live.find(number) != live.end());
                        break;
                    case kCurrentFile:
                    case kDBLockFile:
                    case kInfoLogFile:
                        keep = true;
                        break;
                }

                if (!keep) {
                    if (type == kTableFile) {
                        table_cache_->Evict(number);
                    }
                    Log(options_.info_log, "Delete type=%d #%lld\n",
                        int(type),
                        static_cast<unsigned long long>(number));
                    env_->DeleteFile(dbname_ + "/" + filenames[i]);
                }
            }
        }
        // 暫時不加
    }

    /*// 刪除過期文件
void DBImpl::DeleteObsoleteFiles() {
        if (!bg_error_.ok()) {
            // After a background error, we don't know whether a new version may
            // or may not have been committed, so we cannot safely garbage collect.
            return;
        }

        // Make a set of all of the live files
        std::set<uint64_t> live = pending_outputs_;           // 追加到版本
        versions_->AddLiveFiles(&live);

        std::vector<std::string> filenames;
        env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
        uint64_t number;
        FileType type;
        for (size_t i = 0; i < filenames.size(); i++) {
            if (ParseFileName(filenames[i], &number, &type)) {
                bool keep = true;
                switch (type) {
                    case kLogFile:
                        keep = ((number >= versions_->LogNumber()) ||
                                (number == versions_->PrevLogNumber()));
                        break;
                    case kDescriptorFile:
                        // Keep my manifest file, and any newer incarnations'
                        // (in case there is a race that allows other incarnations)
                        keep = (number >= versions_->ManifestFileNumber());
                        break;
                    case kTableFile:
                        keep = (live.find(number) != live.end());
                        break;
                    case kTempFile:
                        // Any temp files that are currently being written to must
                        // be recorded in pending_outputs_, which is inserted into "live"
                        keep = (live.find(number) != live.end());
                        break;
                    case kCurrentFile:
                    case kDBLockFile:
                    case kInfoLogFile:
                        keep = true;
                        break;
                }

                if (!keep) {
                    if (type == kTableFile) {
                        table_cache_->Evict(number);
                    }
                    Log(options_.info_log, "Delete type=%d #%lld\n",
                        int(type),
                        static_cast<unsigned long long>(number));
                    env_->DeleteFile(dbname_ + "/" + filenames[i]);
                }
            }
        }
        // Log日誌文件從PM中刪除
        std::vector<std::string> filenames1;
        env_->GetChildren(dblogdir_, &filenames1);
        for (size_t i = 0; i < filenames1.size(); i++) {
            if (ParseFileName(filenames1[i], &number, &type)) {
                bool keep = true;
                switch (type) {
                    case kLogFile:
                        keep = ((number >= versions_->LogNumber()) ||
                                (number == versions_->PrevLogNumber()));
                        break;
                    default:
                        keep = false;
                        break;
                }
                if (!keep) {
                    Log(options_.info_log, "Delete type=%d #%lld\n",
                        int(type),
                        static_cast<unsigned long long>(number));
                    env_->DeleteFile(dblogdir_ + "/" + filenames1[i]);
                }
            }

        }
    }*/
Status DBImpl::Recover(VersionEdit* edit, bool *save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    snprintf(buf, sizeof(buf), "%d missing files; e.g.",
             static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);
    ///恢复
    if (mem == NULL) {
      ///////////////////////////////////////
      mem = new MemTable(internal_comparator_, &cur_zone_, &cur_zone_size_, &cur_reserved_zone_, &cur_reserved_zone_size_, &key_zone_map_);
      ///////////////////////////////////////
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, hot_mem_, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, NULL);
      mem->Unref();
      mem = NULL;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == NULL);
    assert(log_ == NULL);
    assert(mem_ == NULL);
    ///////////////////////////////////////
    assert(hot_mem_ == NULL);
    ///////////////////////////////////////
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != NULL) {
        mem_ = mem;
        mem = NULL;
      } else {
        // mem can be NULL if lognum exists but was empty.
        ///////////////////////////////////////
        mem_ = new MemTable(internal_comparator_, &cur_zone_, &cur_zone_size_, &cur_reserved_zone_, &cur_reserved_zone_size_, &key_zone_map_);
        ///////////////////////////////////////
        mem_->Ref();
      }
    }
  }

  if (mem != NULL) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, NULL);
    }
    mem->Unref();
  }

  return status;
}

// 寫L0層
Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base,uint32_t hot) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;                                           // 文件元數據


  meta.number = versions_->NewFileNumber();

  meta.type = hot;

  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);


  Status s;
  {
    mutex_.Unlock();
        if(hot) {
            s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta, fg_stats_);
        } else {
            s = BuildTable(colddbname_, env_, options_, table_cache_, iter, &meta, fg_stats_);
        }
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);


  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != NULL) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest,hot);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}


void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != NULL);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  uint64_t p_1 = env_->NowMicros();
  Status s = WriteLevel0Table(imm_, &edit, base,0);     // 寫當前imm_到SST  // 冷文件
    fg_stats_->flush_memtable_time += env_->NowMicros() - p_1;
    fg_stats_->flush_memtable_count += 1;
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
    DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactHotMemTable() {
  mutex_.AssertHeld();
  assert(hot_imm_ != NULL);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  uint64_t p_1 = clock();
  Status s = WriteLevel0Table(hot_imm_, &edit, base,1);       // 熱文件
  fg_stats_->flush_memtable_time += env_->NowMicros() - p_1;
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    hot_imm_->Unref();
    hot_imm_ = NULL;
    has_hot_imm_.Release_Store(NULL);
    DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

///////////////////////////////////////
void DBImpl::PrintMyStats() { 
  fprintf(stderr, "总写入数据量:\t %lld \n", static_cast<unsigned long long>(fg_stats_->fg_total_write));

  Slice token = "leveldb.stats";
  std::string stats;
  GetProperty(token, &stats);
  fprintf(stderr, "\n%s\n", stats.c_str());

  fprintf(stderr, "Minor Compaction次数\t: %lld \n", static_cast<unsigned long long>(
                fg_stats_->minor_compaction_count));
  fprintf(stderr, "Minor Compaction时间\t: %f \n\n", (
                  (fg_stats_->minor_compaction_time) * 1e-6));

  fprintf(stderr, "macro Compaction次数\t: %lld \n", static_cast<unsigned long long>(fg_stats_->l0_doc_count));
  fprintf(stderr, "macro Compaction时间\t: %f \n\n", (fg_stats_->l0_doc_time * 1e-6));

  fprintf(stderr, "Pick Compaction次數\t: %d \n\n", (fg_stats_->pick_compaction_count));
  fprintf(stderr, "Pick Compaction时间\t: %f \n\n", (fg_stats_->pick_compaction_time * 1e-6));

  fprintf(stderr, "Memtable Get次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->mem_get_count));
  fprintf(stderr, "Memtable Get时间:\t %f \n", (fg_stats_->mem_get_time * 1e-6));
  fprintf(stderr, "读取文件缓存总次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->table_cache_get_count));
  fprintf(stderr, "TableCache Get时间:\t %f \n\n", (fg_stats_->table_cache_get_time * 1e-6));
  fprintf(stderr, "TableCache 查询总次数：\t %f \n\n", (fg_stats_->table_cache_get_time * 1e-6));
  fprintf(stderr, "查找Index时间:\t %f \n", (fg_stats_->find_index_key_time * 1e-6));
  fprintf(stderr, "FindTable时间:\t %f \n", (fg_stats_->find_table_time * 1e-6));
  fprintf(stderr, "InternalGet时间:\t %f \n", (fg_stats_->internal_get_time * 1e-6));
  fprintf(stderr, "InternalGet中BlockReader时间:\t %f \n", (fg_stats_->internal_get_seek_data_block_time * 1e-6));
  fprintf(stderr, "读中调用BlockReader次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->block_reader_r_count));
  fprintf(stderr, "读中调用BlockReader时间:\t %f \n", (fg_stats_->block_reader_r_time * 1e-6));
  fprintf(stderr, "读中BlockReader中访问缓存次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->block_reader_r_read_block_cache_count));
  fprintf(stderr, "读中BlockReader中访问磁盘Block次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->block_reader_r_read_block_count));
  fprintf(stderr, "读中BlockReader中访问磁盘Block时间:\t %f \n", (fg_stats_->block_reader_r_read_block_time * 1e-6));


  fprintf(stderr, "写中调用BlockReader次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->block_reader_w_count));
  fprintf(stderr, "写中调用BlockReader时间:\t %f \n", (fg_stats_->block_reader_w_time * 1e-6));
  fprintf(stderr, "写中BlockReader中访问缓存次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->block_reader_w_read_block_cache_count));
  fprintf(stderr, "写中BlockReader中访问磁盘Block次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->block_reader_w_read_block_count));
  fprintf(stderr, "写中BlockReader中访问磁盘Block时间:\t %f \n", (fg_stats_->block_reader_w_read_block_time * 1e-6));

  fprintf(stderr, "WriteBlock次数:\t %lld \n", static_cast<unsigned long long>(fg_stats_->write_block_count));
  fprintf(stderr, "WriteBlock时间:\t %f \n\n", (fg_stats_->write_block_time * 1e-6));





  for (int i = 0; i < 7; i ++){ 
    fprintf(stderr, "L%d_ReadBlock中file->Read()次数:\t %lld \n", i, static_cast<unsigned long long>(Table::read_block_stats_[i][0]));
    fprintf(stderr, "L%d_ReadBlock中file->Read()时间:\t %f \n", i, (Table::read_block_stats_[i][1] * 1e-6));
    fprintf(stderr, "L%d_ReadBlock中file->Read()大小:\t %lld \n\n", i, static_cast<unsigned long long>(Table::read_block_stats_[i][2]));
  }
  fprintf(stderr, "Compaction中ReadBlock中file->Read()次数:\t %lld \n", static_cast<unsigned long long>(Table::read_block_stats_[7][0]));
  fprintf(stderr, "Compaction中ReadBlock中file->Read()时间:\t %f \n", (Table::read_block_stats_[7][1] * 1e-6));
  fprintf(stderr, "Compaction中ReadBlock中file->Read()大小:\t %lld \n\n", static_cast<unsigned long long>(Table::read_block_stats_[7][2]));

  // for (int i = 0; i < 7; i ++){ 
  //   fprintf(stderr, "L%d_Compaction中写Block个数: %lld \n", i, static_cast<unsigned long long>(fg_stats_->add_block_count[i]));
  // }



  system("ps aux | grep db_bench");
  system("ps aux | grep ycsbc");
  fflush(stderr);

  fg_stats_->Reset();
  for (int i = 0; i < 8; i ++) { 
    Table::read_block_stats_[i][0] = 0;
    Table::read_block_stats_[i][1] = 0;
    Table::read_block_stats_[i][2] = 0;
  }
}
///////////////////////////////////////

void DBImpl::TEST_CompactRange(int level, const Slice* begin,const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == NULL) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      bg_cv_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = NULL;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_cv_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {            // 已經有壓縮，有工人在搬磚，就會結束
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == NULL &&
             hot_imm_ == NULL &&
             manual_compaction_ == NULL &&
             !versions_->NeedsCompaction()) {  // 不可變爲空 不做
    // No work to be done
  } else {                                          // 可以做壓縮
    bg_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);            // 環境中調用，多線程，這個就是放個任務，然後就返回了
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

// 背景壓縮線程
void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  bg_compaction_scheduled_ = false;    // 壓縮一次結束

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  bg_cv_.SignalAll();           // 背景壓縮喚醒
}
  uint64_t time4{0};
  uint64_t time5{0};

void DBImpl::BackgroundCompaction() {       // 背景壓縮。
  mutex_.AssertHeld();

  ///////////////////////////////////////
  if (hot_imm_ != NULL) {
    //// <fg_stats>
    uint64_t bk_cm_start = env_->NowMicros();
    CompactHotMemTable();                                                 // flush hot_imm_;
    fg_stats_->minor_compaction_time += env_->NowMicros() - bk_cm_start;
    fg_stats_->minor_compaction_count += 1;
    return;
  }
  ///////////////////////////////////////

  if (imm_ != NULL) {
    //// <fg_stats>
    uint64_t bk_cm_start = env_->NowMicros();
    CompactMemTable();                                           // flush imm_;
    fg_stats_->minor_compaction_time += env_->NowMicros() - bk_cm_start;
    fg_stats_->minor_compaction_count += 1;
    return;
  }
  // 壓縮過程中如果imu很快生成是否會阻塞
  Compaction* c;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    uint64_t p1 = clock();
    c = versions_->PickCompaction();
    fg_stats_->pick_compaction_time += env_->NowMicros() - p1;
    fg_stats_->pick_compaction_count += 1;
  }

  Status status;
  if (c == NULL) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {               // 移到下一層
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest,f->type);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
      status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);

    //// <FMD_STAGE>
    c->ResetFMDStage();

    c->ReleaseInputs();
    DeleteObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = NULL;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.type = 1;
    out.smallest.Clear();
    out.largest.Clear();

    compact->outputs.push_back(out);
    mutex_.Unlock();
  }
  // 壓縮生成的文件放入PM
  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);

  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile, fg_stats_);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes, nullptr);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  return s;
}

// 整合壓縮結果到版本
Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        level + 1,
        out.number, out.file_size, out.smallest, out.largest,1);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {

//// <FMD_STAGE> <2, 2>
// 刚进DoFineGrainedCompactionWork
compact->compaction->AddFMDStage(2, 2);

  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

//// <FMD_STAGE> <3, 0>
compact->compaction->AddFMDStage(3, 3);

  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

//// <FMD_STAGE> <4, 4>
compact->compaction->AddFMDStage(4, 4);

  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    ///////////////////////////////////////////
    if (has_hot_imm_.NoBarrier_Load() != NULL) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (hot_imm_ != NULL) {
        CompactHotMemTable();
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary

        //// <fg_stats_>
        fg_stats_->minor_compaction_count += 1;
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);

    }
    ///////////////////////////////////////////
    // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != NULL) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != NULL) {
          uint64_t begin4 = clock();
          CompactMemTable();
          uint64_t time6 =  clock() - begin4;
          time4 += time6;
          time5 -= time6;
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary

        //// <fg_stats_>
        fg_stats_->minor_compaction_count += 1;                           ///
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);

    }

    Slice key = input->key();

    if (compact->compaction->ShouldStopBefore(key) &&                        // 應該停止迭代 ？？？
        compact->builder != NULL) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }

//// <FMD_STAGE> <5, 5>
// 跳出for(input->Valid())阶段
compact->compaction->AddFMDStage(5, 5);

  delete input;
  input = NULL;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;

  //// <fg_stats>
  fg_stats_->minor_compaction_time += imm_micros;
  fg_stats_->l0_doc_count += 1;
  fg_stats_->l0_doc_time += stats.micros;

  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;                    /// compaction read
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;                           ////compaction write size
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
  ///////////////////////////////////////
  MemTable* hot_mem;
  MemTable* hot_imm;
  ///////////////////////////////////////
  
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  state->hot_mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  if (state->hot_imm != NULL) state->hot_imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  IterState* cleanup = new IterState;
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  ///////////////////////////////////////
  list.push_back(hot_mem_->NewIterator());
  hot_mem_->Ref();
  if (hot_imm_ != NULL) {
    list.push_back(hot_imm_->NewIterator());
    hot_imm_->Ref();
  }
  ///////////////////////////////////////
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  ///////////////////////////////////////
  cleanup->hot_mem = hot_mem_;
  cleanup->hot_imm = hot_imm_;
  ///////////////////////////////////////
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }
    static uint64_t total_time = 0;
    static uint64_t total_call = 0;

  MemTable* hot_mem = hot_mem_;
  MemTable* hot_imm = hot_imm_;
  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  hot_mem->Ref();
  if (hot_imm != NULL) hot_imm->Ref();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();

    std::string skey = key.ToString();
    uint64_t zone = 0;
    static int hit_hot = 0;
    static int hit_cold = 0;
    static int hit = 0;
    if ((zone = key_zone_map_[skey]) != UINT64_MAX) {
     /* if(zone > 0 && zone <= config::kMaxReservedZoneNumber) {
          hit_hot ++;
      }else {
          hit_cold ++;
      }
      hit++;
      if(hit % 1000000 == 0) {
          std::cout << "hit_cold " << hit_cold << std::endl;
          std::cout << "hit_hot " << hit_hot << std::endl;
          std::cout << "hit_hot/hit_cold " << (double)hit_hot/hit_cold << std::endl;
          hit = 0;
      }*/
    }
    // 添加：找不到返回
    else{
        return Status::NotFound(Slice());
    }


    uint64_t time = clock();
    total_call++;
    //char k[16];

    char zone_key[100];
    EncodeFixed64Big(zone_key,zone);
  //  memcpy(zone_key, k, 8);
    memcpy(zone_key + 8, key.data(), key.size());
    const Slice zkey(zone_key, key.size() + 8);

    // First look in the memtable, then in the immutable memtable (if any).
    // LookupKey lkey(key, snapshot);
    LookupKey lkey(zkey, snapshot);
    ///////////////////////////////////////
    if (hot_mem->Get(lkey, value, &s, fg_stats_)) {
      // Done
    } else if (hot_imm != NULL && hot_imm->Get(lkey, value, &s, fg_stats_)) {
      // Done
    } else if (mem->Get(lkey, value, &s, fg_stats_)) {
      // Done
    } else if (imm != NULL && imm->Get(lkey, value, &s, fg_stats_)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    ///////////////////////////////////////
    mutex_.Lock();
    total_time += clock() - time;
    if(total_call%10000 == 0) {
        std::cout << "LAKV平均读取时间： " << (double)total_time/total_call << std::endl;
    }
  }


  mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();
  return s;
}

///////////////////////////////////////
void DBImpl::Scan(const ReadOptions& options, 
                  const std::string& startKey, 
                  //const Slice& endKey, 
                  int length, 
                  std::vector<std::pair<std::string, std::string> >& kv) {
  //Status s;
  // 不要在这里加锁，NewIterator中也会加锁，这里加锁的话就执行不下去了
  //MutexLock l(&mutex_);
  Iterator* dbIter = NewIterator(options);
  dbIter->Seek(startKey);
  while (dbIter->Valid() && (length > 0)) { 
    kv.push_back(std::make_pair(dbIter->key().ToString(), dbIter->value().ToString()));
    dbIter->Next();
    length --;
  }
  delete dbIter;
}
///////////////////////////////////////

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}
 uint64_t  total_time{0};
Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }
  Status status = MakeRoomForWrite(my_batch == NULL);
  status = MakeRoomForHotWrite(my_batch == NULL);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
          uint64_t start1 = clock();
        status = WriteBatchInternal::InsertInto(updates, hot_mem_, mem_);
        total_time += clock() - start1;
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  static int sleep_num = 0;
  static int wait_num = 0;
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      s = bg_error_;
      break;
    } else if (
        allow_delay &&
        versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {     /// 0 層 太多文件
      //std::cout << "阻塞睡眠次數：" << ++sleep_num << std::endl;
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);                                         /// 睡眠
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size /*&& hot_mem_->ApproximateMemoryUsage() <= options_.write_buffer_size*/)) {  /// mem 有空間
      break;
    } else if (imm_ != NULL /*|| hot_imm_ != NULL*/) {                                    // 等待背景壓縮
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
        std::cout << "等待imm_次數：" << ++wait_num << std::endl;
        bg_cv_.Wait();

    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {  // 等待背景壓縮線程
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
        std::cout << "等待L0文件次數：" << ++wait_num << std::endl;
        bg_cv_.Wait();
    } else {
     // 空間不足
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();     //創建新日誌文件
      WritableFile* lfile = NULL;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);

        imm_ = mem_;
        has_imm_.Release_Store(imm_);
        /////////////////////////////////////// zone 更新
        cur_zone_ += 1;
        cur_zone_size_ = 0;
        mem_ = new MemTable(internal_comparator_, &cur_zone_, &cur_zone_size_, &cur_reserved_zone_,
                            &cur_reserved_zone_size_, &key_zone_map_);
        ///////////////////////////////////////
        mem_->Ref();
      force = false;   // Do not force another compaction if have room
      MaybeScheduleCompaction();          // 重新創建一個memtable之後，可能要處理壓縮
    }
  }
  return s;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForHotWrite(bool force) {

  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (
        allow_delay &&
        versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (hot_mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (hot_imm_ != NULL) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      bg_cv_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      bg_cv_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
    /*  assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = NULL;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);*/
      hot_imm_ = hot_mem_;
      has_hot_imm_.Release_Store(hot_imm_);
      /////////////////////////////////////// Hot zone更新
      cur_reserved_zone_ += 1;
      cur_reserved_zone_size_ = 0;
      hot_mem_ = new MemTable(internal_comparator_, &cur_zone_, &cur_zone_size_, &cur_reserved_zone_, &cur_reserved_zone_size_, &key_zone_map_);
      ///////////////////////////////////////
      hot_mem_->Ref();
      force = false;   // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (hot_mem_) {
      total_usage += hot_mem_->ApproximateMemoryUsage();
    }
    if (hot_imm_) {
      total_usage += hot_imm_->ApproximateMemoryUsage();
    }
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    snprintf(buf, sizeof(buf), "%llu",
             static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = NULL;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == NULL && impl->hot_mem_ == NULL) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      ///////////////////////////////////////
      impl->hot_mem_ = new MemTable(impl->internal_comparator_, &cur_zone_, &cur_zone_size_,
              &cur_reserved_zone_, &cur_reserved_zone_size_, &key_zone_map_);
      impl->hot_mem_->Ref();
      impl->mem_ = new MemTable(impl->internal_comparator_, &cur_zone_, &cur_zone_size_,
              &cur_reserved_zone_, &cur_reserved_zone_size_, &key_zone_map_);
      ///////////////////////////////////////
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->DeleteObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->hot_mem_ != NULL);
    assert(impl->mem_ != NULL);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
