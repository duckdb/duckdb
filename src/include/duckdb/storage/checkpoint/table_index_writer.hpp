//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint/table_index_writer.hpp
//
//
//===----------------------------------------------------------------------===//


#pragma once

#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/storage/index_serialization_info.hpp"

namespace duckdb {
class PartialBlockManager;
class SingleFileCheckpointWriter;

class TableIndexWriter {
public:
  explicit TableIndexWriter(PartialBlockManager &partial_block_manager, IndexSerializationInfo &info);
  virtual ~TableIndexWriter();

public:
  PartialBlockManager &GetPartialBlockManager() const {
    return partial_block_manager;
  }
  IndexSerializationInfo &GetInfo() const {
    return info;
  }
  void AddUnboundIndex(const IndexStorageInfo &storage_info);
  void AddBoundIndex(IndexStorageInfo storage_info, unique_ptr<BoundIndex> index);
  unique_ptr<BoundIndex> TakeBoundIndex(idx_t index) {
    D_ASSERT(index < indexes.size());
    return std::move(indexes[index]);
  }
  IndexSerializationResult &GetResult() {
    return result;
  }
  virtual void FlushPartialBlocks() = 0;
  virtual IndexSerializationFormat GetTargetFormat() const = 0;

protected:
  PartialBlockManager &partial_block_manager;
  IndexSerializationInfo &info;

  IndexSerializationResult result;

  vector<unique_ptr<BoundIndex>> indexes;
};

class SingleFileIndexWriter : public TableIndexWriter {
public:
  explicit SingleFileIndexWriter(PartialBlockManager &partial_block_manager, IndexSerializationInfo &info);

public:
  void FlushPartialBlocks() override;
  IndexSerializationFormat GetTargetFormat() const override;

private:
};

} // namespace duckdb
