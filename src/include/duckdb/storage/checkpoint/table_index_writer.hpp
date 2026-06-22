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
	explicit TableIndexWriter(PartialBlockManager &partial_block_manager, StorageVersion version);
	virtual ~TableIndexWriter();

public:
	PartialBlockManager &GetPartialBlockManager() const {
		return partial_block_manager;
	}
	void AddUnboundIndex(shared_ptr<const IndexStorageInfo> info);
	void AddBoundIndex(IndexStorageInfo storage_info, unique_ptr<BoundIndex> index);
	unique_ptr<BoundIndex> TakeBoundIndex(const idx_t index) {
		D_ASSERT(index < result.size());
		return std::move(result[index].shadow_index);
	}
	vector<CheckpointedIndex> &GetResult() {
		return result;
	}
	virtual void FlushPartialBlocks() = 0;
	virtual IndexSerializationFormat GetTargetFormat() const = 0;

protected:
	PartialBlockManager &partial_block_manager;
	StorageVersion storage_version;

	vector<CheckpointedIndex> result;
};

class SingleFileIndexWriter : public TableIndexWriter {
public:
	explicit SingleFileIndexWriter(PartialBlockManager &partial_block_manager, StorageVersion version);

public:
	void FlushPartialBlocks() override;
	IndexSerializationFormat GetTargetFormat() const override;

private:
};

} // namespace duckdb
