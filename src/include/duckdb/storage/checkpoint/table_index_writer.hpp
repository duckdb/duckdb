//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint/table_index_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/bound_index.hpp"

namespace duckdb {
class Serializer;
class PartialBlockManager;
class SingleFileCheckpointWriter;

struct CheckpointedIndex {
	shared_ptr<const IndexStorageInfo> storage_info;
	unique_ptr<BoundIndex> shadow_index;
};

class TableIndexWriter {
public:
	explicit TableIndexWriter(PartialBlockManager &partial_block_manager, StorageVersion version);
	virtual ~TableIndexWriter();

public:
	void AddUnboundIndex(shared_ptr<const IndexStorageInfo> info);
	void AddBoundIndex(IndexStorageInfo info, unique_ptr<BoundIndex> index);
	unique_ptr<BoundIndex> TakeShadowIndex(idx_t index);
	PartialBlockManager &GetPartialBlockManager() const {
		return partial_block_manager;
	}
	virtual void Serialize(Serializer &serializer) = 0;
	//! Writes the index buffers to disk
	virtual void Flush() = 0;
	//! Get the targeted storage version for the current writer
	StorageVersion GetStorageVersion() const;

protected:
	PartialBlockManager &partial_block_manager;
	StorageVersion storage_version;

	vector<CheckpointedIndex> result;
};

class SingleFileIndexWriter : public TableIndexWriter {
public:
	explicit SingleFileIndexWriter(SingleFileCheckpointWriter &checkpoint_manager,
	                               PartialBlockManager &partial_block_manager, StorageVersion version,
	                               bool debug_verify_blocks);

public:
	void Flush() override;
	void Serialize(Serializer &serializer) override;

private:
	void VerifyBlockUsage();

private:
	SingleFileCheckpointWriter &checkpoint_manager;
	bool debug_verify_blocks;
};

} // namespace duckdb
