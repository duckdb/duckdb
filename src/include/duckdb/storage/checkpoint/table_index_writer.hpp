#pragma once

#include "duckdb/storage/index_storage_info.hpp"
#include "duckdb/execution/index/bound_index.hpp"

#include <duckdb/storage/table/table_index_list.hpp>

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
	std::__wrap_iter<unique_ptr<BoundIndex> *> GetBoundIndexes() {
		return indexes.begin();
	}
	IndexSerializationResult &GetResult() {
		return result;
	}

protected:
	PartialBlockManager &partial_block_manager;
	IndexSerializationInfo &info;

	IndexSerializationResult result;

	vector<IndexStorageInfo> storage_infos;
	vector<unique_ptr<BoundIndex>> indexes;
};

class SingleFileIndexWriter : public TableIndexWriter {
public:
	explicit SingleFileIndexWriter(PartialBlockManager &partial_block_manager, IndexSerializationInfo &info);

public:


private:
	//
};

} // namespace duckdb