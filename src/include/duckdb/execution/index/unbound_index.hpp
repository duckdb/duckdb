//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/unbound_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/storage_index.hpp"

namespace duckdb {

struct BufferedIndexReplays;
class ColumnDataCollection;

class UnboundIndex final : public Index {
private:
	//! The CreateInfo of the index.
	unique_ptr<CreateInfo> create_info;
	//! The serialized storage information of the index.
	//! This contains buffered_replays and mapped_column_ids when they exist.
	unique_ptr<IndexStorageInfo> storage_info;

public:
	UnboundIndex(unique_ptr<CreateInfo> create_info, unique_ptr<IndexStorageInfo> storage_info,
	             TableIOManager &table_io_manager, AttachedDatabase &db);

public:
	bool IsBound() const override {
		return false;
	}
	const string &GetIndexType() const override {
		return GetCreateInfo().index_type;
	}
	const string &GetIndexName() const override {
		return GetCreateInfo().index_name;
	}
	IndexConstraintType GetConstraintType() const override {
		return GetCreateInfo().constraint_type;
	}
	const CreateIndexInfo &GetCreateInfo() const {
		return create_info->Cast<CreateIndexInfo>();
	}
	const IndexStorageInfo &GetStorageInfo() const {
		return *storage_info;
	}
	const vector<unique_ptr<ParsedExpression>> &GetParsedExpressions() const {
		return GetCreateInfo().parsed_expressions;
	}
	const string &GetTableName() const {
		return GetCreateInfo().table;
	}

	void CommitDrop() override;

	//! Buffer Index delete or insert (replay_type) data chunk.
	//! See note above on mapped_column_ids, this function assumes that index_column_chunk maps into
	//! mapped_column_ids_p to get the physical column index for each Indexed column in the chunk.
	void BufferChunk(DataChunk &index_column_chunk, Vector &row_ids, const vector<StorageIndex> &mapped_column_ids_p,
	                 BufferedIndexReplay replay_type);
	bool HasBufferedReplays() const {
		return storage_info->buffered_replays && storage_info->buffered_replays->HasBufferedReplays();
	}

	BufferedIndexReplays &GetBufferedReplays() {
		D_ASSERT(storage_info->buffered_replays);
		return *storage_info->buffered_replays;
	}

	const vector<StorageIndex> &GetMappedColumnIds() const {
		return storage_info->mapped_column_ids;
	}

	//! Move storage_info out for serialization, updating options for the current checkpoint.
	//! The storage_info should be set back on this UnboundIndex after serialization.
	unique_ptr<IndexStorageInfo> TakeStorageInfo(const case_insensitive_map_t<Value> &options);

	//! Set IndexStorageInfo after serialization/deserialization. Takes ownership of the entire storage_info.
	void SetStorageInfo(unique_ptr<IndexStorageInfo> info);
};

} // namespace duckdb
