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

class ColumnDataCollection;

class UnboundIndex final : public Index {
private:
	//! The CreateInfo of the index.
	unique_ptr<CreateInfo> create_info;
	//! The serialized storage information of the index.
	IndexStorageInfo storage_info;
	//! Buffer for WAL replay appends.
	unique_ptr<ColumnDataCollection> buffered_appends;
	//! Maps the column IDs in the buffered appends to the table columns.
	vector<StorageIndex> mapped_column_ids;

public:
	UnboundIndex(unique_ptr<CreateInfo> create_info, IndexStorageInfo storage_info, TableIOManager &table_io_manager,
	             AttachedDatabase &db);

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
		return storage_info;
	}
	const vector<unique_ptr<ParsedExpression>> &GetParsedExpressions() const {
		return GetCreateInfo().parsed_expressions;
	}
	const string &GetTableName() const {
		return GetCreateInfo().table;
	}

	void CommitDrop() override;

	void BufferChunk(DataChunk &chunk, Vector &row_ids, const vector<StorageIndex> &mapped_column_ids_p);
	bool HasBufferedAppends() const {
		return buffered_appends != nullptr;
	}
	ColumnDataCollection &GetBufferedAppends() const {
		return *buffered_appends;
	}
	const vector<StorageIndex> &GetMappedColumnIds() const {
		return mapped_column_ids;
	}
};

} // namespace duckdb
