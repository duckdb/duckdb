//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/unbound_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/index.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"

namespace duckdb {

class UnboundIndex final : public Index {
private:
	// The create info of the index
	unique_ptr<CreateInfo> create_info;

	// The serialized storage info of the index
	IndexStorageInfo storage_info;

public:
	UnboundIndex(unique_ptr<CreateInfo> create_info, IndexStorageInfo storage_info, TableIOManager &table_io_manager,
	             AttachedDatabase &db);

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
};

} // namespace duckdb
