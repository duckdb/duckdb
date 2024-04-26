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
	CreateIndexInfo create_info;

	// The serialized storage info of the index
	IndexStorageInfo storage_info;

	//! Unbound expressions used by the index during optimizations
	// vector<unique_ptr<ParsedExpression>> parsed_expressions;
public:
	UnboundIndex(const CreateIndexInfo &create_info, IndexStorageInfo storage_info, TableIOManager &table_io_manager,
	             AttachedDatabase &db);

	bool IsUnbound() override {
		return true;
	}
	const CreateIndexInfo &GetCreateInfo() const {
		return create_info;
	}
	const IndexStorageInfo &GetStorageInfo() const {
		return storage_info;
	}
	const vector<unique_ptr<ParsedExpression>> &GetParsedExpressions() const {
		return create_info.parsed_expressions;
	}

	void CommitDrop() override;
};

} // namespace duckdb
