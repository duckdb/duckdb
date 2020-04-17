//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_copy_from_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalCopyFromFile : public LogicalOperator {
public:
	LogicalCopyFromFile(idx_t table_index, unique_ptr<CopyInfo> info, vector<SQLType> sql_types)
	    : LogicalOperator(LogicalOperatorType::COPY_FROM_FILE), table_index(table_index), info(move(info)),
	      sql_types(sql_types) {
	}

	idx_t table_index;
	unique_ptr<CopyInfo> info;
	vector<SQLType> sql_types;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, sql_types.size());
	}

protected:
	void ResolveTypes() override {
		for (auto &type : sql_types) {
			types.push_back(GetInternalType(type));
		}
	}
};
} // namespace duckdb
