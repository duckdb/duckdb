//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_copy_from_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

class LogicalCopyFromFile : public LogicalOperator {
public:
	LogicalCopyFromFile(idx_t table_index, CopyFunction function, unique_ptr<FunctionData> info,
	                    vector<LogicalType> sql_types)
	    : LogicalOperator(LogicalOperatorType::COPY_FROM_FILE), table_index(table_index), function(function),
	      info(move(info)), sql_types(move(sql_types)) {
	}

	idx_t table_index;
	//! The copy function to use to read the file
	CopyFunction function;
	//! The binding info containing the set of options for reading the file
	unique_ptr<FunctionData> info;
	//! The set of types to retrieve from the file
	vector<LogicalType> sql_types;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, sql_types.size());
	}

protected:
	void ResolveTypes() override {
		for (auto &type : sql_types) {
			types.push_back(type);
		}
	}
};
} // namespace duckdb
