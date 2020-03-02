//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_copy_to_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalCopyToFile : public LogicalOperator {
public:
	LogicalCopyToFile(unique_ptr<CopyInfo> info)
	    : LogicalOperator(LogicalOperatorType::COPY_TO_FILE), info(move(info)) {
	}

	unique_ptr<CopyInfo> info;
	vector<string> names;
	vector<SQLType> sql_types;

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::INT64);
	}
};
} // namespace duckdb
