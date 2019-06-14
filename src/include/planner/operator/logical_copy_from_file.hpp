//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_copy_from_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/copy_info.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalCopyFromFile : public LogicalOperator {
public:
	LogicalCopyFromFile(unique_ptr<CopyInfo> info, vector<SQLType> sql_types)
	    : LogicalOperator(LogicalOperatorType::COPY_FROM_FILE), info(move(info)), sql_types(sql_types) {
	}

	unique_ptr<CopyInfo> info;
	vector<SQLType> sql_types;

protected:
	void ResolveTypes() override {
		for (auto &type : sql_types) {
			types.push_back(GetInternalType(type));
		}
	}
};
} // namespace duckdb
