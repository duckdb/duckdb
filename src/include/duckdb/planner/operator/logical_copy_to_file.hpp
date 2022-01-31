//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_copy_to_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

class LogicalCopyToFile : public LogicalOperator {
public:
	LogicalCopyToFile(CopyFunction function, unique_ptr<FunctionData> bind_data)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_COPY_TO_FILE), function(function), bind_data(move(bind_data)) {
	}
	CopyFunction function;
	unique_ptr<FunctionData> bind_data;
	std::string file_path;

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType::BIGINT);
	}
};
} // namespace duckdb
