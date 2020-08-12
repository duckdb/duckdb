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
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

class LogicalCopyToFile : public LogicalOperator {
public:
	LogicalCopyToFile(CopyFunction function, unique_ptr<FunctionData> bind_data)
	    : LogicalOperator(LogicalOperatorType::COPY_TO_FILE), function(function), bind_data(move(bind_data)) {
	}
	CopyFunction function;
	unique_ptr<FunctionData> bind_data;

protected:
	void ResolveTypes() override {
		types.push_back(PhysicalType::INT64);
	}
};
} // namespace duckdb
