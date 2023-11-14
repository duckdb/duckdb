//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/operation_converter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {

//! The Operation Converter converts Set operations to joins when possible
class OperationConverter {
public:
	OperationConverter(LogicalOperator &root);

	//! Perform DelimJoin elimination
	void Optimize(unique_ptr<LogicalOperator> &op);
	LogicalOperator &root;

private:
};

} // namespace duckdb
