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
	OperationConverter(LogicalOperator &root, Binder &binder);

	//! Perform DelimJoin elimination
	void Optimize(unique_ptr<LogicalOperator> &op, bool is_root = false);
	LogicalOperator &root;
	Binder &binder;

private:
};

} // namespace duckdb
