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
	OperationConverter() {
	}
	//! Perform DelimJoin elimination
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:

};

} // namespace duckdb
