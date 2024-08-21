//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/deliminator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {

//! The Deliminator optimizer traverses the logical operator tree and removes any redundant DelimGets/DelimJoins
class EmptyResultPullup : LogicalOperatorVisitor {
public:
	EmptyResultPullup() {
	}

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	unique_ptr<LogicalOperator> PullUpEmptyJoinChildren(unique_ptr<LogicalOperator> op);
};


} // namespace duckdb
