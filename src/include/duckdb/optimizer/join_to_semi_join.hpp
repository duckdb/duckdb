//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_to_semi_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

//! Turns an INNER join against a relation that is only read by predicates comparing it with one other relation into a
//! SEMI join on that relation, when the rows the join repeats do not matter.
//! See join_to_semi_join.cpp for the conditions.
class JoinToSemiJoin {
public:
	explicit JoinToSemiJoin(Optimizer &optimizer);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	Optimizer &optimizer;
};

} // namespace duckdb
