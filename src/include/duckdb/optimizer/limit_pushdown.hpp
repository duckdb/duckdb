//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/limit_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
class LogicalOperator;
class ConstraintPropagator;

class LimitPushdown {
public:
	static constexpr idx_t MAX_LIMIT = 8192;
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	static bool CanPushThroughProjection(LogicalOperator &op);

private:
	static unique_ptr<LogicalOperator> OptimizeInternal(unique_ptr<LogicalOperator> op,
	                                                    ConstraintPropagator &propagator, bool &join_pushed,
	                                                    bool &changed);
	static unique_ptr<LogicalOperator> TryPushIntoJoin(unique_ptr<LogicalOperator> op, ConstraintPropagator &propagator,
	                                                   bool &pushed);
	static bool HasLimit(const LogicalOperator &op);
};

} // namespace duckdb
