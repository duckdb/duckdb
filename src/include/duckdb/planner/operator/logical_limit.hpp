//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_limit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalLimit represents a LIMIT clause
class LogicalLimit : public LogicalOperator {
public:
	LogicalLimit(bool is_limit_percent, int64_t limit_val, int64_t offset_val, unique_ptr<Expression> limit,
	             unique_ptr<Expression> offset)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_LIMIT), is_limit_percent(is_limit_percent), limit_val(limit_val),
	      offset_val(offset_val), limit(move(limit)), offset(move(offset)) {
	}

	//! Limit and offset values in case they are constants, used in optimizations.
	bool is_limit_percent = false;
	double limit_val;
	int64_t offset_val;
	//! The maximum amount of elements to emit
	unique_ptr<Expression> limit;
	//! The offset from the start to begin emitting elements
	unique_ptr<Expression> offset;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
