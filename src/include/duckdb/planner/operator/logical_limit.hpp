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
	LogicalLimit(int64_t limit, int64_t offset)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_LIMIT), limit(limit), offset(offset) {
	}

	LogicalLimit(unique_ptr<Expression> limit_expression, int64_t offset)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_LIMIT), limit_expression(move(limit_expression)), offset(offset) {
	}

	//! The maximum amount of elements to emit
	int64_t limit;
	//! The offset from the start to begin emitting elements
	int64_t offset;
	unique_ptr<Expression> limit_expression{};

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
