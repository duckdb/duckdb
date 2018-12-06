//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalAggregate represents an aggregate operation with (optional) GROUP BY
//! operator.
class LogicalWindow : public LogicalOperator {
public:
	LogicalWindow(vector<unique_ptr<Expression>> select_list)
	    : LogicalOperator(LogicalOperatorType::WINDOW, std::move(select_list)) {
	}

	void Accept(LogicalOperatorVisitor *v) override {
			v->Visit(*this);
		}
		vector<string> GetNames() override;

	protected:
		void ResolveTypes() override;
	};
	} // namespace duckdb
