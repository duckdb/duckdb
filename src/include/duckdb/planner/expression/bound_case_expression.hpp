//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_case_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

class BoundCaseExpression : public Expression {
public:
	BoundCaseExpression(unique_ptr<Expression> check, unique_ptr<Expression> res_if_true,
	                    unique_ptr<Expression> res_if_false);

	unique_ptr<Expression> check;
	unique_ptr<Expression> result_if_true;
	unique_ptr<Expression> result_if_false;

public:
	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
