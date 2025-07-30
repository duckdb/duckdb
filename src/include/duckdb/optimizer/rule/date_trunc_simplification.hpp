//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/date_trunc_simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// DateTruncSimplificationRule rewrites an expression of the form
//
//    date_trunc(part, column) <op> constant_rhs
//
// such that the date_trunc is instead applied to the constant RHS and then
// simplified further.
//
// The rules applied are as follows:
//
// date_trunc(part, column) >= constant_rhs   -->   column >= constant_rhs
// date_trunc(part, column) <= constant_rhs   -->   column <= date_trunc(part, date_add(constant_rhs, INTERVAL 1 part))
// date_trunc(part, column) >  constant_rhs   -->   column >  constant_rhs
// date_trunc(part, column) <  constant_rhs   -->   column <  date_trunc(part, date_add(constant_rhs, INTERVAL 1 part))
// date_trunc(part, column) == constant_rhs   -->   column >= date_trunc(part, constant_rhs) AND column <  date_trunc(part, date_add(constant_rhs, interval 1 part))
// date_trunc(part, column) <> constant_rhs   -->   column <  date_trunc(part, constant_rhs) OR  column >= date_trunc(part, date_add(constant_rhs, interval 1 part))
//
class DateTruncSimplificationRule : public Rule {
public:
	explicit DateTruncSimplificationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;

	static string DatePartToFunc(const DatePartSpecifier& date_part);

	unique_ptr<Expression> CreateTrunc(const BoundConstantExpression &date_part,
	                                   const BoundConstantExpression &rhs,
	                                   const LogicalType &return_type);
	unique_ptr<Expression> CreateTruncAdd(const BoundConstantExpression &date_part,
	                                      const BoundConstantExpression &rhs,
	                                      const LogicalType &return_type);

	bool DateIsTruncated(const BoundConstantExpression &date_part, const BoundConstantExpression &rhs);

	unique_ptr<Expression> CastAndEvaluate(unique_ptr<Expression> rhs,
	                                       const LogicalType &return_type);
};

} // namespace duckdb
