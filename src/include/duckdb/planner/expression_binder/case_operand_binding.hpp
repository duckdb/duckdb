//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/case_operand_binding.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/reference_map.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

class BoundSubqueryExpression;

//! Retains subquery sources while binding a CASE operand without repeating its other registrations.
class CaseOperandBinding {
public:
	explicit CaseOperandBinding(ExpressionBinder &binder);
	~CaseOperandBinding();

	void Remember(const BoundSubqueryExpression &expr, const QueryNode &query);
	unique_ptr<Expression> Copy(unique_ptr<Expression> &expr);

private:
	unique_ptr<Expression> CopySubquery(BoundSubqueryExpression &expr);

private:
	ExpressionBinder &binder;
	optional_ptr<CaseOperandBinding> previous;
	reference_map_t<const BoundSubqueryExpression, unique_ptr<QueryNode>> subqueries;
};

} // namespace duckdb
