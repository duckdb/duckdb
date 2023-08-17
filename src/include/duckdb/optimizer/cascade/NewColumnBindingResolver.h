//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/cascade/NewColumnBindingResolver.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"

using namespace duckdb;

namespace gpopt {

//! The ColumnBindingResolver resolves ColumnBindings into base tables
//! (table_index, column_index) into physical indices into the DataChunks that
//! are used within the execution engine
class NewColumnBindingResolver {
public:
	NewColumnBindingResolver();

	void VisitOperator(PhysicalOperator &op);
	static void Verify(PhysicalOperator &op);
    void VisitExpression(duckdb::unique_ptr<Expression> *expression);

protected:
	duckdb::vector<ColumnBinding> bindings;

    void VisitOperatorExpressions(PhysicalOperator &op);
    void EnumerateExpressions(PhysicalOperator &op,
        const std::function<void(duckdb::unique_ptr<Expression> *child)> &callback);
    void VisitOperatorChildren(PhysicalOperator &op);
    void VisitExpressionChildren(Expression &expr);

	duckdb::unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, duckdb::unique_ptr<Expression> *expr_ptr);
    duckdb::unique_ptr<Expression> VisitReplace(BoundAggregateExpression &expr, duckdb::unique_ptr<Expression> *expr_ptr);
    duckdb::unique_ptr<Expression> VisitReplace(BoundBetweenExpression &expr, duckdb::unique_ptr<Expression> *expr_ptr);
    duckdb::unique_ptr<Expression> VisitReplace(BoundCaseExpression &expr, duckdb::unique_ptr<Expression> *expr_ptr);
    duckdb::unique_ptr<Expression> VisitReplace(BoundCastExpression &expr, duckdb::unique_ptr<Expression> *expr_ptr);
    duckdb::unique_ptr<Expression> VisitReplace(BoundComparisonExpression &expr, duckdb::unique_ptr<Expression> *expr_ptr);
    duckdb::unique_ptr<Expression> VisitReplace(BoundConjunctionExpression &expr, duckdb::unique_ptr<Expression> *expr_ptr);
    duckdb::unique_ptr<Expression> VisitReplace(BoundConstantExpression &expr, duckdb::unique_ptr<Expression> *expr_ptr);
    duckdb::unique_ptr<Expression> VisitReplace(BoundDefaultExpression &expr, duckdb::unique_ptr<Expression> *expr_ptr);
    duckdb::unique_ptr<Expression> VisitReplace(BoundFunctionExpression &expr, duckdb::unique_ptr<Expression> *expr_ptr);
    duckdb::unique_ptr<Expression> VisitReplace(BoundOperatorExpression &expr, duckdb::unique_ptr<Expression> *expr_ptr);
    duckdb::unique_ptr<Expression> VisitReplace(BoundParameterExpression &expr, duckdb::unique_ptr<Expression> *expr_ptr);
    duckdb::unique_ptr<Expression> VisitReplace(BoundReferenceExpression &expr, duckdb::unique_ptr<Expression> *expr_ptr);
    duckdb::unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, duckdb::unique_ptr<Expression> *expr_ptr);
    duckdb::unique_ptr<Expression> VisitReplace(BoundWindowExpression &expr, duckdb::unique_ptr<Expression> *expr_ptr);
    duckdb::unique_ptr<Expression> VisitReplace(BoundUnnestExpression &expr, duckdb::unique_ptr<Expression> *expr_ptr);
};
} // namespace duckdb