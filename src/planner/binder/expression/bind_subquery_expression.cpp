#include "parser/expression/subquery_expression.hpp"
#include "planner/binder.hpp"
#include "planner/expression/bound_subquery_expression.hpp"
#include "planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::BindExpression(SubqueryExpression &expr, index_t depth) {
	// first bind the children of the subquery, if any
	if (expr.child) {
		string result = Bind(&expr.child, depth);
		if (!result.empty()) {
			return BindResult(result);
		}
	}
	auto child = (BoundExpression *)expr.child.get();
	// bind the subquery in a new binder
	auto subquery_binder = make_unique<Binder>(context, &binder);
	// the subquery may refer to CTEs from the parent query
	subquery_binder->CTE_bindings = binder.CTE_bindings;
	auto bound_node = subquery_binder->Bind(*expr.subquery);
	// check the correlated columns of the subquery for correlated columns with depth > 1
	for (index_t i = 0; i < subquery_binder->correlated_columns.size(); i++) {
		CorrelatedColumnInfo corr = subquery_binder->correlated_columns[i];
		if (corr.depth > 1) {
			// depth > 1, the column references the query ABOVE the current one
			// add to the set of correlated columns for THIS query
			corr.depth -= 1;
			binder.AddCorrelatedColumn(corr);
		}
	}
	if (expr.subquery_type != SubqueryType::EXISTS && bound_node->types.size() > 1) {
		throw BinderException("Subquery returns %zu columns - expected 1", bound_node->types.size());
	}

	SQLType return_type =
	    expr.subquery_type == SubqueryType::SCALAR ? bound_node->types[0] : SQLType(SQLTypeId::BOOLEAN);

	auto result = make_unique<BoundSubqueryExpression>(GetInternalType(return_type));
	if (expr.subquery_type == SubqueryType::ANY) {
		// ANY comparison
		// cast child and subquery child to equivalent types
		assert(bound_node->types.size() == 1);
		auto compare_type = MaxSQLType(child->sql_type, bound_node->types[0]);
		child->expr = AddCastToType(move(child->expr), child->sql_type, compare_type);
		result->child_type = bound_node->types[0];
		result->child_target = compare_type;
	}
	result->binder = move(subquery_binder);
	result->subquery = move(bound_node);
	result->subquery_type = expr.subquery_type;
	result->child = child ? move(child->expr) : nullptr;
	result->comparison_type = expr.comparison_type;

	return BindResult(move(result), return_type);
}
