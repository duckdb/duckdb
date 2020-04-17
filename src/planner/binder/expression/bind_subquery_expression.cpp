#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

class BoundSubqueryNode : public QueryNode {
public:
	BoundSubqueryNode(unique_ptr<Binder> subquery_binder, unique_ptr<BoundQueryNode> bound_node,
	                  unique_ptr<QueryNode> subquery)
	    : QueryNode(QueryNodeType::BOUND_SUBQUERY_NODE), subquery_binder(move(subquery_binder)),
	      bound_node(move(bound_node)), subquery(move(subquery)) {
	}

	unique_ptr<Binder> subquery_binder;
	unique_ptr<BoundQueryNode> bound_node;
	unique_ptr<QueryNode> subquery;

	const vector<unique_ptr<ParsedExpression>> &GetSelectList() const {
		throw Exception("Cannot get select list of bound subquery node");
	}

	unique_ptr<QueryNode> Copy() {
		throw Exception("Cannot copy bound subquery node");
	}
};

BindResult ExpressionBinder::BindExpression(SubqueryExpression &expr, idx_t depth) {
	if (expr.subquery->type != QueryNodeType::BOUND_SUBQUERY_NODE) {
		assert(depth == 0);
		// first bind the actual subquery in a new binder
		auto subquery_binder = make_unique<Binder>(context, &binder);
		// the subquery may refer to CTEs from the parent query
		subquery_binder->CTE_bindings = binder.CTE_bindings;
		auto bound_node = subquery_binder->BindNode(*expr.subquery);
		// check the correlated columns of the subquery for correlated columns with depth > 1
		for (idx_t i = 0; i < subquery_binder->correlated_columns.size(); i++) {
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
		expr.subquery = make_unique<BoundSubqueryNode>(move(subquery_binder), move(bound_node), move(expr.subquery));
	}
	// now bind the child node of the subquery
	if (expr.child) {
		// first bind the children of the subquery, if any
		string error = Bind(&expr.child, depth);
		if (!error.empty()) {
			return BindResult(error);
		}
	}
	// both binding the child and binding the subquery was successful
	assert(expr.subquery->type == QueryNodeType::BOUND_SUBQUERY_NODE);
	auto bound_subquery = (BoundSubqueryNode *)expr.subquery.get();
	auto child = (BoundExpression *)expr.child.get();
	auto subquery_binder = move(bound_subquery->subquery_binder);
	auto bound_node = move(bound_subquery->bound_node);
	SQLType return_type =
	    expr.subquery_type == SubqueryType::SCALAR ? bound_node->types[0] : SQLType(SQLTypeId::BOOLEAN);
	if (return_type.id == SQLTypeId::UNKNOWN) {
		throw BinderException("Could not determine type of parameters: try adding explicit type casts");
	}

	auto result = make_unique<BoundSubqueryExpression>(GetInternalType(return_type));
	if (expr.subquery_type == SubqueryType::ANY) {
		// ANY comparison
		// cast child and subquery child to equivalent types
		assert(bound_node->types.size() == 1);
		auto compare_type = MaxSQLType(child->sql_type, bound_node->types[0]);
		child->expr = BoundCastExpression::AddCastToType(move(child->expr), child->sql_type, compare_type);
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
