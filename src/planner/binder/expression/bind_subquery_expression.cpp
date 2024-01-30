#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

class BoundSubqueryNode : public QueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::BOUND_SUBQUERY_NODE;

public:
	BoundSubqueryNode(shared_ptr<Binder> subquery_binder, unique_ptr<BoundQueryNode> bound_node,
	                  unique_ptr<SelectStatement> subquery)
	    : QueryNode(QueryNodeType::BOUND_SUBQUERY_NODE), subquery_binder(std::move(subquery_binder)),
	      bound_node(std::move(bound_node)), subquery(std::move(subquery)) {
	}

	shared_ptr<Binder> subquery_binder;
	unique_ptr<BoundQueryNode> bound_node;
	unique_ptr<SelectStatement> subquery;

	const vector<unique_ptr<ParsedExpression>> &GetSelectList() const override {
		throw InternalException("Cannot get select list of bound subquery node");
	}

	string ToString() const override {
		throw InternalException("Cannot ToString bound subquery node");
	}
	unique_ptr<QueryNode> Copy() const override {
		throw InternalException("Cannot copy bound subquery node");
	}

	void Serialize(Serializer &serializer) const override {
		throw InternalException("Cannot serialize bound subquery node");
	}
};

BindResult ExpressionBinder::BindExpression(SubqueryExpression &expr, idx_t depth) {
	if (expr.subquery->node->type != QueryNodeType::BOUND_SUBQUERY_NODE) {
		// first bind the actual subquery in a new binder
		auto subquery_binder = Binder::CreateBinder(context, &binder);
		subquery_binder->can_contain_nulls = true;
		auto bound_node = subquery_binder->BindNode(*expr.subquery->node);
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
			throw BinderException(expr, "Subquery returns %zu columns - expected 1", bound_node->types.size());
		}
		auto prior_subquery = std::move(expr.subquery);
		expr.subquery = make_uniq<SelectStatement>();
		expr.subquery->node =
		    make_uniq<BoundSubqueryNode>(std::move(subquery_binder), std::move(bound_node), std::move(prior_subquery));
	}
	// now bind the child node of the subquery
	if (expr.child) {
		// first bind the children of the subquery, if any
		auto error = Bind(expr.child, depth);
		if (error.HasError()) {
			return BindResult(std::move(error));
		}
	}
	// both binding the child and binding the subquery was successful
	D_ASSERT(expr.subquery->node->type == QueryNodeType::BOUND_SUBQUERY_NODE);
	auto &bound_subquery = expr.subquery->node->Cast<BoundSubqueryNode>();
	auto subquery_binder = std::move(bound_subquery.subquery_binder);
	auto bound_node = std::move(bound_subquery.bound_node);
	LogicalType return_type =
	    expr.subquery_type == SubqueryType::SCALAR ? bound_node->types[0] : LogicalType(LogicalTypeId::BOOLEAN);
	if (return_type.id() == LogicalTypeId::UNKNOWN) {
		return_type = LogicalType::SQLNULL;
	}

	auto result = make_uniq<BoundSubqueryExpression>(return_type);
	if (expr.subquery_type == SubqueryType::ANY) {
		// ANY comparison
		// cast child and subquery child to equivalent types
		D_ASSERT(bound_node->types.size() == 1);
		auto &child = BoundExpression::GetExpression(*expr.child);
		auto child_type = ExpressionBinder::GetExpressionReturnType(*child);
		LogicalType compare_type;
		if (!LogicalType::TryGetMaxLogicalType(context, child_type, bound_node->types[0], compare_type)) {
			throw BinderException(
			    expr, "Cannot compare values of type %s and %s in IN/ANY/ALL clause - an explicit cast is required",
			    child_type.ToString(), bound_node->types[0]);
		}
		child = BoundCastExpression::AddCastToType(context, std::move(child), compare_type);
		result->child_type = bound_node->types[0];
		result->child_target = compare_type;
		result->child = std::move(child);
	}
	result->binder = std::move(subquery_binder);
	result->subquery = std::move(bound_node);
	result->subquery_type = expr.subquery_type;
	result->comparison_type = expr.comparison_type;

	return BindResult(std::move(result));
}

} // namespace duckdb
