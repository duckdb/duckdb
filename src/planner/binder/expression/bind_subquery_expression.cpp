#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
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

bool TypeIsUnnamedStruct(const LogicalType &type) {
	if (type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	return StructType::IsUnnamed(type);
}

void ExtractSubqueryChildren(unique_ptr<Expression> &child, vector<unique_ptr<Expression>> &result,
                             const vector<LogicalType> &types) {
	// two scenarios
	// Single Expression (standard):
	// x IN (...)
	// Multi-Expression/Struct:
	// (a, b) IN (SELECT ...)
	// the latter has an unnamed struct on the LHS that is created by a "ROW" expression
	auto &return_type = child->return_type;
	if (!TypeIsUnnamedStruct(return_type)) {
		// child is not an unnamed struct
		return;
	}
	if (child->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		// not a function
		return;
	}
	auto &function = child->Cast<BoundFunctionExpression>();
	if (function.function.name != "row") {
		// not "ROW"
		return;
	}
	// we found (a, b, ...) - we can extract all children of this function
	// note that we don't always want to do this
	if (types.size() == 1 && TypeIsUnnamedStruct(types[0]) && function.children.size() != types.size()) {
		// old case: we have an unnamed struct INSIDE the subquery as well
		// i.e. (a, b) IN (SELECT (a, b) ...)
		// unnesting the struct is guaranteed to throw an error - match the structs against each-other instead
		return;
	}
	for (auto &row_child : function.children) {
		result.push_back(std::move(row_child));
	}
}

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
	auto &bound_subquery = expr.subquery->node->Cast<BoundSubqueryNode>();
	vector<unique_ptr<Expression>> child_expressions;
	if (expr.subquery_type != SubqueryType::EXISTS) {
		idx_t expected_columns = 1;
		if (expr.child) {
			auto &child = BoundExpression::GetExpression(*expr.child);
			ExtractSubqueryChildren(child, child_expressions, bound_subquery.bound_node->types);
			if (child_expressions.empty()) {
				child_expressions.push_back(std::move(child));
			}
			expected_columns = child_expressions.size();
		}
		if (bound_subquery.bound_node->types.size() != expected_columns) {
			throw BinderException(expr, "Subquery returns %zu columns - expected %d",
			                      bound_subquery.bound_node->types.size(), expected_columns);
		}
	}
	// both binding the child and binding the subquery was successful
	D_ASSERT(expr.subquery->node->type == QueryNodeType::BOUND_SUBQUERY_NODE);
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
		for (idx_t child_idx = 0; child_idx < child_expressions.size(); child_idx++) {
			auto &child = child_expressions[child_idx];
			auto child_type = ExpressionBinder::GetExpressionReturnType(*child);
			auto &subquery_type = bound_node->types[child_idx];
			LogicalType compare_type;
			if (!LogicalType::TryGetMaxLogicalType(context, child_type, subquery_type, compare_type)) {
				throw BinderException(
				    expr, "Cannot compare values of type %s and %s in IN/ANY/ALL clause - an explicit cast is required",
				    child_type.ToString(), subquery_type);
			}
			child = BoundCastExpression::AddCastToType(context, std::move(child), compare_type);
			result->child_types.push_back(subquery_type);
			result->child_targets.push_back(compare_type);
			result->children.push_back(std::move(child));
		}
	}
	result->binder = std::move(subquery_binder);
	result->subquery = std::move(bound_node);
	result->subquery_type = expr.subquery_type;
	result->comparison_type = expr.comparison_type;

	return BindResult(std::move(result));
}

} // namespace duckdb
