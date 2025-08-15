#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/query_node/bound_recursive_cte_node.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

namespace duckdb {

unique_ptr<BoundQueryNode> Binder::BindNode(RecursiveCTENode &statement) {
	auto result = make_uniq<BoundRecursiveCTENode>();

	// first recursively visit the recursive CTE operations
	// the left side is visited first and is added to the BindContext of the right side
	D_ASSERT(statement.left);
	D_ASSERT(statement.right);
	if (statement.union_all && !statement.key_targets.empty()) {
		throw BinderException("UNION ALL cannot be used with USING KEY in recursive CTE.");
	}

	result->ctename = statement.ctename;
	result->union_all = statement.union_all;
	result->setop_index = GenerateTableIndex();

	result->left_binder = Binder::CreateBinder(context, this);
	result->left = result->left_binder->BindNode(*statement.left);

	// the result types of the CTE are the types of the LHS
	result->types = result->left->types;
	// names are picked from the LHS, unless aliases are explicitly specified
	result->names = result->left->names;
	for (idx_t i = 0; i < statement.aliases.size() && i < result->names.size(); i++) {
		result->names[i] = statement.aliases[i];
	}

	// This allows the right side to reference the CTE recursively
	bind_context.AddGenericBinding(result->setop_index, statement.ctename, result->names, result->types);

	result->right_binder = Binder::CreateBinder(context, this);

	// Add bindings of left side to temporary CTE bindings context
	// If there is already a binding for the CTE, we need to remove it first
	// as we are binding a CTE currently, we take precendence over the existing binding.
	// This implements the CTE shadowing behavior.
	result->right_binder->bind_context.RemoveCTEBinding(statement.ctename);
	result->right_binder->bind_context.AddCTEBinding(result->setop_index, statement.ctename, result->names,
	                                                 result->types, !statement.key_targets.empty());

	result->right = result->right_binder->BindNode(*statement.right);
	for (auto &c : result->left_binder->correlated_columns) {
		result->right_binder->AddCorrelatedColumn(c);
	}

	// move the correlated expressions from the child binders to this binder
	MoveCorrelatedExpressions(*result->left_binder);
	MoveCorrelatedExpressions(*result->right_binder);

	// bind specified keys to the referenced column
	auto expression_binder = ExpressionBinder(*this, context);
	for (unique_ptr<ParsedExpression> &expr : statement.key_targets) {
		auto bound_expr = expression_binder.Bind(expr);
		D_ASSERT(bound_expr->type == ExpressionType::BOUND_COLUMN_REF);
		result->key_targets.push_back(std::move(bound_expr));
	}

	ErrorData error;
	FunctionBinder function_binder(*this);
	for (auto &expr : statement.payload_aggregates) {
		D_ASSERT(expr->type == ExpressionType::FUNCTION);
		auto &func_expr = expr->Cast<FunctionExpression>();
		D_ASSERT(func_expr.children.size() == 1);
		auto bound_expr = expression_binder.Bind(func_expr.children[0]);

		if(bound_expr->type != ExpressionType::BOUND_COLUMN_REF) {
			throw BinderException("Payload aggregate must be a column reference");
		}

		// Look up the aggregate function in the catalog
		auto &func = Catalog::GetSystemCatalog(context).GetEntry<AggregateFunctionCatalogEntry>(context, DEFAULT_SCHEMA,
																								func_expr.function_name);

		// Find the best matching aggregate function
		auto best_function_idx = function_binder.BindFunction(func.name, func.functions,
		                                                      {bound_expr->return_type}, error);
		if (!best_function_idx.IsValid()) {
			throw BinderException("No matching aggregate function\n%s", error.Message());
		}
		// Found a matching function, bind it as an aggregate
		auto best_function = func.functions.GetFunctionByOffset(best_function_idx.GetIndex());

		// BTODO: this should be easier
		vector<unique_ptr<Expression>> children;
		children.push_back(std::move(bound_expr));
		auto aggregate = function_binder.BindAggregateFunction(std::move(best_function), std::move(children),
		                                                       nullptr, AggregateType::NON_DISTINCT);
		result->payload_aggregates.push_back(std::move(aggregate));
	}

	// now both sides have been bound we can resolve types
	if (result->left->types.size() != result->right->types.size()) {
		throw BinderException("Set operations can only apply to expressions with the "
		                      "same number of result columns");
	}

	if (!statement.modifiers.empty()) {
		throw NotImplementedException("FIXME: bind modifiers in recursive CTE");
	}

	return std::move(result);
}

} // namespace duckdb
