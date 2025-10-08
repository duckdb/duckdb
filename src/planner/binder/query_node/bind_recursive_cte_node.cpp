#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/query_node/bound_recursive_cte_node.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"

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
	result->internal_types = result->left->types;

	// names are picked from the LHS, unless aliases are explicitly specified
	result->names = result->left->names;
	for (idx_t i = 0; i < statement.aliases.size() && i < result->names.size(); i++) {
		result->names[i] = statement.aliases[i];
	}

	// This allows the right side to reference the CTE recursively
	bind_context.AddGenericBinding(result->setop_index, statement.ctename, result->names, result->types);

	// Create temporary binder to bind expressions
	auto bin = Binder::CreateBinder(context, nullptr);
	ErrorData error;
	FunctionBinder function_binder(*bin);
	bin->bind_context.AddGenericBinding(result->setop_index, statement.ctename, result->names, result->types);
	ExpressionBinder expression_binder(*bin, context);

	// Set contains column indices that are already bound
	unordered_set<idx_t> column_references;
	// Temporary copy of return types that we can modify without having a conflict with binding the aggregates
	vector<LogicalType> return_types = result->types;

	// Bind specified keys to the referenced column
	for (unique_ptr<ParsedExpression> &expr : statement.key_targets) {
		auto bound_expr = expression_binder.Bind(expr);
		D_ASSERT(bound_expr->type == ExpressionType::BOUND_COLUMN_REF);
		auto &bound_ref = bound_expr->Cast<BoundColumnRefExpression>();

		idx_t column_index = bound_ref.binding.column_index;
		if (column_references.find(column_index) != column_references.end()) {
			throw BinderException(bound_ref.GetQueryLocation(),
			                      "Column '%s' referenced multiple times in recursive CTE aggregates",
			                      result->names[column_index]);
		}

		column_references.insert(column_index);
		result->key_targets.push_back(std::move(bound_expr));
	}

	// Bind user-defined aggregates
	for (idx_t payload_idx = 0; payload_idx < statement.payload_aggregates.size(); payload_idx++) {
		auto &expr = statement.payload_aggregates[payload_idx];
		D_ASSERT(expr->type == ExpressionType::FUNCTION);
		auto &func_expr = expr->Cast<FunctionExpression>();

		// Look up the aggregate function in the catalog
		auto &func = Catalog::GetSystemCatalog(context).GetEntry<AggregateFunctionCatalogEntry>(
		    context, DEFAULT_SCHEMA, func_expr.function_name);
		vector<LogicalType> aggregation_input_types;
		vector<unique_ptr<Expression>> bound_children;
		// Bind the children of the aggregate function
		for (auto &child : func_expr.children) {
			auto bound_child = expression_binder.Bind(child);
			aggregation_input_types.push_back(bound_child->return_type);
			bound_children.push_back(std::move(bound_child));
		}

		idx_t aggregate_idx;
		// If user provided an alias, prioritize that.
		// Otherwise, we try to infer the target column from the first argument
		if (func_expr.HasAlias()) {
			auto names_iter = find(result->names.begin(), result->names.end(), func_expr.GetAlias());
			if (names_iter == result->names.end()) {
				throw BinderException(expr->GetQueryLocation(),
				                      "Could not find column with name '%s' to bind aggregate to.",
				                      func_expr.GetAlias());
			}
			aggregate_idx = NumericCast<idx_t>(std::distance(result->names.begin(), names_iter));
			// Create a new bound column reference for the target column
			result->payload_aggregate_dest_map[payload_idx] = make_uniq<BoundColumnRefExpression>(
			    result->types[aggregate_idx], ColumnBinding(result->setop_index, aggregate_idx));
		} else {
			if (bound_children[0]->type != ExpressionType::BOUND_COLUMN_REF) {
				// No alias and no way to infer target column through first argument
				throw BinderException(expr->GetQueryLocation(),
				                      "An aggregate must either have a column reference or an alias.");
			}
			aggregate_idx = bound_children[0]->Cast<BoundColumnRefExpression>().binding.column_index;
		}

		// Find the best matching aggregate function
		auto best_function_idx =
		    function_binder.BindFunction(func.name, func.functions, std::move(aggregation_input_types), error);
		if (!best_function_idx.IsValid()) {
			throw BinderException("No matching aggregate function\n%s", error.Message());
		}
		// Found a matching function, bind it as an aggregate
		auto best_function = func.functions.GetFunctionByOffset(best_function_idx.GetIndex());
		auto aggregate = function_binder.BindAggregateFunction(std::move(best_function), std::move(bound_children),
		                                                       nullptr, AggregateType::NON_DISTINCT);

		if (column_references.find(aggregate_idx) != column_references.end()) {
			throw BinderException(func_expr.GetQueryLocation(),
			                      "Column '%s' referenced multiple times in recursive CTE aggregates",
			                      result->names[aggregate_idx]);
		}

		return_types[aggregate_idx] = aggregate->return_type;
		result->payload_aggregates.push_back(std::move(aggregate));
		column_references.insert(aggregate_idx);
	}

	// Now that we have finished binding all aggregates, we can update the operator types
	result->types = std::move(return_types);

	// If we have key targets, then all the other columns must be aggregated
	if (!result->key_targets.empty()) {
		// Bind every column that is neither referenced as a key nor by an aggregate to a LAST aggregate
		for (idx_t i = 0; i < result->left->types.size(); i++) {
			if (column_references.find(i) == column_references.end()) {
				// Create a new bound column reference for the missing columns
				vector<unique_ptr<Expression>> first_children;
				auto bound =
				    make_uniq<BoundColumnRefExpression>(result->types[i], ColumnBinding(result->setop_index, i));
				first_children.push_back(std::move(bound));

				// Create a last aggregate for the newly bound column reference
				auto first_aggregate = function_binder.BindAggregateFunction(
				    LastFunctionGetter::GetFunction(result->types[i]), std::move(first_children), nullptr,
				    AggregateType::NON_DISTINCT);

				result->payload_aggregates.push_back(std::move(first_aggregate));
			}
		}
	}

	result->right_binder = Binder::CreateBinder(context, this);

	// Add bindings of left side to temporary CTE bindings context
	// If there is already a binding for the CTE, we need to remove it first
	// as we are binding a CTE currently, we take precedence over the existing binding.
	// This implements the CTE shadowing behavior.
	result->right_binder->bind_context.RemoveCTEBinding(statement.ctename);
	result->right_binder->bind_context.AddCTEBinding(result->setop_index, statement.ctename, result->names,
	                                                 result->internal_types, result->types,
	                                                 !statement.key_targets.empty());

	result->right = result->right_binder->BindNode(*statement.right);
	for (auto &c : result->left_binder->correlated_columns) {
		result->right_binder->AddCorrelatedColumn(c);
	}

	// move the correlated expressions from the child binders to this binder
	MoveCorrelatedExpressions(*result->left_binder);
	MoveCorrelatedExpressions(*result->right_binder);

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
