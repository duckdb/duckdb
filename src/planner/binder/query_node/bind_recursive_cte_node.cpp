#include "duckdb/common/enums/deprecated_using_key_syntax.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_recursive_cte.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

BoundStatement Binder::BindNode(RecursiveCTENode &statement) {
	// first recursively visit the recursive CTE operations
	// the left side is visited first and is added to the BindContext of the right side
	D_ASSERT(statement.left);
	D_ASSERT(statement.right);

	auto is_using_key = !statement.key_targets.empty();

	if (is_using_key) {
		auto setting = Settings::Get<DeprecatedUsingKeySyntaxSetting>(context);

		// The USING KEY currently implement is actually the UNION ALL variant,
		// but we use UNION syntax. This stands in the way of a possible addition of
		// the UNION variant, so we will deprecate the UNION syntax for now (with
		// the ability to still use it via a setting). Once enough time has elapsed
		// and users have migrated relevant code to using UNION ALL syntax, we can
		// declare the UNION syntax either as illegal syntax or implement the UNION
		// variant proper (again with the ability to override the UNION syntax to
		// use the "old" UNION ALL variant).

		bool warn_deprecated_syntax = setting == DeprecatedUsingKeySyntax::DEFAULT && !statement.union_all;
		const string msg =
		    "Deprecated UNION in USING KEY CTE detected."
		    "Please transition to using UNION ALL, before DuckDB's next release. \n"
		    "Use SET deprecated_using_key_syntax='UNION_AS_UNION_ALL' to enable the deprecated behavior. \n"
		    "For more information, see "
		    "https://duckdb.org/docs/stable/sql/query_syntax/with#recursive-ctes-with-using-key.";

		if (warn_deprecated_syntax) {
			DUCKDB_LOG_WARNING(context, msg);
		}
	}

	auto ctename = statement.ctename;
	auto union_all = statement.union_all;
	auto setop_index = GenerateTableIndex();

	auto left_binder = Binder::CreateBinder(context, this);
	auto left = left_binder->BindNode(*statement.left);

	BoundStatement result;
	// the result types of the CTE are the types of the LHS
	result.types = left.types;
	vector<LogicalType> internal_types = left.types;
	vector<unique_ptr<Expression>> key_targets, payload_aggregates;

	// names are picked from the LHS, unless aliases are explicitly specified
	result.names = left.names;
	for (idx_t i = 0; i < statement.aliases.size() && i < result.names.size(); i++) {
		result.names[i] = statement.aliases[i];
	}

	// This allows the right side to reference the CTE recursively
	bind_context.AddGenericBinding(setop_index, statement.ctename, result.names, result.types);

	// Create temporary binder to bind expressions
	auto aggregate_binder = Binder::CreateBinder(context, nullptr);
	ErrorData error;
	FunctionBinder function_binder(*aggregate_binder);
	aggregate_binder->bind_context.AddGenericBinding(setop_index, statement.ctename, result.names, result.types);
	ExpressionBinder expression_binder(*aggregate_binder, context);

	// Set contains column indices that are already bound
	unordered_set<idx_t> key_references;
	unordered_map<idx_t, unique_ptr<Expression>> payload_references;
	// Temporary copy of return types that we can modify without having a conflict with binding the aggregates
	vector<LogicalType> return_types = result.types;

	// Bind specified keys to the referenced column
	for (idx_t expr_idx = 0; expr_idx < statement.key_targets.size(); expr_idx++) {
		auto &expr = statement.key_targets[expr_idx];

		if (expr->type == ExpressionType::COLUMN_REF) {
			if (expr->HasAlias()) {
				throw BinderException(expr->GetQueryLocation(),
				                      "In USING KEY, only direct calls to an aggregate function can have an alias.");
			}

			auto bound_expr = expression_binder.Bind(expr);
			auto &bound_ref = bound_expr->Cast<BoundColumnRefExpression>();

			idx_t column_index = bound_ref.binding.column_index;
			if (key_references.find(column_index) != key_references.end()) {
				continue;
			}

			key_references.insert(column_index);
			key_targets.push_back(std::move(bound_expr));
		} else if (expr->type == ExpressionType::FUNCTION) {
			auto &func_expr = expr->Cast<FunctionExpression>();

			if (func_expr.filter) {
				throw BinderException(func_expr.filter->GetQueryLocation(),
				                      "FILTER clause is not yet supported for aggregates in USING KEY");
			}

			if (!func_expr.order_bys->orders.empty()) {
				throw BinderException(func_expr.GetQueryLocation(),
				                      "ORDER BY clause is not yet supported for aggregates in USING KEY");
			}

			if (func_expr.distinct) {
				throw BinderException(func_expr.GetQueryLocation(),
				                      "DISTINCT is not yet supported for aggregates in USING KEY");
			}

			QueryErrorContext error_context(expr->GetQueryLocation());

			EntryLookupInfo function_lookup(CatalogType::AGGREGATE_FUNCTION_ENTRY, func_expr.function_name,
			                                error_context);
			auto entry =
			    GetCatalogEntry(func_expr.catalog, DEFAULT_SCHEMA, function_lookup, OnEntryNotFound::RETURN_NULL);

			if (!entry || entry->type != CatalogType::AGGREGATE_FUNCTION_ENTRY) {
				throw BinderException(
				    expr->GetQueryLocation(),
				    "'%s' can't be used in the USING KEY clause.\n"
				    "It has to be either a column name as a key or a direct call to an aggregate function.",
				    expr->ToString());
			}
			auto &func = entry->Cast<AggregateFunctionCatalogEntry>();

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
				auto names_iter = find(result.names.begin(), result.names.end(), func_expr.GetAlias());
				if (names_iter == result.names.end()) {
					throw BinderException(expr->GetQueryLocation(),
					                      "Could not find column with name '%s' to bind aggregate to.",
					                      func_expr.GetAlias());
				}
				aggregate_idx = NumericCast<idx_t>(std::distance(result.names.begin(), names_iter));
			} else {
				if (bound_children.empty() || bound_children[0]->type != ExpressionType::BOUND_COLUMN_REF) {
					// No alias and no way to infer target column through first argument
					throw BinderException(
					    expr->GetQueryLocation(),
					    "In USING KEY, an aggregate must either have a column reference or an alias.");
				}
				aggregate_idx = bound_children[0]->Cast<BoundColumnRefExpression>().binding.column_index;
			}

			// Find the best matching aggregate function
			auto best_function_idx =
			    function_binder.BindFunction(func.name, func.functions, aggregation_input_types, error);
			if (!best_function_idx.IsValid()) {
				throw BinderException("No matching aggregate function\n%s", error.Message());
			}
			// Found a matching function, bind it as an aggregate
			auto best_function = func.functions.GetFunctionByOffset(best_function_idx.GetIndex());
			auto aggregate = function_binder.BindAggregateFunction(std::move(best_function), std::move(bound_children),
			                                                       nullptr, AggregateType::NON_DISTINCT);

			if (payload_references.find(aggregate_idx) != payload_references.end()) {
				throw BinderException(func_expr.GetQueryLocation(),
				                      "Column '%s' referenced multiple times in USING KEY clause.\n"
				                      "Try using an alias for one of the aggregates.",
				                      result.names[aggregate_idx]);
			}

			if (key_references.find(aggregate_idx) != key_references.end()) {
				throw BinderException(func_expr.GetQueryLocation(),
				                      "Column '%s' cannot be used as both key and aggregate in USING KEY clause.\n"
				                      "Try using an alias for the aggregation.",
				                      result.names[aggregate_idx]);
			}

			return_types[aggregate_idx] = aggregate->return_type;
			payload_references[aggregate_idx] = std::move(aggregate);
		} else {
			throw BinderException(
			    expr->GetQueryLocation(),
			    "'%s' can't be used in the USING KEY clause.\n"
			    "It has to be either a column name as a key or a direct call to an aggregate function.",
			    expr->ToString());
		}
	}

	if (key_targets.empty() && !payload_references.empty()) {
		throw BinderException("USING KEY clause requires at least one key column.");
	}

	// Now that we have finished binding all aggregates, we can update the operator types
	result.types = std::move(return_types);

	// If we have key targets, then all the other columns must be aggregated
	if (!key_targets.empty()) {
		// Bind every column that is neither referenced as a key nor by an aggregate to a LAST aggregate
		for (idx_t i = 0; i < left.types.size(); i++) {
			if (key_references.find(i) == key_references.end()) {
				if (payload_references.find(i) == payload_references.end()) {
					// Create a new bound column reference for the missing columns
					vector<unique_ptr<Expression>> first_children;
					auto bound = make_uniq<BoundColumnRefExpression>(result.types[i], ColumnBinding(setop_index, i));
					first_children.push_back(std::move(bound));

					// Create a last aggregate for the newly bound column reference
					auto first_aggregate = function_binder.BindAggregateFunction(
					    LastFunctionGetter::GetFunction(result.types[i]), std::move(first_children), nullptr,
					    AggregateType::NON_DISTINCT);

					payload_aggregates.push_back(std::move(first_aggregate));
				} else {
					payload_aggregates.push_back(std::move(payload_references[i]));
				}
			}
		}
	}

	auto right_binder = Binder::CreateBinder(context, this);

	// Add bindings of left side to temporary CTE bindings context
	BindingAlias cte_alias(statement.ctename);
	right_binder->bind_context.AddCTEBinding(setop_index, std::move(cte_alias), result.names, internal_types);

	BindingAlias recurring_alias("recurring", statement.ctename);
	right_binder->bind_context.AddCTEBinding(setop_index, std::move(recurring_alias), result.names, result.types);

	auto right = right_binder->BindNode(*statement.right);
	for (auto &c : left_binder->correlated_columns) {
		right_binder->AddCorrelatedColumn(c);
	}

	// move the correlated expressions from the child binders to this binder
	MoveCorrelatedExpressions(*left_binder);
	MoveCorrelatedExpressions(*right_binder);

	// now both sides have been bound we can resolve types
	if (left.types.size() != right.types.size()) {
		throw BinderException("Set operations can only apply to expressions with the "
		                      "same number of result columns");
	}

	if (!statement.modifiers.empty()) {
		throw NotImplementedException("FIXME: bind modifiers in recursive CTE");
	}

	// Generate the logical plan for the left and right sides of the set operation
	left_binder->is_outside_flattened = is_outside_flattened;
	right_binder->is_outside_flattened = is_outside_flattened;

	auto left_node = std::move(left.plan);
	auto right_node = std::move(right.plan);

	// check if there are any unplanned subqueries left in either child
	has_unplanned_dependent_joins = has_unplanned_dependent_joins || left_binder->has_unplanned_dependent_joins ||
	                                right_binder->has_unplanned_dependent_joins;

	// for both the left and right sides, cast them to the same types
	left_node = CastLogicalOperatorToTypes(left.types, internal_types, std::move(left_node));
	right_node = CastLogicalOperatorToTypes(right.types, internal_types, std::move(right_node));

	auto recurring_binding = right_binder->GetCTEBinding(BindingAlias("recurring", ctename));
	bool ref_recurring = recurring_binding && recurring_binding->IsReferenced();

	// Check if there is a reference to the recursive or recurring table, if not create a set operator.
	auto cte_binding = right_binder->GetCTEBinding(BindingAlias(ctename));
	bool ref_cte = cte_binding && cte_binding->IsReferenced();
	if (!ref_cte && !ref_recurring) {
		auto root =
		    make_uniq<LogicalSetOperation>(setop_index, result.types.size(), std::move(left_node),
		                                   std::move(right_node), LogicalOperatorType::LOGICAL_UNION, union_all);
		result.plan = std::move(root);
	} else {
		auto root = make_uniq<LogicalRecursiveCTE>(ctename, setop_index, result.types.size(), union_all,
		                                           std::move(key_targets), std::move(left_node), std::move(right_node));
		root->ref_recurring = ref_recurring;
		root->internal_types = std::move(internal_types);
		root->payload_aggregates = std::move(payload_aggregates);
		result.plan = std::move(root);
	}
	return result;
}

} // namespace duckdb
