#include "duckdb/optimizer/remote_pushdown_optimizer.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

CatalogPushdownResult::CatalogPushdownResult(CatalogReferenceType reference_type_p) : reference_type(reference_type_p) {
}

CatalogPushdownResult CatalogPushdownResult::Unknown() {
	return CatalogPushdownResult(CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE);
}

CatalogPushdownResult CatalogPushdownResult::NoCatalogReference() {
	return CatalogPushdownResult(CatalogReferenceType::NO_CATALOG_REFERENCED);
}

CatalogPushdownResult CatalogPushdownResult::RemoteReference(Catalog &catalog) {
	CatalogPushdownResult result(CatalogReferenceType::SINGLE_REMOTE_CATALOG);
	result.catalog = catalog;
	return result;
}

RemotePushdownOptimizer::RemotePushdownOptimizer(Binder &binder)
    : binder(binder), owned_pushdown_state(make_uniq<RemotePushdownState>()), pushdown_state(*owned_pushdown_state) {
}

RemotePushdownOptimizer::RemotePushdownOptimizer(optional_ptr<RemotePushdownOptimizer> parent_p)
    : binder(parent_p->binder), parent(parent_p), pushdown_state(parent->pushdown_state) {
	// inherit table / column names from parent (for correlated subquery detection)
	local_table_names = parent->local_table_names;
}

void RemotePushdownOptimizer::FindRemoteCatalogsInSearchPath() {
	if (pushdown_state.search_path_initialized) {
		return;
	}
	pushdown_state.search_path_initialized = true;
	auto &client_data = ClientData::Get(binder.context);
	// iterate over all catalogs mentioned in the search path and check if they are remote
	auto search_path = client_data.catalog_search_path->Get();
	// Deduplicate by catalog name.
	identifier_set_t seen_remote_catalogs;
	for (auto &entry : search_path) {
		auto catalog_entry = Catalog::GetCatalogEntry(binder.context, entry.catalog);
		if (!catalog_entry) {
			continue;
		}
		if (!catalog_entry->Supports(RemoteCapability::EXECUTE_QUERY_NODE)) {
			pushdown_state.local_catalogs_in_search_path.push_back(entry);
		} else {
			if (seen_remote_catalogs.insert(catalog_entry->GetName()).second) {
				pushdown_state.remote_catalogs_in_search_path.push_back(*catalog_entry);
			}
		}
	}
}

CatalogPushdownResult RemotePushdownOptimizer::Merge(CatalogPushdownResult a, CatalogPushdownResult b) {
	if (a.reference_type == CatalogReferenceType::NO_CATALOG_REFERENCED &&
	    b.reference_type == CatalogReferenceType::NO_CATALOG_REFERENCED) {
		// both sides refer to no catalog - result is no catalog reference, but with unified references
		auto result = CatalogPushdownResult::NoCatalogReference();
		result.used_expressions.insert(result.used_expressions.end(), a.used_expressions.begin(),
		                               a.used_expressions.end());
		result.used_expressions.insert(result.used_expressions.end(), b.used_expressions.begin(),
		                               b.used_expressions.end());
		result.used_table_constructs.insert(result.used_table_constructs.end(), a.used_table_constructs.begin(),
		                                    a.used_table_constructs.end());
		result.used_table_constructs.insert(result.used_table_constructs.end(), b.used_table_constructs.begin(),
		                                    b.used_table_constructs.end());
		result.used_nodes.insert(result.used_nodes.end(), a.used_nodes.begin(), a.used_nodes.end());
		result.used_nodes.insert(result.used_nodes.end(), b.used_nodes.begin(), b.used_nodes.end());
		return result;
	}
	if (a.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG &&
	    b.reference_type == CatalogReferenceType::NO_CATALOG_REFERENCED) {
		// swap "a" and "b" so the merge happens below
		return Merge(b, a);
	}
	if (a.reference_type == CatalogReferenceType::NO_CATALOG_REFERENCED &&
	    b.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		// "a" refers to no catalog, "b" refers to a single remote catalog
		// check if "b" supports all constructs referenced in "a"
		auto &remote_catalog = *b.catalog;
		for (auto &expr : a.used_expressions) {
			if (!remote_catalog.SupportsPushdown(expr.get())) {
				// pushdown not supported - result is UNKNOWN_CATALOG_REFERENCE
				return CatalogPushdownResult::Unknown();
			}
		}
		for (auto &table_construct : a.used_table_constructs) {
			if (!remote_catalog.SupportsPushdown(table_construct.get())) {
				// pushdown not supported - result is UNKNOWN_CATALOG_REFERENCE
				return CatalogPushdownResult::Unknown();
			}
		}
		for (auto &query_node : a.used_nodes) {
			if (!remote_catalog.SupportsPushdown(query_node.get())) {
				// pushdown not supported - result is UNKNOWN_CATALOG_REFERENCE
				return CatalogPushdownResult::Unknown();
			}
		}
		return b;
	}
	if (a.reference_type == CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE ||
	    b.reference_type == CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE) {
		return CatalogPushdownResult::Unknown();
	}
	// Both are SINGLE_REMOTE_CATALOG - only valid if they refer to the same catalog
	if (a.catalog == b.catalog) {
		return a;
	}
	return CatalogPushdownResult::Unknown();
}

void RemotePushdownOptimizer::Rewrite(unique_ptr<SQLStatement> &statement) {
	CatalogPushdownResult result;
	switch (statement->type) {
	case StatementType::SELECT_STATEMENT:
		result = Rewrite(*statement->Cast<SelectStatement>().node);
		break;
	case StatementType::INSERT_STATEMENT:
		result = Rewrite(*statement->Cast<InsertStatement>().node);
		break;
	case StatementType::DELETE_STATEMENT:
		result = Rewrite(*statement->Cast<DeleteStatement>().node);
		break;
	case StatementType::UPDATE_STATEMENT:
		result = Rewrite(*statement->Cast<UpdateStatement>().node);
		break;
	case StatementType::EXPLAIN_STATEMENT:
		Rewrite(statement->Cast<ExplainStatement>().stmt);
		return;
	default:
		return;
	}
	FinishPushdown(statement, result);
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(QueryNode &node) {
	if (!cte_results.empty()) {
		throw InternalException(
		    "RemotePushdownOptimizer already has CTEs defined - this means no child was created correctly");
	}
	for (auto &cte_pair : node.cte_map.map) {
		const Identifier &cte_name = cte_pair.first;
		auto &cte_info = *cte_pair.second;
		CatalogPushdownResult cte_result;
		if (cte_info.query_node) {
			RemotePushdownOptimizer child_optimizer(this);
			cte_result = child_optimizer.Rewrite(*cte_info.query_node);
		} else {
			cte_result = CatalogPushdownResult::Unknown();
		}
		for (auto &key : cte_info.key_targets) {
			cte_result = Merge(cte_result, Rewrite(key));
		}
		cte_results[cte_name] = cte_result;
	}
	CatalogPushdownResult result;
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		result = RewriteNode(node.Cast<SelectNode>());
		break;
	case QueryNodeType::INSERT_QUERY_NODE:
		result = RewriteNode(node.Cast<InsertQueryNode>());
		break;
	case QueryNodeType::DELETE_QUERY_NODE:
		result = RewriteNode(node.Cast<DeleteQueryNode>());
		break;
	case QueryNodeType::UPDATE_QUERY_NODE:
		result = RewriteNode(node.Cast<UpdateQueryNode>());
		break;
	case QueryNodeType::SET_OPERATION_NODE:
		result = RewriteNode(node.Cast<SetOperationNode>());
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE:
		result = RewriteNode(node.Cast<RecursiveCTENode>());
		break;
	default:
		return CatalogPushdownResult::Unknown();
	}
	// Merge results of all CTEs defined in this scope
	// FIXME: this is only necessary because we push all CTEs, including unreferenced ones, to the result
	// if we pruned unreferenced CTEs we could remove this
	for (auto &cte_pair : node.cte_map.map) {
		auto it = cte_results.find(cte_pair.first);
		if (it != cte_results.end()) {
			result = Merge(result, it->second);
		}
	}
	if (result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		if (!result.catalog->SupportsPushdown(node)) {
			// bail - referenced catalog does not support pushing down this node type
			result = CatalogPushdownResult::Unknown();
		}
	} else if (result.reference_type == CatalogReferenceType::NO_CATALOG_REFERENCED) {
		result.used_nodes.push_back(node);
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::RewriteNode(RecursiveCTENode &node) {
	RemotePushdownOptimizer left_optimizer(this);
	CatalogPushdownResult left_result = left_optimizer.Rewrite(*node.left);

	// for recursive CTEs - the right-hand side of the CTE can refer to the recursive CTE itself
	// we use whatever the CatalogPushdownResult of the LHS was to count this reference
	RemotePushdownOptimizer recursive_optimizer(this);
	recursive_optimizer.cte_results[node.ctename] = left_result;

	RemotePushdownOptimizer right_optimizer(&recursive_optimizer);
	CatalogPushdownResult right_result = right_optimizer.Rewrite(*node.right);

	auto result = Merge(left_result, right_result);
	for (auto &key : node.key_targets) {
		result = Merge(result, Rewrite(key));
	}
	for (auto &modifier : node.modifiers) {
		switch (modifier->type) {
		case ResultModifierType::ORDER_MODIFIER: {
			auto &order_mod = modifier->Cast<OrderModifier>();
			for (auto &order : order_mod.orders) {
				// ORDER BY entries cannot be constant-folded - a bare integer literal is a
				// positional reference there
				result = Merge(result, Rewrite(order.expression, ExpressionFoldingMode::FOLD_CHILDREN_ONLY));
			}
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &limit_mod = modifier->Cast<LimitModifier>();
			if (limit_mod.limit) {
				result = Merge(result, Rewrite(limit_mod.limit));
			}
			if (limit_mod.offset) {
				result = Merge(result, Rewrite(limit_mod.offset));
			}
			break;
		}
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct_mod = modifier->Cast<DistinctModifier>();
			for (auto &expr : distinct_mod.distinct_on_targets) {
				// DISTINCT ON entries cannot be constant-folded - a bare integer literal is a
				// positional reference there
				result = Merge(result, Rewrite(expr, ExpressionFoldingMode::FOLD_CHILDREN_ONLY));
			}
			break;
		}
		default:
			break;
		}
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::RewriteNode(SelectNode &node) {
	auto from_result = CatalogPushdownResult::NoCatalogReference();
	if (node.from_table) {
		from_result = Rewrite(node.from_table);
	}

	// Merge from_table result with all expressions to determine if the whole node can be pushed
	CatalogPushdownResult result = from_result;
	for (auto &expr : node.select_list) {
		result = Merge(result, Rewrite(expr));
	}
	if (node.where_clause) {
		result = Merge(result, Rewrite(node.where_clause));
	}
	for (auto &expr : node.groups.group_expressions) {
		// GROUP BY entries cannot be constant-folded - a bare integer literal is a
		// positional reference there
		result = Merge(result, Rewrite(expr, ExpressionFoldingMode::FOLD_CHILDREN_ONLY));
	}
	if (node.having) {
		result = Merge(result, Rewrite(node.having));
	}
	if (node.qualify) {
		result = Merge(result, Rewrite(node.qualify));
	}
	for (auto &modifier : node.modifiers) {
		switch (modifier->type) {
		case ResultModifierType::ORDER_MODIFIER: {
			auto &order_mod = modifier->Cast<OrderModifier>();
			for (auto &order : order_mod.orders) {
				// ORDER BY entries cannot be constant-folded - a bare integer literal is a
				// positional reference there
				result = Merge(result, Rewrite(order.expression, ExpressionFoldingMode::FOLD_CHILDREN_ONLY));
			}
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &limit_mod = modifier->Cast<LimitModifier>();
			if (limit_mod.limit) {
				result = Merge(result, Rewrite(limit_mod.limit));
			}
			if (limit_mod.offset) {
				result = Merge(result, Rewrite(limit_mod.offset));
			}
			break;
		}
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct_mod = modifier->Cast<DistinctModifier>();
			for (auto &expr : distinct_mod.distinct_on_targets) {
				// DISTINCT ON entries cannot be constant-folded - a bare integer literal is a
				// positional reference there
				result = Merge(result, Rewrite(expr, ExpressionFoldingMode::FOLD_CHILDREN_ONLY));
			}
			break;
		}
		default:
			break;
		}
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::RewriteNode(InsertQueryNode &node) {
	// first bind the target table for the insert
	BaseTableRef target_ref;
	target_ref.catalog_name = node.catalog;
	target_ref.schema_name = node.schema;
	target_ref.table_name = node.table;

	RemotePushdownOptimizer target_optimizer(this);
	auto result = target_optimizer.Rewrite(target_ref);
	if (node.select_statement) {
		RemotePushdownOptimizer select_optimizer(this);
		auto select_result = select_optimizer.Rewrite(*node.select_statement->node);
		result = Merge(result, select_result);
		if (select_result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
			bool push_select_only = result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG;
			if (!push_select_only && !result.catalog->SupportsPushdown(node)) {
				// the catalog cannot execute the INSERT itself remotely - push down only the SELECT part
				push_select_only = true;
			}
			if (push_select_only) {
				FinishPushdown(node.select_statement->node, select_result);
			}
		}
	}
	if (node.on_conflict_info) {
		if (node.on_conflict_info->condition) {
			auto condition_result = Rewrite(node.on_conflict_info->condition);
			result = Merge(result, condition_result);
		}
		if (node.on_conflict_info->set_info) {
			if (node.on_conflict_info->set_info->condition) {
				auto condition_result = Rewrite(node.on_conflict_info->set_info->condition);
				result = Merge(result, condition_result);
			}
			for (auto &expr : node.on_conflict_info->set_info->expressions) {
				auto expr_result = Rewrite(expr);
				result = Merge(result, expr_result);
			}
		}
	}
	for (auto &expr : node.returning_list) {
		auto expr_result = Rewrite(expr);
		result = Merge(result, expr_result);
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::RewriteNode(DeleteQueryNode &node) {
	auto result = Rewrite(node.table);
	vector<CatalogPushdownResult> using_results;
	for (auto &using_clause : node.using_clauses) {
		auto using_result = Rewrite(using_clause);
		using_results.push_back(using_result);
		result = Merge(result, using_result);
	}

	if (node.condition) {
		auto condition_result = Rewrite(node.condition);
		result = Merge(result, condition_result);
	}
	for (auto &expr : node.returning_list) {
		auto expr_result = Rewrite(expr);
		result = Merge(result, expr_result);
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::RewriteNode(UpdateQueryNode &node) {
	auto result = Rewrite(node.table);
	auto from_result = CatalogPushdownResult::NoCatalogReference();
	if (node.from_table) {
		from_result = Rewrite(node.from_table);
		result = Merge(result, from_result);
	}

	if (node.set_info) {
		if (node.set_info->condition) {
			auto condition_result = Rewrite(node.set_info->condition);
			result = Merge(result, condition_result);
		}

		for (auto &expr : node.set_info->expressions) {
			auto expr_result = Rewrite(expr);
			result = Merge(result, expr_result);
		}
	}
	for (auto &expr : node.returning_list) {
		auto expr_result = Rewrite(expr);
		result = Merge(result, expr_result);
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::RewriteNode(SetOperationNode &node) {
	// Rewrite each child independently so we can push down individual children if needed
	vector<CatalogPushdownResult> child_results;
	child_results.reserve(node.children.size());
	auto result = CatalogPushdownResult::NoCatalogReference();
	for (auto &child : node.children) {
		RemotePushdownOptimizer child_optimizer(this);
		auto child_result = child_optimizer.Rewrite(*child);
		result = Merge(result, child_result);
		child_results.push_back(child_result);
	}

	// Check result modifiers (ORDER BY / LIMIT on the set operation itself)
	bool has_expression_modifiers = false;
	for (auto &modifier : node.modifiers) {
		switch (modifier->type) {
		case ResultModifierType::ORDER_MODIFIER: {
			auto &order_mod = modifier->Cast<OrderModifier>();
			for (auto &order : order_mod.orders) {
				// ORDER BY entries cannot be constant-folded - a bare integer literal is a
				// positional reference there
				result = Merge(result, Rewrite(order.expression, ExpressionFoldingMode::FOLD_CHILDREN_ONLY));
				has_expression_modifiers = true;
			}
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &limit_mod = modifier->Cast<LimitModifier>();
			if (limit_mod.limit) {
				result = Merge(result, Rewrite(limit_mod.limit));
				has_expression_modifiers = true;
			}
			if (limit_mod.offset) {
				result = Merge(result, Rewrite(limit_mod.offset));
				has_expression_modifiers = true;
			}
			break;
		}
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct_mod = modifier->Cast<DistinctModifier>();
			for (auto &expr : distinct_mod.distinct_on_targets) {
				// DISTINCT ON entries cannot be constant-folded - a bare integer literal is a
				// positional reference there
				result = Merge(result, Rewrite(expr, ExpressionFoldingMode::FOLD_CHILDREN_ONLY));
				has_expression_modifiers = true;
			}
			break;
		}
		default:
			break;
		}
	}
	// If the whole set operation resolves to a single remote catalog, propagate upward.
	if (result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		return result;
	}
	if (has_expression_modifiers) {
		// if the set operation has any modifiers (e.g. ORDER BY <expr>) then binding can go wrong if we do a pushdown
		// into children, since we might have something like SELECT i + 1 FROM remote UNION ALL ... ORDER BY i + 1
		// this requires "peeking into" the child query to figure out that the expressions match
		// for now just be safe and skip pushdown into individual queries in this scenario
		return result;
	}
	for (idx_t i = 0; i < node.children.size(); i++) {
		FinishPushdown(node.children[i], child_results[i]);
	}
	return result;
}

void RemotePushdownOptimizer::TrackLocalTable(const TableRef &ref) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE:
		TrackLocalTable(ref.Cast<BaseTableRef>());
		break;
	case TableReferenceType::TABLE_FUNCTION:
		TrackLocalTable(ref.Cast<TableFunctionRef>());
		break;
	case TableReferenceType::SUBQUERY:
		TrackLocalTable(ref.Cast<SubqueryRef>());
		break;
	default:
		break;
	}
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(unique_ptr<TableRef> &ref) {
	CatalogPushdownResult result;
	switch (ref->type) {
	case TableReferenceType::BASE_TABLE:
		result = Rewrite(ref->Cast<BaseTableRef>());
		break;
	case TableReferenceType::JOIN:
		result = Rewrite(ref->Cast<JoinRef>());
		break;
	case TableReferenceType::SUBQUERY:
		result = Rewrite(ref->Cast<SubqueryRef>());
		break;
	case TableReferenceType::EXPRESSION_LIST:
		result = Rewrite(ref->Cast<ExpressionListRef>());
		break;
	case TableReferenceType::TABLE_FUNCTION:
		result = Rewrite(ref->Cast<TableFunctionRef>());
		break;
	case TableReferenceType::EMPTY_FROM:
	case TableReferenceType::COLUMN_DATA:
		result = CatalogPushdownResult::NoCatalogReference();
		break;
	default:
		return CatalogPushdownResult::Unknown();
	}
	if (result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		// the table reference is fully remote - check if the remote catalog supports pushing it down
		// (e.g. DuckDB-specific join types or TABLESAMPLE clauses cannot be sent to most remotes)
		if (!result.catalog->SupportsPushdown(*ref)) {
			TrackLocalTable(*ref);
			return CatalogPushdownResult::Unknown();
		}
	} else if (result.reference_type == CatalogReferenceType::NO_CATALOG_REFERENCED) {
		// record the table reference so a remote catalog can veto it during a later merge
		result.used_table_constructs.push_back(*ref);
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(ExpressionListRef &ref) {
	auto result = CatalogPushdownResult::NoCatalogReference();
	for (auto &row : ref.values) {
		for (auto &expr : row) {
			result = Merge(result, Rewrite(expr));
		}
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(SubqueryRef &ref) {
	RemotePushdownOptimizer child_binder(this);
	auto result = child_binder.Rewrite(*ref.subquery->node);
	if (result.reference_type == CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE) {
		TrackLocalTable(ref);
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::RewriteTableFunctionOnly(TableFunctionRef &ref) {
	if (ref.function->GetExpressionClass() != ExpressionClass::FUNCTION) {
		throw InternalException("RemotePushdownOptimizer: TableFunctionRef does not hold a function expression");
	}
	auto &func_expr = ref.function->Cast<FunctionExpression>();

	// Figure out
	Identifier catalog_name = func_expr.Catalog();
	Identifier schema_name = func_expr.Schema();
	Binder::BindSchemaOrCatalog(binder.context, catalog_name, schema_name);

	// If the function has an explicit catalog prefix, check if it's remote
	if (!catalog_name.empty()) {
		auto catalog = Catalog::GetCatalogEntry(binder.context, catalog_name);
		if (catalog && catalog->Supports(RemoteCapability::EXECUTE_QUERY_NODE) && catalog->SupportsPushdown(ref)) {
			// "catalog" is remote and we can pushdown this function
			return CatalogPushdownResult::RemoteReference(*catalog);
		}
		// catalog was not found or catalog does not support pushdown - bail on pushdown for now
		TrackLocalTable(ref);
		return CatalogPushdownResult::Unknown();
	}

	// we have an unqualified table function
	// this function can either live in a local / system catalog, or in a remote (if it is in the search path)
	// check the search path
	FindRemoteCatalogsInSearchPath();
	EntryLookupInfo func_lookup(CatalogType::TABLE_FUNCTION_ENTRY, func_expr.FunctionName());
	for (auto &local_entry : pushdown_state.local_catalogs_in_search_path) {
		const Identifier &schema = schema_name.empty() ? local_entry.schema : schema_name;
		auto entry =
		    Catalog::GetEntry(binder.context, local_entry.catalog, schema, func_lookup, OnEntryNotFound::RETURN_NULL);
		if (entry && entry->type == CatalogType::TABLE_FUNCTION_ENTRY) {
			auto &tf_entry = entry->Cast<TableFunctionCatalogEntry>();
			bool is_set_returning = false;
			for (auto &func : tf_entry.functions.functions) {
				if (func.return_type == TableFunctionReturnType::SET_RETURNING_FUNCTION) {
					is_set_returning = true;
					break;
				}
			}
			if (!is_set_returning) {
				// TABLE_RETURNING_FUNCTION - blocks pushdown; track alias so correlated
				// refs from nested lateral subqueries are detected
				TrackLocalTable(ref);
				return CatalogPushdownResult::Unknown();
			}
			// SET_RETURNING_FUNCTION: neutral, recurse into args
			// the generic TableRef dispatch records the function so a remote catalog can veto it
			auto result = CatalogPushdownResult::NoCatalogReference();
			for (auto &arg : func_expr.GetArgumentsMutable()) {
				result = Merge(result, Rewrite(arg.GetExpressionMutable()));
			}
			return result;
		}
	}
	// we did not find the table function in a local catalog
	if (pushdown_state.remote_catalogs_in_search_path.size() == 1) {
		// if we have a single catalog in the remote search path - assume the function lives there
		auto &remote_catalog = pushdown_state.remote_catalogs_in_search_path[0].get();
		if (remote_catalog.Supports(RemoteCapability::EXECUTE_QUERY_NODE) && remote_catalog.SupportsPushdown(ref)) {
			// "catalog" is remote and we can pushdown this function
			return CatalogPushdownResult::RemoteReference(remote_catalog);
		}
	}
	// we couldn't find the function locally or remotely - skip pushing down
	TrackLocalTable(ref);
	return CatalogPushdownResult::Unknown();
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(TableFunctionRef &ref) {
	// rewrite the table function only
	auto result = RewriteTableFunctionOnly(ref);
	if (result.reference_type == CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE) {
		// don't bother recursing - we can never pushdown
		return result;
	}
	// recurse into the function arguments
	auto &func_expr = ref.function->Cast<FunctionExpression>();
	for (auto &arg : func_expr.GetArgumentsMutable()) {
		result = Merge(result, Rewrite(arg.GetExpressionMutable()));
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(JoinRef &ref) {
	auto left_result = Rewrite(ref.left);

	// the right side of a join can be correlated to the left side - use a child optimizer to track this
	RemotePushdownOptimizer child_optimizer(this);
	auto right_result = child_optimizer.Rewrite(ref.right);

	auto result = Merge(left_result, right_result);
	// Also analyze the join condition - it may contain subqueries or local macro calls
	// that affect whether the join can be pushed as a whole.
	if (ref.condition) {
		result = Merge(result, Rewrite(ref.condition));
	}
	return result;
}

void RemotePushdownOptimizer::TrackLocalTable(const BaseTableRef &ref) {
	if (!ref.alias.empty()) {
		local_table_names.insert(ref.alias);
	} else {
		local_table_names.insert(ref.table_name);
	}
}

void RemotePushdownOptimizer::TrackLocalTable(const TableFunctionRef &ref) {
	if (!ref.alias.empty()) {
		local_table_names.insert(ref.alias);
	} else {
		local_table_names.insert(ref.function->Cast<FunctionExpression>().FunctionName());
	}
}

void RemotePushdownOptimizer::TrackLocalTable(const SubqueryRef &ref) {
	if (!ref.alias.empty()) {
		local_table_names.insert(ref.alias);
	} else {
		local_table_names.insert("unnamed_subquery");
	}
}

bool RemotePushdownOptimizer::RefersToCTE(const Identifier &cte_name, CatalogPushdownResult &result) const {
	auto entry = cte_results.find(cte_name);
	if (entry != cte_results.end()) {
		result = entry->second;
		return true;
	}
	if (parent) {
		return parent->RefersToCTE(cte_name, result);
	}
	return false;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(BaseTableRef &ref) {
	// Resolve schema_name-as-catalog ambiguity using the binder's own resolution logic
	Identifier catalog_name = ref.catalog_name;
	Identifier schema_name = ref.schema_name;
	Binder::BindSchemaOrCatalog(binder.context, catalog_name, schema_name);

	// Case 0: check if this is a CTE reference (must have no explicit catalog/schema)
	if (catalog_name.empty() && schema_name.empty()) {
		CatalogPushdownResult pushdown_result;
		if (RefersToCTE(ref.table_name, pushdown_result)) {
			if (pushdown_result.reference_type == CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE) {
				// Local/unknown CTE - track as local for correlated subquery detection
				TrackLocalTable(ref);
			}
			return pushdown_result;
		}
	}

	// Case 1: catalog is explicitly specified - check if it's a remote catalog
	if (!catalog_name.empty()) {
		auto catalog = Catalog::GetCatalogEntry(binder.context, catalog_name);
		if (catalog && catalog->Supports(RemoteCapability::EXECUTE_QUERY_NODE)) {
			// verify the table actually exists in the remote catalog - if it does not, fall back
			// to the binder so it can report a proper error message
			EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, ref.table_name);
			const auto &schema = schema_name.empty() ? Identifier(DEFAULT_SCHEMA) : schema_name;
			auto entry = Catalog::GetEntry(binder.context, catalog->GetName(), schema, table_lookup,
			                               OnEntryNotFound::RETURN_NULL);
			if (!entry) {
				TrackLocalTable(ref);
				return CatalogPushdownResult::Unknown();
			}
			return CatalogPushdownResult::RemoteReference(*catalog);
		}
		// A local table always blocks pushdown of any query that contains it.
		// Returning UNKNOWN (not NO_CATALOG) ensures Merge(SINGLE_REMOTE, UNKNOWN) = UNKNOWN
		// rather than the otherwise-neutral SINGLE_REMOTE.
		TrackLocalTable(ref);
		return CatalogPushdownResult::Unknown();
	}

	// Case 2: no explicit catalog - lazily populate search path catalogs on first use
	FindRemoteCatalogsInSearchPath();

	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, ref.table_name);

	if (pushdown_state.remote_catalogs_in_search_path.size() != 1) {
		TrackLocalTable(ref);
		return CatalogPushdownResult::Unknown();
	}

	for (auto &local_entry : pushdown_state.local_catalogs_in_search_path) {
		// If the ref specifies a schema, use it; otherwise use the search path schema
		const auto &schema = schema_name.empty() ? local_entry.schema : schema_name;
		auto entry =
		    Catalog::GetEntry(binder.context, local_entry.catalog, schema, table_lookup, OnEntryNotFound::RETURN_NULL);
		if (entry) {
			TrackLocalTable(ref);
			// Same as Case 1: local table → UNKNOWN to prevent Merge from treating it as neutral.
			return CatalogPushdownResult::Unknown();
		}
	}

	// Not found in any local catalog - push to the single remote catalog in the search path,
	// but only if the table actually exists there (otherwise fall back to the binder for a proper error)
	auto &remote_catalog = pushdown_state.remote_catalogs_in_search_path.front().get();
	const auto &schema = schema_name.empty() ? Identifier(DEFAULT_SCHEMA) : schema_name;
	auto entry =
	    Catalog::GetEntry(binder.context, remote_catalog.GetName(), schema, table_lookup, OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		TrackLocalTable(ref);
		return CatalogPushdownResult::Unknown();
	}
	return CatalogPushdownResult::RemoteReference(remote_catalog);
}

bool RemotePushdownOptimizer::RefersToLocalTable(const ColumnRefExpression &col_ref) const {
	// figuring out if a column refers to a local table is challenging without knowing all of the columns
	// challenges are:
	// (1) we might have a subquery (e.g. FROM (SELECT ...), (SELECT ...))
	//   - when binding the second subquery we need to know the schema of the first subquery
	// (2) we might have CTEs (e.g. FROM cte, (SELECT ...))
	//   - when binding the subquery we need to know the schema of the CTE
	// (3) we might have struct columns (e.g. FROM tbl, (SELECT struct_col.field)
	//   - we need to correctly deal with this scenario and figure out which struct col this column references
	// this is effectively having to re-implement many components of binding but with some information missing
	if (local_table_names.empty()) {
		return false;
	}
	return true;
}

ExpressionPushdownResult RemotePushdownOptimizer::AnalyzeExpression(const SubqueryExpression &subquery_expr) {
	ExpressionPushdownResult state;
	RemotePushdownOptimizer child_optimizer(this);
	state.result = child_optimizer.Rewrite(*subquery_expr.Subquery()->node);
	return state;
}

CatalogPushdownResult RemotePushdownOptimizer::CheckCatalogQualification(const ParsedExpression &expr,
                                                                         const Identifier &catalog_p,
                                                                         const Identifier &schema_p) {
	Identifier catalog_name = catalog_p;
	Identifier schema_name = schema_p;
	Binder::BindSchemaOrCatalog(binder.context, catalog_name, schema_name);
	if (!catalog_name.empty()) {
		auto catalog = Catalog::GetCatalogEntry(binder.context, catalog_name);
		if (catalog && catalog->Supports(RemoteCapability::EXECUTE_QUERY_NODE)) {
			// the generic expression dispatch verifies that the catalog supports pushing down this expression
			return CatalogPushdownResult::RemoteReference(*catalog);
		}
		// Explicitly local-catalog: block pushdown.
		return CatalogPushdownResult::Unknown();
	}
	return CatalogPushdownResult::NoCatalogReference();
}

ExpressionPushdownResult RemotePushdownOptimizer::AnalyzeExpression(const FunctionExpression &func) {
	ExpressionPushdownResult state;
	state.result = CheckCatalogQualification(func, func.Catalog(), func.Schema());
	// look up the function once - this determines both whether it can be constant-folded and
	// whether it is a macro in a local catalog (which cannot be evaluated remotely)
	EntryLookupInfo function_lookup(CatalogType::SCALAR_FUNCTION_ENTRY, func.FunctionName());
	auto entry =
	    Catalog::GetEntry(binder.context, func.Catalog(), func.Schema(), function_lookup, OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		return state;
	}
	// aggregate-style modifiers cannot be constant-folded
	bool foldable_modifiers = !func.Filter() && !func.Distinct() && !func.ExportState() &&
	                          (!func.OrderBy() || func.OrderBy()->orders.empty());
	switch (entry->type) {
	case CatalogType::MACRO_ENTRY:
		// scalar macros can be folded - the stability of the expansion is checked after binding
		if (foldable_modifiers) {
			state.foldability = ExpressionFoldability::FOLDABLE;
		}
		if (!entry->ParentCatalog().Supports(RemoteCapability::EXECUTE_QUERY_NODE)) {
			// macros in local catalogs cannot be evaluated remotely - if the macro is not folded
			// away, pushdown is blocked
			state.result = CatalogPushdownResult::Unknown();
		}
		break;
	case CatalogType::SCALAR_FUNCTION_ENTRY: {
		// at least one overload must be non-volatile (the selected overload is verified after binding)
		auto &scalar_entry = entry->Cast<ScalarFunctionCatalogEntry>();
		for (auto &overload : scalar_entry.functions.functions) {
			if (foldable_modifiers && overload.GetStability() != FunctionStability::VOLATILE) {
				state.foldability = ExpressionFoldability::FOLDABLE;
				break;
			}
		}
		break;
	}
	default:
		break;
	}
	return state;
}

ExpressionPushdownResult RemotePushdownOptimizer::AnalyzeExpression(const WindowExpression &func) {
	ExpressionPushdownResult state;
	state.result = CheckCatalogQualification(func, func.Catalog(), func.Schema());
	return state;
}

ExpressionPushdownResult RemotePushdownOptimizer::AnalyzeExpression(const TypeExpression &type_expr) {
	ExpressionPushdownResult state;
	state.result = CheckCatalogQualification(type_expr, type_expr.GetCatalog(), type_expr.GetSchema());
	return state;
}

ExpressionPushdownResult RemotePushdownOptimizer::AnalyzeExpression(const ColumnRefExpression &col_ref) {
	ExpressionPushdownResult state;
	if (RefersToLocalTable(col_ref)) {
		// column refers to local table - bail
		state.result = CatalogPushdownResult::Unknown();
	}
	return state;
}

RemotePushdownOptimizer::ConstantFoldResult
RemotePushdownOptimizer::TryConstantFold(unique_ptr<ParsedExpression> &expr) {
	// bind a copy of the expression (binding modifies the expression in-place)
	unique_ptr<Expression> bound_expr;
	try {
		auto expr_copy = expr->Copy();
		auto fold_binder = Binder::CreateBinder(binder.context);
		ConstantBinder constant_binder(*fold_binder, binder.context, "remote pushdown");
		bound_expr = constant_binder.Bind(expr_copy);
	} catch (std::exception &) {
		// the expression cannot be bound as a constant (e.g. no matching function overload)
		return ConstantFoldResult::NOT_FOLDABLE;
	}
	if (!bound_expr || bound_expr->HasParameter() || bound_expr->HasSubquery()) {
		return ConstantFoldResult::NOT_FOLDABLE;
	}
	if (bound_expr->IsVolatile()) {
		// volatile functions (random(), ...) must be re-evaluated on every row
		return ConstantFoldResult::NOT_FOLDABLE;
	}
	if (!bound_expr->IsConsistent()) {
		// functions like now() must be re-evaluated (re-folded) when a prepared statement is re-executed
		binder.SetAlwaysRequireRebind();
	}
	Value fold_result;
	if (!ExpressionExecutor::TryEvaluateScalar(binder.context, *bound_expr, fold_result)) {
		// evaluating the expression raises an error (e.g. an out-of-range error)
		return ConstantFoldResult::FOLD_ERROR;
	}
	auto folded = make_uniq<ConstantExpression>(std::move(fold_result));
	// preserve the name DuckDB would generate for the original expression
	folded->SetAlias(expr->GetAlias().empty() ? Identifier(expr->ToString()) : expr->GetAlias());
	folded->SetQueryLocation(expr->GetQueryLocation());
	expr = std::move(folded);
	return ConstantFoldResult::FOLDED;
}

ExpressionPushdownResult RemotePushdownOptimizer::AnalyzeExpression(const ParsedExpression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::SUBQUERY:
		return AnalyzeExpression(expr.Cast<SubqueryExpression>());
	case ExpressionClass::FUNCTION:
		return AnalyzeExpression(expr.Cast<FunctionExpression>());
	case ExpressionClass::WINDOW:
		return AnalyzeExpression(expr.Cast<WindowExpression>());
	case ExpressionClass::TYPE:
		return AnalyzeExpression(expr.Cast<TypeExpression>());
	case ExpressionClass::COLUMN_REF:
		return AnalyzeExpression(expr.Cast<ColumnRefExpression>());
	case ExpressionClass::CONSTANT:
	case ExpressionClass::CAST:
	case ExpressionClass::COMPARISON:
	case ExpressionClass::BETWEEN:
	case ExpressionClass::CONJUNCTION:
	case ExpressionClass::OPERATOR:
	case ExpressionClass::CASE: {
		// deterministic expression types - foldable when all inputs are foldable
		ExpressionPushdownResult state;
		state.foldability = ExpressionFoldability::FOLDABLE;
		return state;
	}
	default:
		return ExpressionPushdownResult();
	}
}

ExpressionPushdownResult RemotePushdownOptimizer::RewriteExpression(unique_ptr<ParsedExpression> &expr,
                                                                    ExpressionFoldingMode mode) {
	// rewrite the children - foldable subtrees are folded at the last possible moment, so that
	// every maximal foldable subtree is bound and evaluated exactly once
	auto result = CatalogPushdownResult::NoCatalogReference();
	vector<reference<unique_ptr<ParsedExpression>>> foldable_children;
	bool all_children_foldable = true;
	ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
		auto child_state = RewriteExpression(child, ExpressionFoldingMode::FOLD_EXPRESSION);
		if (child_state.foldability == ExpressionFoldability::FOLDABLE) {
			// defer - the subtree is folded when it turns out to be a maximal foldable subtree
			foldable_children.push_back(child);
		} else {
			all_children_foldable = false;
			result = Merge(std::move(result), std::move(child_state.result));
		}
	});
	auto state = AnalyzeExpression(*expr);
	if (mode == ExpressionFoldingMode::FOLD_EXPRESSION && all_children_foldable &&
	    state.foldability == ExpressionFoldability::FOLDABLE) {
		// this expression is itself foldable - defer folding to the parent
		ExpressionPushdownResult folding_deferred;
		folding_deferred.foldability = ExpressionFoldability::FOLDABLE;
		return folding_deferred;
	}
	// this expression is not foldable - fold the (maximal) foldable children now
	for (auto &child : foldable_children) {
		result = Merge(std::move(result), FoldExpression(child.get()));
	}
	result = Merge(std::move(result), std::move(state.result));
	if (result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		// the expression is fully remote - check if the remote catalog supports pushing it down
		if (!result.catalog->SupportsPushdown(*expr)) {
			result = CatalogPushdownResult::Unknown();
		}
	} else if (result.reference_type == CatalogReferenceType::NO_CATALOG_REFERENCED) {
		// record the expression so a remote catalog can veto pushdown of any expression class
		// (functions, comparisons, operators, star expressions, parameters, etc.) during a later merge
		result.used_expressions.push_back(*expr);
	}
	state.result = std::move(result);
	state.foldability = ExpressionFoldability::NOT_FOLDABLE;
	return state;
}

CatalogPushdownResult RemotePushdownOptimizer::FoldExpression(unique_ptr<ParsedExpression> &expr) {
	if (expr->GetExpressionClass() != ExpressionClass::CONSTANT) {
		// replace the expression with its locally-evaluated result
		switch (TryConstantFold(expr)) {
		case ConstantFoldResult::FOLD_ERROR:
			// evaluating is guaranteed to fail - keep the query local so the user sees DuckDB's error
			return CatalogPushdownResult::Unknown();
		case ConstantFoldResult::NOT_FOLDABLE:
			// binding did not succeed after all - process the expression without re-attempting the fold
			return RewriteExpression(expr, ExpressionFoldingMode::FOLD_CHILDREN_ONLY).result;
		case ConstantFoldResult::FOLDED:
			break;
		}
	}
	// record the constant so a remote catalog can verify that it is supported
	auto result = CatalogPushdownResult::NoCatalogReference();
	result.used_expressions.push_back(*expr);
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(unique_ptr<ParsedExpression> &expr, ExpressionFoldingMode mode) {
	auto state = RewriteExpression(expr, mode);
	if (state.foldability == ExpressionFoldability::FOLDABLE) {
		// the entire expression is foldable - fold it at the root
		return FoldExpression(expr);
	}
	return state.result;
}

unique_ptr<TableRef> RemotePushdownOptimizer::CreateRemoteFunctionRef(CatalogPushdownResult &result,
                                                                      unique_ptr<QueryNode> node) {
	return result.catalog->RemoteExecute(binder.context, std::move(node));
}

void RemotePushdownOptimizer::StripCatalogName(TableRef &ref, const Identifier &catalog_name) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base = ref.Cast<BaseTableRef>();
		if (base.catalog_name == catalog_name) {
			base.catalog_name = "";
		} else if (base.catalog_name.empty() && base.schema_name == catalog_name) {
			// 2-part name (schema.table) where the schema is actually the catalog being pushed to
			base.schema_name = "";
		}
		break;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		StripCatalogName(*join.left, catalog_name);
		StripCatalogName(*join.right, catalog_name);
		if (join.condition) {
			StripCatalogName(*join.condition, catalog_name);
		}
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &sq = ref.Cast<SubqueryRef>();
		StripCatalogName(*sq.subquery->node, catalog_name);
		break;
	}
	case TableReferenceType::TABLE_FUNCTION: {
		auto &tf = ref.Cast<TableFunctionRef>();
		if (tf.function) {
			StripCatalogName(*tf.function, catalog_name);
		}
		break;
	}
	case TableReferenceType::EXPRESSION_LIST: {
		auto &el = ref.Cast<ExpressionListRef>();
		for (auto &row : el.values) {
			for (auto &expr : row) {
				StripCatalogName(*expr, catalog_name);
			}
		}
		break;
	}
	default:
		break;
	}
}

void RemotePushdownOptimizer::StripCatalogName(ParsedExpression &expr, const Identifier &catalog_name) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &col_ref = expr.Cast<ColumnRefExpression>();
		// Strip catalog prefix from qualified column references, normalising to exactly table.col (2 parts).
		// Require at least 3 names: a 2-part ref like "rpc.field" is either table.col or struct-column.field —
		// not catalog-qualified — so stripping would be wrong.
		// For 3-part  catalog.table.col        → table.col   (one level stripped)
		// For 4-part  catalog.schema.table.col → table.col   (catalog + schema stripped)
		if (col_ref.ColumnNames().size() >= 3 && col_ref.ColumnNames()[0] == catalog_name) {
			Identifier table_name = col_ref.ColumnNames()[col_ref.ColumnNames().size() - 2];
			Identifier col_name = col_ref.ColumnNames()[col_ref.ColumnNames().size() - 1];
			col_ref.ColumnNamesMutable() = {std::move(table_name), std::move(col_name)};
		}
		return;
	}
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subq = expr.Cast<SubqueryExpression>();
		StripCatalogName(*subq.SubqueryMutable()->node, catalog_name);
		if (subq.GetChild()) {
			StripCatalogName(*subq.GetChildMutable(), catalog_name);
		}
		return;
	}
	// Strip catalog prefix from explicitly-qualified function/window/type calls.
	// Also handle 2-part names (schema.func/type) where the schema is actually the remote catalog name
	// (e.g. "rpc.my_func()" parsed as schema="rpc", catalog="").
	if (expr.GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &func = expr.Cast<FunctionExpression>();
		if (func.Catalog() == catalog_name) {
			func.CatalogMutable() = "";
		} else if (func.Catalog().empty() && func.Schema() == catalog_name) {
			func.SchemaMutable() = "";
		}
		// Fall through to EnumerateChildren to also strip catalog refs inside arguments
	} else if (expr.GetExpressionClass() == ExpressionClass::WINDOW) {
		auto &win = expr.Cast<WindowExpression>();
		if (win.Catalog() == catalog_name) {
			win.CatalogMutable() = "";
		} else if (win.Catalog().empty() && win.Schema() == catalog_name) {
			win.SchemaMutable() = "";
		}
		// Fall through to EnumerateChildren to strip catalog refs inside partitions/orders/children
	} else if (expr.GetExpressionClass() == ExpressionClass::CAST) {
		// CastExpression stores the cast target as a LogicalType, not an expression child — EnumerateChildren
		// only visits the value being cast. For unbound (user-defined) types we must strip the catalog from the
		// embedded TypeExpression and reconstruct the LogicalType::UNBOUND wrapper.
		auto &cast_expr = expr.Cast<CastExpression>();
		auto &target_type = cast_expr.TargetTypeMutable();
		if (target_type.id() == LogicalTypeId::UNBOUND) {
			auto type_expr = UnboundType::GetTypeExpression(target_type)->Copy();
			StripCatalogName(*type_expr, catalog_name);
			target_type = LogicalType::UNBOUND(std::move(type_expr));
		}
		// Fall through to EnumerateChildren to strip catalog refs inside the cast argument
	} else if (expr.GetExpressionClass() == ExpressionClass::TYPE) {
		// TypeExpression (used as a type argument) may carry catalog/schema qualifiers.
		auto &type_expr = expr.Cast<TypeExpression>();
		if (type_expr.GetCatalog() == catalog_name) {
			type_expr.SetCatalog("");
		} else if (type_expr.GetCatalog().empty() && type_expr.GetSchema() == catalog_name) {
			type_expr.SetSchema("");
		}
		// Fall through to EnumerateChildren to strip catalog refs inside type parameters
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](ParsedExpression &child) { StripCatalogName(child, catalog_name); });
}

void RemotePushdownOptimizer::StripCatalogName(QueryNode &node, const Identifier &catalog_name) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		// Strip within CTE definitions so the remote receives catalog-free SQL
		for (auto &cte_pair : select.cte_map.map) {
			if (cte_pair.second->query_node) {
				StripCatalogName(*cte_pair.second->query_node, catalog_name);
			}
			for (auto &key : cte_pair.second->key_targets) {
				StripCatalogName(*key, catalog_name);
			}
		}
		if (select.from_table) {
			StripCatalogName(*select.from_table, catalog_name);
		}
		for (auto &expr : select.select_list) {
			StripCatalogName(*expr, catalog_name);
		}
		if (select.where_clause) {
			StripCatalogName(*select.where_clause, catalog_name);
		}
		for (auto &expr : select.groups.group_expressions) {
			StripCatalogName(*expr, catalog_name);
		}
		if (select.having) {
			StripCatalogName(*select.having, catalog_name);
		}
		if (select.qualify) {
			StripCatalogName(*select.qualify, catalog_name);
		}
		for (auto &modifier : select.modifiers) {
			switch (modifier->type) {
			case ResultModifierType::ORDER_MODIFIER: {
				auto &order_mod = modifier->Cast<OrderModifier>();
				for (auto &order : order_mod.orders) {
					StripCatalogName(*order.expression, catalog_name);
				}
				break;
			}
			case ResultModifierType::LIMIT_MODIFIER: {
				auto &limit_mod = modifier->Cast<LimitModifier>();
				if (limit_mod.limit) {
					StripCatalogName(*limit_mod.limit, catalog_name);
				}
				if (limit_mod.offset) {
					StripCatalogName(*limit_mod.offset, catalog_name);
				}
				break;
			}
			case ResultModifierType::DISTINCT_MODIFIER: {
				auto &distinct_mod = modifier->Cast<DistinctModifier>();
				for (auto &expr : distinct_mod.distinct_on_targets) {
					StripCatalogName(*expr, catalog_name);
				}
				break;
			}
			default:
				break;
			}
		}
		break;
	}
	case QueryNodeType::INSERT_QUERY_NODE: {
		auto &insert = node.Cast<InsertQueryNode>();
		for (auto &cte_pair : insert.cte_map.map) {
			if (cte_pair.second->query_node) {
				StripCatalogName(*cte_pair.second->query_node, catalog_name);
			}
			for (auto &key : cte_pair.second->key_targets) {
				StripCatalogName(*key, catalog_name);
			}
		}
		// Strip from the target table's catalog/schema fields (these are what ToString() serializes)
		if (insert.catalog == catalog_name) {
			insert.catalog = "";
		} else if (insert.catalog.empty() && insert.schema == catalog_name) {
			insert.schema = "";
		}
		if (insert.select_statement) {
			StripCatalogName(*insert.select_statement->node, catalog_name);
		}
		if (insert.on_conflict_info) {
			if (insert.on_conflict_info->condition) {
				StripCatalogName(*insert.on_conflict_info->condition, catalog_name);
			}
			if (insert.on_conflict_info->set_info) {
				if (insert.on_conflict_info->set_info->condition) {
					StripCatalogName(*insert.on_conflict_info->set_info->condition, catalog_name);
				}
				for (auto &expr : insert.on_conflict_info->set_info->expressions) {
					StripCatalogName(*expr, catalog_name);
				}
			}
		}
		for (auto &expr : insert.returning_list) {
			StripCatalogName(*expr, catalog_name);
		}
		break;
	}
	case QueryNodeType::DELETE_QUERY_NODE: {
		auto &del = node.Cast<DeleteQueryNode>();
		for (auto &cte_pair : del.cte_map.map) {
			if (cte_pair.second->query_node) {
				StripCatalogName(*cte_pair.second->query_node, catalog_name);
			}
			for (auto &key : cte_pair.second->key_targets) {
				StripCatalogName(*key, catalog_name);
			}
		}
		if (del.table) {
			StripCatalogName(*del.table, catalog_name);
		}
		if (del.condition) {
			StripCatalogName(*del.condition, catalog_name);
		}
		for (auto &clause : del.using_clauses) {
			StripCatalogName(*clause, catalog_name);
		}
		for (auto &expr : del.returning_list) {
			StripCatalogName(*expr, catalog_name);
		}
		break;
	}
	case QueryNodeType::UPDATE_QUERY_NODE: {
		auto &upd = node.Cast<UpdateQueryNode>();
		for (auto &cte_pair : upd.cte_map.map) {
			if (cte_pair.second->query_node) {
				StripCatalogName(*cte_pair.second->query_node, catalog_name);
			}
			for (auto &key : cte_pair.second->key_targets) {
				StripCatalogName(*key, catalog_name);
			}
		}
		if (upd.table) {
			StripCatalogName(*upd.table, catalog_name);
		}
		if (upd.from_table) {
			StripCatalogName(*upd.from_table, catalog_name);
		}
		if (upd.set_info) {
			if (upd.set_info->condition) {
				StripCatalogName(*upd.set_info->condition, catalog_name);
			}
			for (auto &expr : upd.set_info->expressions) {
				StripCatalogName(*expr, catalog_name);
			}
		}
		for (auto &expr : upd.returning_list) {
			StripCatalogName(*expr, catalog_name);
		}
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop = node.Cast<SetOperationNode>();
		for (auto &cte_pair : setop.cte_map.map) {
			if (cte_pair.second->query_node) {
				StripCatalogName(*cte_pair.second->query_node, catalog_name);
			}
			for (auto &key : cte_pair.second->key_targets) {
				StripCatalogName(*key, catalog_name);
			}
		}
		for (auto &child : setop.children) {
			StripCatalogName(*child, catalog_name);
		}
		for (auto &modifier : setop.modifiers) {
			switch (modifier->type) {
			case ResultModifierType::ORDER_MODIFIER: {
				auto &order_mod = modifier->Cast<OrderModifier>();
				for (auto &order : order_mod.orders) {
					StripCatalogName(*order.expression, catalog_name);
				}
				break;
			}
			case ResultModifierType::LIMIT_MODIFIER: {
				auto &limit_mod = modifier->Cast<LimitModifier>();
				if (limit_mod.limit) {
					StripCatalogName(*limit_mod.limit, catalog_name);
				}
				if (limit_mod.offset) {
					StripCatalogName(*limit_mod.offset, catalog_name);
				}
				break;
			}
			case ResultModifierType::DISTINCT_MODIFIER: {
				auto &distinct_mod = modifier->Cast<DistinctModifier>();
				for (auto &expr : distinct_mod.distinct_on_targets) {
					StripCatalogName(*expr, catalog_name);
				}
				break;
			}
			default:
				break;
			}
		}
		break;
	}
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &rec = node.Cast<RecursiveCTENode>();
		for (auto &cte_pair : rec.cte_map.map) {
			if (cte_pair.second->query_node) {
				StripCatalogName(*cte_pair.second->query_node, catalog_name);
			}
			for (auto &key : cte_pair.second->key_targets) {
				StripCatalogName(*key, catalog_name);
			}
		}
		for (auto &key : rec.key_targets) {
			StripCatalogName(*key, catalog_name);
		}
		for (auto &modifier : rec.modifiers) {
			switch (modifier->type) {
			case ResultModifierType::ORDER_MODIFIER: {
				auto &order_mod = modifier->Cast<OrderModifier>();
				for (auto &order : order_mod.orders) {
					StripCatalogName(*order.expression, catalog_name);
				}
				break;
			}
			case ResultModifierType::LIMIT_MODIFIER: {
				auto &limit_mod = modifier->Cast<LimitModifier>();
				if (limit_mod.limit) {
					StripCatalogName(*limit_mod.limit, catalog_name);
				}
				if (limit_mod.offset) {
					StripCatalogName(*limit_mod.offset, catalog_name);
				}
				break;
			}
			case ResultModifierType::DISTINCT_MODIFIER: {
				auto &distinct_mod = modifier->Cast<DistinctModifier>();
				for (auto &expr : distinct_mod.distinct_on_targets) {
					StripCatalogName(*expr, catalog_name);
				}
				break;
			}
			default:
				break;
			}
		}
		if (rec.left) {
			StripCatalogName(*rec.left, catalog_name);
		}
		if (rec.right) {
			StripCatalogName(*rec.right, catalog_name);
		}
		break;
	}
	default:
		break;
	}
}

void RemotePushdownOptimizer::StripCatalogName(SQLStatement &statement, const Identifier &catalog_name) {
	switch (statement.type) {
	case StatementType::SELECT_STATEMENT:
		StripCatalogName(*statement.Cast<SelectStatement>().node, catalog_name);
		break;
	case StatementType::INSERT_STATEMENT:
		StripCatalogName(*statement.Cast<InsertStatement>().node, catalog_name);
		break;
	case StatementType::DELETE_STATEMENT:
		StripCatalogName(*statement.Cast<DeleteStatement>().node, catalog_name);
		break;
	case StatementType::UPDATE_STATEMENT:
		StripCatalogName(*statement.Cast<UpdateStatement>().node, catalog_name);
		break;
	default:
		break;
	}
}

unique_ptr<QueryNode> GetNodeFromStatement(SQLStatement &statement) {
	switch (statement.type) {
	case StatementType::SELECT_STATEMENT:
		return std::move(statement.Cast<SelectStatement>().node);
	case StatementType::INSERT_STATEMENT:
		return std::move(statement.Cast<InsertStatement>().node);
	case StatementType::DELETE_STATEMENT:
		return std::move(statement.Cast<DeleteStatement>().node);
	case StatementType::UPDATE_STATEMENT:
		return std::move(statement.Cast<UpdateStatement>().node);
	default:
		return nullptr;
	}
}

void RemotePushdownOptimizer::FinishPushdown(unique_ptr<SQLStatement> &statement, CatalogPushdownResult result) {
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		return;
	}
	// Strip the catalog name so the remote server doesn't recursively re-push
	StripCatalogName(*statement, result.catalog->GetName());
	auto node = GetNodeFromStatement(*statement);
	if (!node) {
		return;
	}

	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = CreateRemoteFunctionRef(result, std::move(node));
	auto select_stmt = make_uniq<SelectStatement>();
	select_stmt->node = std::move(select_node);
	statement = std::move(select_stmt);
}

void RemotePushdownOptimizer::FinishPushdown(unique_ptr<QueryNode> &node, CatalogPushdownResult result) {
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		return;
	}
	// FIXME: work-around for referencing a CTE in a parent
	// if this query refers to a CTE in the parent we can't push down only this node
	// as the parent CTE is lost. For now we just block all pushdown if the parent has a CTE.
	// we could fix this in a better way by either (1) moving the parent CTE into this node
	// or (2) tracking if we actually refer to a parent CTE
	for (auto opt = this; opt; opt = opt->parent.get()) {
		for (auto &cte_entry : opt->cte_results) {
			auto ref_type = cte_entry.second.reference_type;
			if (ref_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG ||
			    ref_type == CatalogReferenceType::NO_CATALOG_REFERENCED) {
				return;
			}
		}
	}
	StripCatalogName(*node, result.catalog->GetName());
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = CreateRemoteFunctionRef(result, std::move(node));
	node = std::move(select_node);
}

} // namespace duckdb
