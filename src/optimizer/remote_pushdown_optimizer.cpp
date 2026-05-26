#include "duckdb/optimizer/remote_pushdown_optimizer.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
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
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"

namespace duckdb {

RemotePushdownOptimizer::RemotePushdownOptimizer(Binder &binder) : binder(binder) {
}

void RemotePushdownOptimizer::FindRemoteCatalogsInSearchPath() {
	if (search_path_initialized) {
		return;
	}
	search_path_initialized = true;
	auto &client_data = ClientData::Get(binder.context);
	// iterate over all catalogs mentioned in the search path and check if they are remote
	auto search_path = client_data.catalog_search_path->Get();
	// The search path always contains an INVALID_CATALOG ("") sentinel that resolves to the
	// current default catalog at lookup time. After "USE rpc", this sentinel resolves to the
	// same remote catalog that is already listed explicitly, causing the same catalog to appear
	// twice in remote_catalogs_in_search_path. The size() != 1 guard in Rewrite(BaseTableRef)
	// then incorrectly blocks all unqualified-table pushdown. Deduplicate by catalog name.
	case_insensitive_set_t seen_remote_catalogs;
	for (auto &entry : search_path) {
		auto catalog_entry = Catalog::GetCatalogEntry(binder.context, entry.catalog);
		if (!catalog_entry) {
			continue;
		}
		if (!catalog_entry->IsRemoteCatalog()) {
			local_catalogs_in_search_path.push_back(entry);
		} else {
			if (seen_remote_catalogs.insert(catalog_entry->GetName()).second) {
				remote_catalogs_in_search_path.push_back(*catalog_entry);
			}
		}
	}
}

CatalogPushdownResult RemotePushdownOptimizer::Merge(CatalogPushdownResult a, CatalogPushdownResult b) {
	if (a.reference_type == CatalogReferenceType::NO_CATALOG_REFERENCED) {
		return b;
	}
	if (b.reference_type == CatalogReferenceType::NO_CATALOG_REFERENCED) {
		return a;
	}
	if (a.reference_type == CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE ||
	    b.reference_type == CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE) {
		return {};
	}
	// Both are SINGLE_REMOTE_CATALOG - only valid if they refer to the same catalog
	if (a.catalog == b.catalog) {
		return a;
	}
	return {};
}

void RemotePushdownOptimizer::Rewrite(unique_ptr<SQLStatement> &statement) {
	CatalogPushdownResult result;
	switch (statement->type) {
	case StatementType::SELECT_STATEMENT:
		result = Rewrite(*statement->Cast<SelectStatement>().node);
		break;
	case StatementType::INSERT_STATEMENT: {
		auto &dml_node = *statement->Cast<InsertStatement>().node;
		auto saved_returning = std::move(dml_node.returning_list);
		result = Rewrite(dml_node);
		dml_node.returning_list = std::move(saved_returning);
		if (result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG && !dml_node.returning_list.empty()) {
			CatalogPushdownResult ret_result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
			for (auto &expr : dml_node.returning_list) {
				ret_result = Merge(ret_result, Rewrite(*expr));
			}
			auto combined = Merge(result, ret_result);
			if (combined.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
				TryPushDMLWithLocalReturning(statement, result);
				return;
			}
			result = combined;
		}
		break;
	}
	case StatementType::DELETE_STATEMENT: {
		auto &dml_node = *statement->Cast<DeleteStatement>().node;
		auto saved_returning = std::move(dml_node.returning_list);
		result = Rewrite(dml_node);
		dml_node.returning_list = std::move(saved_returning);
		if (result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG && !dml_node.returning_list.empty()) {
			CatalogPushdownResult ret_result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
			for (auto &expr : dml_node.returning_list) {
				ret_result = Merge(ret_result, Rewrite(*expr));
			}
			auto combined = Merge(result, ret_result);
			if (combined.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
				TryPushDMLWithLocalReturning(statement, result);
				return;
			}
			result = combined;
		}
		break;
	}
	case StatementType::UPDATE_STATEMENT: {
		auto &dml_node = *statement->Cast<UpdateStatement>().node;
		auto saved_returning = std::move(dml_node.returning_list);
		result = Rewrite(dml_node);
		dml_node.returning_list = std::move(saved_returning);
		if (result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG && !dml_node.returning_list.empty()) {
			CatalogPushdownResult ret_result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
			for (auto &expr : dml_node.returning_list) {
				ret_result = Merge(ret_result, Rewrite(*expr));
			}
			auto combined = Merge(result, ret_result);
			if (combined.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
				TryPushDMLWithLocalReturning(statement, result);
				return;
			}
			result = combined;
		}
		break;
	}
	case StatementType::MERGE_INTO_STATEMENT: {
		auto &merge = statement->Cast<MergeIntoStatement>();
		auto saved_returning = std::move(merge.returning_list);
		result = Rewrite(merge);
		merge.returning_list = std::move(saved_returning);
		if (result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG && !merge.returning_list.empty()) {
			CatalogPushdownResult ret_result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
			for (auto &expr : merge.returning_list) {
				ret_result = Merge(ret_result, Rewrite(*expr));
			}
			auto combined = Merge(result, ret_result);
			if (combined.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
				TryPushDMLWithLocalReturning(statement, result);
				return;
			}
			result = combined;
		}
		break;
	}
	case StatementType::EXPLAIN_STATEMENT:
		Rewrite(statement->Cast<ExplainStatement>().stmt);
		return;
	default:
		return;
	}
	FinishPushdown(statement, result);
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(QueryNode &node) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		return Rewrite(node.Cast<SelectNode>());
	case QueryNodeType::INSERT_QUERY_NODE:
		return Rewrite(node.Cast<InsertQueryNode>());
	case QueryNodeType::DELETE_QUERY_NODE:
		return Rewrite(node.Cast<DeleteQueryNode>());
	case QueryNodeType::UPDATE_QUERY_NODE:
		return Rewrite(node.Cast<UpdateQueryNode>());
	case QueryNodeType::SET_OPERATION_NODE:
		return Rewrite(node.Cast<SetOperationNode>());
	case QueryNodeType::RECURSIVE_CTE_NODE:
		return Rewrite(node.Cast<RecursiveCTENode>());
	default:
		return {};
	}
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(RecursiveCTENode &node) {
	// Process any nested CTEs attached to this recursive CTE node (inherits cte_map from QueryNode).
	// These CTEs are serialized in ToString() and must be analyzed and restored correctly.
	case_insensitive_map_t<CatalogPushdownResult> outer_cte_results;
	CatalogPushdownResult cte_combined {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	// Save nested CTE query_nodes before analysis. Rewrite(JoinRef) inside a nested CTE body
	// pushes individual SINGLE_REMOTE children in-place even when the JoinRef result is UNKNOWN
	// (mixed join). If the overall RecursiveCTENode result is UNKNOWN, those stale quack wrappers
	// must be rolled back: the recursive CTE body executes natively, and if the outer query also
	// has a quack streaming slot open, the stale wrapper inside the nested CTE would open a
	// concurrent second streaming connection → "Multiple streaming scans".
	case_insensitive_map_t<unique_ptr<QueryNode>> saved_cte_nodes;
	for (auto &cte_pair : node.cte_map.map) {
		if (cte_pair.second->query_node) {
			saved_cte_nodes[cte_pair.first] = cte_pair.second->query_node->Copy();
		}
	}
	for (auto &cte_pair : node.cte_map.map) {
		const string &cte_name = cte_pair.first;
		auto &cte_info = *cte_pair.second;
		auto it = cte_results.find(cte_name);
		if (it != cte_results.end()) {
			outer_cte_results[cte_name] = it->second;
		}
		CatalogPushdownResult cte_result;
		if (cte_info.query_node) {
			cte_result = Rewrite(*cte_info.query_node);
		} else {
			cte_result = {};
		}
		for (auto &key : cte_info.key_targets) {
			cte_result = Merge(cte_result, Rewrite(*key));
		}
		cte_results[cte_name] = cte_result;
		cte_combined = Merge(cte_combined, cte_result);
	}

	// Register the CTE's own name as neutral so the self-reference in the recursive arm
	// is treated as NO_CATALOG_REFERENCED rather than triggering an unknown catalog lookup.
	// The caller (Rewrite(SelectNode)) will overwrite this with the final result afterwards.
	// Save any outer CTE with the same name first: an inner recursive CTE may shadow an outer
	// CTE of the same name; without saving, erasing the self-reference placeholder below would
	// permanently remove the outer entry from cte_results, breaking later references to it.
	bool had_outer_ctename = false;
	CatalogPushdownResult saved_ctename_result;
	auto outer_ctename_it = cte_results.find(node.ctename);
	if (outer_ctename_it != cte_results.end()) {
		saved_ctename_result = outer_ctename_it->second;
		had_outer_ctename = true;
	}
	cte_results[node.ctename] = {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};

	// Save left and right before analysis. Rewrite(SelectNode) calls Rewrite(JoinRef) which pushes
	// individual SINGLE_REMOTE children in-place via FinishPushdown even when the JoinRef result is
	// UNKNOWN (mixed join). If the overall RecursiveCTENode result is UNKNOWN, those stale quack
	// wrappers must be rolled back: the CTE body executes natively, and if the outer query also has
	// a quack streaming slot open (e.g. an individually-pushed remote FROM), the stale wrapper inside
	// the CTE arm would open a concurrent second streaming connection → "Multiple streaming scans".
	auto saved_left = node.left->Copy();
	auto saved_right = node.right->Copy();

	CatalogPushdownResult left_result = Rewrite(*node.left);
	CatalogPushdownResult right_result = Rewrite(*node.right);

	// Remove the self-reference placeholder so the caller's assignment is authoritative.
	// If an outer CTE of the same name was shadowed, restore it now.
	if (had_outer_ctename) {
		cte_results[node.ctename] = saved_ctename_result;
	} else {
		cte_results.erase(node.ctename);
	}

	// Restore shadowed outer CTE entries
	for (auto &cte_pair : node.cte_map.map) {
		auto it = outer_cte_results.find(cte_pair.first);
		if (it != outer_cte_results.end()) {
			cte_results[cte_pair.first] = it->second;
		} else {
			cte_results.erase(cte_pair.first);
		}
	}

	auto result = Merge(left_result, right_result);
	result = Merge(result, cte_combined);
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		node.left = std::move(saved_left);
		node.right = std::move(saved_right);
		// Restore nested CTE bodies to remove any stale quack wrappers inserted in-place.
		for (auto &cte_pair : node.cte_map.map) {
			auto it = saved_cte_nodes.find(cte_pair.first);
			if (it != saved_cte_nodes.end() && it->second) {
				cte_pair.second->query_node = std::move(it->second);
			}
		}
	}
	return result;
}

// Returns true if ref or any descendant in a JoinRef tree is a TABLE_FUNCTION ref.
// Used in Rewrite(SetOperationNode) to detect whether any child SelectNode already holds
// a quack streaming slot — either because its whole from_table was pushed to quack, or because
// a JoinRef child inside the from_table was individually pushed to quack — so that modifier
// subqueries are not independently pushed into a concurrent streaming connection.
static bool HasTableFunctionInTree(const TableRef &ref) {
	switch (ref.type) {
	case TableReferenceType::TABLE_FUNCTION:
		return true;
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		return (join.left && HasTableFunctionInTree(*join.left)) || (join.right && HasTableFunctionInTree(*join.right));
	}
	default:
		return false;
	}
}

// Returns true if any SELECT descendant of a QueryNode tree has a quack streaming slot open
// (i.e., a TABLE_FUNCTION in its from_table). Needed in Rewrite(SetOperationNode) to detect
// streaming slots hidden inside nested set operations (e.g. an inner UNION with mixed children
// where one was individually pushed) — those are missed by the direct SELECT_NODE child check.
static bool QueryNodeHasTableFunctionPush(const QueryNode &node) {
	if (node.type == QueryNodeType::SELECT_NODE) {
		auto &sel = node.Cast<SelectNode>();
		return sel.from_table && HasTableFunctionInTree(*sel.from_table);
	}
	if (node.type == QueryNodeType::SET_OPERATION_NODE) {
		for (auto &child : node.Cast<SetOperationNode>().children) {
			if (child && QueryNodeHasTableFunctionPush(*child)) {
				return true;
			}
		}
	}
	return false;
}

// Recursively collect the table name and alias from every BaseTableRef in a TableRef tree.
// Used to register remote-table names as "locally visible" when an all-remote JoinRef was
// NOT individually pushed to quack, so HasLocalTableReference can detect correlated subquery
// references to those tables and prevent them from being pushed independently.
static void CollectTableAliases(const TableRef &ref, case_insensitive_set_t &out) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base = ref.Cast<BaseTableRef>();
		out.insert(base.table_name);
		if (!base.alias.empty()) {
			out.insert(base.alias);
		}
		break;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		if (join.left) {
			CollectTableAliases(*join.left, out);
		}
		if (join.right) {
			CollectTableAliases(*join.right, out);
		}
		break;
	}
	default:
		if (!ref.alias.empty()) {
			out.insert(ref.alias);
		}
		break;
	}
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(SelectNode &node) {
	// Analyze CTE definitions and register their catalog results for reference lookup.
	// CTEs are processed in order so later CTEs can reference earlier ones.
	case_insensitive_map_t<CatalogPushdownResult> outer_cte_results;
	for (auto &cte_pair : node.cte_map.map) {
		const string &cte_name = cte_pair.first;
		auto &cte_info = *cte_pair.second;
		// Save any outer CTE entry with the same name (shadowing)
		auto it = cte_results.find(cte_name);
		if (it != cte_results.end()) {
			outer_cte_results[cte_name] = it->second;
		}
		CatalogPushdownResult cte_result;
		if (cte_info.query_node) {
			cte_result = Rewrite(*cte_info.query_node);
		} else {
			cte_result = {CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE, nullptr};
		}
		for (auto &key : cte_info.key_targets) {
			cte_result = Merge(cte_result, Rewrite(*key));
		}
		cte_results[cte_name] = cte_result;
	}

	// Rewrite from_table first - its result is tracked separately for potential individual pushdown.
	// Reset from_pushed_catalog_names / from_pushed_table_aliases so that entries pushed by
	// JoinRef children during from_table processing are cleanly collected.
	from_pushed_catalog_names.clear();
	from_pushed_table_aliases.clear();
	CatalogPushdownResult from_result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	if (node.from_table) {
		from_result = Rewrite(node.from_table);
	}

	// Merge from_table result with all expressions to determine if the whole node can be pushed
	CatalogPushdownResult result = from_result;
	for (auto &expr : node.select_list) {
		result = Merge(result, Rewrite(*expr));
	}
	if (node.where_clause) {
		result = Merge(result, Rewrite(*node.where_clause));
	}
	for (auto &expr : node.groups.group_expressions) {
		result = Merge(result, Rewrite(*expr));
	}
	if (node.having) {
		result = Merge(result, Rewrite(*node.having));
	}
	if (node.qualify) {
		result = Merge(result, Rewrite(*node.qualify));
	}
	for (auto &modifier : node.modifiers) {
		switch (modifier->type) {
		case ResultModifierType::ORDER_MODIFIER: {
			auto &order_mod = modifier->Cast<OrderModifier>();
			for (auto &order : order_mod.orders) {
				result = Merge(result, Rewrite(*order.expression));
			}
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &limit_mod = modifier->Cast<LimitModifier>();
			if (limit_mod.limit) {
				result = Merge(result, Rewrite(*limit_mod.limit));
			}
			if (limit_mod.offset) {
				result = Merge(result, Rewrite(*limit_mod.offset));
			}
			break;
		}
		case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
			auto &limit_mod = modifier->Cast<LimitPercentModifier>();
			if (limit_mod.limit) {
				result = Merge(result, Rewrite(*limit_mod.limit));
			}
			if (limit_mod.offset) {
				result = Merge(result, Rewrite(*limit_mod.offset));
			}
			break;
		}
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct_mod = modifier->Cast<DistinctModifier>();
			for (auto &expr : distinct_mod.distinct_on_targets) {
				result = Merge(result, Rewrite(*expr));
			}
			break;
		}
		default:
			break;
		}
	}

	// Merge results of all CTEs defined in this scope: even unreferenced CTEs are serialized
	// into the SQL string when the whole query is pushed. A local CTE body (UNKNOWN) or a
	// CTE from a different remote catalog would fail on the target remote server. Including
	// all CTE results here ensures they are considered even when the outer query does not
	// explicitly reference them.
	for (auto &cte_pair : node.cte_map.map) {
		auto it = cte_results.find(cte_pair.first);
		if (it != cte_results.end()) {
			result = Merge(result, it->second);
		}
	}

	// If the whole SELECT points to a single remote catalog, propagate upward
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		// Track whether any JoinRef children were individually pushed. Must be captured before
		// from_pushed_catalog_names is consumed by the stripping block below.
		bool had_join_child_pushdowns = !from_pushed_catalog_names.empty();
		{
			// When a remote table was individually pushed — either the whole from_table (no-CTE
			// case, via FinishPushdown below) or a JoinRef child (both CTE and no-CTE cases, via
			// Rewrite(JoinRef)) — outer expressions (SELECT list, WHERE, HAVING, GROUP BY, etc.)
			// may contain catalog-qualified column refs like "rpc.t1.i" that the binder can no
			// longer resolve because "rpc.t1" is now "quack_query_by_name(...) AS t1" in FROM.
			// Strip those catalog prefixes so the binder resolves them via the table alias.
			// Stripping must happen even when CTEs are present: Rewrite(JoinRef) pushes individual
			// SINGLE_REMOTE children in-place regardless of outer CTE scope, so outer expressions
			// referencing those children by catalog-qualified name must still be stripped.
			// Collect catalogs pushed by JoinRef children (from_pushed_catalog_names) and, when
			// no CTEs are present, the directly-pushed from_table catalog (from from_result).
			// When CTEs are present, from_table is not pushed by FinishPushdown (the call is
			// inside the cte_map.empty() guard below), so only JoinRef child catalogs apply.
			vector<string> pushed_cats = std::move(from_pushed_catalog_names);
			from_pushed_catalog_names.clear();
			if (node.cte_map.map.empty() &&
			    from_result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG && from_result.catalog) {
				bool found = false;
				for (auto &existing : pushed_cats) {
					if (StringUtil::CIEquals(existing, from_result.catalog->GetName())) {
						found = true;
						break;
					}
				}
				if (!found) {
					pushed_cats.push_back(from_result.catalog->GetName());
				}
			}
			for (auto &cat_name : pushed_cats) {
				// In the partial-pushdown context (individual JoinRef children pushed) we must NOT
				// recurse into subquery bodies: those subqueries were not pushed and still contain
				// their original remote table refs (e.g. "rpc.t2"). Stripping those refs would
				// produce an unqualified "t2" that fails to bind locally.
				for (auto &expr : node.select_list) {
					StripCatalogName(*expr, cat_name, false);
				}
				if (node.where_clause) {
					StripCatalogName(*node.where_clause, cat_name, false);
				}
				if (node.having) {
					StripCatalogName(*node.having, cat_name, false);
				}
				if (node.qualify) {
					StripCatalogName(*node.qualify, cat_name, false);
				}
				for (auto &expr : node.groups.group_expressions) {
					StripCatalogName(*expr, cat_name, false);
				}
				for (auto &modifier : node.modifiers) {
					switch (modifier->type) {
					case ResultModifierType::ORDER_MODIFIER: {
						auto &order_mod = modifier->Cast<OrderModifier>();
						for (auto &order : order_mod.orders) {
							StripCatalogName(*order.expression, cat_name, false);
						}
						break;
					}
					case ResultModifierType::LIMIT_MODIFIER: {
						auto &limit_mod = modifier->Cast<LimitModifier>();
						if (limit_mod.limit) {
							StripCatalogName(*limit_mod.limit, cat_name, false);
						}
						if (limit_mod.offset) {
							StripCatalogName(*limit_mod.offset, cat_name, false);
						}
						break;
					}
					case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
						auto &limit_mod = modifier->Cast<LimitPercentModifier>();
						if (limit_mod.limit) {
							StripCatalogName(*limit_mod.limit, cat_name, false);
						}
						if (limit_mod.offset) {
							StripCatalogName(*limit_mod.offset, cat_name, false);
						}
						break;
					}
					case ResultModifierType::DISTINCT_MODIFIER: {
						auto &distinct_mod = modifier->Cast<DistinctModifier>();
						for (auto &expr : distinct_mod.distinct_on_targets) {
							StripCatalogName(*expr, cat_name, false);
						}
						break;
					}
					default:
						break;
					}
				}
			}
		}
		// When CTEs are present, do not push individual children via FinishPushdown: a
		// CTE-referencing FROM clause cannot be sent to the remote without its CTE definition.
		if (node.cte_map.map.empty()) {
			// Otherwise, push down only the from_table component if possible
			if (node.from_table) {
				FinishPushdown(node.from_table, from_result);
			}
			// After FinishPushdown the from_table may have been replaced by a table-function ref.
			// Its alias is now the entry point for outer column refs inside subqueries. Track it
			// in local_table_names so HasLocalTableReference inside PushdownSubqueries detects
			// correlated refs to the pushed alias and does not push them to the remote independently.
			// Also register aliases of any JoinRef children that were individually pushed, so that
			// correlated WHERE/HAVING subqueries referencing those aliases are correctly detected.
			// Detect actual pushdown by checking whether FinishPushdown replaced the from_table with
			// a TABLE_FUNCTION ref. FinishPushdown skips JoinRef (always) and SubqueryRef when outer
			// SINGLE_REMOTE CTEs are in scope — those cases must not be counted as streaming slots.
			// Pre-existing TABLE_FUNCTION refs with non-SINGLE_REMOTE results are excluded via the
			// from_result guard (FinishPushdown is a no-op for non-SINGLE_REMOTE results).
			bool from_table_actually_pushed =
			    from_result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG && node.from_table &&
			    node.from_table->type == TableReferenceType::TABLE_FUNCTION;
			string pushed_from_alias;
			if (from_table_actually_pushed && !node.from_table->alias.empty()) {
				pushed_from_alias = node.from_table->alias;
				local_table_names.insert(pushed_from_alias);
			}
			// Consume the JoinRef-pushed aliases collected during from_table processing.
			vector<string> join_pushed_aliases = std::move(from_pushed_table_aliases);
			from_pushed_table_aliases.clear();
			for (auto &alias : join_pushed_aliases) {
				local_table_names.insert(alias);
			}
			// Only skip subquery pushdown when a quack streaming connection is actually occupied:
			// either because the from_table was wrapped in a quack table-function ref, or because
			// JoinRef children were individually pushed. An all-remote JoinRef that FinishPushdown
			// skipped does NOT occupy a streaming slot, so subquery pushdown is safe there.
			// When the from_table is an all-remote JoinRef that was NOT pushed (SINGLE_REMOTE but
			// type==JOIN), we must temporarily register its table names in local_table_names so
			// HasLocalTableReference can detect correlated subquery refs to those remote tables.
			// Without this, a subquery referencing t1.i from an outer "rpc.t1 JOIN rpc.t2" would
			// be incorrectly classified as non-correlated and pushed to the remote, where the
			// reference to t1 fails because t1 is not in that subquery's FROM clause.
			case_insensitive_set_t remote_join_table_aliases;
			if (!from_table_actually_pushed && !had_join_child_pushdowns && node.from_table &&
			    from_result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
				CollectTableAliases(*node.from_table, remote_join_table_aliases);
				for (auto &alias : remote_join_table_aliases) {
					local_table_names.insert(alias);
				}
			}
			if (!from_table_actually_pushed && !had_join_child_pushdowns) {
				// Push down any subquery expressions in WHERE/HAVING/QUALIFY/SELECT list/GROUP BY/ORDER BY
				if (node.where_clause) {
					PushdownSubqueries(node.where_clause);
				}
				if (node.having) {
					PushdownSubqueries(node.having);
				}
				if (node.qualify) {
					PushdownSubqueries(node.qualify);
				}
				for (auto &expr : node.select_list) {
					PushdownSubqueries(expr);
				}
				for (auto &expr : node.groups.group_expressions) {
					PushdownSubqueries(expr);
				}
				for (auto &modifier : node.modifiers) {
					switch (modifier->type) {
					case ResultModifierType::ORDER_MODIFIER: {
						auto &order_mod = modifier->Cast<OrderModifier>();
						for (auto &order : order_mod.orders) {
							PushdownSubqueries(order.expression);
						}
						break;
					}
					case ResultModifierType::LIMIT_MODIFIER: {
						auto &limit_mod = modifier->Cast<LimitModifier>();
						if (limit_mod.limit) {
							PushdownSubqueries(limit_mod.limit);
						}
						if (limit_mod.offset) {
							PushdownSubqueries(limit_mod.offset);
						}
						break;
					}
					case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
						auto &limit_mod = modifier->Cast<LimitPercentModifier>();
						if (limit_mod.limit) {
							PushdownSubqueries(limit_mod.limit);
						}
						if (limit_mod.offset) {
							PushdownSubqueries(limit_mod.offset);
						}
						break;
					}
					case ResultModifierType::DISTINCT_MODIFIER: {
						auto &distinct_mod = modifier->Cast<DistinctModifier>();
						for (auto &expr : distinct_mod.distinct_on_targets) {
							PushdownSubqueries(expr);
						}
						break;
					}
					default:
						break;
					}
				}
			} // end if (from_table_actually_pushed || had_join_child_pushdowns)
			for (auto &alias : remote_join_table_aliases) {
				local_table_names.erase(alias);
			}
			if (!pushed_from_alias.empty()) {
				local_table_names.erase(pushed_from_alias);
			}
			for (auto &alias : join_pushed_aliases) {
				local_table_names.erase(alias);
			}
		}
	}

	// Restore shadowed outer CTE entries (inner CTEs go out of scope)
	for (auto &cte_pair : node.cte_map.map) {
		auto it = outer_cte_results.find(cte_pair.first);
		if (it != outer_cte_results.end()) {
			cte_results[cte_pair.first] = it->second;
		} else {
			cte_results.erase(cte_pair.first);
		}
	}

	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(InsertQueryNode &node) {
	// Process CTE definitions attached to this DML node so that any CTE reference in
	// the source SELECT or ON CONFLICT clause is correctly classified.
	case_insensitive_map_t<CatalogPushdownResult> outer_cte_results;
	CatalogPushdownResult cte_combined {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	for (auto &cte_pair : node.cte_map.map) {
		const string &cte_name = cte_pair.first;
		auto &cte_info = *cte_pair.second;
		auto it = cte_results.find(cte_name);
		if (it != cte_results.end()) {
			outer_cte_results[cte_name] = it->second;
		}
		CatalogPushdownResult cte_result;
		if (cte_info.query_node) {
			cte_result = Rewrite(*cte_info.query_node);
		} else {
			cte_result = {};
		}
		for (auto &key : cte_info.key_targets) {
			cte_result = Merge(cte_result, Rewrite(*key));
		}
		cte_results[cte_name] = cte_result;
		cte_combined = Merge(cte_combined, cte_result);
	}

	// InsertQueryNode stores the target table in catalog/schema/table string fields, not in table_ref
	// (table_ref is only set for ON CONFLICT cases and is an alias ref)
	BaseTableRef target_ref;
	target_ref.catalog_name = node.catalog;
	target_ref.schema_name = node.schema;
	target_ref.table_name = node.table;
	// Save local name state before analyzing the INSERT target. The target table is the
	// destination; its columns/name are not in scope for the source SELECT's FROM clause.
	// Without this, TrackLocalTable adds the local target's column names to
	// local_table_column_names, and HasLocalTableReference then falsely marks source-SELECT
	// subqueries as correlated to those columns — blocking valid remote pushdowns and
	// potentially causing "Multiple streaming scans" when the FROM table is individually
	// pushed while the falsely-blocked WHERE subquery stays local and opens a second stream.
	auto pre_target_local_names = local_table_names;
	auto pre_target_local_col_names = local_table_column_names;
	CatalogPushdownResult result = Rewrite(target_ref);

	// Merge CTE results: even unreferenced CTEs are serialized when the DML is pushed.
	result = Merge(result, cte_combined);

	// Target table must be remote for the whole INSERT to be pushed to the remote.
	// If it is local, push the SELECT source subquery individually if all-remote so that
	// DuckDB can execute: INSERT INTO local_t SELECT * FROM quack_query_by_name(...).
	// Skip individual pushdown when CTEs are present: the source may reference a local CTE.
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		// Always restore pre-target local name state so the INSERT target's column names do not
		// leak into the outer scope. Without this, an INSERT-as-CTE-body with sub-CTEs (which
		// skips the individual-source-push path) leaves TrackLocalTable's additions in
		// local_table_column_names, causing HasLocalTableReference false positives for the outer
		// query's subqueries that happen to use the same unqualified column names.
		local_table_names = std::move(pre_target_local_names);
		local_table_column_names = std::move(pre_target_local_col_names);
		if (node.cte_map.map.empty() && node.select_statement) {
			auto select_result = Rewrite(*node.select_statement->node);
			FinishPushdown(node.select_statement->node, select_result);
		}
		for (auto &cte_pair : node.cte_map.map) {
			auto it = outer_cte_results.find(cte_pair.first);
			if (it != outer_cte_results.end()) {
				cte_results[cte_pair.first] = it->second;
			} else {
				cte_results.erase(cte_pair.first);
			}
		}
		return {};
	}
	{
		// Save before both select_statement and on_conflict_info rewrites. Rewrite(SelectNode)
		// has a side effect of pushing remote JoinRef children even when the overall result is
		// UNKNOWN; if on_conflict_info later makes the result UNKNOWN, select_statement must be
		// restored so native INSERT execution does not encounter stale quack wrappers.
		// Save on_conflict_info for the same reason: its expressions may contain subqueries
		// whose JoinRef children get partially pushed in-place during Rewrite.
		unique_ptr<QueryNode> saved_select;
		if (node.select_statement) {
			saved_select = node.select_statement->node->Copy();
			result = Merge(result, Rewrite(*node.select_statement->node));
		}
		unique_ptr<OnConflictInfo> saved_conflict_info;
		if (node.on_conflict_info) {
			saved_conflict_info = node.on_conflict_info->Copy();
			if (node.on_conflict_info->condition) {
				result = Merge(result, Rewrite(*node.on_conflict_info->condition));
			}
			if (node.on_conflict_info->set_info) {
				if (node.on_conflict_info->set_info->condition) {
					result = Merge(result, Rewrite(*node.on_conflict_info->set_info->condition));
				}
				for (auto &expr : node.on_conflict_info->set_info->expressions) {
					result = Merge(result, Rewrite(*expr));
				}
			}
		}
		// Save returning_list: Rewrite may push JoinRef children inside subquery bodies in-place.
		// If returning_list makes result UNKNOWN after select_statement/on_conflict_info were
		// SINGLE_REMOTE (their JoinRef children already pushed), roll back to prevent stale
		// quack wrappers causing "Multiple streaming scans" in native execution.
		vector<unique_ptr<ParsedExpression>> saved_returning;
		for (auto &expr : node.returning_list) {
			saved_returning.push_back(expr->Copy());
		}
		for (auto &expr : node.returning_list) {
			result = Merge(result, Rewrite(*expr));
		}
		if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
			if (saved_select) {
				node.select_statement->node = std::move(saved_select);
			}
			if (saved_conflict_info) {
				node.on_conflict_info = std::move(saved_conflict_info);
			}
			node.returning_list = std::move(saved_returning);
		}
	}
	for (auto &cte_pair : node.cte_map.map) {
		auto it = outer_cte_results.find(cte_pair.first);
		if (it != outer_cte_results.end()) {
			cte_results[cte_pair.first] = it->second;
		} else {
			cte_results.erase(cte_pair.first);
		}
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(DeleteQueryNode &node) {
	// Process CTE definitions attached to this DML node so that CTE references in the
	// WHERE condition or USING clauses are correctly classified.
	case_insensitive_map_t<CatalogPushdownResult> outer_cte_results;
	CatalogPushdownResult cte_combined {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	for (auto &cte_pair : node.cte_map.map) {
		const string &cte_name = cte_pair.first;
		auto &cte_info = *cte_pair.second;
		auto it = cte_results.find(cte_name);
		if (it != cte_results.end()) {
			outer_cte_results[cte_name] = it->second;
		}
		CatalogPushdownResult cte_result;
		if (cte_info.query_node) {
			cte_result = Rewrite(*cte_info.query_node);
		} else {
			cte_result = {};
		}
		for (auto &key : cte_info.key_targets) {
			cte_result = Merge(cte_result, Rewrite(*key));
		}
		cte_results[cte_name] = cte_result;
		cte_combined = Merge(cte_combined, cte_result);
	}

	CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	if (node.table) {
		result = Rewrite(node.table);
	}

	// Merge CTE results: even unreferenced CTEs are serialized when the DML is pushed.
	result = Merge(result, cte_combined);

	// Target table must be remote for the whole DELETE to be pushed to the remote.
	// If it is local, still push individual remote subqueries in condition/USING for efficiency,
	// but only when no CTEs are present: a CTE-referencing subquery cannot be pushed individually.
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		if (node.cte_map.map.empty()) {
			// Push subqueries in the condition only when there are no USING clauses.
			// USING clause tables create outer-scope bindings: a subquery correlated with a
			// remote USING alias (e.g. WHERE i IN (SELECT j FROM rpc.t2 WHERE t2.k = rt.col))
			// must NOT be pushed independently. HasLocalTableReference only tracks local table
			// names, so remote USING aliases go undetected. Skipping pushdown when USING is
			// present is conservative but correct.
			if (node.condition && node.using_clauses.empty()) {
				PushdownSubqueries(node.condition);
			}
			// Push individual remote USING clauses and collect the catalog names pushed,
			// so that catalog-qualified column refs like "rpc.t1.i" in the WHERE condition
			// can be stripped to "t1.i" before binding (the alias on the pushed wrapper is "t1").
			from_pushed_catalog_names.clear();
			from_pushed_table_aliases.clear();
			for (auto &clause : node.using_clauses) {
				auto clause_result = Rewrite(clause);
				FinishPushdown(clause, clause_result);
				if (clause_result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG &&
				    clause_result.catalog) {
					const string &cat_name = clause_result.catalog->GetName();
					bool found = false;
					for (auto &existing : from_pushed_catalog_names) {
						if (StringUtil::CIEquals(existing, cat_name)) {
							found = true;
							break;
						}
					}
					if (!found) {
						from_pushed_catalog_names.push_back(cat_name);
					}
				}
			}
			if (node.condition) {
				for (auto &cat_name : from_pushed_catalog_names) {
					StripCatalogName(*node.condition, cat_name, false);
				}
			}
			from_pushed_catalog_names.clear();
			from_pushed_table_aliases.clear();
		}
		for (auto &cte_pair : node.cte_map.map) {
			auto it = outer_cte_results.find(cte_pair.first);
			if (it != outer_cte_results.end()) {
				cte_results[cte_pair.first] = it->second;
			} else {
				cte_results.erase(cte_pair.first);
			}
		}
		return {};
	}
	// Save condition and USING clauses together. Rewrite(ParsedExpression) on a condition
	// containing scalar subqueries can push their inner JoinRef children; if USING clauses
	// later make result UNKNOWN, both must be restored so native DELETE does not encounter
	// stale quack wrappers causing "Multiple streaming scans" errors.
	{
		unique_ptr<ParsedExpression> saved_condition;
		if (node.condition) {
			saved_condition = node.condition->Copy();
			result = Merge(result, Rewrite(*node.condition));
		}
		vector<unique_ptr<TableRef>> saved_using;
		for (auto &clause : node.using_clauses) {
			saved_using.push_back(clause->Copy());
		}
		for (auto &clause : node.using_clauses) {
			result = Merge(result, Rewrite(clause));
		}
		// Save returning_list: Rewrite may push JoinRef children inside subquery bodies in-place.
		// If returning_list makes result UNKNOWN after condition/USING were SINGLE_REMOTE (their
		// JoinRef children already pushed), roll back to prevent stale quack wrappers.
		vector<unique_ptr<ParsedExpression>> saved_returning;
		for (auto &expr : node.returning_list) {
			saved_returning.push_back(expr->Copy());
		}
		for (auto &expr : node.returning_list) {
			result = Merge(result, Rewrite(*expr));
		}
		if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
			node.using_clauses = std::move(saved_using);
			if (saved_condition) {
				node.condition = std::move(saved_condition);
			}
			node.returning_list = std::move(saved_returning);
		}
	}
	for (auto &cte_pair : node.cte_map.map) {
		auto it = outer_cte_results.find(cte_pair.first);
		if (it != outer_cte_results.end()) {
			cte_results[cte_pair.first] = it->second;
		} else {
			cte_results.erase(cte_pair.first);
		}
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(UpdateQueryNode &node) {
	// Process CTE definitions attached to this DML node so that CTE references in the
	// FROM clause or SET expressions are correctly classified.
	case_insensitive_map_t<CatalogPushdownResult> outer_cte_results;
	CatalogPushdownResult cte_combined {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	for (auto &cte_pair : node.cte_map.map) {
		const string &cte_name = cte_pair.first;
		auto &cte_info = *cte_pair.second;
		auto it = cte_results.find(cte_name);
		if (it != cte_results.end()) {
			outer_cte_results[cte_name] = it->second;
		}
		CatalogPushdownResult cte_result;
		if (cte_info.query_node) {
			cte_result = Rewrite(*cte_info.query_node);
		} else {
			cte_result = {};
		}
		for (auto &key : cte_info.key_targets) {
			cte_result = Merge(cte_result, Rewrite(*key));
		}
		cte_results[cte_name] = cte_result;
		cte_combined = Merge(cte_combined, cte_result);
	}

	CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	if (node.table) {
		result = Rewrite(node.table);
	}

	// Merge CTE results: even unreferenced CTEs are serialized when the DML is pushed.
	result = Merge(result, cte_combined);

	// Target table must be remote for the whole UPDATE to be pushed to the remote.
	// If it is local, still push individual remote subqueries in FROM/SET for efficiency,
	// but only when no CTEs are present: a CTE-referencing subquery cannot be pushed individually.
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		if (node.cte_map.map.empty()) {
			string pushed_from_alias;
			vector<string> update_pushed_cats;
			bool update_has_streaming_from = false;
			case_insensitive_set_t remote_from_join_aliases;
			from_pushed_catalog_names.clear();
			from_pushed_table_aliases.clear();
			if (node.from_table) {
				auto from_result = Rewrite(node.from_table);
				// Track whether any JoinRef children were individually pushed as quack streams
				// (before from_pushed_catalog_names is moved into update_pushed_cats).
				bool had_join_child_pushdowns = !from_pushed_catalog_names.empty();
				FinishPushdown(node.from_table, from_result);
				// Track alias of a pushed remote FROM table so correlated refs in SET expressions
				// are correctly detected and not pushed to the remote independently.
				if (from_result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG &&
				    !node.from_table->alias.empty()) {
					pushed_from_alias = node.from_table->alias;
					local_table_names.insert(pushed_from_alias);
				}
				// Register JoinRef-pushed aliases in local_table_names so HasLocalTableReference
				// inside PushdownSubqueries correctly detects correlated SET-expression subqueries
				// that reference a pushed JoinRef child by alias.
				for (auto &alias : from_pushed_table_aliases) {
					local_table_names.insert(alias);
				}
				// Collect pushed catalog names (direct + JoinRef children) to strip from SET exprs.
				update_pushed_cats = std::move(from_pushed_catalog_names);
				from_pushed_catalog_names.clear();
				if (from_result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG && from_result.catalog) {
					const string &cat_name = from_result.catalog->GetName();
					bool found = false;
					for (auto &e : update_pushed_cats) {
						if (StringUtil::CIEquals(e, cat_name)) {
							found = true;
							break;
						}
					}
					if (!found) {
						update_pushed_cats.push_back(cat_name);
					}
				}
				// Track whether a streaming quack connection is actually occupied by the FROM table.
				// FinishPushdown replaces pushed refs with a TABLE_FUNCTION wrapper; if it was
				// skipped (e.g. for a JoinRef or SubqueryRef with outer SINGLE_REMOTE CTEs),
				// no streaming slot is open. Use type==TABLE_FUNCTION as the definitive signal,
				// guarded by SINGLE_REMOTE to exclude pre-existing local table-function refs.
				bool from_actually_pushed = from_result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG &&
				                            node.from_table->type == TableReferenceType::TABLE_FUNCTION;
				update_has_streaming_from = from_actually_pushed || had_join_child_pushdowns;
				// When the FROM table is an all-remote JoinRef that FinishPushdown skips, its
				// individual table names are absent from local_table_names. Without them,
				// HasLocalTableReference inside PushdownSubqueries cannot detect SET-expression
				// subqueries correlated to those outer FROM tables
				// (e.g. SET col = (SELECT j FROM rpc.t3 WHERE t3.k = from_t1.k) FROM rpc.t1 JOIN rpc.t2).
				// Collect and temporarily register them, mirroring the remote_join_table_aliases
				// pattern used in Rewrite(SelectNode).
				if (!update_has_streaming_from &&
				    from_result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
					CollectTableAliases(*node.from_table, remote_from_join_aliases);
					for (auto &alias : remote_from_join_aliases) {
						local_table_names.insert(alias);
					}
				}
			}
			if (node.set_info) {
				// Strip pushed catalog names so "rpc.from_t.col" refs remain resolvable
				// after "rpc.from_t" becomes "quack_query_by_name(...) AS from_t".
				// Do not recurse into subquery bodies (false): those subqueries were not
				// pushed and still contain remote table refs that must remain qualified.
				for (auto &cat_name : update_pushed_cats) {
					if (node.set_info->condition) {
						StripCatalogName(*node.set_info->condition, cat_name, false);
					}
					for (auto &expr : node.set_info->expressions) {
						StripCatalogName(*expr, cat_name, false);
					}
				}
				// Only push SET-expression subqueries when the FROM is not already streaming.
				if (!update_has_streaming_from) {
					if (node.set_info->condition) {
						PushdownSubqueries(node.set_info->condition);
					}
					for (auto &expr : node.set_info->expressions) {
						PushdownSubqueries(expr);
					}
				}
			}
			for (auto &alias : remote_from_join_aliases) {
				local_table_names.erase(alias);
			}
			if (!pushed_from_alias.empty()) {
				local_table_names.erase(pushed_from_alias);
			}
			for (auto &alias : from_pushed_table_aliases) {
				local_table_names.erase(alias);
			}
			from_pushed_table_aliases.clear();
		}
		for (auto &cte_pair : node.cte_map.map) {
			auto it = outer_cte_results.find(cte_pair.first);
			if (it != outer_cte_results.end()) {
				cte_results[cte_pair.first] = it->second;
			} else {
				cte_results.erase(cte_pair.first);
			}
		}
		return {};
	}
	{
		// Save from_table before both from_table and set_info rewrites. Rewrite(JoinRef) wraps
		// remote children even when the overall result is UNKNOWN; if set_info later makes result
		// UNKNOWN, from_table must be restored so native UPDATE does not encounter stale quack
		// wrappers causing "Multiple streaming scans" errors.
		// Save set_info for the same reason: its expressions may contain subqueries whose JoinRef
		// children get partially pushed in-place during Rewrite.
		unique_ptr<TableRef> saved_from;
		if (node.from_table) {
			saved_from = node.from_table->Copy();
			result = Merge(result, Rewrite(node.from_table));
		}
		unique_ptr<UpdateSetInfo> saved_set_info;
		if (node.set_info) {
			saved_set_info = node.set_info->Copy();
			if (node.set_info->condition) {
				result = Merge(result, Rewrite(*node.set_info->condition));
			}
			for (auto &expr : node.set_info->expressions) {
				result = Merge(result, Rewrite(*expr));
			}
		}
		// Save returning_list: Rewrite may push JoinRef children inside subquery bodies in-place.
		// If returning_list makes result UNKNOWN after from_table/set_info were SINGLE_REMOTE
		// (their JoinRef children already pushed), roll back to prevent stale quack wrappers.
		vector<unique_ptr<ParsedExpression>> saved_returning;
		for (auto &expr : node.returning_list) {
			saved_returning.push_back(expr->Copy());
		}
		for (auto &expr : node.returning_list) {
			result = Merge(result, Rewrite(*expr));
		}
		if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
			if (saved_from) {
				node.from_table = std::move(saved_from);
			}
			if (saved_set_info) {
				node.set_info = std::move(saved_set_info);
			}
			node.returning_list = std::move(saved_returning);
		}
	}
	for (auto &cte_pair : node.cte_map.map) {
		auto it = outer_cte_results.find(cte_pair.first);
		if (it != outer_cte_results.end()) {
			cte_results[cte_pair.first] = it->second;
		} else {
			cte_results.erase(cte_pair.first);
		}
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(MergeIntoStatement &stmt) {
	// Process CTE definitions attached to this MERGE INTO statement
	case_insensitive_map_t<CatalogPushdownResult> outer_cte_results;
	CatalogPushdownResult cte_combined {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	for (auto &cte_pair : stmt.cte_map.map) {
		const string &cte_name = cte_pair.first;
		auto &cte_info = *cte_pair.second;
		auto it = cte_results.find(cte_name);
		if (it != cte_results.end()) {
			outer_cte_results[cte_name] = it->second;
		}
		CatalogPushdownResult cte_result;
		if (cte_info.query_node) {
			cte_result = Rewrite(*cte_info.query_node);
		} else {
			cte_result = {};
		}
		for (auto &key : cte_info.key_targets) {
			cte_result = Merge(cte_result, Rewrite(*key));
		}
		cte_results[cte_name] = cte_result;
		cte_combined = Merge(cte_combined, cte_result);
	}

	// Analyze the target table - must be remote for full pushdown
	CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	if (stmt.target) {
		result = Rewrite(stmt.target);
	}

	// Merge CTE results: even unreferenced CTEs are serialized when the DML is pushed.
	result = Merge(result, cte_combined);

	// Target must be remote; MERGE INTO cannot partially push with a local target.
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		for (auto &cte_pair : stmt.cte_map.map) {
			auto it = outer_cte_results.find(cte_pair.first);
			if (it != outer_cte_results.end()) {
				cte_results[cte_pair.first] = it->second;
			} else {
				cte_results.erase(cte_pair.first);
			}
		}
		return {};
	}

	// Analyze the source. Rewrite(JoinRef) has a side effect of wrapping remote children in
	// quack_query_by_name even when the overall result is UNKNOWN. Save the source and restore
	// it if the full-push analysis fails (either from source itself, or from join_condition /
	// actions processing below) so MERGE INTO can still execute natively without stale wrappers.
	{
		auto saved_source = stmt.source->Copy();
		result = Merge(result, Rewrite(stmt.source));
		if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
			stmt.source = std::move(saved_source);
			for (auto &cte_pair : stmt.cte_map.map) {
				auto it = outer_cte_results.find(cte_pair.first);
				if (it != outer_cte_results.end()) {
					cte_results[cte_pair.first] = it->second;
				} else {
					cte_results.erase(cte_pair.first);
				}
			}
			return {};
		}

		// Save join_condition and actions before analysis. Rewrite(ParsedExpression) on a
		// subquery that contains a mixed JoinRef (some SINGLE_REMOTE children, some local)
		// will push the remote JoinRef children to quack wrappers in-place. If join_condition
		// or actions later make the overall result UNKNOWN, those stale wrappers must be
		// rolled back so native MERGE INTO execution does not encounter concurrent quack
		// streaming connections from both the source scan and the modified expressions.
		unique_ptr<ParsedExpression> saved_join_condition;
		if (stmt.join_condition) {
			saved_join_condition = stmt.join_condition->Copy();
		}
		map<MergeActionCondition, vector<unique_ptr<MergeIntoAction>>> saved_actions;
		for (auto &entry : stmt.actions) {
			auto &vec = saved_actions[entry.first];
			for (auto &action : entry.second) {
				vec.push_back(action->Copy());
			}
		}

		// Analyze ON join condition
		if (stmt.join_condition) {
			result = Merge(result, Rewrite(*stmt.join_condition));
		}

		// Analyze WHEN MATCHED/NOT MATCHED action conditions and SET/INSERT expressions
		for (auto &entry : stmt.actions) {
			for (auto &action : entry.second) {
				if (action->condition) {
					result = Merge(result, Rewrite(*action->condition));
				}
				if (action->update_info) {
					if (action->update_info->condition) {
						result = Merge(result, Rewrite(*action->update_info->condition));
					}
					for (auto &expr : action->update_info->expressions) {
						result = Merge(result, Rewrite(*expr));
					}
				}
				for (auto &expr : action->expressions) {
					result = Merge(result, Rewrite(*expr));
				}
			}
		}

		if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
			stmt.source = std::move(saved_source);
			if (saved_join_condition) {
				stmt.join_condition = std::move(saved_join_condition);
			}
			stmt.actions = std::move(saved_actions);
		}
	}

	for (auto &cte_pair : stmt.cte_map.map) {
		auto it = outer_cte_results.find(cte_pair.first);
		if (it != outer_cte_results.end()) {
			cte_results[cte_pair.first] = it->second;
		} else {
			cte_results.erase(cte_pair.first);
		}
	}
	return result;
}

static void UnqualifyColumnRefs(ParsedExpression &expr, const string &catalog_name) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &col_ref = expr.Cast<ColumnRefExpression>();
		// In UNION/INTERSECT/EXCEPT ORDER BY, the output has no table associations.
		// Strip ALL qualifiers to just the column name when the leading qualifier matches the
		// pushed catalog (e.g. "rpc.t1.i" → "i", "rpc.i" → "i").
		if (col_ref.column_names.size() > 1 && StringUtil::CIEquals(col_ref.column_names[0], catalog_name)) {
			string col_name = col_ref.column_names.back();
			col_ref.column_names = {std::move(col_name)};
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](ParsedExpression &child) { UnqualifyColumnRefs(child, catalog_name); });
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(SetOperationNode &node) {
	// Process CTE definitions attached to this set operation so that CTE references inside
	// the children are correctly classified (e.g. WITH cte AS (...) SELECT * FROM cte UNION ALL ...).
	case_insensitive_map_t<CatalogPushdownResult> outer_cte_results;
	CatalogPushdownResult cte_combined {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	for (auto &cte_pair : node.cte_map.map) {
		const string &cte_name = cte_pair.first;
		auto &cte_info = *cte_pair.second;
		auto it = cte_results.find(cte_name);
		if (it != cte_results.end()) {
			outer_cte_results[cte_name] = it->second;
		}
		CatalogPushdownResult cte_result;
		if (cte_info.query_node) {
			cte_result = Rewrite(*cte_info.query_node);
		} else {
			cte_result = {};
		}
		for (auto &key : cte_info.key_targets) {
			cte_result = Merge(cte_result, Rewrite(*key));
		}
		cte_results[cte_name] = cte_result;
		cte_combined = Merge(cte_combined, cte_result);
	}

	// Save each child before analysis. Rewrite(SelectNode) calls Rewrite(JoinRef) which pushes
	// individual SINGLE_REMOTE children in-place via FinishPushdown even when the JoinRef result is
	// UNKNOWN (mixed join). If that child's overall result is UNKNOWN, those stale quack wrappers
	// must be rolled back so the child executes natively. A UNION where one child is pushed to quack
	// (slot 1) and another child has a stale wrapper (slot 2) would trigger "Multiple streaming scans".
	vector<unique_ptr<QueryNode>> saved_children;
	saved_children.reserve(node.children.size());
	for (auto &child : node.children) {
		saved_children.push_back(child->Copy());
	}
	// Rewrite each child independently so we can push down individual children if needed
	vector<CatalogPushdownResult> child_results;
	child_results.reserve(node.children.size());
	CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	for (auto &child : node.children) {
		auto child_result = Rewrite(*child);
		result = Merge(result, child_result);
		child_results.push_back(child_result);
	}

	// Merge CTE results: even unreferenced CTEs are serialized when the set operation is pushed.
	result = Merge(result, cte_combined);
	// Save modifier expressions before rewriting them. Rewrite(ParsedExpression) on a subquery
	// inside a modifier (e.g. ORDER BY (SELECT j FROM rpc.t2 JOIN local_t ON ...)) has the side
	// effect of wrapping SINGLE_REMOTE JoinRef children in quack functions in-place. If the
	// overall set-operation result is not SINGLE_REMOTE, those stale wrappers must be rolled back:
	// individual child pushdown (below) opens one quack streaming slot per pushed child, and a
	// modifier subquery running with a stale wrapper would try to open an additional concurrent
	// slot, triggering "Multiple streaming scans" errors.
	vector<unique_ptr<ResultModifier>> saved_modifiers;
	for (auto &modifier : node.modifiers) {
		saved_modifiers.push_back(modifier->Copy());
	}
	// Check result modifiers (ORDER BY / LIMIT on the set operation itself)
	for (auto &modifier : node.modifiers) {
		switch (modifier->type) {
		case ResultModifierType::ORDER_MODIFIER: {
			auto &order_mod = modifier->Cast<OrderModifier>();
			for (auto &order : order_mod.orders) {
				result = Merge(result, Rewrite(*order.expression));
			}
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &limit_mod = modifier->Cast<LimitModifier>();
			if (limit_mod.limit) {
				result = Merge(result, Rewrite(*limit_mod.limit));
			}
			if (limit_mod.offset) {
				result = Merge(result, Rewrite(*limit_mod.offset));
			}
			break;
		}
		case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
			auto &limit_mod = modifier->Cast<LimitPercentModifier>();
			if (limit_mod.limit) {
				result = Merge(result, Rewrite(*limit_mod.limit));
			}
			if (limit_mod.offset) {
				result = Merge(result, Rewrite(*limit_mod.offset));
			}
			break;
		}
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct_mod = modifier->Cast<DistinctModifier>();
			for (auto &expr : distinct_mod.distinct_on_targets) {
				result = Merge(result, Rewrite(*expr));
			}
			break;
		}
		default:
			break;
		}
	}
	// If the whole set operation resolves to a single remote catalog, propagate upward
	if (result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		for (auto &cte_pair : node.cte_map.map) {
			auto it = outer_cte_results.find(cte_pair.first);
			if (it != outer_cte_results.end()) {
				cte_results[cte_pair.first] = it->second;
			} else {
				cte_results.erase(cte_pair.first);
			}
		}
		return result;
	}
	// Restore modifiers to undo any in-place JoinRef partial-pushdown side effects from the
	// Rewrite calls above. The saved copies carry no stale quack wrappers, so modifier
	// subqueries will execute via DuckDB's own remote scanning rather than opening a competing
	// quack streaming connection alongside any individually-pushed child below.
	node.modifiers = std::move(saved_modifiers);
	// Before restoring UNKNOWN children, check whether any of them had remote refs pushed in-place
	// by Rewrite(JoinRef). TABLE_FUNCTION nodes appear inside an UNKNOWN child only when Rewrite
	// pushed a SINGLE_REMOTE join side in-place — this is a reliable indicator that the child
	// will open a native quack connection when executed (scanning rpc.* tables). If such a child
	// exists alongside an individually-pushed SINGLE_REMOTE sibling (quack streaming slot 1), the
	// native quack scan would open a concurrent second slot → "Multiple streaming scans". Children
	// whose UNKNOWN result comes only from unqualified local table names (e.g. local_t) do NOT get
	// TABLE_FUNCTION wrappers during Rewrite and therefore do not open any quack connection —
	// they are safe siblings and must not block individual pushdown of SINGLE_REMOTE siblings.
	bool any_risky_unknown_sibling = false;
	for (idx_t i = 0; i < node.children.size(); i++) {
		if (child_results[i].reference_type == CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE &&
		    node.children[i] && QueryNodeHasTableFunctionPush(*node.children[i])) {
			any_risky_unknown_sibling = true;
			break;
		}
	}
	// Restore children whose result is not SINGLE_REMOTE. Rewrite(JoinRef) pushes individual
	// SINGLE_REMOTE sides in-place even when the JoinRef result is UNKNOWN (mixed join). Those
	// stale wrappers remain in the child after Rewrite. If a sibling child is individually pushed
	// to a quack streaming slot (SINGLE_REMOTE), and this child's stale wrapper opens another slot
	// during native execution, both slots would be active concurrently → "Multiple streaming scans".
	// SINGLE_REMOTE children are never partially wrapped (their JoinRefs are either fully pushed or
	// not pushed at all), so restoring them is unnecessary and would incorrectly undo their analysis.
	for (idx_t i = 0; i < node.children.size(); i++) {
		if (child_results[i].reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
			node.children[i] = std::move(saved_children[i]);
		}
	}
	// Otherwise push down individual children that can be pushed, but only when no CTEs
	// are present (a CTE-referencing child cannot be pushed individually) AND no sibling
	// child is a risky UNKNOWN (see above: UNKNOWN child with detected remote refs that would
	// open a concurrent native quack connection alongside the pushed sibling's streaming slot).
	if (node.cte_map.map.empty() && !any_risky_unknown_sibling) {
		for (idx_t i = 0; i < node.children.size(); i++) {
			FinishPushdown(node.children[i], child_results[i]);
		}
		// Strip catalog names from ORDER BY / DISTINCT ON expressions on the set operation itself.
		// When a child is individually pushed (e.g., rpc.t1 → quack_query_by_name(...) AS t1),
		// ORDER BY expressions like "rpc.t1.i" must become just "i": the UNION output has no
		// table associations, so even "t1.i" would fail. Any qualifier whose leading part matches
		// the pushed catalog is stripped entirely — only the final column name is preserved.
		for (idx_t i = 0; i < child_results.size(); i++) {
			if (child_results[i].reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG ||
			    !child_results[i].catalog) {
				continue;
			}
			const string &cat_name = child_results[i].catalog->GetName();
			for (auto &modifier : node.modifiers) {
				switch (modifier->type) {
				case ResultModifierType::ORDER_MODIFIER: {
					auto &order_mod = modifier->Cast<OrderModifier>();
					for (auto &order : order_mod.orders) {
						UnqualifyColumnRefs(*order.expression, cat_name);
					}
					break;
				}
				case ResultModifierType::DISTINCT_MODIFIER: {
					auto &distinct_mod = modifier->Cast<DistinctModifier>();
					for (auto &expr : distinct_mod.distinct_on_targets) {
						UnqualifyColumnRefs(*expr, cat_name);
					}
					break;
				}
				default:
					break;
				}
			}
		}
		// Push remote scalar subqueries in the set operation's own modifiers, but only when
		// no child holds an active quack streaming connection. A streaming slot is open when:
		//   (a) FinishPushdown(QueryNode) successfully wrapped a child → the child is now a
		//       SelectNode whose from_table is TABLE_FUNCTION (child_result == SINGLE_REMOTE).
		//   (b) FinishPushdown(TableRef) wrapped the child SelectNode's from_table individually
		//       because the from_table is SINGLE_REMOTE but the SelectNode has UNKNOWN expressions
		//       → from_table type is TABLE_FUNCTION even though child_result is UNKNOWN.
		//   (c) Rewrite(JoinRef) pushed one side of a mixed-catalog join in-place → a TABLE_FUNCTION
		//       ref appears as a descendant of the child SelectNode's JoinRef from_table, even
		//       though child_result is UNKNOWN.
		// HasTableFunctionInTree detects all three: TABLE_FUNCTION at the from_table root (cases a/b)
		// or anywhere in the JoinRef subtree (case c).
		// QueryNodeHasTableFunctionPush extends the check to nested set operations so that streaming
		// slots hidden inside a child SetOperationNode (e.g., inner UNION with one side pushed) are
		// correctly detected — those would be missed by checking only direct SELECT_NODE children.
		bool any_child_pushed = false;
		for (auto &child : node.children) {
			if (child && QueryNodeHasTableFunctionPush(*child)) {
				any_child_pushed = true;
				break;
			}
		}
		if (!any_child_pushed) {
			for (auto &modifier : node.modifiers) {
				switch (modifier->type) {
				case ResultModifierType::ORDER_MODIFIER: {
					auto &order_mod = modifier->Cast<OrderModifier>();
					for (auto &order : order_mod.orders) {
						PushdownSubqueries(order.expression);
					}
					break;
				}
				case ResultModifierType::LIMIT_MODIFIER: {
					auto &limit_mod = modifier->Cast<LimitModifier>();
					if (limit_mod.limit) {
						PushdownSubqueries(limit_mod.limit);
					}
					if (limit_mod.offset) {
						PushdownSubqueries(limit_mod.offset);
					}
					break;
				}
				case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
					auto &limit_mod = modifier->Cast<LimitPercentModifier>();
					if (limit_mod.limit) {
						PushdownSubqueries(limit_mod.limit);
					}
					if (limit_mod.offset) {
						PushdownSubqueries(limit_mod.offset);
					}
					break;
				}
				case ResultModifierType::DISTINCT_MODIFIER: {
					auto &distinct_mod = modifier->Cast<DistinctModifier>();
					for (auto &expr : distinct_mod.distinct_on_targets) {
						PushdownSubqueries(expr);
					}
					break;
				}
				default:
					break;
				}
			}
		}
	}
	for (auto &cte_pair : node.cte_map.map) {
		auto it = outer_cte_results.find(cte_pair.first);
		if (it != outer_cte_results.end()) {
			cte_results[cte_pair.first] = it->second;
		} else {
			cte_results.erase(cte_pair.first);
		}
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(unique_ptr<TableRef> &ref) {
	switch (ref->type) {
	case TableReferenceType::BASE_TABLE:
		// Propagate the detection result up - BaseTableRef is never pushed down individually
		return Rewrite(ref->Cast<BaseTableRef>());
	case TableReferenceType::JOIN:
		return Rewrite(ref->Cast<JoinRef>());
	case TableReferenceType::SUBQUERY:
		return Rewrite(ref->Cast<SubqueryRef>());
	case TableReferenceType::EXPRESSION_LIST:
		return Rewrite(ref->Cast<ExpressionListRef>());
	case TableReferenceType::TABLE_FUNCTION:
		return Rewrite(ref->Cast<TableFunctionRef>());
	case TableReferenceType::EMPTY_FROM:
	case TableReferenceType::COLUMN_DATA:
		return {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	case TableReferenceType::PIVOT: {
		// PIVOT/UNPIVOT is never pushed to remote.  However, we must still Rewrite the source
		// so that its local table names and column names are populated in local_table_names /
		// local_table_column_names.  Without this, a correlated WHERE subquery referencing
		// a column from the PIVOT source is incorrectly classified as non-correlated and
		// pushed to the remote where the source table does not exist.
		auto &piv = ref->Cast<PivotRef>();
		if (piv.source) {
			Rewrite(piv.source);
		}
		if (!ref->alias.empty()) {
			local_table_names.insert(ref->alias);
		}
		return {};
	}
	default:
		return {};
	}
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(ExpressionListRef &ref) {
	CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	for (auto &row : ref.values) {
		for (auto &expr : row) {
			result = Merge(result, Rewrite(*expr));
		}
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(SubqueryRef &ref) {
	// Save outer scope so inner table/column names from the subquery body don't leak out.
	// A FROM-clause subquery creates its own binding scope: tables and columns defined inside
	// are not visible in the outer query (only the alias is). Without save/restore, column
	// names of local tables inside the subquery pollute local_table_column_names, causing
	// false-positive correlated-ref detections for later unqualified column refs.
	// Also save/restore from_pushed_catalog_names / from_pushed_table_aliases: the inner
	// subquery's JoinRef pushes are scoped to the subquery and must not appear in the outer
	// SelectNode's pushed set.
	auto saved_local_table_names = local_table_names;
	auto saved_local_table_column_names = local_table_column_names;
	auto saved_from_pushed_catalog_names = std::move(from_pushed_catalog_names);
	auto saved_from_pushed_table_aliases = std::move(from_pushed_table_aliases);
	from_pushed_catalog_names.clear();
	from_pushed_table_aliases.clear();
	// Save the subquery node before analysis. Rewrite(SelectNode) calls Rewrite(JoinRef) which
	// pushes individual SINGLE_REMOTE children in-place via FinishPushdown even when the overall
	// JoinRef result is UNKNOWN (mixed join). If the subquery body ends up UNKNOWN (e.g. because
	// a JoinRef inside it is mixed), those stale quack wrappers must be rolled back; otherwise
	// native execution of the SubqueryRef would open multiple concurrent quack streaming
	// connections ("Multiple streaming scans" error).
	auto saved_subquery_node = ref.subquery->node->Copy();
	auto result = Rewrite(*ref.subquery->node);
	from_pushed_catalog_names = std::move(saved_from_pushed_catalog_names);
	from_pushed_table_aliases = std::move(saved_from_pushed_table_aliases);
	// If the subquery references outer local tables (e.g., a lateral join), block pushdown.
	// HasLocalTableReference is called before restoring so it can see the saved outer tables
	// (for SINGLE_REMOTE results the inner Rewrite adds no local entries, so the sets are
	// identical to the saved state at this point).
	if (result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG &&
	    HasLocalTableReference(*ref.subquery->node)) {
		result = {CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE, nullptr};
	}
	// Restore the subquery node when result is not SINGLE_REMOTE to remove any stale quack
	// wrappers inserted in-place by Rewrite(JoinRef) during mixed-JoinRef analysis above.
	// When result IS SINGLE_REMOTE the enclosing FinishPushdown will wrap the whole subquery,
	// so the internal state does not matter and restore is unnecessary.
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		ref.subquery->node = std::move(saved_subquery_node);
	}
	local_table_names = std::move(saved_local_table_names);
	local_table_column_names = std::move(saved_local_table_column_names);
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG && !ref.alias.empty()) {
		local_table_names.insert(ref.alias);
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(TableFunctionRef &ref) {
	if (ref.function->GetExpressionClass() != ExpressionClass::FUNCTION) {
		return {};
	}
	auto &func_expr = ref.function->Cast<FunctionExpression>();

	// Resolve schema-as-catalog ambiguity: "rpc.my_tvf()" is parsed as schema="rpc", catalog="",
	// but "rpc" is a catalog name.  Apply the same resolution that BaseTableRef uses so that
	// catalog-qualified TVF calls are correctly recognized as remote.
	string catalog_name = func_expr.catalog;
	string schema_name = func_expr.schema;
	Binder::BindSchemaOrCatalog(binder.context, catalog_name, schema_name);

	// If the function has an explicit catalog prefix, check if it's remote
	if (!catalog_name.empty()) {
		auto catalog = Catalog::GetCatalogEntry(binder.context, catalog_name);
		if (catalog && catalog->IsRemoteCatalog()) {
			// Check args: a local macro or UNKNOWN expression in args blocks pushdown
			CatalogPushdownResult result {CatalogReferenceType::SINGLE_REMOTE_CATALOG, catalog};
			for (auto &arg : func_expr.children) {
				result = Merge(result, Rewrite(*arg));
			}
			return result;
		}
		// Local catalog (explicitly qualified): StripCatalogName only strips the remote
		// catalog's name. An explicit local qualifier like "memory.main.range(0,10)" would
		// survive stripping and appear in the pushed SQL, which the remote cannot resolve.
		// Block pushdown for ALL explicitly-catalogued local functions regardless of whether
		// they are SET_RETURNING (which is neutral when used unqualified) or TABLE_RETURNING.
		// Returning NO_CATALOG_REFERENCED here would cause Merge(NO_CATALOG, SINGLE_REMOTE)
		// = SINGLE_REMOTE, incorrectly classifying a mixed query as all-remote.
		if (!ref.alias.empty()) {
			local_table_names.insert(ref.alias);
		}
		return {};
	}

	// Determine whether the function is a SET_RETURNING_FUNCTION (like range(), generate_series())
	// SET_RETURNING_FUNCTION entries are neutral: they don't belong to any catalog and can be pushed
	FindRemoteCatalogsInSearchPath();
	EntryLookupInfo func_lookup(CatalogType::TABLE_FUNCTION_ENTRY, func_expr.function_name);
	for (auto &local_entry : local_catalogs_in_search_path) {
		const string &schema = schema_name.empty() ? local_entry.schema : schema_name;
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
				if (!ref.alias.empty()) {
					local_table_names.insert(ref.alias);
				}
				return {};
			}
			// SET_RETURNING_FUNCTION: neutral, recurse into args
			CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
			for (auto &arg : func_expr.children) {
				result = Merge(result, Rewrite(*arg));
			}
			return result;
		}
	}
	// Not found in local catalogs - unknown function, blocks pushdown; track alias
	// so any lateral subquery that references this function's output is detected
	if (!ref.alias.empty()) {
		local_table_names.insert(ref.alias);
	}
	return {};
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(JoinRef &ref) {
	// Rewrite both sides independently, tracking their individual results
	auto left_result = Rewrite(ref.left);
	auto right_result = Rewrite(ref.right);
	auto result = Merge(left_result, right_result);
	// Also analyze the join condition - it may contain subqueries or local macro calls
	// that affect whether the join can be pushed as a whole.
	// Save condition before analysis: Rewrite may push inner JoinRef children in-place inside
	// scalar subqueries. If the condition makes the result UNKNOWN and we fall through to
	// individual child pushdown, restoring prevents concurrent quack streaming connections —
	// individually-pushed left/right (up to 2 slots) + stale wrapper inside condition = 3 slots.
	unique_ptr<ParsedExpression> saved_condition;
	if (ref.condition) {
		saved_condition = ref.condition->Copy();
		result = Merge(result, Rewrite(*ref.condition));
	}
	// If both sides (and the condition) resolve to the same remote catalog, propagate upward
	if (result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		return result;
	}
	// Restore condition to remove any stale quack wrappers Rewrite inserted in-place.
	// Catalog stripping (below) will strip the remote catalog prefix from the restored condition.
	if (saved_condition) {
		ref.condition = std::move(saved_condition);
	}
	// The JoinRef cannot be pushed as a whole — push each side individually where possible.
	// Before pushing the right side, check whether it has a correlated reference to the left
	// side's tables (e.g., a LATERAL subquery whose WHERE clause references the left table's
	// output columns by alias). Such a right side depends on the left's row-by-row output and
	// must NOT be pushed independently: the generated SQL would reference a left alias that is
	// not in scope for a standalone remote execution.
	// Detect this by temporarily registering the left aliases in local_table_names and calling
	// HasLocalTableReference on the right-side TableRef.
	// NOTE: This detection is intentionally deferred until after the SINGLE_REMOTE guard above
	// so it does NOT affect the overall JoinRef classification. When both sides are SINGLE_REMOTE
	// the guard exits early and the join is pushed as a whole — correct, since on the remote the
	// lateral's left table IS in scope. Detecting correlation during Rewrite(right) would
	// incorrectly downgrade right from SINGLE_REMOTE to UNKNOWN, turning a pushable all-remote
	// lateral into an unpushable mixed query and causing "Multiple streaming scans" errors.
	case_insensitive_set_t left_aliases;
	CollectTableAliases(*ref.left, left_aliases);
	// Only track aliases not already in local_table_names to avoid erasing pre-existing entries.
	case_insensitive_set_t newly_added;
	for (auto &alias : left_aliases) {
		if (local_table_names.insert(alias).second) {
			newly_added.insert(alias);
		}
	}
	bool right_correlated_to_left = (right_result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG &&
	                                 HasLocalTableReference(*ref.right));
	for (auto &alias : newly_added) {
		local_table_names.erase(alias);
	}
	FinishPushdown(ref.left, left_result);
	FinishPushdown(ref.right, right_correlated_to_left ? CatalogPushdownResult {} : right_result);
	// Strip catalog name from the join condition and record pushed catalogs so the enclosing
	// SelectNode can strip those catalog names from its outer expressions (SELECT list, WHERE,
	// HAVING, etc.). Without this, catalog-qualified refs like "rpc.t1.i" in the join condition
	// or outer expressions become unresolvable after "rpc.t1" is replaced by
	// "quack_query_by_name(...) AS t1" in the FROM clause.
	// Only record a child's catalog in from_pushed_catalog_names when FinishPushdown actually
	// replaced the child with a TABLE_FUNCTION ref (i.e. a quack streaming slot is now occupied).
	// When FinishPushdown is skipped (JoinRef child, CTE ref, SubqueryRef with outer CTEs),
	// the child stays in the FROM tree and no streaming slot is opened. Recording it anyway
	// would set had_join_child_pushdowns=true in the enclosing SelectNode, incorrectly blocking
	// PushdownSubqueries for WHERE/HAVING subqueries that could safely be pushed.
	for (idx_t ci = 0; ci < 2; ci++) {
		auto &child_result = (ci == 0) ? left_result : right_result;
		auto &child_ref = (ci == 0) ? ref.left : ref.right;
		if (child_result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
			continue;
		}
		const string &cat_name = child_result.catalog->GetName();
		// Always strip from the join condition regardless of whether the child was pushed.
		// Catalog-qualified column refs are resolvable via the table name alone once the
		// catalog prefix is removed, so stripping is safe even for unpushed children.
		if (ref.condition) {
			StripCatalogName(*ref.condition, cat_name, false);
		}
		// Only track as an active streaming slot when FinishPushdown actually replaced the child.
		if (child_ref->type != TableReferenceType::TABLE_FUNCTION) {
			continue;
		}
		bool already_tracked = false;
		for (auto &existing : from_pushed_catalog_names) {
			if (StringUtil::CIEquals(existing, cat_name)) {
				already_tracked = true;
				break;
			}
		}
		if (!already_tracked) {
			from_pushed_catalog_names.push_back(cat_name);
		}
	}
	// Track aliases of individually-pushed remote children (and CTE/SubqueryRef children that were
	// skipped by FinishPushdown but whose aliases could be correlated-to by outer subqueries) so
	// that HasLocalTableReference inside PushdownSubqueries correctly detects correlated
	// WHERE/HAVING subqueries. We check the result type (SINGLE_REMOTE) rather than the post-push
	// ref type (TABLE_FUNCTION): for CTE refs and SubqueryRef-with-outer-CTEs that FinishPushdown
	// skips, the alias is still worth tracking to block pushdown of subqueries that reference it
	// (they would depend on the CTE definition unavailable on the remote).
	for (idx_t ci = 0; ci < 2; ci++) {
		auto &child_pushed_result = (ci == 0) ? left_result : right_result;
		if (child_pushed_result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
			continue;
		}
		auto *child_ref = (ci == 0) ? ref.left.get() : ref.right.get();
		if (!child_ref || child_ref->alias.empty()) {
			continue;
		}
		bool already_tracked = false;
		for (auto &existing : from_pushed_table_aliases) {
			if (StringUtil::CIEquals(existing, child_ref->alias)) {
				already_tracked = true;
				break;
			}
		}
		if (!already_tracked) {
			from_pushed_table_aliases.push_back(child_ref->alias);
		}
	}
	return result;
}

void RemotePushdownOptimizer::TrackLocalTable(const BaseTableRef &ref, optional_ptr<CatalogEntry> entry) {
	local_table_names.insert(ref.table_name);
	if (!ref.alias.empty()) {
		local_table_names.insert(ref.alias);
	}
	if (entry && entry->type == CatalogType::TABLE_ENTRY) {
		for (auto &col : entry->Cast<TableCatalogEntry>().GetColumns().Logical()) {
			local_table_column_names.insert(col.Name());
		}
	}
}

bool RemotePushdownOptimizer::IsLocalMacro(const FunctionExpression &func) {
	// If explicitly qualified with a catalog, check whether that catalog is remote
	if (!func.catalog.empty()) {
		auto catalog = Catalog::GetCatalogEntry(binder.context, func.catalog);
		if (catalog && catalog->IsRemoteCatalog()) {
			return false;
		}
		// Local catalog - check if the function is a macro
		const string &schema = func.schema.empty() ? DEFAULT_SCHEMA : func.schema;
		EntryLookupInfo macro_lookup(CatalogType::MACRO_ENTRY, func.function_name);
		auto entry =
		    Catalog::GetEntry(binder.context, func.catalog, schema, macro_lookup, OnEntryNotFound::RETURN_NULL);
		if (entry && entry->type == CatalogType::MACRO_ENTRY) {
			return true;
		}
		EntryLookupInfo table_macro_lookup(CatalogType::TABLE_MACRO_ENTRY, func.function_name);
		auto table_entry =
		    Catalog::GetEntry(binder.context, func.catalog, schema, table_macro_lookup, OnEntryNotFound::RETURN_NULL);
		return table_entry && table_entry->type == CatalogType::TABLE_MACRO_ENTRY;
	}

	// Unqualified function - search local catalogs for a macro with this name
	FindRemoteCatalogsInSearchPath();
	for (auto &local_entry : local_catalogs_in_search_path) {
		const string &schema = func.schema.empty() ? local_entry.schema : func.schema;
		EntryLookupInfo macro_lookup(CatalogType::MACRO_ENTRY, func.function_name);
		auto entry =
		    Catalog::GetEntry(binder.context, local_entry.catalog, schema, macro_lookup, OnEntryNotFound::RETURN_NULL);
		if (entry && entry->type == CatalogType::MACRO_ENTRY) {
			return true;
		}
		EntryLookupInfo table_macro_lookup(CatalogType::TABLE_MACRO_ENTRY, func.function_name);
		auto table_entry = Catalog::GetEntry(binder.context, local_entry.catalog, schema, table_macro_lookup,
		                                     OnEntryNotFound::RETURN_NULL);
		if (table_entry && table_entry->type == CatalogType::TABLE_MACRO_ENTRY) {
			return true;
		}
	}
	return false;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(BaseTableRef &ref) {
	// Resolve schema_name-as-catalog ambiguity using the binder's own resolution logic
	string catalog_name = ref.catalog_name;
	string schema_name = ref.schema_name;
	Binder::BindSchemaOrCatalog(binder.context, catalog_name, schema_name);

	// Case 0: check if this is a CTE reference (must have no explicit catalog/schema)
	if (catalog_name.empty() && schema_name.empty()) {
		auto cte_it = cte_results.find(ref.table_name);
		if (cte_it != cte_results.end()) {
			if (cte_it->second.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
				// Local/unknown CTE - track as local for correlated subquery detection
				local_table_names.insert(ref.table_name);
				if (!ref.alias.empty()) {
					local_table_names.insert(ref.alias);
				}
			}
			return cte_it->second;
		}
	}

	// Case 1: catalog is explicitly specified - check if it's a remote catalog
	if (!catalog_name.empty()) {
		auto catalog = Catalog::GetCatalogEntry(binder.context, catalog_name);
		if (catalog && catalog->IsRemoteCatalog()) {
			return {CatalogReferenceType::SINGLE_REMOTE_CATALOG, catalog};
		}
		// Local catalog: look up the entry to populate column names for correlated ref detection
		if (catalog) {
			const string &schema = schema_name.empty() ? DEFAULT_SCHEMA : schema_name;
			EntryLookupInfo explicit_table_lookup(CatalogType::TABLE_ENTRY, ref.table_name);
			auto found_entry = Catalog::GetEntry(binder.context, catalog_name, schema, explicit_table_lookup,
			                                     OnEntryNotFound::RETURN_NULL);
			TrackLocalTable(ref, found_entry);
		} else {
			TrackLocalTable(ref);
		}
		// A local table always blocks pushdown of any query that contains it.
		// Returning UNKNOWN (not NO_CATALOG) ensures Merge(SINGLE_REMOTE, UNKNOWN) = UNKNOWN
		// rather than the otherwise-neutral SINGLE_REMOTE.
		return {};
	}

	// Case 2: no explicit catalog - lazily populate search path catalogs on first use
	FindRemoteCatalogsInSearchPath();

	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, ref.table_name);

	if (remote_catalogs_in_search_path.size() != 1) {
		// Can't determine remote catalog; try to get column info from local catalogs for correlated subquery detection
		optional_ptr<CatalogEntry> found_entry;
		for (auto &local_entry : local_catalogs_in_search_path) {
			const auto &schema = schema_name.empty() ? local_entry.schema : schema_name;
			found_entry = Catalog::GetEntry(binder.context, local_entry.catalog, schema, table_lookup,
			                                OnEntryNotFound::RETURN_NULL);
			if (found_entry) {
				break;
			}
		}
		TrackLocalTable(ref, found_entry);
		return {};
	}

	for (auto &local_entry : local_catalogs_in_search_path) {
		// If the ref specifies a schema, use it; otherwise use the search path schema
		const auto &schema = schema_name.empty() ? local_entry.schema : schema_name;
		auto entry =
		    Catalog::GetEntry(binder.context, local_entry.catalog, schema, table_lookup, OnEntryNotFound::RETURN_NULL);
		if (entry) {
			TrackLocalTable(ref, entry);
			// Same as Case 1: local table → UNKNOWN to prevent Merge from treating it as neutral.
			return {};
		}
	}

	// Not found in any local catalog - push to the single remote catalog in the search path
	return {CatalogReferenceType::SINGLE_REMOTE_CATALOG, remote_catalogs_in_search_path.front().get()};
}

bool RemotePushdownOptimizer::HasLocalTableReference(ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &col_ref = expr.Cast<ColumnRefExpression>();
		if (col_ref.column_names.size() >= 2) {
			// The second-to-last entry is the table qualifier (e.g. "tbl" in "tbl.col" or "cat.schema.tbl.col")
			const auto &table_name = col_ref.column_names[col_ref.column_names.size() - 2];
			return local_table_names.count(table_name) > 0;
		}
		// Unqualified column: correlated if the name matches a column of any outer local table
		return local_table_column_names.count(col_ref.column_names[0]) > 0;
	}
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subq = expr.Cast<SubqueryExpression>();
		if (subq.child && HasLocalTableReference(*subq.child)) {
			return true;
		}
		return HasLocalTableReference(*subq.subquery->node);
	}
	bool found = false;
	ParsedExpressionIterator::EnumerateChildren(expr, [&](ParsedExpression &child) {
		if (!found) {
			found = HasLocalTableReference(child);
		}
	});
	return found;
}

bool RemotePushdownOptimizer::HasLocalTableReference(QueryNode &node) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		for (auto &expr : select.select_list) {
			if (HasLocalTableReference(*expr)) {
				return true;
			}
		}
		if (select.where_clause && HasLocalTableReference(*select.where_clause)) {
			return true;
		}
		if (select.having && HasLocalTableReference(*select.having)) {
			return true;
		}
		if (select.qualify && HasLocalTableReference(*select.qualify)) {
			return true;
		}
		for (auto &expr : select.groups.group_expressions) {
			if (HasLocalTableReference(*expr)) {
				return true;
			}
		}
		// Also check from_table: table function args and join conditions can carry correlated
		// references (e.g. LATERAL (SELECT * FROM rpc.fn(outer_col)) or ON outer_col = t.i).
		if (select.from_table && HasLocalTableReference(*select.from_table)) {
			return true;
		}
		// Also check CTE bodies: a CTE defined inside a lateral subquery may reference outer
		// local table columns (e.g. WITH cte AS (SELECT i FROM rpc.t1 WHERE i = outer_col)).
		// Without this check, the correlated ref hidden in the CTE body is missed and the
		// lateral is incorrectly classified as fully-remote and pushed to the remote server.
		for (auto &cte_pair : select.cte_map.map) {
			if (cte_pair.second->query_node && HasLocalTableReference(*cte_pair.second->query_node)) {
				return true;
			}
			for (auto &key : cte_pair.second->key_targets) {
				if (HasLocalTableReference(*key)) {
					return true;
				}
			}
		}
		// Also check modifiers: ORDER BY, DISTINCT ON, and LIMIT expressions can carry
		// correlated references to outer local tables (e.g. LATERAL ... ORDER BY outer_col).
		for (auto &modifier : select.modifiers) {
			switch (modifier->type) {
			case ResultModifierType::ORDER_MODIFIER: {
				for (auto &order : modifier->Cast<OrderModifier>().orders) {
					if (HasLocalTableReference(*order.expression)) {
						return true;
					}
				}
				break;
			}
			case ResultModifierType::DISTINCT_MODIFIER: {
				for (auto &expr : modifier->Cast<DistinctModifier>().distinct_on_targets) {
					if (HasLocalTableReference(*expr)) {
						return true;
					}
				}
				break;
			}
			case ResultModifierType::LIMIT_MODIFIER: {
				auto &lm = modifier->Cast<LimitModifier>();
				if (lm.limit && HasLocalTableReference(*lm.limit)) {
					return true;
				}
				if (lm.offset && HasLocalTableReference(*lm.offset)) {
					return true;
				}
				break;
			}
			case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
				auto &lm = modifier->Cast<LimitPercentModifier>();
				if (lm.limit && HasLocalTableReference(*lm.limit)) {
					return true;
				}
				if (lm.offset && HasLocalTableReference(*lm.offset)) {
					return true;
				}
				break;
			}
			default:
				break;
			}
		}
		return false;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop = node.Cast<SetOperationNode>();
		for (auto &cte_pair : setop.cte_map.map) {
			if (cte_pair.second->query_node && HasLocalTableReference(*cte_pair.second->query_node)) {
				return true;
			}
			for (auto &key : cte_pair.second->key_targets) {
				if (HasLocalTableReference(*key)) {
					return true;
				}
			}
		}
		for (auto &child : setop.children) {
			if (HasLocalTableReference(*child)) {
				return true;
			}
		}
		for (auto &modifier : setop.modifiers) {
			switch (modifier->type) {
			case ResultModifierType::ORDER_MODIFIER: {
				for (auto &order : modifier->Cast<OrderModifier>().orders) {
					if (HasLocalTableReference(*order.expression)) {
						return true;
					}
				}
				break;
			}
			case ResultModifierType::DISTINCT_MODIFIER: {
				for (auto &expr : modifier->Cast<DistinctModifier>().distinct_on_targets) {
					if (HasLocalTableReference(*expr)) {
						return true;
					}
				}
				break;
			}
			case ResultModifierType::LIMIT_MODIFIER: {
				auto &lm = modifier->Cast<LimitModifier>();
				if (lm.limit && HasLocalTableReference(*lm.limit)) {
					return true;
				}
				if (lm.offset && HasLocalTableReference(*lm.offset)) {
					return true;
				}
				break;
			}
			case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
				auto &lm = modifier->Cast<LimitPercentModifier>();
				if (lm.limit && HasLocalTableReference(*lm.limit)) {
					return true;
				}
				if (lm.offset && HasLocalTableReference(*lm.offset)) {
					return true;
				}
				break;
			}
			default:
				break;
			}
		}
		return false;
	}
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &rec = node.Cast<RecursiveCTENode>();
		for (auto &cte_pair : rec.cte_map.map) {
			if (cte_pair.second->query_node && HasLocalTableReference(*cte_pair.second->query_node)) {
				return true;
			}
			for (auto &key : cte_pair.second->key_targets) {
				if (HasLocalTableReference(*key)) {
					return true;
				}
			}
		}
		return (rec.left && HasLocalTableReference(*rec.left)) || (rec.right && HasLocalTableReference(*rec.right));
	}
	case QueryNodeType::INSERT_QUERY_NODE: {
		// DML nodes can appear as CTE bodies (writeable CTEs) or in lateral subqueries.
		// Their body expressions can carry correlated references to outer local tables, so
		// falling through to "return false" would incorrectly classify the enclosing subquery
		// as non-correlated — causing it to be pushed to the remote where the outer local
		// table does not exist.
		auto &insert = node.Cast<InsertQueryNode>();
		for (auto &cte_pair : insert.cte_map.map) {
			if (cte_pair.second->query_node && HasLocalTableReference(*cte_pair.second->query_node)) {
				return true;
			}
			for (auto &key : cte_pair.second->key_targets) {
				if (HasLocalTableReference(*key)) {
					return true;
				}
			}
		}
		if (insert.select_statement && HasLocalTableReference(*insert.select_statement->node)) {
			return true;
		}
		if (insert.on_conflict_info) {
			if (insert.on_conflict_info->condition && HasLocalTableReference(*insert.on_conflict_info->condition)) {
				return true;
			}
			if (insert.on_conflict_info->set_info) {
				if (insert.on_conflict_info->set_info->condition &&
				    HasLocalTableReference(*insert.on_conflict_info->set_info->condition)) {
					return true;
				}
				for (auto &expr : insert.on_conflict_info->set_info->expressions) {
					if (HasLocalTableReference(*expr)) {
						return true;
					}
				}
			}
		}
		for (auto &expr : insert.returning_list) {
			if (HasLocalTableReference(*expr)) {
				return true;
			}
		}
		return false;
	}
	case QueryNodeType::DELETE_QUERY_NODE: {
		auto &del = node.Cast<DeleteQueryNode>();
		for (auto &cte_pair : del.cte_map.map) {
			if (cte_pair.second->query_node && HasLocalTableReference(*cte_pair.second->query_node)) {
				return true;
			}
			for (auto &key : cte_pair.second->key_targets) {
				if (HasLocalTableReference(*key)) {
					return true;
				}
			}
		}
		if (del.condition && HasLocalTableReference(*del.condition)) {
			return true;
		}
		for (auto &clause : del.using_clauses) {
			if (HasLocalTableReference(*clause)) {
				return true;
			}
		}
		for (auto &expr : del.returning_list) {
			if (HasLocalTableReference(*expr)) {
				return true;
			}
		}
		return false;
	}
	case QueryNodeType::UPDATE_QUERY_NODE: {
		auto &upd = node.Cast<UpdateQueryNode>();
		for (auto &cte_pair : upd.cte_map.map) {
			if (cte_pair.second->query_node && HasLocalTableReference(*cte_pair.second->query_node)) {
				return true;
			}
			for (auto &key : cte_pair.second->key_targets) {
				if (HasLocalTableReference(*key)) {
					return true;
				}
			}
		}
		if (upd.from_table && HasLocalTableReference(*upd.from_table)) {
			return true;
		}
		if (upd.set_info) {
			if (upd.set_info->condition && HasLocalTableReference(*upd.set_info->condition)) {
				return true;
			}
			for (auto &expr : upd.set_info->expressions) {
				if (HasLocalTableReference(*expr)) {
					return true;
				}
			}
		}
		for (auto &expr : upd.returning_list) {
			if (HasLocalTableReference(*expr)) {
				return true;
			}
		}
		return false;
	}
	default:
		return false;
	}
}

bool RemotePushdownOptimizer::HasLocalTableReference(TableRef &ref) {
	switch (ref.type) {
	case TableReferenceType::TABLE_FUNCTION: {
		auto &tf = ref.Cast<TableFunctionRef>();
		return tf.function && HasLocalTableReference(*tf.function);
	}
	case TableReferenceType::SUBQUERY: {
		// A SubqueryRef in a FROM clause (e.g. inside a JoinRef's left/right) may contain
		// correlated references to outer local tables in its body. Without this case the JoinRef
		// handler recurses into SubqueryRef via HasLocalTableReference(TableRef&) and hits the
		// default (return false), silently missing the correlated ref and allowing incorrect pushdown.
		auto &sq = ref.Cast<SubqueryRef>();
		return sq.subquery && HasLocalTableReference(*sq.subquery->node);
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		if (HasLocalTableReference(*join.left)) {
			return true;
		}
		if (HasLocalTableReference(*join.right)) {
			return true;
		}
		return join.condition && HasLocalTableReference(*join.condition);
	}
	case TableReferenceType::EXPRESSION_LIST: {
		auto &el = ref.Cast<ExpressionListRef>();
		for (auto &row : el.values) {
			for (auto &expr : row) {
				if (HasLocalTableReference(*expr)) {
					return true;
				}
			}
		}
		return false;
	}
	case TableReferenceType::PIVOT: {
		auto &piv = ref.Cast<PivotRef>();
		if (piv.source && HasLocalTableReference(*piv.source)) {
			return true;
		}
		for (auto &agg : piv.aggregates) {
			if (HasLocalTableReference(*agg)) {
				return true;
			}
		}
		for (auto &pivot_col : piv.pivots) {
			for (auto &expr : pivot_col.pivot_expressions) {
				if (HasLocalTableReference(*expr)) {
					return true;
				}
			}
			for (auto &entry : pivot_col.entries) {
				if (entry.expr && HasLocalTableReference(*entry.expr)) {
					return true;
				}
			}
		}
		return false;
	}
	default:
		return false;
	}
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery_expr = expr.Cast<SubqueryExpression>();
		CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
		// EnumerateChildren for SUBQUERY only visits `child` (e.g., left side of IN), not subquery->node
		if (subquery_expr.child) {
			result = Merge(result, Rewrite(*subquery_expr.child));
		}
		// Save outer scope: Rewrite(*subquery->node) may add the subquery's own local tables/columns,
		// and its JoinRef pushes must not appear in the outer SelectNode's from_pushed_catalog_names
		// or from_pushed_table_aliases.
		auto saved_local_table_names = local_table_names;
		auto saved_local_table_column_names = local_table_column_names;
		auto saved_from_pushed = std::move(from_pushed_catalog_names);
		auto saved_from_pushed_aliases = std::move(from_pushed_table_aliases);
		from_pushed_catalog_names.clear();
		from_pushed_table_aliases.clear();
		// Save subquery node before analysis: Rewrite(JoinRef) inside the body may push individual
		// SINGLE_REMOTE children in-place even when the JoinRef result is UNKNOWN (mixed join).
		// If the overall subquery result ends up UNKNOWN, restore to remove those stale wrappers
		// so native execution does not open multiple concurrent quack streaming connections.
		auto saved_subquery_node = subquery_expr.subquery->node->Copy();
		auto subquery_result = Rewrite(*subquery_expr.subquery->node);
		// A SINGLE_REMOTE result is only valid if the subquery has no correlated references to outer local tables
		if (subquery_result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG &&
		    HasLocalTableReference(*subquery_expr.subquery->node)) {
			subquery_result = {CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE, nullptr};
		}
		if (subquery_result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
			subquery_expr.subquery->node = std::move(saved_subquery_node);
		}
		local_table_names = std::move(saved_local_table_names);
		local_table_column_names = std::move(saved_local_table_column_names);
		from_pushed_catalog_names = std::move(saved_from_pushed);
		from_pushed_table_aliases = std::move(saved_from_pushed_aliases);
		result = Merge(result, subquery_result);
		return result;
	}
	// For CAST expressions with an unbound (user-defined) type, also scan the embedded type expression for catalog
	// references. EnumerateChildren for CAST only visits the child value, not the cast_type LogicalType field.
	if (expr.GetExpressionClass() == ExpressionClass::CAST) {
		auto &cast_expr = expr.Cast<CastExpression>();
		CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
		if (cast_expr.cast_type.id() == LogicalTypeId::UNBOUND) {
			result = Merge(result, Rewrite(*UnboundType::GetTypeExpression(cast_expr.cast_type)));
		}
		result = Merge(result, Rewrite(*cast_expr.child));
		return result;
	}
	// Handle function and window expressions: resolve catalog qualifiers.
	auto check_catalog_qualified_expr = [&](const string &raw_catalog,
	                                        const string &raw_schema) -> CatalogPushdownResult {
		string catalog_name = raw_catalog;
		string schema_name = raw_schema;
		Binder::BindSchemaOrCatalog(binder.context, catalog_name, schema_name);
		if (!catalog_name.empty()) {
			auto catalog = Catalog::GetCatalogEntry(binder.context, catalog_name);
			if (catalog && catalog->IsRemoteCatalog()) {
				CatalogPushdownResult result {CatalogReferenceType::SINGLE_REMOTE_CATALOG, catalog};
				ParsedExpressionIterator::EnumerateChildren(
				    expr, [&](ParsedExpression &child) { result = Merge(result, Rewrite(child)); });
				return result;
			}
			// Explicitly local-catalog: block pushdown.
			return {CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE, nullptr};
		}
		return {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	};
	if (expr.GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &func = expr.Cast<FunctionExpression>();
		auto cat_result = check_catalog_qualified_expr(func.catalog, func.schema);
		if (cat_result.reference_type != CatalogReferenceType::NO_CATALOG_REFERENCED) {
			return cat_result;
		}
		// Unqualified function: local macros can't be pushed to remote.
		if (IsLocalMacro(func)) {
			return {CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE, nullptr};
		}
	} else if (expr.GetExpressionClass() == ExpressionClass::WINDOW) {
		auto &win = expr.Cast<WindowExpression>();
		auto cat_result = check_catalog_qualified_expr(win.catalog, win.schema);
		if (cat_result.reference_type != CatalogReferenceType::NO_CATALOG_REFERENCED) {
			return cat_result;
		}
	} else if (expr.GetExpressionClass() == ExpressionClass::TYPE) {
		// Catalog-qualified user-defined types (e.g. "rpc.my_type" or "rpc.schema.my_type") reference a catalog.
		auto &type_expr = expr.Cast<TypeExpression>();
		auto cat_result = check_catalog_qualified_expr(type_expr.GetCatalog(), type_expr.GetSchema());
		if (cat_result.reference_type != CatalogReferenceType::NO_CATALOG_REFERENCED) {
			return cat_result;
		}
		// Unqualified type: fall through to EnumerateChildren for type parameters
	}
	CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](ParsedExpression &child) { result = Merge(result, Rewrite(child)); });
	return result;
}

void RemotePushdownOptimizer::PushdownSubqueries(unique_ptr<ParsedExpression> &expr) {
	if (!expr) {
		return;
	}
	if (expr->GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery_expr = expr->Cast<SubqueryExpression>();
		if (subquery_expr.child) {
			PushdownSubqueries(subquery_expr.child);
		}
		auto saved_local_table_names = local_table_names;
		auto saved_local_table_column_names = local_table_column_names;
		auto saved_from_pushed = std::move(from_pushed_catalog_names);
		auto saved_from_pushed_aliases = std::move(from_pushed_table_aliases);
		from_pushed_catalog_names.clear();
		from_pushed_table_aliases.clear();
		// Save subquery node: Rewrite(JoinRef) inside the body may push individual SINGLE_REMOTE
		// children in-place even when the JoinRef result is UNKNOWN. If the subquery is correlated
		// (has_correlated_ref), restore the saved copy to remove those stale quack wrappers so
		// the subquery executes natively without opening multiple concurrent quack connections.
		auto saved_subquery_node = subquery_expr.subquery->node->Copy();
		auto result = Rewrite(*subquery_expr.subquery->node);
		bool has_correlated_ref = HasLocalTableReference(*subquery_expr.subquery->node);
		local_table_names = std::move(saved_local_table_names);
		local_table_column_names = std::move(saved_local_table_column_names);
		from_pushed_catalog_names = std::move(saved_from_pushed);
		from_pushed_table_aliases = std::move(saved_from_pushed_aliases);
		if (!has_correlated_ref) {
			FinishPushdown(subquery_expr.subquery->node, result);
		} else {
			subquery_expr.subquery->node = std::move(saved_subquery_node);
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { PushdownSubqueries(child); });
}

unique_ptr<TableFunctionRef> RemotePushdownOptimizer::CreateRemoteFunctionRef(CatalogPushdownResult &result,
                                                                              string remote_sql) {
	D_ASSERT(result.catalog);
	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(make_uniq<ConstantExpression>(Value(result.catalog->GetName())));
	args.push_back(make_uniq<ConstantExpression>(Value(std::move(remote_sql))));
	auto func_ref = make_uniq<TableFunctionRef>();
	func_ref->function = make_uniq<FunctionExpression>(result.catalog->GetRemoteExecuteFunction(), std::move(args));
	return func_ref;
}

void RemotePushdownOptimizer::StripCatalogName(TableRef &ref, const string &catalog_name) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base = ref.Cast<BaseTableRef>();
		if (StringUtil::CIEquals(base.catalog_name, catalog_name)) {
			base.catalog_name = "";
		} else if (base.catalog_name.empty() && StringUtil::CIEquals(base.schema_name, catalog_name)) {
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

void RemotePushdownOptimizer::StripCatalogName(ParsedExpression &expr, const string &catalog_name,
                                               bool strip_subquery_bodies) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &col_ref = expr.Cast<ColumnRefExpression>();
		// Strip catalog prefix from qualified column references (e.g. catalog.schema.table.col -> schema.table.col).
		// Require at least 2 names: stripping an unqualified single-name ref that happens to match the catalog
		// name (e.g. a column literally called "rpc") would leave an empty column_names vector.
		if (col_ref.column_names.size() >= 2 && StringUtil::CIEquals(col_ref.column_names[0], catalog_name)) {
			col_ref.column_names.erase(col_ref.column_names.begin());
		}
		return;
	}
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subq = expr.Cast<SubqueryExpression>();
		// When strip_subquery_bodies is false (partial-pushdown context: individual JoinRef children pushed),
		// do NOT recurse into the subquery body. The subquery was not pushed, so its FROM clause still
		// contains the original remote table refs (e.g. "rpc.t2"). Stripping them would produce an
		// unqualified "t2" that cannot be resolved locally when the subquery is executed locally.
		// The child expression (e.g. left side of IN) still needs stripping because it is an outer-scope
		// column ref (e.g. "rpc.t1.i" that should become "t1.i"). When strip_subquery_bodies is true
		// (full-pushdown context) the entire subquery is being serialized as remote SQL and must be stripped.
		if (strip_subquery_bodies) {
			StripCatalogName(*subq.subquery->node, catalog_name);
		}
		if (subq.child) {
			StripCatalogName(*subq.child, catalog_name, strip_subquery_bodies);
		}
		return;
	}
	// Strip catalog prefix from explicitly-qualified function/window/type calls.
	// Also handle 2-part names (schema.func/type) where the schema is actually the remote catalog name
	// (e.g. "rpc.my_func()" parsed as schema="rpc", catalog="").
	if (expr.GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &func = expr.Cast<FunctionExpression>();
		if (StringUtil::CIEquals(func.catalog, catalog_name)) {
			func.catalog = "";
		} else if (func.catalog.empty() && StringUtil::CIEquals(func.schema, catalog_name)) {
			func.schema = "";
		}
		// Fall through to EnumerateChildren to also strip catalog refs inside arguments
	} else if (expr.GetExpressionClass() == ExpressionClass::WINDOW) {
		auto &win = expr.Cast<WindowExpression>();
		if (StringUtil::CIEquals(win.catalog, catalog_name)) {
			win.catalog = "";
		} else if (win.catalog.empty() && StringUtil::CIEquals(win.schema, catalog_name)) {
			win.schema = "";
		}
		// Fall through to EnumerateChildren to strip catalog refs inside partitions/orders/children
	} else if (expr.GetExpressionClass() == ExpressionClass::CAST) {
		// CastExpression stores the cast target as a LogicalType, not an expression child — EnumerateChildren
		// only visits the value being cast. For unbound (user-defined) types we must strip the catalog from the
		// embedded TypeExpression and reconstruct the LogicalType::UNBOUND wrapper.
		auto &cast_expr = expr.Cast<CastExpression>();
		if (cast_expr.cast_type.id() == LogicalTypeId::UNBOUND) {
			auto type_expr = UnboundType::GetTypeExpression(cast_expr.cast_type)->Copy();
			StripCatalogName(*type_expr, catalog_name, strip_subquery_bodies);
			cast_expr.cast_type = LogicalType::UNBOUND(std::move(type_expr));
		}
		// Fall through to EnumerateChildren to strip catalog refs inside the cast argument
	} else if (expr.GetExpressionClass() == ExpressionClass::TYPE) {
		// TypeExpression (used as a type argument) may carry catalog/schema qualifiers.
		auto &type_expr = expr.Cast<TypeExpression>();
		if (StringUtil::CIEquals(type_expr.GetCatalog(), catalog_name)) {
			type_expr.SetCatalog("");
		} else if (type_expr.GetCatalog().empty() && StringUtil::CIEquals(type_expr.GetSchema(), catalog_name)) {
			type_expr.SetSchema("");
		}
		// Fall through to EnumerateChildren to strip catalog refs inside type parameters
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](ParsedExpression &child) { StripCatalogName(child, catalog_name, strip_subquery_bodies); });
}

void RemotePushdownOptimizer::StripCatalogName(QueryNode &node, const string &catalog_name) {
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
			case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
				auto &limit_mod = modifier->Cast<LimitPercentModifier>();
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
		if (StringUtil::CIEquals(insert.catalog, catalog_name)) {
			insert.catalog = "";
		} else if (insert.catalog.empty() && StringUtil::CIEquals(insert.schema, catalog_name)) {
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
			case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
				auto &limit_mod = modifier->Cast<LimitPercentModifier>();
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

void RemotePushdownOptimizer::StripCatalogName(SQLStatement &statement, const string &catalog_name) {
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
	case StatementType::MERGE_INTO_STATEMENT: {
		auto &merge = statement.Cast<MergeIntoStatement>();
		for (auto &cte_pair : merge.cte_map.map) {
			if (cte_pair.second->query_node) {
				StripCatalogName(*cte_pair.second->query_node, catalog_name);
			}
			for (auto &key : cte_pair.second->key_targets) {
				StripCatalogName(*key, catalog_name);
			}
		}
		if (merge.target) {
			StripCatalogName(*merge.target, catalog_name);
		}
		if (merge.source) {
			StripCatalogName(*merge.source, catalog_name);
		}
		if (merge.join_condition) {
			StripCatalogName(*merge.join_condition, catalog_name, true);
		}
		for (auto &entry : merge.actions) {
			for (auto &action : entry.second) {
				if (action->condition) {
					StripCatalogName(*action->condition, catalog_name, true);
				}
				if (action->update_info) {
					if (action->update_info->condition) {
						StripCatalogName(*action->update_info->condition, catalog_name, true);
					}
					for (auto &expr : action->update_info->expressions) {
						StripCatalogName(*expr, catalog_name, true);
					}
				}
				for (auto &expr : action->expressions) {
					StripCatalogName(*expr, catalog_name, true);
				}
			}
		}
		for (auto &expr : merge.returning_list) {
			StripCatalogName(*expr, catalog_name, true);
		}
		break;
	}
	default:
		break;
	}
}

// Collect unique unqualified column names from RETURNING expressions.
// name_to_qualifier maps each unqualified name to the table qualifier first seen with it.
// Returns false when the same unqualified name appears with a DIFFERENT table qualifier —
// the caller must then bail out and fall back to native execution rather than generating
// an ambiguous "RETURNING col" clause on the remote (which fails with "ambiguous column").
static bool CollectColumnNames(ParsedExpression &expr, vector<string> &col_names,
                               case_insensitive_map_t<string> &name_to_qualifier) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &col_ref = expr.Cast<ColumnRefExpression>();
		const string &col = col_ref.column_names.back();
		const string qualifier =
		    col_ref.column_names.size() >= 2 ? col_ref.column_names[col_ref.column_names.size() - 2] : string();
		auto it = name_to_qualifier.find(col);
		if (it == name_to_qualifier.end()) {
			name_to_qualifier[col] = qualifier;
			col_names.push_back(col);
		} else if (!StringUtil::CIEquals(it->second, qualifier)) {
			// Same unqualified name, different table qualifier → ambiguous on the remote.
			return false;
		}
		return true;
	}
	bool ok = true;
	ParsedExpressionIterator::EnumerateChildren(expr, [&](ParsedExpression &child) {
		if (ok) {
			ok = CollectColumnNames(child, col_names, name_to_qualifier);
		}
	});
	return ok;
}

static void StripAllTableQualifiers(ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &col_ref = expr.Cast<ColumnRefExpression>();
		if (col_ref.column_names.size() > 1) {
			col_ref.column_names = {col_ref.column_names.back()};
		}
		return;
	}
	if (expr.GetExpressionClass() == ExpressionClass::STAR) {
		// "t.*" → "*": clear the table qualifier so the outer SELECT binds against the
		// returning_sub alias rather than looking for the now-absent table "t".
		expr.Cast<StarExpression>().relation_name = "";
	}
	// Note: SUBQUERY expressions are intentionally NOT handled specially here.
	// EnumerateChildren for SUBQUERY visits only the outer 'child' (left side of IN/NOT IN),
	// not the subquery body. Stripping ALL qualifiers inside the subquery body would cause
	// wrong results when the local table and the DML target share a column name
	// (e.g. WHERE local_t.id < t1.id → WHERE id < id, which is a tautology). Non-correlated
	// subqueries work without body stripping because their bodies don't reference the DML target.
	// Correlated subqueries with column-name collisions give a binder error either way.
	ParsedExpressionIterator::EnumerateChildren(expr, [&](ParsedExpression &child) { StripAllTableQualifiers(child); });
}

void RemotePushdownOptimizer::TryPushDMLWithLocalReturning(unique_ptr<SQLStatement> &statement,
                                                           CatalogPushdownResult body_result) {
	vector<unique_ptr<ParsedExpression>> *returning_list = nullptr;
	switch (statement->type) {
	case StatementType::INSERT_STATEMENT:
		returning_list = &statement->Cast<InsertStatement>().node->returning_list;
		break;
	case StatementType::DELETE_STATEMENT:
		returning_list = &statement->Cast<DeleteStatement>().node->returning_list;
		break;
	case StatementType::UPDATE_STATEMENT:
		returning_list = &statement->Cast<UpdateStatement>().node->returning_list;
		break;
	case StatementType::MERGE_INTO_STATEMENT:
		returning_list = &statement->Cast<MergeIntoStatement>().returning_list;
		break;
	default:
		return;
	}
	D_ASSERT(returning_list && !returning_list->empty());

	// If any RETURNING expression is a star (*, t.*, * REPLACE (...), etc.) we cannot enumerate
	// the expanded column set at rewrite time (that requires schema knowledge available only after
	// binding).  Use RETURNING * for the remote so all columns are returned, which lets the outer
	// SELECT correctly evaluate star expansions and other expressions against the full row.
	bool has_star_expr = false;
	for (auto &expr : *returning_list) {
		if (expr->GetExpressionClass() == ExpressionClass::STAR) {
			has_star_expr = true;
			break;
		}
	}

	// Collect unique column names referenced in the RETURNING expressions (only needed without stars)
	vector<string> col_names;
	if (!has_star_expr) {
		case_insensitive_map_t<string> name_to_qualifier;
		for (auto &expr : *returning_list) {
			if (!CollectColumnNames(*expr, col_names, name_to_qualifier)) {
				// The same unqualified column name appears with different table qualifiers.
				// Generating "RETURNING col" would be ambiguous on the remote. Bail out so
				// the DML falls back to native execution rather than producing a remote error.
				return;
			}
		}
		// If no column refs were found (e.g. RETURNING (SELECT max(local_col) FROM local_t)),
		// fall back to RETURNING * so the remote executes the DML and returns one row per
		// modified row. The outer SELECT then evaluates the local RETURNING expressions once
		// per returned row, giving the correct cardinality without needing specific columns.
		if (col_names.empty()) {
			has_star_expr = true;
		}
	}

	// Build outer SELECT list: copies of RETURNING expressions with table/catalog qualifiers stripped
	vector<unique_ptr<ParsedExpression>> outer_select_list;
	for (auto &expr : *returning_list) {
		auto outer_expr = expr->Copy();
		StripAllTableQualifiers(*outer_expr);
		outer_select_list.push_back(std::move(outer_expr));
	}

	// Replace RETURNING with simplified expression(s) for remote execution:
	// - Star present (or no column refs): use a bare * so the remote returns all columns and
	//   outer star expansions work, or supplies row-presence for purely local RETURNING expressions.
	// - No star: use explicit column refs extracted above.
	returning_list->clear();
	if (has_star_expr) {
		returning_list->push_back(make_uniq<StarExpression>());
	} else {
		for (auto &col : col_names) {
			returning_list->push_back(make_uniq<ColumnRefExpression>(col));
		}
	}

	// Strip catalog name from the whole statement and serialize as remote SQL
	StripCatalogName(*statement, body_result.catalog->GetName());
	string remote_sql = statement->ToString();

	// Build: SELECT <outer_exprs> FROM (SELECT * FROM quack_query_by_name(...)) returning_sub
	auto inner_select_node = make_uniq<SelectNode>();
	inner_select_node->select_list.push_back(make_uniq<StarExpression>());
	inner_select_node->from_table = CreateRemoteFunctionRef(body_result, std::move(remote_sql));
	auto inner_stmt = make_uniq<SelectStatement>();
	inner_stmt->node = std::move(inner_select_node);

	auto outer_select_node = make_uniq<SelectNode>();
	outer_select_node->select_list = std::move(outer_select_list);
	outer_select_node->from_table = make_uniq<SubqueryRef>(std::move(inner_stmt), "returning_sub");
	auto outer_stmt = make_uniq<SelectStatement>();
	outer_stmt->node = std::move(outer_select_node);
	statement = std::move(outer_stmt);
}

// Returns true when pushing this SelectNode would produce duplicate column names in the
// SELECT * wrapper that FinishPushdown generates around quack_query_by_name.
// The binder has no schema information about a table function's output until after
// materialization, so it cannot resolve duplicate column names from a join result.
//
// FIXME: If remote-execute table functions could advertise their output schema (column
// names and types) before rows are materialized, DuckDB could disambiguate duplicate
// column names and the entire join could be pushed as a single remote SQL string.
// Until then, queries whose SELECT list exposes duplicate names over a join (e.g.
// "SELECT a.i, b.i FROM t JOIN t") cannot be pushed; they fall back to native remote
// scanning which, for multi-table joins in quack, results in "Multiple streaming scans".
static bool JoinSelectHasDuplicateOutputNames(const SelectNode &select) {
	if (!select.from_table || select.from_table->type != TableReferenceType::JOIN) {
		return false;
	}
	case_insensitive_set_t seen;
	for (auto &expr : select.select_list) {
		// STAR expressions expand to the joined table schemas at bind time; without
		// knowing those schemas here we cannot determine if there are duplicates.
		// Leave SELECT * queries to the existing behavior (may fail at bind time
		// if the joined tables share column names, which is the pre-existing limitation).
		if (expr->GetExpressionClass() == ExpressionClass::STAR) {
			continue;
		}
		string name;
		if (!expr->GetAlias().empty()) {
			name = expr->GetAlias();
		} else if (expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
			name = expr->Cast<ColumnRefExpression>().column_names.back();
		} else {
			// Aggregates, scalars etc. produce auto-generated unique names; safe to skip
			continue;
		}
		if (!seen.insert(name).second) {
			return true;
		}
	}
	return false;
}

static bool QueryNodeWouldProduceDuplicateNames(const QueryNode &node) {
	if (node.type == QueryNodeType::SELECT_NODE) {
		return JoinSelectHasDuplicateOutputNames(node.Cast<SelectNode>());
	}
	// For set operations, the output schema is taken from the first child. If the first child
	// would produce duplicate names (e.g. "SELECT a.i, b.i FROM t JOIN t UNION ALL ..."), the
	// pushed quack_query_by_name wrapper would expose those duplicates to the local binder, which
	// cannot resolve them without schema information.
	if (node.type == QueryNodeType::SET_OPERATION_NODE) {
		auto &setop = node.Cast<SetOperationNode>();
		if (!setop.children.empty()) {
			return QueryNodeWouldProduceDuplicateNames(*setop.children[0]);
		}
	}
	return false;
}

void RemotePushdownOptimizer::FinishPushdown(unique_ptr<SQLStatement> &statement, CatalogPushdownResult result) {
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		return;
	}
	if (statement->type == StatementType::SELECT_STATEMENT &&
	    QueryNodeWouldProduceDuplicateNames(*statement->Cast<SelectStatement>().node)) {
		return;
	}
	// Strip the catalog name so the remote server doesn't recursively re-push
	StripCatalogName(*statement, result.catalog->GetName());
	string remote_sql = statement->ToString();
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = CreateRemoteFunctionRef(result, std::move(remote_sql));
	auto select_stmt = make_uniq<SelectStatement>();
	select_stmt->node = std::move(select_node);
	statement = std::move(select_stmt);
}

void RemotePushdownOptimizer::FinishPushdown(unique_ptr<QueryNode> &node, CatalogPushdownResult result) {
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		return;
	}
	if (QueryNodeWouldProduceDuplicateNames(*node)) {
		return;
	}
	// If any outer SINGLE_REMOTE CTE is in scope, this node may reference it by name.
	// Pushing the node standalone (without its WITH definition) would serialize
	// "SELECT * FROM cte_name" which fails on the remote because cte_name is not a real
	// table there. Skip pushdown; the node will execute locally against the CTE definition.
	// This mirrors the SubqueryRef guard in FinishPushdown(TableRef).
	for (auto &cte_entry : cte_results) {
		if (cte_entry.second.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
			return;
		}
	}
	StripCatalogName(*node, result.catalog->GetName());
	string remote_sql = node->ToString();
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = CreateRemoteFunctionRef(result, std::move(remote_sql));
	node = std::move(select_node);
}

void RemotePushdownOptimizer::FinishPushdown(unique_ptr<TableRef> &ref, CatalogPushdownResult result) {
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		return;
	}
	// A JoinRef cannot be atomically wrapped in SELECT * FROM quack_fn(...): the wrapper
	// loses all table aliases so outer column refs like "a.i" become unresolvable, and
	// self-joins would expose duplicate column names. Skip the pushdown; each table in
	// the join is accessed via native remote scanning, which hits the "Multiple streaming
	// scans" documented limitation if more than one quack connection is needed.
	if (ref->type == TableReferenceType::JOIN) {
		return;
	}
	// Do not individually push a CTE reference. The CTE body lives in the enclosing
	// SelectNode's WITH clause and is only serialized when the entire query is pushed as
	// a unit. Pushing the bare reference ("SELECT * FROM cte_name") without the WITH
	// definition would fail on the remote server because the CTE name is not a real table.
	// This situation arises when Rewrite(JoinRef) tries to push one child of a join that
	// contains a remote CTE reference alongside a local table.
	if (ref->type == TableReferenceType::BASE_TABLE) {
		auto &base = ref->Cast<BaseTableRef>();
		if (base.catalog_name.empty() && base.schema_name.empty() && cte_results.count(base.table_name) > 0) {
			return;
		}
	}
	// Do not individually push a SubqueryRef when there are outer SINGLE_REMOTE CTEs in scope.
	// The SubqueryRef body may reference one of those CTEs. Pushing the body standalone
	// (without its WITH clause) would produce SQL like "SELECT * FROM (SELECT * FROM cte_name)"
	// that fails on the remote because cte_name is not a real table there.
	// We check for SINGLE_REMOTE CTEs specifically: if all outer CTEs are UNKNOWN (local), the
	// SubqueryRef's Rewrite result would already be UNKNOWN (since it references a local CTE),
	// so FinishPushdown would be a no-op. Only a SINGLE_REMOTE CTE could make a
	// SubqueryRef appear SINGLE_REMOTE while actually depending on an outer CTE definition.
	if (ref->type == TableReferenceType::SUBQUERY) {
		for (auto &cte_entry : cte_results) {
			if (cte_entry.second.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
				return;
			}
		}
	}
	string alias = ref->alias;
	// For a BaseTableRef with no explicit alias the table name is the implicit alias
	// (e.g. "rpc.t1" is referenced as "t1" in column refs like "t1.i" in WHERE).
	// Preserve it so that table-qualified column refs in the outer query remain
	// resolvable after the FROM is replaced by an anonymous table function ref.
	if (alias.empty() && ref->type == TableReferenceType::BASE_TABLE) {
		alias = ref->Cast<BaseTableRef>().table_name;
	}
	StripCatalogName(*ref, result.catalog->GetName());
	// For BaseTableRef, clear the alias before serializing: the alias is forwarded to the outer
	// table-function wrapper (func_ref->alias) and must NOT appear in the remote SQL. Including
	// it (e.g. "SELECT * FROM t1 AS ft") is redundant and may cause parse errors on remote
	// servers with limited SQL parsers. SubqueryRef aliases are intentionally left intact because
	// SQL requires subqueries in FROM to carry an alias ("SELECT * FROM (...) AS sub").
	if (ref->type == TableReferenceType::BASE_TABLE) {
		ref->alias = "";
	}
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(ref);
	string remote_sql = select_node->ToString();
	auto func_ref = CreateRemoteFunctionRef(result, std::move(remote_sql));
	func_ref->alias = std::move(alias);
	ref = std::move(func_ref);
}

} // namespace duckdb
