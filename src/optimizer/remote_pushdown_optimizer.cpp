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

RemotePushdownOptimizer::RemotePushdownOptimizer(RemotePushdownOptimizer &parent_p) : binder(parent_p.binder), parent(parent_p) {
	// inherit table / column names from parent (for correlated subquery detection)
	local_table_names = parent_p.local_table_names;
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
		throw InternalException("RemotePushdownOptimizer already has CTEs defined - this means no child was created correctly");
	}
	for (auto &cte_pair : node.cte_map.map) {
		const string &cte_name = cte_pair.first;
		auto &cte_info = *cte_pair.second;
		CatalogPushdownResult cte_result;
		if (cte_info.query_node) {
			RemotePushdownOptimizer child_optimizer(*this);
			cte_result = child_optimizer.Rewrite(*cte_info.query_node);
		} else {
			cte_result = {CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE, nullptr};
		}
		for (auto &key : cte_info.key_targets) {
			cte_result = Merge(cte_result, Rewrite(*key));
		}
		cte_results[cte_name] = cte_result;
	}
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

	// Track whether ctename was already in local_table_names before the recursive arm.
	// Rewrite(BaseTableRef) adds ctename to local_table_names when it encounters the self-reference
	// (classified as NO_CATALOG_REFERENCED). After the recursive arm completes, ctename must be
	// erased from local_table_names so it does not leak into the outer scope as a false "local table"
	// that would incorrectly block optimization of outer subqueries referencing a column like r.col.
	bool ctename_was_local = local_table_names.count(node.ctename) > 0;

	RemotePushdownOptimizer left_optimizer(*this);
	RemotePushdownOptimizer right_optimizer(*this);
	CatalogPushdownResult left_result = left_optimizer.Rewrite(*node.left);
	CatalogPushdownResult right_result = right_optimizer.Rewrite(*node.right);

	// Remove ctename leak from local_table_names (added by Rewrite(BaseTableRef) when it sees
	// the self-reference classified as NO_CATALOG_REFERENCED in the recursive arm).
	if (!ctename_was_local) {
		local_table_names.erase(node.ctename);
	}

	// Remove the self-reference placeholder so the caller's assignment is authoritative.
	// If an outer CTE of the same name was shadowed, restore it now.
	if (had_outer_ctename) {
		cte_results[node.ctename] = saved_ctename_result;
	} else {
		cte_results.erase(node.ctename);
	}
	auto result = Merge(left_result, right_result);
	return result;
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
	// Rewrite from_table first - its result is tracked separately for potential individual pushdown.
	// Reset from_pushed_catalog_names / from_pushed_table_aliases so that entries pushed by
	// JoinRef children during from_table processing are cleanly collected.
	if (!from_pushed_catalog_names.empty() || !from_pushed_table_aliases.empty()) {
		throw InternalException("from_pushed_catalog_names is not empty - forgot to create a recursive RemotePushdownOptimizer?");
	}
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
			vector<string> pushed_cats = std::move(from_pushed_catalog_names);
			from_pushed_catalog_names.clear();
			if (node.cte_map.map.empty() && from_result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG &&
				from_result.catalog) {
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
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(InsertQueryNode &node) {
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
	CatalogPushdownResult result = Rewrite(target_ref);

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
		if (node.cte_map.map.empty() && node.select_statement) {
			auto select_result = Rewrite(*node.select_statement->node);
			FinishPushdown(node.select_statement->node, select_result);
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
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(DeleteQueryNode &node) {
	CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	if (node.table) {
		result = Rewrite(node.table);
	}

	// Target table must be remote for the whole DELETE to be pushed to the remote.
	// If it is local, still push individual remote subqueries in condition/USING for efficiency,
	// but only when no CTEs are present: a CTE-referencing subquery cannot be pushed individually.
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		if (node.cte_map.map.empty()) {
			// Push individual remote USING clauses and collect the catalog names pushed,
			// so that catalog-qualified column refs like "rpc.t1.i" in the WHERE condition
			// can be stripped to "t1.i" before binding (the alias on the pushed wrapper is "t1").
			//
			// Pushing a USING clause wraps it in a quack TABLE_FUNCTION streaming connection.
			// Mixing a TABLE_FUNCTION streaming connection with a native quack scan of another
			// remote USING clause triggers "Multiple streaming scans". When multiple USING clauses
			// are SINGLE_REMOTE, don't push any of them: native quack catalog scanning of
			// multiple remote tables is sequential and avoids concurrent streaming slots.
			from_pushed_catalog_names.clear();
			from_pushed_table_aliases.clear();
			// Save all USING clauses before analysis: Rewrite(JoinRef) inside a USING clause
			// can push individual JoinRef children in-place even when the JoinRef result is
			// UNKNOWN. If we decide not to push (multiple remote clauses), restore all copies
			// to remove those stale quack wrappers before native execution.
			vector<unique_ptr<TableRef>> saved_using_clauses;
			for (auto &clause : node.using_clauses) {
				saved_using_clauses.push_back(clause->Copy());
			}
			vector<CatalogPushdownResult> using_clause_results;
			using_clause_results.reserve(node.using_clauses.size());
			for (auto &clause : node.using_clauses) {
				using_clause_results.push_back(Rewrite(clause));
			}
			// Count how many USING clauses are SINGLE_REMOTE (would open streaming slots if pushed).
			idx_t remote_using_count = 0;
			for (auto &r : using_clause_results) {
				if (r.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
					++remote_using_count;
				}
			}
			if (remote_using_count <= 1) {
				// At most one streaming slot needed: push SINGLE_REMOTE USING clauses.
				// Non-SINGLE_REMOTE clauses must be restored from their saved copies: Rewrite(JoinRef)
				// inside a mixed-catalog USING clause (e.g. rpc.t2 JOIN local_t) pushes individual
				// SINGLE_REMOTE children in-place, leaving stale TABLE_FUNCTION wrappers. Without
				// restoring, those stale wrappers would open a concurrent second quack streaming slot
				// alongside the SINGLE_REMOTE USING clause being pushed here.
				for (idx_t i = 0; i < node.using_clauses.size(); i++) {
					if (using_clause_results[i].reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
						node.using_clauses[i] = std::move(saved_using_clauses[i]);
						continue;
					}
					FinishPushdown(node.using_clauses[i], using_clause_results[i]);
					if (using_clause_results[i].catalog) {
						const string &cat_name = using_clause_results[i].catalog->GetName();
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
			} else {
				// Multiple remote USING clauses: restore all to remove stale quack wrappers
				// inserted in-place by Rewrite(JoinRef). Native quack catalog scanning handles
				// multiple remote tables sequentially without opening concurrent streaming slots.
				for (idx_t i = 0; i < node.using_clauses.size(); i++) {
					node.using_clauses[i] = std::move(saved_using_clauses[i]);
				}
			}
			if (node.condition) {
				for (auto &cat_name : from_pushed_catalog_names) {
					StripCatalogName(*node.condition, cat_name, false);
				}
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
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(UpdateQueryNode &node) {
	auto result = Rewrite(node.table);
	if (node.from_table) {
		auto from_result = Rewrite(node.from_table);
		result = Merge(result, from_result);

		if (from_result.reference_type == CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE) {
			if (node.from_table->alias.empty()) {
				local_table_names.insert(node.from_table->alias);
			} else {
				// FIXME: is this right?
				local_table_names.insert("unnamed_subquery");
			}
		}
	}

	if (node.set_info) {
		if (node.set_info->condition) {
			auto condition_result = Rewrite(*node.set_info->condition);
			result = Merge(result, condition_result);
		}

		for(auto &expr : node.set_info->expressions) {
			auto expr_result = Rewrite(*expr);
			result = Merge(result, expr_result);
		}
	}
	for (auto &expr : node.returning_list) {
		auto expr_result = Rewrite(*expr);
		result = Merge(result, expr_result);
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(SetOperationNode &node) {
	// Rewrite each child independently so we can push down individual children if needed
	vector<CatalogPushdownResult> child_results;
	child_results.reserve(node.children.size());
	CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	for (auto &child : node.children) {
		RemotePushdownOptimizer child_optimizer(*this);
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
				result = Merge(result, Rewrite(*order.expression));
				has_expression_modifiers = true;
			}
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &limit_mod = modifier->Cast<LimitModifier>();
			if (limit_mod.limit) {
				result = Merge(result, Rewrite(*limit_mod.limit));
				has_expression_modifiers = true;
			}
			if (limit_mod.offset) {
				result = Merge(result, Rewrite(*limit_mod.offset));
				has_expression_modifiers = true;
			}
			break;
		}
		case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
			auto &limit_mod = modifier->Cast<LimitPercentModifier>();
			if (limit_mod.limit) {
				result = Merge(result, Rewrite(*limit_mod.limit));
				has_expression_modifiers = true;
			}
			if (limit_mod.offset) {
				result = Merge(result, Rewrite(*limit_mod.offset));
				has_expression_modifiers = true;
			}
			break;
		}
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct_mod = modifier->Cast<DistinctModifier>();
			for (auto &expr : distinct_mod.distinct_on_targets) {
				result = Merge(result, Rewrite(*expr));
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

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(unique_ptr<TableRef> &ref) {
	switch (ref->type) {
	case TableReferenceType::BASE_TABLE:
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
	RemotePushdownOptimizer child_binder(*this);
	auto result = child_binder.Rewrite(*ref.subquery->node);
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
	auto left_result = Rewrite(ref.left);

	// the left side of a join can be correlated to the left side - use a child optimizer to track this
	RemotePushdownOptimizer child_optimizer(*this);
	auto right_result = child_optimizer.Rewrite(ref.right);
	auto result = Merge(left_result, right_result);
	// Also analyze the join condition - it may contain subqueries or local macro calls
	// that affect whether the join can be pushed as a whole.
	if (ref.condition) {
		result = Merge(result, Rewrite(*ref.condition));
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

bool RemotePushdownOptimizer::RefersToCTE(const string &cte_name, CatalogPushdownResult &result) const {
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
	string catalog_name = ref.catalog_name;
	string schema_name = ref.schema_name;
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
		if (catalog && catalog->IsRemoteCatalog()) {
			return {CatalogReferenceType::SINGLE_REMOTE_CATALOG, catalog};
		}
		// A local table always blocks pushdown of any query that contains it.
		// Returning UNKNOWN (not NO_CATALOG) ensures Merge(SINGLE_REMOTE, UNKNOWN) = UNKNOWN
		// rather than the otherwise-neutral SINGLE_REMOTE.
		TrackLocalTable(ref);
		return {};
	}

	// Case 2: no explicit catalog - lazily populate search path catalogs on first use
	FindRemoteCatalogsInSearchPath();

	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, ref.table_name);

	if (remote_catalogs_in_search_path.size() != 1) {
		TrackLocalTable(ref);
		return {};
	}

	for (auto &local_entry : local_catalogs_in_search_path) {
		// If the ref specifies a schema, use it; otherwise use the search path schema
		const auto &schema = schema_name.empty() ? local_entry.schema : schema_name;
		auto entry =
			Catalog::GetEntry(binder.context, local_entry.catalog, schema, table_lookup, OnEntryNotFound::RETURN_NULL);
		if (entry) {
			TrackLocalTable(ref);
			// Same as Case 1: local table → UNKNOWN to prevent Merge from treating it as neutral.
			return {};
		}
	}

	// Not found in any local catalog - push to the single remote catalog in the search path
	return {CatalogReferenceType::SINGLE_REMOTE_CATALOG, remote_catalogs_in_search_path.front().get()};
}

bool RemotePushdownOptimizer::RefersToLocalTable(ColumnRefExpression &col_ref) const {
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

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery_expr = expr.Cast<SubqueryExpression>();
		CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
		// EnumerateChildren for SUBQUERY only visits `child` (e.g., left side of IN), not subquery->node
		if (subquery_expr.child) {
			result = Merge(result, Rewrite(*subquery_expr.child));
		}

		RemotePushdownOptimizer child_optimizer(*this);
		auto subquery_result = child_optimizer.Rewrite(*subquery_expr.subquery->node);
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
	} else if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &col_ref = expr.Cast<ColumnRefExpression>();
		if (RefersToLocalTable(col_ref)) {
			// column refers to local table - bail
			return {};
		}

	}
	CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](ParsedExpression &child) { result = Merge(result, Rewrite(child)); });
	return result;
}

unique_ptr<TableRef> RemotePushdownOptimizer::CreateRemoteFunctionRef(CatalogPushdownResult &result,
                                                                      unique_ptr<QueryNode> node) {
	return result.catalog->RemotePushdown(binder.context, std::move(node));
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

unique_ptr<QueryNode> GetNodeFromStatement(SQLStatement &statement) {
	switch (statement.type) {
	case StatementType::SELECT_STATEMENT:
		return std::move(statement.Cast<SelectStatement>().node);
		break;
	case StatementType::INSERT_STATEMENT:
		return std::move(statement.Cast<InsertStatement>().node);
		break;
	case StatementType::DELETE_STATEMENT:
		return std::move(statement.Cast<DeleteStatement>().node);
		break;
	case StatementType::UPDATE_STATEMENT:
		return std::move(statement.Cast<UpdateStatement>().node);
		break;
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
	// If any outer SINGLE_REMOTE CTE is in scope (including parent optimizer scopes), this node
	// may reference it by name. Pushing the node standalone (without its WITH definition) would
	// serialize "SELECT * FROM cte_name" which fails on the remote because cte_name is not a
	// real table there. Skip pushdown; the node will execute locally against the CTE definition.
	// This mirrors the SubqueryRef guard in FinishPushdown(TableRef).
	for (const RemotePushdownOptimizer *opt = this; opt; opt = opt->parent.get()) {
		for (auto &cte_entry : opt->cte_results) {
			if (cte_entry.second.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
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
	auto func_ref = CreateRemoteFunctionRef(result, std::move(select_node));
	func_ref->alias = std::move(alias);
	ref = std::move(func_ref);
}

} // namespace duckdb
