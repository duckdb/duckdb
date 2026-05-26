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
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"

namespace duckdb {
RemotePushdownOptimizer::RemotePushdownOptimizer(Binder &binder) : binder(binder) {
}

RemotePushdownOptimizer::RemotePushdownOptimizer(RemotePushdownOptimizer &parent_p)
    : binder(parent_p.binder), parent(parent_p) {
	// inherit table / column names from parent (for correlated subquery detection)
	local_table_names = parent_p.local_table_names;
}

void RemotePushdownOptimizer::FindRemoteCatalogsInSearchPath() {
	if (search_path_initialized) {
		return;
	}
	// Inherit from the nearest ancestor that has already scanned, avoiding redundant catalog lookups
	// in child optimizers created for subqueries/CTEs. The search path is constant within a query.
	for (const RemotePushdownOptimizer *p = parent.get(); p; p = p->parent.get()) {
		if (p->search_path_initialized) {
			search_path_initialized = true;
			remote_catalogs_in_search_path = p->remote_catalogs_in_search_path;
			local_catalogs_in_search_path = p->local_catalogs_in_search_path;
			return;
		}
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
		throw InternalException(
		    "RemotePushdownOptimizer already has CTEs defined - this means no child was created correctly");
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
	RemotePushdownOptimizer left_optimizer(*this);
	CatalogPushdownResult left_result = left_optimizer.Rewrite(*node.left);

	// for recursive CTEs - the right-hand side of the CTE can refer to the recursive CTE itself
	// we use whatever the CatalogPushdownResult of the LHS was to count this reference
	RemotePushdownOptimizer recursive_optimizer(*this);
	recursive_optimizer.cte_results[node.ctename] = left_result;

	RemotePushdownOptimizer right_optimizer(recursive_optimizer);
	CatalogPushdownResult right_result = right_optimizer.Rewrite(*node.right);

	auto result = Merge(left_result, right_result);
	for (auto &key : node.key_targets) {
		result = Merge(result, Rewrite(*key));
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
	for (auto &cte_pair : node.cte_map.map) {
		auto it = cte_results.find(cte_pair.first);
		if (it != cte_results.end()) {
			result = Merge(result, it->second);
		}
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(SelectNode &node) {
	// Save and reset the pending strip catalogs set by child JoinRef individual pushdown.
	// Scoped to this SelectNode so nested selects (via child optimizers) don't interfere.
	vector<string> saved_pending = std::move(pending_outer_strip_catalogs);
	pending_outer_strip_catalogs = {};

	CatalogPushdownResult from_result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	if (node.from_table) {
		from_result = Rewrite(node.from_table);
	}

	// Apply pending catalog stripping from individual JoinRef child pushdown.
	// Use strip_subquery_bodies=false: subqueries in SELECT/WHERE are NOT being pushed
	// individually and must keep their catalog-qualified references intact.
	for (auto &cat : pending_outer_strip_catalogs) {
		for (auto &expr : node.select_list) {
			StripCatalogName(*expr, cat, false);
		}
		if (node.where_clause) {
			StripCatalogName(*node.where_clause, cat, false);
		}
		for (auto &expr : node.groups.group_expressions) {
			StripCatalogName(*expr, cat, false);
		}
		if (node.having) {
			StripCatalogName(*node.having, cat, false);
		}
		if (node.qualify) {
			StripCatalogName(*node.qualify, cat, false);
		}
		for (auto &modifier : node.modifiers) {
			if (modifier->type == ResultModifierType::ORDER_MODIFIER) {
				auto &order_mod = modifier->Cast<OrderModifier>();
				for (auto &order : order_mod.orders) {
					StripCatalogName(*order.expression, cat, false);
				}
			} else if (modifier->type == ResultModifierType::DISTINCT_MODIFIER) {
				auto &distinct_mod = modifier->Cast<DistinctModifier>();
				for (auto &expr : distinct_mod.distinct_on_targets) {
					StripCatalogName(*expr, cat, false);
				}
			}
		}
	}
	pending_outer_strip_catalogs = std::move(saved_pending);

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
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(InsertQueryNode &node) {
	// InsertQueryNode stores the target table in catalog/schema/table string fields, not in table_ref
	// (table_ref is only set for ON CONFLICT cases and is an alias ref)
	BaseTableRef target_ref;
	target_ref.catalog_name = node.catalog;
	target_ref.schema_name = node.schema;
	target_ref.table_name = node.table;

	RemotePushdownOptimizer target_optimizer(*this);
	auto result = target_optimizer.Rewrite(target_ref);
	if (node.select_statement) {
		RemotePushdownOptimizer select_optimizer(*this);
		auto select_result = select_optimizer.Rewrite(*node.select_statement->node);
		result = Merge(result, select_result);
		if (select_result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG &&
		    result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
			FinishPushdown(node.select_statement->node, select_result);
		}
	}
	if (node.on_conflict_info) {
		if (node.on_conflict_info->condition) {
			auto condition_result = Rewrite(*node.on_conflict_info->condition);
			result = Merge(result, condition_result);
		}
		if (node.on_conflict_info->set_info) {
			if (node.on_conflict_info->set_info->condition) {
				auto condition_result = Rewrite(*node.on_conflict_info->set_info->condition);
				result = Merge(result, condition_result);
			}
			for (auto &expr : node.on_conflict_info->set_info->expressions) {
				auto expr_result = Rewrite(*expr);
				result = Merge(result, expr_result);
			}
		}
	}
	for (auto &expr : node.returning_list) {
		auto expr_result = Rewrite(*expr);
		result = Merge(result, expr_result);
	}
	for (auto &cte_pair : node.cte_map.map) {
		auto it = cte_results.find(cte_pair.first);
		if (it != cte_results.end()) {
			result = Merge(result, it->second);
		}
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(DeleteQueryNode &node) {
	auto result = Rewrite(node.table);
	vector<CatalogPushdownResult> using_results;
	for (auto &using_clause : node.using_clauses) {
		auto using_result = Rewrite(using_clause);
		using_results.push_back(using_result);
		result = Merge(result, using_result);
	}
	if (node.condition) {
		auto condition_result = Rewrite(*node.condition);
		result = Merge(result, condition_result);
	}
	for (auto &expr : node.returning_list) {
		auto expr_result = Rewrite(*expr);
		result = Merge(result, expr_result);
	}
	for (auto &cte_pair : node.cte_map.map) {
		auto it = cte_results.find(cte_pair.first);
		if (it != cte_results.end()) {
			result = Merge(result, it->second);
		}
	}
	// Individual pushdown: for mixed DELETE (local target, remote USING clause), push remote USING
	// clauses individually and strip catalog refs from the WHERE condition and RETURNING list.
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		for (idx_t i = 0; i < node.using_clauses.size(); i++) {
			if (using_results[i].reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
				string old_tname, new_talias;
				if (node.using_clauses[i]->type == TableReferenceType::BASE_TABLE) {
					auto &base = node.using_clauses[i]->Cast<BaseTableRef>();
					old_tname = base.table_name;
					new_talias = base.alias.empty() ? base.table_name : base.alias;
				}
				auto catalog_name = PushJoinChild(node.using_clauses[i], using_results[i]);
				if (!catalog_name.empty()) {
					if (node.condition) {
						StripCatalogName(*node.condition, catalog_name, false);
						if (!old_tname.empty() && !StringUtil::CIEquals(old_tname, new_talias)) {
							RenameTableInExpr(*node.condition, old_tname, new_talias);
						}
					}
					for (auto &expr : node.returning_list) {
						StripCatalogName(*expr, catalog_name, false);
						if (!old_tname.empty() && !StringUtil::CIEquals(old_tname, new_talias)) {
							RenameTableInExpr(*expr, old_tname, new_talias);
						}
					}
				}
			}
		}
	}
	return result;
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(UpdateQueryNode &node) {
	auto result = Rewrite(node.table);
	CatalogPushdownResult from_result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr};
	if (node.from_table) {
		from_result = Rewrite(node.from_table);
		result = Merge(result, from_result);
	}

	if (node.set_info) {
		if (node.set_info->condition) {
			auto condition_result = Rewrite(*node.set_info->condition);
			result = Merge(result, condition_result);
		}

		for (auto &expr : node.set_info->expressions) {
			auto expr_result = Rewrite(*expr);
			result = Merge(result, expr_result);
		}
	}
	for (auto &expr : node.returning_list) {
		auto expr_result = Rewrite(*expr);
		result = Merge(result, expr_result);
	}
	for (auto &cte_pair : node.cte_map.map) {
		auto it = cte_results.find(cte_pair.first);
		if (it != cte_results.end()) {
			result = Merge(result, it->second);
		}
	}
	// Individual pushdown: for mixed UPDATE (local target, remote FROM clause), push the remote FROM
	// table individually and strip catalog refs from SET expressions, WHERE condition, and RETURNING.
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG && node.from_table &&
	    from_result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		string old_tname, new_talias;
		if (node.from_table->type == TableReferenceType::BASE_TABLE) {
			auto &base = node.from_table->Cast<BaseTableRef>();
			old_tname = base.table_name;
			new_talias = base.alias.empty() ? base.table_name : base.alias;
		}
		auto catalog_name = PushJoinChild(node.from_table, from_result);
		if (!catalog_name.empty()) {
			if (node.set_info) {
				if (node.set_info->condition) {
					StripCatalogName(*node.set_info->condition, catalog_name, false);
					if (!old_tname.empty() && !StringUtil::CIEquals(old_tname, new_talias)) {
						RenameTableInExpr(*node.set_info->condition, old_tname, new_talias);
					}
				}
				for (auto &expr : node.set_info->expressions) {
					StripCatalogName(*expr, catalog_name, false);
					if (!old_tname.empty() && !StringUtil::CIEquals(old_tname, new_talias)) {
						RenameTableInExpr(*expr, old_tname, new_talias);
					}
				}
			}
			for (auto &expr : node.returning_list) {
				StripCatalogName(*expr, catalog_name, false);
				if (!old_tname.empty() && !StringUtil::CIEquals(old_tname, new_talias)) {
					RenameTableInExpr(*expr, old_tname, new_talias);
				}
			}
		}
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
	// Merge unreferenced CTEs: even if no arm references a CTE it is still serialized into
	// the pushed SQL string.  A local-body CTE (UNKNOWN) must block full-query pushdown.
	for (auto &cte_pair : node.cte_map.map) {
		auto it = cte_results.find(cte_pair.first);
		if (it != cte_results.end()) {
			result = Merge(result, it->second);
		}
	}
	// If the whole set operation resolves to a single remote catalog, propagate upward.
	if (result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		return result;
	}
	// Push individual children that resolve to a single remote catalog.
	for (idx_t i = 0; i < node.children.size(); i++) {
		FinishPushdown(node.children[i], child_results[i]);
	}
	// After individual child pushdown, ORDER BY expressions on the set operation output can no longer
	// use catalog.table.col qualifiers — the UNION/INTERSECT/EXCEPT output has no table associations.
	// Strip any catalog-prefixed column refs in ORDER BY down to just the column name.
	if (has_expression_modifiers) {
		for (idx_t i = 0; i < node.children.size(); i++) {
			if (child_results[i].reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
				for (auto &modifier : node.modifiers) {
					if (modifier->type == ResultModifierType::ORDER_MODIFIER) {
						auto &order_mod = modifier->Cast<OrderModifier>();
						for (auto &order : order_mod.orders) {
							StripSetOpOrderByExpr(*order.expression, child_results[i].catalog->GetName());
						}
					}
				}
			}
		}
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
	// Track the alias only for subqueries that reference local data (UNKNOWN). A subquery
	// with NO_CATALOG_REFERENCED (e.g. "SELECT 1") has no local data and must not be
	// tracked: doing so would make any outer column ref to this alias return UNKNOWN from
	// RefersToLocalTable, incorrectly blocking pushdown of queries like
	// "SELECT t1.i FROM rpc.t1 t1, (SELECT 1 AS v) sub WHERE t1.i < sub.v".
	if (result.reference_type == CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE && !ref.alias.empty()) {
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

string RemotePushdownOptimizer::PushJoinChild(unique_ptr<TableRef> &ref, CatalogPushdownResult result) {
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		return "";
	}
	if (ref->type != TableReferenceType::BASE_TABLE) {
		return "";
	}
	auto &base = ref->Cast<BaseTableRef>();
	// Resolve the actual catalog/schema split (e.g. "rpc.t1" is parsed as schema="rpc", catalog=""
	// but BindSchemaOrCatalog recognises "rpc" as a catalog, giving catalog="rpc", schema="").
	string resolved_catalog = base.catalog_name;
	string resolved_schema = base.schema_name;
	Binder::BindSchemaOrCatalog(binder.context, resolved_catalog, resolved_schema);

	// Don't push CTE references — they are local definitions that don't exist on the remote server.
	// A CTE always has no catalog or schema qualifier after resolution.
	if (resolved_catalog.empty() && resolved_schema.empty()) {
		CatalogPushdownResult cte_check;
		if (RefersToCTE(base.table_name, cte_check)) {
			return "";
		}
	}

	string alias = base.alias.empty() ? base.table_name : base.alias;

	// Build SELECT * FROM <table_without_catalog_prefix> and push to remote.
	// Use resolved_schema (which is empty for simple "rpc.t1") so the remote sees "t1" not "rpc.t1".
	auto inner_ref = make_uniq<BaseTableRef>();
	inner_ref->schema_name = resolved_schema;
	inner_ref->table_name = base.table_name;

	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(inner_ref);

	auto func_ref = CreateRemoteFunctionRef(result, std::move(select_node));
	func_ref->alias = alias;
	ref = std::move(func_ref);

	return result.catalog->GetName();
}

CatalogPushdownResult RemotePushdownOptimizer::Rewrite(JoinRef &ref) {
	// Capture pending size before processing children: new entries (from inner JoinRef pushdowns)
	// must be used to strip THIS JoinRef's ON condition as well as the outer SELECT expressions.
	idx_t pending_before = pending_outer_strip_catalogs.size();

	auto left_result = Rewrite(ref.left);

	// the right side of a join can be correlated to the left side - use a child optimizer to track this
	RemotePushdownOptimizer child_optimizer(*this);
	auto right_result = child_optimizer.Rewrite(ref.right);

	// Propagate pending strip catalogs from the right-side child optimizer (set by inner JoinRef
	// pushdowns on that side) into the current scope so the outer SelectNode can strip SELECT/WHERE.
	for (auto &cat : child_optimizer.pending_outer_strip_catalogs) {
		pending_outer_strip_catalogs.push_back(cat);
	}

	// Strip this JoinRef's own ON condition for all catalogs that were pushed during child processing
	// (both from left-side inner JoinRefs and from the propagated right-side child entries).
	// strip_subquery_bodies=false: subqueries within the ON condition are NOT being pushed individually.
	if (ref.condition) {
		for (idx_t i = pending_before; i < pending_outer_strip_catalogs.size(); i++) {
			StripCatalogName(*ref.condition, pending_outer_strip_catalogs[i], false);
		}
	}

	auto result = Merge(left_result, right_result);
	// Also analyze the join condition - it may contain subqueries or local macro calls
	// that affect whether the join can be pushed as a whole.
	if (ref.condition) {
		result = Merge(result, Rewrite(*ref.condition));
	}

	// For mixed joins (one remote child, one local), push the remote child individually.
	// After pushing, strip catalog-qualified refs from the ON condition so the binder can
	// resolve them against the pushed table's alias instead of the original catalog.table name.
	// Use strip_subquery_bodies=false to avoid incorrectly stripping subqueries that are NOT pushed.
	// When the pushed table has an explicit alias (e.g. rpc.t1 AS rt1), StripCatalogName reduces
	// "rpc.t1.col" to "t1.col", but the binder only knows "rt1". RenameTableInExpr fixes that up.
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		if (left_result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG &&
		    right_result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
			string old_tname, new_talias;
			if (ref.left->type == TableReferenceType::BASE_TABLE) {
				auto &base = ref.left->Cast<BaseTableRef>();
				old_tname = base.table_name;
				new_talias = base.alias.empty() ? base.table_name : base.alias;
			}
			auto catalog_name = PushJoinChild(ref.left, left_result);
			if (!catalog_name.empty()) {
				if (ref.condition) {
					StripCatalogName(*ref.condition, catalog_name, false);
					if (!old_tname.empty() && !StringUtil::CIEquals(old_tname, new_talias)) {
						RenameTableInExpr(*ref.condition, old_tname, new_talias);
					}
				}
				pending_outer_strip_catalogs.push_back(catalog_name);
			}
		} else if (right_result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG &&
		           left_result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
			string old_tname, new_talias;
			if (ref.right->type == TableReferenceType::BASE_TABLE) {
				auto &base = ref.right->Cast<BaseTableRef>();
				old_tname = base.table_name;
				new_talias = base.alias.empty() ? base.table_name : base.alias;
			}
			auto catalog_name = PushJoinChild(ref.right, right_result);
			if (!catalog_name.empty()) {
				if (ref.condition) {
					StripCatalogName(*ref.condition, catalog_name, false);
					if (!old_tname.empty() && !StringUtil::CIEquals(old_tname, new_talias)) {
						RenameTableInExpr(*ref.condition, old_tname, new_talias);
					}
				}
				pending_outer_strip_catalogs.push_back(catalog_name);
			}
		}
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

void RemotePushdownOptimizer::StripSetOpOrderByExpr(ParsedExpression &expr, const string &catalog_name) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &col_ref = expr.Cast<ColumnRefExpression>();
		// In a set operation output there are no table associations, so reduce any catalog-prefixed ref to
		// just the column name. Even a 2-part "t1.i" would fail to bind in the UNION output.
		if (col_ref.column_names.size() >= 2 && StringUtil::CIEquals(col_ref.column_names[0], catalog_name)) {
			string col_name = col_ref.column_names.back();
			col_ref.column_names = {col_name};
		}
		return;
	}
	// Recurse into function arguments and other composite expressions so catalog-prefixed column refs
	// nested inside expressions like coalesce(rpc.t1.i, 0) are also stripped to their bare column name.
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](ParsedExpression &child) { StripSetOpOrderByExpr(child, catalog_name); });
}

void RemotePushdownOptimizer::RenameTableInExpr(ParsedExpression &expr, const string &old_table,
                                                const string &new_alias) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &col_ref = expr.Cast<ColumnRefExpression>();
		// Rename table.col → alias.col for 2-part refs where the table part matches old_table.
		// This is called after StripCatalogName has reduced catalog.table.col → table.col, so the
		// table name is now the first element of a 2-part ref.
		if (col_ref.column_names.size() == 2 && StringUtil::CIEquals(col_ref.column_names[0], old_table)) {
			col_ref.column_names[0] = new_alias;
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](ParsedExpression &child) { RenameTableInExpr(child, old_table, new_alias); });
}

void RemotePushdownOptimizer::StripCatalogName(ParsedExpression &expr, const string &catalog_name,
                                               bool strip_subquery_bodies) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &col_ref = expr.Cast<ColumnRefExpression>();
		// Strip catalog prefix from qualified column references (e.g. catalog.table.col -> table.col or
		// catalog.schema.table.col -> schema.table.col). Require at least 3 names: a 2-part ref like "rpc.field"
		// is either table.col or struct-column.field — not catalog-qualified — so stripping would be wrong.
		if (col_ref.column_names.size() >= 3 && StringUtil::CIEquals(col_ref.column_names[0], catalog_name)) {
			col_ref.column_names.erase(col_ref.column_names.begin());
		}
		return;
	}
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subq = expr.Cast<SubqueryExpression>();
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
	// If any outer SINGLE_REMOTE CTE is in scope (including parent optimizer scopes), this node
	// may reference it by name. Pushing the node standalone (without its WITH definition) would
	// serialize "SELECT * FROM cte_name" which fails on the remote because cte_name is not a
	// real table there. Skip pushdown; the node will execute locally against the CTE definition.
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

} // namespace duckdb
