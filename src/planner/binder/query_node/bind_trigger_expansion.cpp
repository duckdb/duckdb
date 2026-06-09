#include "duckdb/planner/binder.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp"
#include "duckdb/common/enums/cte_materialize.hpp"
#include "duckdb/common/enums/trigger_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/common_table_expression_info.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

#include <algorithm>

namespace duckdb {

unique_ptr<BoundStatement> Binder::TryExpandTriggers(QueryNode &node, TableCatalogEntry &table,
                                                     TriggerEventType event_type) {
	D_ASSERT(node.type == QueryNodeType::INSERT_QUERY_NODE || node.type == QueryNodeType::UPDATE_QUERY_NODE ||
	         node.type == QueryNodeType::DELETE_QUERY_NODE);
	auto &expanded_tables = global_binder_state->trigger_expanded_tables;
	if (expanded_tables.find(table) != expanded_tables.end()) {
		if (global_binder_state->trigger_creation_table == &table) {
			throw NotImplementedException("Recursive trigger chains are not yet supported (trigger cycle detected "
			                              "through trigger \"%s\" on table \"%s\")",
			                              global_binder_state->trigger_creation_name, table.name);
		}
		return nullptr;
	}
	auto txn = table.ParentCatalog().GetCatalogTransaction(context);
	auto before_triggers = table.GetTriggersForEvent(txn, TriggerTiming::BEFORE, event_type);
	auto after_triggers = table.GetTriggersForEvent(txn, TriggerTiming::AFTER, event_type);

	// UPDATE OF <cols>: drop triggers whose OF list is disjoint from the SET list.
	// Triggers without an OF list are unrestricted and always fire.
	if (event_type == TriggerEventType::UPDATE_EVENT && node.type == QueryNodeType::UPDATE_QUERY_NODE) {
		auto &update_node = node.Cast<UpdateQueryNode>();
		case_insensitive_set_t updated_columns;
		if (update_node.set_info) {
			updated_columns.insert(update_node.set_info->columns.begin(), update_node.set_info->columns.end());
		}
		auto trigger_does_not_fire = [&](const_reference<TriggerCatalogEntry> trig) {
			const auto &of_cols = trig.get().columns;
			return !of_cols.empty() && std::none_of(of_cols.begin(), of_cols.end(),
			                                        [&](const string &c) { return updated_columns.count(c) > 0; });
		};
		auto drop_non_firing = [&](vector<const_reference<TriggerCatalogEntry>> &triggers) {
			triggers.erase(std::remove_if(triggers.begin(), triggers.end(), trigger_does_not_fire), triggers.end());
		};
		drop_non_firing(before_triggers);
		drop_non_firing(after_triggers);
	}

	if (before_triggers.empty() && after_triggers.empty()) {
		return nullptr;
	}
	if (node.type == QueryNodeType::INSERT_QUERY_NODE) {
		auto &insert_node = node.Cast<InsertQueryNode>();
		if (insert_node.on_conflict_info && insert_node.on_conflict_info->action_type != OnConflictAction::NOTHING) {
			// Only AFTER triggers can carry REFERENCING NEW TABLE — BEFORE rejects it at CREATE time.
			for (auto &trigger : after_triggers) {
				if (!trigger.get().referencing_new_table.empty()) {
					throw NotImplementedException(
					    "ON CONFLICT DO UPDATE is not yet supported with REFERENCING NEW TABLE AS triggers");
				}
			}
		}
	}
	expanded_tables.insert(table);
	auto bound = ExpandTriggers(node, table, before_triggers, after_triggers);

	// Erasing from the set, so we will track expanded tables only while we're on the same node in the recursive stack,
	// meaning we're on the same "trigger" in the trigger chain.
	expanded_tables.erase(table);
	return make_uniq<BoundStatement>(std::move(bound));
}

static constexpr const char *TRIGGER_BASE_CTE_PREFIX = "__duckdb_trigger_base_";
static constexpr const char *TRIGGER_BODY_CTE_PREFIX = "__duckdb_trigger_body_";
static constexpr const char *TRIGGER_BEFORE_BODY_CTE_PREFIX = "__duckdb_trigger_before_body_";

static unique_ptr<CommonTableExpressionInfo>
MakeTransitionTableAliasCTE(const string &base_cte_name, const case_insensitive_set_t &exclude_columns) {
	auto alias_cte = make_uniq<CommonTableExpressionInfo>();
	auto alias_select = make_uniq<SelectNode>();
	auto star = make_uniq<StarExpression>();
	// Exclude virtual columns projected for RETURNING so trigger bodies scanning
	// the transition table with SELECT * don't see implementation-level columns.
	for (auto &name : exclude_columns) {
		star->ExcludeListMutable().insert(QualifiedColumnName(name));
	}
	alias_select->select_list.push_back(std::move(star));
	auto alias_ref = make_uniq<BaseTableRef>();
	alias_ref->table_name = base_cte_name;
	alias_select->from_table = std::move(alias_ref);
	alias_cte->query_node = std::move(alias_select);
	alias_cte->materialized = CTEMaterialize::CTE_MATERIALIZE_DEFAULT;
	return alias_cte;
}

static vector<unique_ptr<ParsedExpression>> &GetDMLReturningList(QueryNode &node) {
	switch (node.type) {
	case QueryNodeType::INSERT_QUERY_NODE:
		return node.Cast<InsertQueryNode>().returning_list;
	case QueryNodeType::UPDATE_QUERY_NODE:
		return node.Cast<UpdateQueryNode>().returning_list;
	case QueryNodeType::DELETE_QUERY_NODE:
		return node.Cast<DeleteQueryNode>().returning_list;
	default:
		throw InternalException("GetDMLReturningList: unexpected node type");
	}
}

static string GetTableAlias(const QueryNode &node, const string &fallback) {
	switch (node.type) {
	case QueryNodeType::INSERT_QUERY_NODE: {
		auto &n = node.Cast<InsertQueryNode>();
		if (n.table_ref && !n.table_ref->alias.empty()) {
			return n.table_ref->alias;
		}
		return fallback;
	}
	case QueryNodeType::UPDATE_QUERY_NODE: {
		auto &n = node.Cast<UpdateQueryNode>();
		if (n.table && !n.table->alias.empty()) {
			return n.table->alias;
		}
		return fallback;
	}
	case QueryNodeType::DELETE_QUERY_NODE: {
		auto &n = node.Cast<DeleteQueryNode>();
		if (n.table && !n.table->alias.empty()) {
			return n.table->alias;
		}
		return fallback;
	}
	default:
		D_ASSERT(false);
		return fallback;
	}
}

// Alias the base CTE to the target table name so table-qualified RETURNING refs resolve.
static unique_ptr<SelectNode> BuildTriggerOuterSelect(const QueryNode &node, const TableCatalogEntry &table,
                                                      const string &base_cte_name,
                                                      vector<unique_ptr<ParsedExpression>> returning_list) {
	auto outer = make_uniq<SelectNode>();
	if (returning_list.empty()) {
		outer->select_list.push_back(
		    make_uniq<FunctionExpression>("count_star", vector<unique_ptr<ParsedExpression>>()));
	} else {
		outer->select_list = std::move(returning_list);
	}
	auto from_ref = make_uniq<BaseTableRef>();
	from_ref->table_name = base_cte_name;
	from_ref->alias = GetTableAlias(node, table.name);
	outer->from_table = std::move(from_ref);
	return outer;
}

// Returns the canonical virtual-column name `colref` refers to on the target table
// (e.g. "rowid"), or empty if it is not a virtual-column ref. Mirrors no-trigger
// binding: when an alias is in effect only the alias qualifies.
static string MatchVirtualColumn(const ColumnRefExpression &colref, const virtual_column_map_t &virtual_cols,
                                 const string &target_alias) {
	auto &cn = colref.ColumnNames();
	if (cn.empty()) {
		return string();
	}
	for (auto &vc : virtual_cols) {
		if (StringUtil::CIEquals(cn.back(), vc.second.name) &&
		    (cn.size() == 1 || StringUtil::CIEquals(cn[cn.size() - 2], target_alias))) {
			return vc.second.name;
		}
	}
	return string();
}

// Project virtual columns (rowid, etc.) referenced in the user's RETURNING list into the inner
// DML's RETURNING so the base CTE materialises them by name.  Returns the projected names so
// transition-table alias stars can exclude them.  rowid is only supported on DELETE (consistent
// with the no-trigger path).
static case_insensitive_set_t InjectVirtualColumns(SelectNode &outer,
                                                   vector<unique_ptr<ParsedExpression>> &base_returning,
                                                   const TableCatalogEntry &table, const QueryNode &node) {
	auto virtual_cols = table.GetVirtualColumns();
	auto target_alias = GetTableAlias(node, table.name);

	// Rewrite each virtual-column ref to its bare name (dropping catalog/schema/table
	// qualifiers) so it binds against the base-CTE column we project below.
	case_insensitive_set_t injected_names;
	for (auto &expr : outer.select_list) {
		ParsedExpressionIterator::VisitExpressionMutable<ColumnRefExpression>(*expr, [&](ColumnRefExpression &colref) {
			auto vcn = MatchVirtualColumn(colref, virtual_cols, target_alias);
			if (!vcn.empty()) {
				injected_names.insert(vcn);
				colref.ColumnNamesMutable() = {vcn};
			}
		});
	}

	// Project each referenced virtual column into the inner DML's RETURNING, and
	// exclude it from any RETURNING * so it is not emitted twice.
	for (auto &vcn : injected_names) {
		base_returning.push_back(make_uniq<ColumnRefExpression>(vcn));
		for (auto &expr : outer.select_list) {
			ParsedExpressionIterator::VisitExpressionMutable<StarExpression>(
			    *expr, [&](StarExpression &star) { star.ExcludeListMutable().insert(QualifiedColumnName(vcn)); });
		}
	}
	return injected_names;
}

static unique_ptr<CommonTableExpressionInfo> BuildBaseCTE(const QueryNode &node, SelectNode &outer,
                                                          const TableCatalogEntry &table, bool has_returning,
                                                          case_insensitive_set_t &injected_names_out) {
	auto base_cte_node = node.Copy();
	auto &base_returning = GetDMLReturningList(*base_cte_node);
	base_returning.push_back(make_uniq<StarExpression>());

	if (has_returning) {
		injected_names_out = InjectVirtualColumns(outer, base_returning, table, node);
	}

	auto cte = make_uniq<CommonTableExpressionInfo>();
	cte->query_node = std::move(base_cte_node);
	cte->materialized = CTEMaterialize::CTE_MATERIALIZE_ALWAYS;
	cte->is_trigger_generated = true;
	return cte;
}

static void AddAfterTriggerCTEs(SelectNode &outer, const vector<const_reference<TriggerCatalogEntry>> &after_triggers,
                                const string &base_cte_name, const string &uuid_suffix,
                                const case_insensitive_set_t &injected_names) {
	for (idx_t i = 0; i < after_triggers.size(); i++) {
		auto &trigger = after_triggers[i].get();
		auto body_cte_name = string(TRIGGER_BODY_CTE_PREFIX) + to_string(i + 1) + "_" + uuid_suffix;

		auto trig_cte = make_uniq<CommonTableExpressionInfo>();
		trig_cte->query_node = trigger.trigger_action->Copy();
		trig_cte->materialized = CTEMaterialize::CTE_MATERIALIZE_DEFAULT;
		trig_cte->is_trigger_generated = true;

		// Scoped to this body so sibling triggers can't see the alias.
		auto &body_map = trig_cte->query_node->cte_map.map;
		if (!trigger.referencing_new_table.empty() && body_map.find(trigger.referencing_new_table) == body_map.end()) {
			body_map[trigger.referencing_new_table] = MakeTransitionTableAliasCTE(base_cte_name, injected_names);
		}
		if (!trigger.referencing_old_table.empty() && body_map.find(trigger.referencing_old_table) == body_map.end()) {
			body_map[trigger.referencing_old_table] = MakeTransitionTableAliasCTE(base_cte_name, injected_names);
		}

		outer.cte_map.map[body_cte_name] = std::move(trig_cte);
	}
}

// Rare path: a compound RETURNING expression containing a virtual-column ref (e.g. rowid + 1)
// would otherwise bind rowid as a plain CTE column in the outer SELECT, folding the implicit
// cast and diverging from the no-trigger column name.  For each such expression, bind a copy
// against the real DML table in a child binder to discover the canonical name, then pin it as
// an explicit alias.  The child binder shares global_binder_state, so trigger re-expansion for
// the same table is suppressed.  Errors are ignored here — the subsequent Bind(*outer) hits and
// reports the same one.
static void PinVirtualColumnExpressionNames(Binder &binder, ClientContext &context, const QueryNode &node,
                                            const TableCatalogEntry &table,
                                            vector<unique_ptr<ParsedExpression>> &returning_list) {
	auto virtual_cols = table.GetVirtualColumns();
	if (virtual_cols.empty()) {
		return;
	}
	auto target_alias = GetTableAlias(node, table.name);

	auto contains_vc_ref = [&](const ParsedExpression &expr) {
		bool found = false;
		ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(expr, [&](const ColumnRefExpression &cr) {
			if (!MatchVirtualColumn(cr, virtual_cols, target_alias).empty()) {
				found = true;
			}
		});
		return found;
	};

	vector<idx_t> fix_indices;
	for (idx_t i = 0; i < returning_list.size(); i++) {
		auto &expr = returning_list[i];
		auto ec = expr->GetExpressionClass();
		// Plain colrefs and stars keep their canonical name without help; aliased exprs are pinned already.
		if (expr->HasAlias() || ec == ExpressionClass::COLUMN_REF || ec == ExpressionClass::STAR) {
			continue;
		}
		if (contains_vc_ref(*expr)) {
			fix_indices.push_back(i);
		}
	}
	if (fix_indices.empty()) {
		return;
	}

	auto temp_node = node.Copy();
	auto &temp_returning = GetDMLReturningList(*temp_node);
	for (auto idx : fix_indices) {
		temp_returning.push_back(returning_list[idx]->Copy());
	}
	try {
		auto name_binder = Binder::CreateBinder(context, optional_ptr<Binder>(&binder));
		auto bound_stmt = name_binder->Bind(*temp_node);
		D_ASSERT(bound_stmt.names.size() == fix_indices.size());
		for (idx_t j = 0; j < fix_indices.size(); j++) {
			returning_list[fix_indices[j]]->SetAlias(bound_stmt.names[j]);
		}
	} catch (const Exception &) {
		// Name-discovery is best-effort; Bind(*outer) will surface any real error.
	}
}

BoundStatement Binder::ExpandTriggers(QueryNode &node, TableCatalogEntry &table,
                                      const vector<const_reference<TriggerCatalogEntry>> &before_triggers,
                                      const vector<const_reference<TriggerCatalogEntry>> &after_triggers) {
	D_ASSERT(!before_triggers.empty() || !after_triggers.empty());
	D_ASSERT(node.type == QueryNodeType::INSERT_QUERY_NODE || node.type == QueryNodeType::UPDATE_QUERY_NODE ||
	         node.type == QueryNodeType::DELETE_QUERY_NODE);

	// Drain before copying so the copy's returning_list starts empty.
	auto user_returning_list = std::move(GetDMLReturningList(node));
	bool has_returning = !user_returning_list.empty();

	if (has_returning) {
		PinVirtualColumnExpressionNames(*this, context, node, table, user_returning_list);
	}

	auto uuid_suffix = UUID::ToString(UUID::GenerateRandomUUID());
	auto base_cte_name = TRIGGER_BASE_CTE_PREFIX + uuid_suffix;

	auto outer = BuildTriggerOuterSelect(node, table, base_cte_name, std::move(user_returning_list));

	// CTE order: BEFORE bodies → base DML → AFTER bodies.
	for (idx_t i = 0; i < before_triggers.size(); i++) {
		auto body_cte_name = string(TRIGGER_BEFORE_BODY_CTE_PREFIX) + to_string(i + 1) + "_" + uuid_suffix;
		auto trig_cte = make_uniq<CommonTableExpressionInfo>();
		trig_cte->query_node = before_triggers[i].get().trigger_action->Copy();
		trig_cte->materialized = CTEMaterialize::CTE_MATERIALIZE_DEFAULT;
		trig_cte->is_trigger_generated = true;
		outer->cte_map.map[body_cte_name] = std::move(trig_cte);
	}

	case_insensitive_set_t injected_names;
	outer->cte_map.map[base_cte_name] = BuildBaseCTE(node, *outer, table, has_returning, injected_names);

	AddAfterTriggerCTEs(*outer, after_triggers, base_cte_name, uuid_suffix, injected_names);

	auto bound = Bind(*outer);
	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = has_returning ? StatementReturnType::QUERY_RESULT : StatementReturnType::CHANGED_ROWS;
	return bound;
}

} // namespace duckdb
