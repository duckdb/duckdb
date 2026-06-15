#include "duckdb/planner/binder.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp"
#include "duckdb/common/enums/cte_materialize.hpp"
#include "duckdb/common/enums/trigger_type.hpp"
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
#include "duckdb/common/column_index.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression_binder/returning_binder.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <algorithm>
#include <unordered_set>

namespace duckdb {

// Defined in binder.cpp — rejects 'excluded'-qualified columns in RETURNING, matching the no-trigger path.
void VerifyNotExcluded(const ParsedExpression &root_expr);

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
		identifier_set_t updated_columns;
		if (update_node.set_info) {
			updated_columns.insert(update_node.set_info->columns.begin(), update_node.set_info->columns.end());
		}
		auto trigger_does_not_fire = [&](const_reference<TriggerCatalogEntry> trig) {
			const auto &of_cols = trig.get().columns;
			return !of_cols.empty() && std::none_of(of_cols.begin(), of_cols.end(),
			                                        [&](const Identifier &c) { return updated_columns.count(c) > 0; });
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

	// Only track this table as expanded for the duration of its own subtree in the trigger chain.
	expanded_tables.erase(table);
	return make_uniq<BoundStatement>(std::move(bound));
}

static constexpr const char *TRIGGER_BASE_CTE_PREFIX = "__duckdb_trigger_base_";
static constexpr const char *TRIGGER_BODY_CTE_PREFIX = "__duckdb_trigger_body_";
static constexpr const char *TRIGGER_BEFORE_BODY_CTE_PREFIX = "__duckdb_trigger_before_body_";

static unique_ptr<CommonTableExpressionInfo> MakeTransitionTableAliasCTE(const Identifier &base_cte_name,
                                                                         const identifier_set_t &exclude_columns) {
	auto alias_cte = make_uniq<CommonTableExpressionInfo>();
	auto alias_select = make_uniq<SelectNode>();
	auto star = make_uniq<StarExpression>();
	// Hide injected virtual columns from trigger bodies that SELECT * over the transition table.
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

static Identifier GetTableAlias(const QueryNode &node, const Identifier &fallback) {
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

// Raw table_ref alias (empty if none) — an empty alias lets the binding derive
// catalog.schema.table from the entry, so qualified RETURNING columns resolve.
static Identifier GetReturningAlias(const QueryNode &node) {
	switch (node.type) {
	case QueryNodeType::INSERT_QUERY_NODE:
		return node.Cast<InsertQueryNode>().table_ref ? node.Cast<InsertQueryNode>().table_ref->alias : Identifier();
	case QueryNodeType::UPDATE_QUERY_NODE:
		return node.Cast<UpdateQueryNode>().table ? node.Cast<UpdateQueryNode>().table->alias : Identifier();
	case QueryNodeType::DELETE_QUERY_NODE:
		return node.Cast<DeleteQueryNode>().table ? node.Cast<DeleteQueryNode>().table->alias : Identifier();
	default:
		return Identifier();
	}
}

// Virtual columns exposed to RETURNING — only DELETE, mirroring the no-trigger path.
static virtual_column_map_t ReturningVirtualColumns(const QueryNode &node, const TableCatalogEntry &table) {
	if (node.type == QueryNodeType::DELETE_QUERY_NODE) {
		return table.GetVirtualColumns();
	}
	return virtual_column_map_t();
}

// Virtual columns the RETURNING list references, so the base CTE can materialise them (RETURNING *
// does not include rowid).  Matched on the trailing identifier — the only table in scope is the
// target.  Returns (id, name) pairs, deduplicated.
static vector<pair<column_t, Identifier>> ReferencedVirtualColumns(vector<unique_ptr<ParsedExpression>> &returning_list,
                                                                   const virtual_column_map_t &virtual_cols,
                                                                   const TableCatalogEntry &table) {
	vector<pair<column_t, Identifier>> result;
	if (virtual_cols.empty()) {
		return result;
	}
	// A virtual column shadowed by a real column of the same name is not virtual here: RETURNING *
	// already materialises the real column, and injecting would duplicate the name.
	identifier_set_t real_names;
	for (auto &col : table.GetColumns().Logical()) {
		real_names.insert(col.Name());
	}
	unordered_set<column_t> seen;
	for (auto &expr : returning_list) {
		ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(*expr, [&](const ColumnRefExpression &cr) {
			auto &cn = cr.ColumnNames();
			if (cn.empty()) {
				return;
			}
			for (auto &vc : virtual_cols) {
				if (real_names.find(vc.second.name) != real_names.end()) {
					continue;
				}
				if (cn.back() == vc.second.name && seen.insert(vc.first).second) {
					result.emplace_back(vc.first, vc.second.name);
				}
			}
		});
	}
	return result;
}

static void AddAfterTriggerCTEs(SelectNode &outer, const vector<const_reference<TriggerCatalogEntry>> &after_triggers,
                                const Identifier &base_cte_name, const string &uuid_suffix,
                                const identifier_set_t &injected_names) {
	for (idx_t i = 0; i < after_triggers.size(); i++) {
		auto &trigger = after_triggers[i].get();
		Identifier body_cte_name(string(TRIGGER_BODY_CTE_PREFIX) + to_string(i + 1) + "_" + uuid_suffix);

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

// Build the trigger CTE chain as a SelectNode: BEFORE bodies → base DML → AFTER bodies.
// For RETURNING the outer projection is a plain SELECT *, rebound through ReturningBinder later.
static unique_ptr<SelectNode> BuildTriggerChain(const QueryNode &node, const TableCatalogEntry &table,
                                                const vector<const_reference<TriggerCatalogEntry>> &before_triggers,
                                                const vector<const_reference<TriggerCatalogEntry>> &after_triggers,
                                                const string &uuid_suffix, const Identifier &base_cte_name,
                                                bool has_returning,
                                                const vector<pair<column_t, Identifier>> &injected_virtuals) {
	auto outer = make_uniq<SelectNode>();
	if (has_returning) {
		outer->select_list.push_back(make_uniq<StarExpression>());
	} else {
		outer->select_list.push_back(
		    make_uniq<FunctionExpression>("count_star", vector<unique_ptr<ParsedExpression>>()));
	}
	auto from_ref = make_uniq<BaseTableRef>();
	from_ref->table_name = base_cte_name;
	from_ref->alias = GetTableAlias(node, table.name);
	outer->from_table = std::move(from_ref);

	// BEFORE bodies (no transition tables — REFERENCING is rejected at CREATE time).
	for (idx_t i = 0; i < before_triggers.size(); i++) {
		Identifier body_cte_name(string(TRIGGER_BEFORE_BODY_CTE_PREFIX) + to_string(i + 1) + "_" + uuid_suffix);
		auto trig_cte = make_uniq<CommonTableExpressionInfo>();
		trig_cte->query_node = before_triggers[i].get().trigger_action->Copy();
		trig_cte->materialized = CTEMaterialize::CTE_MATERIALIZE_DEFAULT;
		trig_cte->is_trigger_generated = true;
		outer->cte_map.map[body_cte_name] = std::move(trig_cte);
	}

	// Base CTE: the DML with RETURNING * (full rows for transition tables), plus referenced
	// virtual columns appended so they are materialised for the RETURNING projection.
	auto base_node = node.Copy();
	auto &base_returning = GetDMLReturningList(*base_node);
	base_returning.push_back(make_uniq<StarExpression>());
	identifier_set_t injected_names;
	for (auto &vc : injected_virtuals) {
		base_returning.push_back(make_uniq<ColumnRefExpression>(vc.second));
		injected_names.insert(vc.second);
	}
	auto base_cte = make_uniq<CommonTableExpressionInfo>();
	base_cte->query_node = std::move(base_node);
	base_cte->materialized = CTEMaterialize::CTE_MATERIALIZE_ALWAYS;
	base_cte->is_trigger_generated = true;
	outer->cte_map.map[base_cte_name] = std::move(base_cte);

	AddAfterTriggerCTEs(*outer, after_triggers, base_cte_name, uuid_suffix, injected_names);
	return outer;
}

BoundStatement Binder::ExpandTriggers(QueryNode &node, TableCatalogEntry &table,
                                      const vector<const_reference<TriggerCatalogEntry>> &before_triggers,
                                      const vector<const_reference<TriggerCatalogEntry>> &after_triggers) {
	D_ASSERT(!before_triggers.empty() || !after_triggers.empty());
	D_ASSERT(node.type == QueryNodeType::INSERT_QUERY_NODE || node.type == QueryNodeType::UPDATE_QUERY_NODE ||
	         node.type == QueryNodeType::DELETE_QUERY_NODE);

	// Drain before copying so the base CTE copy's returning_list starts empty.
	auto user_returning_list = std::move(GetDMLReturningList(node));
	bool has_returning = !user_returning_list.empty();

	auto virtual_columns = ReturningVirtualColumns(node, table);
	auto injected_virtuals = has_returning ? ReferencedVirtualColumns(user_returning_list, virtual_columns, table)
	                                       : vector<pair<column_t, Identifier>>();

	auto uuid_suffix = UUID::ToString(UUID::GenerateRandomUUID());
	Identifier base_cte_name(TRIGGER_BASE_CTE_PREFIX + uuid_suffix);

	auto outer = BuildTriggerChain(node, table, before_triggers, after_triggers, uuid_suffix, base_cte_name,
	                               has_returning, injected_virtuals);
	auto chain = Bind(*outer);

	if (!has_returning) {
		auto &properties = GetStatementProperties();
		properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
		properties.return_type = StatementReturnType::CHANGED_ROWS;
		return chain;
	}

	// SELECT * over the base CTE yields a projection with contiguous bindings (index, 0..n-1), so we
	// bind the RETURNING list against that index directly as an identity base-table binding.
	auto child_bindings = chain.plan->GetColumnBindings();
	D_ASSERT(!child_bindings.empty() && child_bindings.size() == chain.types.size());
	auto base_index = child_bindings[0].table_index;

	// Bind RETURNING with ReturningBinder over a base-table binding of the real table — same rules as
	// the no-trigger path (rejects aggregates/subqueries/windows, resolves qualified columns, virtual
	// vs real rowid).  bound_columns is the identity over all logical columns (generated included)
	// because our RETURNING * child materialises every logical column in order.
	auto returning_binder_owner = Binder::CreateBinder(context);
	vector<Identifier> col_names;
	vector<LogicalType> col_types;
	vector<ColumnIndex> bound_columns;
	idx_t logical_count = 0;
	for (auto &col : table.GetColumns().Logical()) {
		col_names.push_back(col.Name());
		col_types.push_back(col.Type());
		bound_columns.emplace_back(logical_count);
		logical_count++;
	}
	returning_binder_owner->bind_context.AddBaseTable(base_index, GetReturningAlias(node), col_names, col_types,
	                                                  bound_columns, table, std::move(virtual_columns));
	ReturningBinder returning_binder(*returning_binder_owner, context);

	vector<unique_ptr<ParsedExpression>> expanded_returning;
	returning_binder_owner->ExpandStarExpressions(user_returning_list, expanded_returning);
	if (expanded_returning.empty()) {
		throw BinderException("RETURNING list is empty!");
	}
	BoundStatement result;
	vector<unique_ptr<Expression>> proj_exprs;
	for (auto &returning_expr : expanded_returning) {
		VerifyNotExcluded(*returning_expr);
		LogicalType result_type;
		auto bound_expr = returning_binder.Bind(returning_expr, &result_type);
		result.names.push_back(bound_expr->GetName());
		result.types.push_back(result_type);
		proj_exprs.push_back(std::move(bound_expr));
	}

	// Virtual columns bind to a sentinel id, but the chain materialised them as trailing columns at
	// logical_count + k.  Remap those bindings to their real positions.
	if (!injected_virtuals.empty()) {
		unordered_map<column_t, idx_t> remap;
		for (idx_t k = 0; k < injected_virtuals.size(); k++) {
			remap[injected_virtuals[k].first] = logical_count + k;
		}
		for (auto &expr : proj_exprs) {
			ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
			    expr, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &) {
				    auto &binding = colref.BindingMutable();
				    if (binding.table_index != base_index) {
					    return;
				    }
				    auto it = remap.find(binding.column_index.GetIndexUnsafe());
				    if (it != remap.end()) {
					    binding.column_index = ProjectionIndex(it->second);
				    }
			    });
		}
	}

	auto returning_projection = make_uniq<LogicalProjection>(GenerateTableIndex(), std::move(proj_exprs));
	returning_projection->AddChild(std::move(chain.plan));
	result.plan = std::move(returning_projection);

	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

} // namespace duckdb
