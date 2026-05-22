#include "duckdb/planner/statement_rewriter.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
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
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

StatementRewriter::StatementRewriter(Binder &binder) : binder(binder) {
}

void StatementRewriter::FindRemoteCatalogsInSearchPath() {
	if (search_path_initialized) {
		return;
	}
	search_path_initialized = true;
	auto &client_data = ClientData::Get(binder.context);
	// iterate over all catalogs mentioned in the search path and check if they are remote
	auto search_path = client_data.catalog_search_path->Get();
	for (auto &entry : search_path) {
		auto catalog_entry = Catalog::GetCatalogEntry(binder.context, entry.catalog);
		if (!catalog_entry) {
			continue;
		}
		if (!catalog_entry->IsRemoteCatalog()) {
			local_catalogs_in_search_path.push_back(entry);
		} else {
			remote_catalogs_in_search_path.push_back(*catalog_entry);
		}
	}
}

CatalogPushdownResult StatementRewriter::Merge(CatalogPushdownResult a, CatalogPushdownResult b) {
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

void StatementRewriter::Rewrite(unique_ptr<SQLStatement> &statement) {
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

CatalogPushdownResult StatementRewriter::Rewrite(QueryNode &node) {
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
	default:
		return {};
	}
}

CatalogPushdownResult StatementRewriter::Rewrite(SelectNode &node) {
	// A SELECT with CTEs is too complex to analyze for pushdown
	if (!node.cte_map.map.empty()) {
		return {};
	}

	// Rewrite from_table first - its result is tracked separately for potential individual pushdown
	CatalogPushdownResult from_result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr, {}};
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

	// If the whole SELECT points to a single remote catalog, propagate upward
	if (result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		return result;
	}
	// Otherwise, push down only the from_table component if possible
	if (node.from_table) {
		FinishPushdown(node.from_table, from_result);
	}
	return result;
}

CatalogPushdownResult StatementRewriter::Rewrite(InsertQueryNode &node) {
	CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr, {}};
	// InsertQueryNode stores the target table in catalog/schema/table string fields, not in table_ref
	// (table_ref is only set for ON CONFLICT cases and is an alias ref)
	BaseTableRef target_ref;
	target_ref.catalog_name = node.catalog;
	target_ref.schema_name = node.schema;
	target_ref.table_name = node.table;
	result = Merge(result, Rewrite(target_ref));
	if (node.select_statement) {
		result = Merge(result, Rewrite(*node.select_statement->node));
	}
	return result;
}

CatalogPushdownResult StatementRewriter::Rewrite(DeleteQueryNode &node) {
	if (node.table) {
		return Rewrite(node.table);
	}
	return {};
}

CatalogPushdownResult StatementRewriter::Rewrite(UpdateQueryNode &node) {
	if (node.table) {
		return Rewrite(node.table);
	}
	return {};
}

CatalogPushdownResult StatementRewriter::Rewrite(SetOperationNode &node) {
	// Rewrite each child independently so we can push down individual children if needed
	vector<CatalogPushdownResult> child_results;
	child_results.reserve(node.children.size());
	CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr, {}};
	for (auto &child : node.children) {
		auto child_result = Rewrite(*child);
		result = Merge(result, child_result);
		child_results.push_back(child_result);
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
		return result;
	}
	// Otherwise push down individual children that can be pushed
	for (idx_t i = 0; i < node.children.size(); i++) {
		FinishPushdown(node.children[i], child_results[i]);
	}
	return result;
}

CatalogPushdownResult StatementRewriter::Rewrite(unique_ptr<TableRef> &ref) {
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
	case TableReferenceType::EMPTY_FROM:
	case TableReferenceType::COLUMN_DATA:
		return {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr, {}};
	default:
		return {};
	}
}

CatalogPushdownResult StatementRewriter::Rewrite(ExpressionListRef &ref) {
	CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr, {}};
	for (auto &row : ref.values) {
		for (auto &expr : row) {
			result = Merge(result, Rewrite(*expr));
		}
	}
	return result;
}

CatalogPushdownResult StatementRewriter::Rewrite(SubqueryRef &ref) {
	return Rewrite(*ref.subquery->node);
}

CatalogPushdownResult StatementRewriter::Rewrite(JoinRef &ref) {
	// Rewrite both sides independently, tracking their individual results
	auto left_result = Rewrite(ref.left);
	auto right_result = Rewrite(ref.right);
	auto result = Merge(left_result, right_result);
	// If both sides resolve to the same remote catalog, propagate upward
	if (result.reference_type == CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		return result;
	}
	// Otherwise push down each side individually
	FinishPushdown(ref.left, left_result);
	FinishPushdown(ref.right, right_result);
	return result;
}

CatalogPushdownResult StatementRewriter::Rewrite(BaseTableRef &ref) {
	// Resolve schema_name-as-catalog ambiguity using the binder's own resolution logic
	string catalog_name = ref.catalog_name;
	string schema_name = ref.schema_name;
	Binder::BindSchemaOrCatalog(binder.context, catalog_name, schema_name);

	// Case 1: catalog is explicitly specified - check if it's a remote catalog
	if (!catalog_name.empty()) {
		auto catalog = Catalog::GetCatalogEntry(binder.context, catalog_name);
		if (catalog && catalog->IsRemoteCatalog()) {
			return {CatalogReferenceType::SINGLE_REMOTE_CATALOG, catalog, schema_name};
		}
		return {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr, {}};
	}

	// Case 2: no explicit catalog - lazily populate search path catalogs on first use
	FindRemoteCatalogsInSearchPath();
	if (remote_catalogs_in_search_path.size() != 1) {
		return {};
	}

	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, ref.table_name);

	for (auto &local_entry : local_catalogs_in_search_path) {
		// If the ref specifies a schema, use it; otherwise use the search path schema
		const auto &schema = schema_name.empty() ? local_entry.schema : schema_name;
		if (Catalog::GetEntry(binder.context, local_entry.catalog, schema, table_lookup,
		                      OnEntryNotFound::RETURN_NULL)) {
			return {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr, {}};
		}
	}

	// Not found in any local catalog - push to the single remote catalog in the search path
	return {CatalogReferenceType::SINGLE_REMOTE_CATALOG, remote_catalogs_in_search_path.front().get(), schema_name};
}

CatalogPushdownResult StatementRewriter::Rewrite(ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		return {CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE, nullptr, {}};
	}
	CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED, nullptr, {}};
	ParsedExpressionIterator::EnumerateChildren(expr, [&](ParsedExpression &child) {
		if (result.reference_type == CatalogReferenceType::NO_CATALOG_REFERENCED) {
			result = Rewrite(child);
		}
	});
	return result;
}

unique_ptr<TableFunctionRef> StatementRewriter::CreateRemoteFunctionRef(CatalogPushdownResult &result,
                                                                        string remote_sql) {
	D_ASSERT(result.catalog);
	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(make_uniq<ConstantExpression>(Value(result.catalog->GetName())));
	args.push_back(make_uniq<ConstantExpression>(Value(std::move(remote_sql))));
	auto func_ref = make_uniq<TableFunctionRef>();
	func_ref->function = make_uniq<FunctionExpression>(result.catalog->GetRemoteExecuteFunction(), std::move(args));
	return func_ref;
}

void StatementRewriter::StripCatalogName(TableRef &ref, const string &catalog_name) {
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
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &sq = ref.Cast<SubqueryRef>();
		StripCatalogName(*sq.subquery->node, catalog_name);
		break;
	}
	default:
		break;
	}
}

void StatementRewriter::StripCatalogName(QueryNode &node, const string &catalog_name) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		if (select.from_table) {
			StripCatalogName(*select.from_table, catalog_name);
		}
		break;
	}
	case QueryNodeType::INSERT_QUERY_NODE: {
		auto &insert = node.Cast<InsertQueryNode>();
		// Strip from the target table's catalog/schema fields (these are what ToString() serializes)
		if (insert.catalog == catalog_name) {
			insert.catalog = "";
		} else if (insert.catalog.empty() && insert.schema == catalog_name) {
			insert.schema = "";
		}
		if (insert.select_statement) {
			StripCatalogName(*insert.select_statement->node, catalog_name);
		}
		break;
	}
	case QueryNodeType::DELETE_QUERY_NODE: {
		auto &del = node.Cast<DeleteQueryNode>();
		if (del.table) {
			StripCatalogName(*del.table, catalog_name);
		}
		break;
	}
	case QueryNodeType::UPDATE_QUERY_NODE: {
		auto &upd = node.Cast<UpdateQueryNode>();
		if (upd.table) {
			StripCatalogName(*upd.table, catalog_name);
		}
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop = node.Cast<SetOperationNode>();
		for (auto &child : setop.children) {
			StripCatalogName(*child, catalog_name);
		}
		break;
	}
	default:
		break;
	}
}

void StatementRewriter::StripCatalogName(SQLStatement &statement, const string &catalog_name) {
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

void StatementRewriter::FinishPushdown(unique_ptr<SQLStatement> &statement, CatalogPushdownResult result) {
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
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

void StatementRewriter::FinishPushdown(unique_ptr<QueryNode> &node, CatalogPushdownResult result) {
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		return;
	}
	StripCatalogName(*node, result.catalog->GetName());
	string remote_sql = node->ToString();
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = CreateRemoteFunctionRef(result, std::move(remote_sql));
	node = std::move(select_node);
}

void StatementRewriter::FinishPushdown(unique_ptr<TableRef> &ref, CatalogPushdownResult result) {
	if (result.reference_type != CatalogReferenceType::SINGLE_REMOTE_CATALOG) {
		return;
	}
	string alias = ref->alias;
	StripCatalogName(*ref, result.catalog->GetName());
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(ref);
	string remote_sql = select_node->ToString();
	auto func_ref = CreateRemoteFunctionRef(result, std::move(remote_sql));
	func_ref->alias = std::move(alias);
	ref = std::move(func_ref);
}

} // namespace duckdb
