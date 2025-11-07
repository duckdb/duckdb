#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/cluster_statement.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include <algorithm>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/enums/merge_action_type.hpp>
#include <duckdb/parser/tableref/subqueryref.hpp>
#include <duckdb/planner/bound_result_modifier.hpp>
#include <duckdb/planner/expression_binder/order_binder.hpp>
#include <duckdb/planner/expression_binder/select_bind_state.hpp>
#include <duckdb/planner/operator/logical_create_table.hpp>
#include <duckdb/planner/operator/logical_delete.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_order.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>

namespace duckdb {

BoundStatement Binder::Bind(ClusterStatement &stmt) {
	auto bound_table = Bind(*stmt.target);
	auto &root = bound_table.plan;
	if (root->type != LogicalOperatorType::LOGICAL_GET) {
		throw BinderException("Can only cluster base table");
	}
	auto &get = root->Cast<LogicalGet>();
	auto table_ptr = get.GetTable();
	if (!table_ptr) {
		throw BinderException("Can only cluster base table");
	}
	auto &table = *table_ptr;
	if (!table.temporary) {
		auto &properties = GetStatementProperties();
		properties.RegisterDBModify(table.catalog, context);
	}
	if (table.HasPrimaryKey()) {
		throw BinderException("Cannot cluster a table with a primary key");
	}

	if (!table.GetStorageInfo(context).index_info.empty()) {
		throw BinderException("Cannot cluster a table with indexes");
	}

	// Make a select node that selects everything, with the order by modifiers from the cluster statement
	vector<unique_ptr<ParsedExpression>> select_list;
	auto select_node = make_uniq<SelectNode>();

	// Pick everything, but not generated columns
	for (auto &col : table.GetColumns().Logical()) {
		if (col.Generated()) {
			continue;
		}
		select_node->select_list.push_back(make_uniq<ColumnRefExpression>(col.GetName()));
	}

	auto order_modifier = make_uniq<OrderModifier>();
	order_modifier->orders = std::move(stmt.modifiers);
	select_node->modifiers.emplace_back(std::move(order_modifier));

	auto select_and_sort = BindSelectNode(*select_node, std::move(bound_table));

	// Now make the create table
	// Get the base CreateTableInfo from the TableCatalogEntry, but modify it so that it uses REPLACE_ON_CONFLICT
	auto unbound_create_table_info = table.GetInfo();
	unbound_create_table_info->on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	auto create_table_info = BindCreateTableInfo(std::move(unbound_create_table_info));

	auto &schema = create_table_info->schema;
	auto create_table = make_uniq<LogicalCreateTable>(schema, std::move(create_table_info));
	create_table->children.push_back(std::move(select_and_sort.plan));

	BoundStatement result;
	result.plan = std::move(create_table);
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};

	return result;
}

} // namespace duckdb
