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

	// Create the delete
	auto del = make_uniq<LogicalDelete>(table, GenerateTableIndex());
	del->bound_constraints = BindConstraints(table);
	del->AddChild(std::move(root));

	// bind the row id columns and add them to the projection list
	BindRowIdColumns(table, get, del->expressions);

	// Set return chunk
	del->return_chunk = true;
	auto update_table_index = GenerateTableIndex();
	del->table_index = update_table_index;

	// Make star expression to return everything
	vector<unique_ptr<ParsedExpression>> returning_list;
	returning_list.push_back(make_uniq<StarExpression>());

	// Bind returning
	auto returning = BindReturning(std::move(returning_list), table, stmt.target->alias, update_table_index,
	                               unique_ptr_cast<LogicalDelete, LogicalOperator>(std::move(del)));

	// Make a select node that selects everything, with the order by modifiers
	vector<unique_ptr<ParsedExpression>> select_list;
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	auto order_modifier = make_uniq<OrderModifier>();
	order_modifier->orders = std::move(stmt.modifiers);
	select_node->modifiers.emplace_back(std::move(order_modifier));

	// Now bind the select node
	// Make a subquery to wrap the returning. We need to do this so that the order modfiers get bound to the top level
	// projection (and not to the already resolved columns of the delete)
	auto sub_binder = Binder::CreateBinder(context, this);

	// Add the returning as a subquery
	string subquery_alias = stmt.target->alias.empty() ? "__cluster_subquery" : stmt.target->alias;
	SubqueryRef subquery_ref(nullptr, subquery_alias);
	sub_binder->bind_context.AddSubquery(returning.plan->GetRootIndex(), subquery_ref.alias, subquery_ref, returning);

	auto select_and_sort = sub_binder->BindSelectNode(*select_node, std::move(returning));

	// Now make the insert
	auto insert = make_uniq<LogicalInsert>(table, GenerateTableIndex());
	insert->AddChild(std::move(select_and_sort.plan));

	BoundStatement result;
	result.plan = std::move(insert);
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};

	return result;
}

} // namespace duckdb
