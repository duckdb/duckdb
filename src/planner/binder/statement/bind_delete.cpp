#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/expression_binder/returning_binder.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

BoundStatement Binder::Bind(DeleteStatement &stmt) {
	BoundStatement result;

	// visit the table reference
	auto bound_table = Bind(*stmt.table);
	if (bound_table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("Can only delete from base table!");
	}
	auto &table_binding = bound_table->Cast<BoundBaseTableRef>();
	auto &table = table_binding.table;

	auto root = CreatePlan(*bound_table);
	auto &get = root->Cast<LogicalGet>();
	D_ASSERT(root->type == LogicalOperatorType::LOGICAL_GET);

	if (!table.temporary) {
		// delete from persistent table: not read only!
		auto &properties = GetStatementProperties();
		properties.RegisterDBModify(table.catalog, context);
	}

	// Add CTEs as bindable
	AddCTEMap(stmt.cte_map);

	// plan any tables from the various using clauses
	if (!stmt.using_clauses.empty()) {
		unique_ptr<LogicalOperator> child_operator;
		for (auto &using_clause : stmt.using_clauses) {
			// bind the using clause
			auto using_binder = Binder::CreateBinder(context, this);
			auto bound_node = using_binder->Bind(*using_clause);
			auto op = CreatePlan(*bound_node);
			if (child_operator) {
				// already bound a child: create a cross product to unify the two
				child_operator = LogicalCrossProduct::Create(std::move(child_operator), std::move(op));
			} else {
				child_operator = std::move(op);
			}
			bind_context.AddContext(std::move(using_binder->bind_context));
		}
		if (child_operator) {
			root = LogicalCrossProduct::Create(std::move(root), std::move(child_operator));
		}
	}

	// project any additional columns required for the condition
	unique_ptr<Expression> condition;
	if (stmt.condition) {
		WhereBinder binder(*this, context);
		condition = binder.Bind(stmt.condition);

		PlanSubqueries(condition, root);
		auto filter = make_uniq<LogicalFilter>(std::move(condition));
		filter->AddChild(std::move(root));
		root = std::move(filter);
	}
	// create the delete node
	auto del = make_uniq<LogicalDelete>(table, GenerateTableIndex());
	del->bound_constraints = BindConstraints(table);
	del->AddChild(std::move(root));

	// set up the delete expression
	auto &column_ids = get.GetColumnIds();
	del->expressions.push_back(
	    make_uniq<BoundColumnRefExpression>(table.GetRowIdType(), ColumnBinding(get.table_index, column_ids.size())));
	get.AddColumnId(COLUMN_IDENTIFIER_ROW_ID);

	if (!stmt.returning_list.empty()) {
		del->return_chunk = true;

		auto update_table_index = GenerateTableIndex();
		del->table_index = update_table_index;

		unique_ptr<LogicalOperator> del_as_logicaloperator = std::move(del);
		return BindReturning(std::move(stmt.returning_list), table, stmt.table->alias, update_table_index,
		                     std::move(del_as_logicaloperator), std::move(result));
	}
	result.plan = std::move(del);
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};

	auto &properties = GetStatementProperties();
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::CHANGED_ROWS;

	return result;
}

} // namespace duckdb
