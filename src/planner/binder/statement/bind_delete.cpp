#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/expression_binder/returning_binder.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

BoundStatement Binder::Bind(DeleteStatement &stmt) {
	// visit the table reference
	auto bound_table = Bind(*stmt.table);
	auto root = std::move(bound_table.plan);
	if (root->type != LogicalOperatorType::LOGICAL_GET) {
		throw BinderException("Can only delete from base table");
	}
	auto &get = root->Cast<LogicalGet>();
	auto table_ptr = get.GetTable();
	if (!table_ptr) {
		throw BinderException("Can only delete from base table");
	}
	auto &table = *table_ptr;
	if (!table.temporary) {
		// delete from persistent table: not read only!
		auto &properties = GetStatementProperties();
		properties.RegisterDBModify(table.catalog, context);
	}

	// plan any tables from the various using clauses
	if (!stmt.using_clauses.empty()) {
		unique_ptr<LogicalOperator> child_operator;
		for (auto &using_clause : stmt.using_clauses) {
			// bind the using clause
			auto using_binder = Binder::CreateBinder(context, this);
			auto op = using_binder->Bind(*using_clause);
			if (child_operator) {
				// already bound a child: create a cross product to unify the two
				child_operator = LogicalCrossProduct::Create(std::move(child_operator), std::move(op.plan));
			} else {
				child_operator = std::move(op.plan);
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

	// bind the row id columns and add them to the projection list
	BindRowIdColumns(table, get, del->expressions);

	if (!stmt.returning_list.empty()) {
		del->return_chunk = true;

		auto update_table_index = GenerateTableIndex();
		del->table_index = update_table_index;

		unique_ptr<LogicalOperator> del_as_logicaloperator = std::move(del);
		return BindReturning(std::move(stmt.returning_list), table, stmt.table->alias, update_table_index,
		                     std::move(del_as_logicaloperator));
	}
	BoundStatement result;
	result.plan = std::move(del);
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};

	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::CHANGED_ROWS;

	return result;
}

} // namespace duckdb
