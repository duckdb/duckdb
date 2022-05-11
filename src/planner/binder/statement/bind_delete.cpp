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

namespace duckdb {

BoundStatement Binder::Bind(DeleteStatement &stmt) {
	BoundStatement result;

	// visit the table reference
	auto bound_table = Bind(*stmt.table);
	if (bound_table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("Can only delete from base table!");
	}
	auto &table_binding = (BoundBaseTableRef &)*bound_table;
	auto table = table_binding.table;

	auto root = CreatePlan(*bound_table);
	auto &get = (LogicalGet &)*root;
	D_ASSERT(root->type == LogicalOperatorType::LOGICAL_GET);

	if (!table->temporary) {
		// delete from persistent table: not read only!
		properties.read_only = false;
	}

	// plan any tables from the various using clauses
	if (!stmt.using_clauses.empty()) {
		unique_ptr<LogicalOperator> child_operator;
		for (auto &using_clause : stmt.using_clauses) {
			// bind the using clause
			auto bound_node = Bind(*using_clause);
			auto op = CreatePlan(*bound_node);
			if (child_operator) {
				// already bound a child: create a cross product to unify the two
				auto cross_product = make_unique<LogicalCrossProduct>();
				cross_product->children.push_back(move(child_operator));
				cross_product->children.push_back(move(op));
				child_operator = move(cross_product);
			} else {
				child_operator = move(op);
			}
		}
		if (child_operator) {
			auto cross_product = make_unique<LogicalCrossProduct>();
			cross_product->children.push_back(move(root));
			cross_product->children.push_back(move(child_operator));
			root = move(cross_product);
		}
	}

	// project any additional columns required for the condition
	unique_ptr<Expression> condition;
	if (stmt.condition) {
		WhereBinder binder(*this, context);
		condition = binder.Bind(stmt.condition);

		PlanSubqueries(&condition, &root);
		auto filter = make_unique<LogicalFilter>(move(condition));
		filter->AddChild(move(root));
		root = move(filter);
	}
	// create the delete node
	auto del = make_unique<LogicalDelete>(table);
	del->AddChild(move(root));

	// set up the delete expression
	del->expressions.push_back(make_unique<BoundColumnRefExpression>(
	    LogicalType::ROW_TYPE, ColumnBinding(get.table_index, get.column_ids.size())));
	get.column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);

	if (!stmt.returning_list.empty()) {
		del->return_chunk = true;

		auto update_table_index = GenerateTableIndex();
		del->table_index = update_table_index;

		unique_ptr<LogicalOperator> del_as_logicaloperator = move(del);
		return BindReturning(move(stmt.returning_list), table, update_table_index, move(del_as_logicaloperator),
		                     move(result));
	} else {
		result.plan = move(del);
		result.names = {"Count"};
		result.types = {LogicalType::BIGINT};
		properties.allow_stream_result = false;
		properties.return_type = StatementReturnType::CHANGED_ROWS;
	}
	return result;
}

} // namespace duckdb
