#include "parser/statement/update_statement.hpp"
#include "planner/binder.hpp"
#include "planner/expression/bound_default_expression.hpp"
#include "planner/expression_binder/update_binder.hpp"
#include "planner/expression_binder/where_binder.hpp"
#include "planner/statement/bound_update_statement.hpp"
#include "planner/tableref/bound_basetableref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundSQLStatement> Binder::Bind(UpdateStatement &stmt) {
	auto result = make_unique<BoundUpdateStatement>();
	// visit the table reference
	result->table = Bind(*stmt.table);
	if (result->table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("Can only update base table!");
	}
	auto table = ((BoundBaseTableRef &)*result->table).table;
	result->proj_index = GenerateTableIndex();
	// project any additional columns required for the condition/expressions
	if (stmt.condition) {
		WhereBinder binder(*this, context);
		result->condition = binder.Bind(stmt.condition);
	}
	assert(stmt.columns.size() == stmt.expressions.size());
	for (uint64_t i = 0; i < stmt.columns.size(); i++) {
		auto &colname = stmt.columns[i];
		auto &expr = stmt.expressions[i];
		if (!table->ColumnExists(colname)) {
			throw BinderException("Referenced update column %s not found in table!", colname.c_str());
		}
		auto &column = table->GetColumn(colname);
		result->column_ids.push_back(column.oid);

		if (expr->type == ExpressionType::VALUE_DEFAULT) {
			result->expressions.push_back(
			    make_unique<BoundDefaultExpression>(GetInternalType(column.type), column.type));
		} else {
			UpdateBinder binder(*this, context);
			binder.target_type = column.type;
			auto bound_expr = binder.Bind(expr);
			result->expressions.push_back(move(bound_expr));
		}
	}
	return move(result);
}
