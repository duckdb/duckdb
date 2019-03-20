#include "parser/statement/update_statement.hpp"
#include "planner/binder.hpp"
#include "planner/expression_binder/update_binder.hpp"
#include "planner/expression_binder/where_binder.hpp"
#include "planner/statement/bound_update_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundSQLStatement> Binder::Bind(UpdateStatement &stmt) {
	auto result = make_unique<BoundUpdateStatement>();
	// visit the table reference
	result->table = Bind(*stmt.table);
	assert(result->table->type == TableReferenceType::BASE_TABLE);
	auto table = ((BoundBaseTableRef&) *result->table).table;
	result->proj_index = GenerateTableIndex();
	// project any additional columns required for the condition/expressions
	if (stmt.condition) {
		WhereBinder binder(*this, context);
		result->condition = binder.Bind(stmt.condition);
	}
	assert(stmt.columns.size() == stmt.expressions.size());
	for (size_t i = 0; i < stmt.columns.size(); i++) {
		auto &colname = stmt.columns[i];
		auto &expr = stmt.expressions[i];
		if (!table->ColumnExists(colname)) {
			throw BinderException("Referenced update column %s not found in table!", colname.c_str());
		}
		auto &column = table->GetColumn(colname);
		result->column_ids.push_back(column.oid);

		if (expr->type == ExpressionType::VALUE_DEFAULT) {
			result->expression.push_back(make_unique<BoundDefaultExpression>(GetInternalType(column.type), column.type));
		} else {
			UpdateBinder binder(*this, context);
			result->expressions.push_back(binder.Bind(expression));
			// FIXME: check if we need to create a cast
			// // now check if we have to create a cast
			// auto expression = move(stmt.expressions[i]);
			// if (expression->return_type != column.type || expression->HasParameter()) {
			// 	// differing types, create a cast
			// 	expression = make_unique<CastExpression>(column.type, move(expression));
			// 	VisitExpression(&expression);
			// 	expression->ResolveType();
			// }
		}

	}
	return move(result);
}
