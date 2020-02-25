#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/statement/bound_delete_statement.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundSQLStatement> Binder::Bind(DeleteStatement &stmt) {
	auto result = make_unique<BoundDeleteStatement>();
	// visit the table reference
	result->table = Bind(*stmt.table);
	if (result->table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("Can only delete from base table!");
	}
	auto &tableref = (BoundBaseTableRef &)*result->table;
	if (!tableref.table->temporary) {
		// delete from persistent table: not read only!
		this->read_only = false;
	}
	// project any additional columns required for the condition
	if (stmt.condition) {
		WhereBinder binder(*this, context);
		result->condition = binder.Bind(stmt.condition);
	}
	return move(result);
}
