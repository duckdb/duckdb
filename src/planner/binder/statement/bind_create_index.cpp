#include "duckdb/parser/statement/create_index_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/planner/statement/bound_create_index_statement.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundSQLStatement> Binder::Bind(CreateIndexStatement &stmt) {
	auto result = make_unique<BoundCreateIndexStatement>();
	// create an index: not read only!
	this->read_only = false;
	// visit the table reference
	result->table = Bind(*stmt.table);
	if (result->table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("Cannot create index on a view!");
	}
	IndexBinder binder(*this, context);
	for (auto &expr : stmt.expressions) {
		result->expressions.push_back(binder.Bind(expr));
	}
	result->info = move(stmt.info);
	return move(result);
}
