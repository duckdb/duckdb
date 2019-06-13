#include "parser/statement/create_index_statement.hpp"
#include "planner/binder.hpp"
#include "planner/expression_binder/index_binder.hpp"
#include "planner/statement/bound_create_index_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundSQLStatement> Binder::Bind(CreateIndexStatement &stmt) {
	auto result = make_unique<BoundCreateIndexStatement>();
	// visit the table reference
	result->table = Bind(*stmt.table);
	if (result->table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("Cannot create index on a view!");
	}
	if (stmt.expressions.size() > 1) {
		throw NotImplementedException("Multidimensional indexes not supported yet");
	}
	// visit the expressions
	IndexBinder binder(*this, context);
	for (auto &expr : stmt.expressions) {
		result->expressions.push_back(binder.Bind(expr));
	}
	result->info = move(stmt.info);
	return move(result);
}
