#include "parser/statement/create_index_statement.hpp"
#include "planner/binder.hpp"
#include "planner/expression_binder/index_binder.hpp"

using namespace duckdb;
using namespace std;

void Binder::Bind(CreateIndexStatement &stmt) {
	// visit the table reference
	AcceptChild(&stmt.table);
	// visit the expressions
	for (auto &expr : stmt.expressions) {
		IndexBinder binder(*this, context);
		binder.BindAndResolveType(&expr);
		expr->ClearStatistics();
	}
}
