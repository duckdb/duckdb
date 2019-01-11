#include "parser/statement/copy_statement.hpp"
#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;

void Binder::Bind(CopyStatement &stmt) {
	if (stmt.select_statement) {
		Bind(*stmt.select_statement);
	}
}
