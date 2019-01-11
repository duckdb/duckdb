#include "parser/statement/alter_table_statement.hpp"
#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;

void Binder::Bind(AlterTableStatement &stmt) {
	// visit the table reference
	AcceptChild(&stmt.table);
}
