#include "parser/statement/create_view_statement.hpp"
#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;

void Binder::Bind(CreateViewStatement &stmt) {
	// bind any constraints
	Bind(*stmt.info->query);

}
