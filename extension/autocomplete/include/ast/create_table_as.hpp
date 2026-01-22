#pragma once
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {
struct CreateTableAs {
	bool with_data = false;
	unique_ptr<SelectStatement> select_statement;
	ColumnList column_names;
};

} // namespace duckdb
