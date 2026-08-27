#include "duckdb.hpp"

using namespace duckdb;

int main() {
	DuckDB db(nullptr);
	Connection connection(db);
	auto result = connection.Query("SELECT 42");
	if (!result || result->HasError()) {
		return 1;
	}
	return result->GetValue(0, 0).GetValue<int32_t>() == 42 ? 0 : 1;
}
