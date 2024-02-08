#include "duckdb_python/pystatement.hpp"

namespace duckdb {

void DuckDBPyStatement::Initialize(py::handle &m) {
	auto relation_module = py::class_<DuckDBPyStatement>(m, "Statement", py::module_local());
}

DuckDBPyStatement::DuckDBPyStatement(unique_ptr<SQLStatement> statement) : statement(std::move(statement)) {
}

} // namespace duckdb
