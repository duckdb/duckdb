#include "duckdb_python/typing.hpp"
#include "duckdb_python/pytype.hpp"

namespace duckdb {

static void DefineBaseTypes(py::handle &m) {
	m.attr("SQLNULL") = DuckDBPyType(LogicalType::SQLNULL);
	m.attr("BOOLEAN") = DuckDBPyType(LogicalType::BOOLEAN);
	m.attr("TINYINT") = DuckDBPyType(LogicalType::TINYINT);
	m.attr("UTINYINT") = DuckDBPyType(LogicalType::UTINYINT);
	m.attr("SMALLINT") = DuckDBPyType(LogicalType::SMALLINT);
	m.attr("USMALLINT") = DuckDBPyType(LogicalType::USMALLINT);
	m.attr("INTEGER") = DuckDBPyType(LogicalType::INTEGER);
	m.attr("UINTEGER") = DuckDBPyType(LogicalType::UINTEGER);
	m.attr("BIGINT") = DuckDBPyType(LogicalType::BIGINT);
	m.attr("UBIGINT") = DuckDBPyType(LogicalType::UBIGINT);
	m.attr("HUGEINT") = DuckDBPyType(LogicalType::HUGEINT);
	m.attr("UUID") = DuckDBPyType(LogicalType::UUID);
	m.attr("FLOAT") = DuckDBPyType(LogicalType::FLOAT);
	m.attr("DOUBLE") = DuckDBPyType(LogicalType::DOUBLE);
	m.attr("DATE") = DuckDBPyType(LogicalType::DATE);

	m.attr("TIMESTAMP") = DuckDBPyType(LogicalType::TIMESTAMP);
	m.attr("TIMESTAMP_MS") = DuckDBPyType(LogicalType::TIMESTAMP_MS);
	m.attr("TIMESTAMP_NS") = DuckDBPyType(LogicalType::TIMESTAMP_NS);
	m.attr("TIMESTAMP_S") = DuckDBPyType(LogicalType::TIMESTAMP_S);

	m.attr("TIME") = DuckDBPyType(LogicalType::TIME);

	m.attr("TIME_TZ") = DuckDBPyType(LogicalType::TIME_TZ);
	m.attr("TIMESTAMP_TZ") = DuckDBPyType(LogicalType::TIMESTAMP_TZ);

	m.attr("VARCHAR") = DuckDBPyType(LogicalType::VARCHAR);

	m.attr("BLOB") = DuckDBPyType(LogicalType::BLOB);
	m.attr("BIT") = DuckDBPyType(LogicalType::BIT);
	m.attr("INTERVAL") = DuckDBPyType(LogicalType::INTERVAL);
}

void DuckDBPyTyping::Initialize(py::module_ &parent) {
	auto m = parent.def_submodule("typing", "This module contains classes and methods related to typing");
	DuckDBPyType::Initialize(m);

	DefineBaseTypes(m);
}

} // namespace duckdb
