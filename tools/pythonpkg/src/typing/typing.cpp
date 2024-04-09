#include "duckdb_python/typing.hpp"
#include "duckdb_python/pytype.hpp"

namespace duckdb {

static void DefineBaseTypes(py::handle &m) {
	m.attr("SQLNULL") = make_refcounted<DuckDBPyType>(LogicalType::SQLNULL);
	m.attr("BOOLEAN") = make_refcounted<DuckDBPyType>(LogicalType::BOOLEAN);
	m.attr("TINYINT") = make_refcounted<DuckDBPyType>(LogicalType::TINYINT);
	m.attr("UTINYINT") = make_refcounted<DuckDBPyType>(LogicalType::UTINYINT);
	m.attr("SMALLINT") = make_refcounted<DuckDBPyType>(LogicalType::SMALLINT);
	m.attr("USMALLINT") = make_refcounted<DuckDBPyType>(LogicalType::USMALLINT);
	m.attr("INTEGER") = make_refcounted<DuckDBPyType>(LogicalType::INTEGER);
	m.attr("UINTEGER") = make_refcounted<DuckDBPyType>(LogicalType::UINTEGER);
	m.attr("BIGINT") = make_refcounted<DuckDBPyType>(LogicalType::BIGINT);
	m.attr("UBIGINT") = make_refcounted<DuckDBPyType>(LogicalType::UBIGINT);
	m.attr("HUGEINT") = make_refcounted<DuckDBPyType>(LogicalType::HUGEINT);
	m.attr("UHUGEINT") = make_refcounted<DuckDBPyType>(LogicalType::UHUGEINT);
	m.attr("UUID") = make_refcounted<DuckDBPyType>(LogicalType::UUID);
	m.attr("FLOAT") = make_refcounted<DuckDBPyType>(LogicalType::FLOAT);
	m.attr("DOUBLE") = make_refcounted<DuckDBPyType>(LogicalType::DOUBLE);
	m.attr("DATE") = make_refcounted<DuckDBPyType>(LogicalType::DATE);

	m.attr("TIMESTAMP") = make_refcounted<DuckDBPyType>(LogicalType::TIMESTAMP);
	m.attr("TIMESTAMP_MS") = make_refcounted<DuckDBPyType>(LogicalType::TIMESTAMP_MS);
	m.attr("TIMESTAMP_NS") = make_refcounted<DuckDBPyType>(LogicalType::TIMESTAMP_NS);
	m.attr("TIMESTAMP_S") = make_refcounted<DuckDBPyType>(LogicalType::TIMESTAMP_S);

	m.attr("TIME") = make_refcounted<DuckDBPyType>(LogicalType::TIME);

	m.attr("TIME_TZ") = make_refcounted<DuckDBPyType>(LogicalType::TIME_TZ);
	m.attr("TIMESTAMP_TZ") = make_refcounted<DuckDBPyType>(LogicalType::TIMESTAMP_TZ);

	m.attr("VARCHAR") = make_refcounted<DuckDBPyType>(LogicalType::VARCHAR);

	m.attr("BLOB") = make_refcounted<DuckDBPyType>(LogicalType::BLOB);
	m.attr("BIT") = make_refcounted<DuckDBPyType>(LogicalType::BIT);
	m.attr("INTERVAL") = make_refcounted<DuckDBPyType>(LogicalType::INTERVAL);
}

void DuckDBPyTyping::Initialize(py::module_ &parent) {
	auto m = parent.def_submodule("typing", "This module contains classes and methods related to typing");
	DuckDBPyType::Initialize(m);

	DefineBaseTypes(m);
}

} // namespace duckdb
