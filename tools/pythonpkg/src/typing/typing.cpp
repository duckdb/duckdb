#include "duckdb_python/typing.hpp"
#include "duckdb_python/pytype.hpp"

namespace duckdb {

static void DefineBaseTypes(py::handle &m) {
	m.attr("SQLNULL") = make_shared_ptr<DuckDBPyType>(LogicalType::SQLNULL);
	m.attr("BOOLEAN") = make_shared_ptr<DuckDBPyType>(LogicalType::BOOLEAN);
	m.attr("TINYINT") = make_shared_ptr<DuckDBPyType>(LogicalType::TINYINT);
	m.attr("UTINYINT") = make_shared_ptr<DuckDBPyType>(LogicalType::UTINYINT);
	m.attr("SMALLINT") = make_shared_ptr<DuckDBPyType>(LogicalType::SMALLINT);
	m.attr("USMALLINT") = make_shared_ptr<DuckDBPyType>(LogicalType::USMALLINT);
	m.attr("INTEGER") = make_shared_ptr<DuckDBPyType>(LogicalType::INTEGER);
	m.attr("UINTEGER") = make_shared_ptr<DuckDBPyType>(LogicalType::UINTEGER);
	m.attr("BIGINT") = make_shared_ptr<DuckDBPyType>(LogicalType::BIGINT);
	m.attr("UBIGINT") = make_shared_ptr<DuckDBPyType>(LogicalType::UBIGINT);
	m.attr("HUGEINT") = make_shared_ptr<DuckDBPyType>(LogicalType::HUGEINT);
	m.attr("UHUGEINT") = make_shared_ptr<DuckDBPyType>(LogicalType::UHUGEINT);
	m.attr("UUID") = make_shared_ptr<DuckDBPyType>(LogicalType::UUID);
	m.attr("FLOAT") = make_shared_ptr<DuckDBPyType>(LogicalType::FLOAT);
	m.attr("DOUBLE") = make_shared_ptr<DuckDBPyType>(LogicalType::DOUBLE);
	m.attr("DATE") = make_shared_ptr<DuckDBPyType>(LogicalType::DATE);

	m.attr("TIMESTAMP") = make_shared_ptr<DuckDBPyType>(LogicalType::TIMESTAMP);
	m.attr("TIMESTAMP_MS") = make_shared_ptr<DuckDBPyType>(LogicalType::TIMESTAMP_MS);
	m.attr("TIMESTAMP_NS") = make_shared_ptr<DuckDBPyType>(LogicalType::TIMESTAMP_NS);
	m.attr("TIMESTAMP_S") = make_shared_ptr<DuckDBPyType>(LogicalType::TIMESTAMP_S);

	m.attr("TIME") = make_shared_ptr<DuckDBPyType>(LogicalType::TIME);

	m.attr("TIME_TZ") = make_shared_ptr<DuckDBPyType>(LogicalType::TIME_TZ);
	m.attr("TIMESTAMP_TZ") = make_shared_ptr<DuckDBPyType>(LogicalType::TIMESTAMP_TZ);

	m.attr("VARCHAR") = make_shared_ptr<DuckDBPyType>(LogicalType::VARCHAR);

	m.attr("BLOB") = make_shared_ptr<DuckDBPyType>(LogicalType::BLOB);
	m.attr("BIT") = make_shared_ptr<DuckDBPyType>(LogicalType::BIT);
	m.attr("INTERVAL") = make_shared_ptr<DuckDBPyType>(LogicalType::INTERVAL);
}

void DuckDBPyTyping::Initialize(py::module_ &parent) {
	auto m = parent.def_submodule("typing", "This module contains classes and methods related to typing");
	DuckDBPyType::Initialize(m);

	DefineBaseTypes(m);
}

} // namespace duckdb
