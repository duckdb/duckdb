#include "duckdb_python/pystatement.hpp"

namespace duckdb {

static void InitializeReadOnlyProperties(py::class_<DuckDBPyStatement, unique_ptr<DuckDBPyStatement>> &m) {
	m.def_property_readonly("type", &DuckDBPyStatement::Type, "Get the type of the statement.")
	    .def_property_readonly("query", &DuckDBPyStatement::Query, "Get the query equivalent to this statement.")
	    .def_property_readonly("named_parameters", &DuckDBPyStatement::NamedParameters,
	                           "Get the map of named parameters this statement has.");
}

void DuckDBPyStatement::Initialize(py::handle &m) {
	auto relation_module =
	    py::class_<DuckDBPyStatement, unique_ptr<DuckDBPyStatement>>(m, "Statement", py::module_local());
	InitializeReadOnlyProperties(relation_module);
}

DuckDBPyStatement::DuckDBPyStatement(unique_ptr<SQLStatement> statement) : statement(std::move(statement)) {
}

unique_ptr<SQLStatement> DuckDBPyStatement::GetStatement() {
	return statement->Copy();
}

string DuckDBPyStatement::Query() const {
	auto &loc = statement->stmt_location;
	auto &length = statement->stmt_length;
	return statement->query.substr(loc, length);
}

py::set DuckDBPyStatement::NamedParameters() const {
	py::set result;
	auto &named_parameters = statement->named_param_map;
	for (auto &param : named_parameters) {
		result.add(param.first);
	}
	return result;
}

StatementType DuckDBPyStatement::Type() const {
	return statement->type;
}

} // namespace duckdb
