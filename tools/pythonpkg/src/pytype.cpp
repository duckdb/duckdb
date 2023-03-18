#include "duckdb_python/pytype.hpp"

namespace duckdb {

DuckDBPyType::DuckDBPyType(LogicalType type) : type(type) {
}

void DuckDBPyType::Initialize(py::handle &m) {
	auto connection_module = py::class_<DuckDBPyType, shared_ptr<DuckDBPyType>>(m, "DuckDBPyType", py::module_local());

	connection_module.def("__repr__", &DuckDBPyType::ToString, "Stringified representation of the type object");
}

string DuckDBPyType::ToString() const {
	return type.ToString();
}

const LogicalType &DuckDBPyType::Type() const {
	return type;
}

} // namespace duckdb
