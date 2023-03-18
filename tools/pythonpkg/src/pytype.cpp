#include "duckdb_python/pytype.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

DuckDBPyType::DuckDBPyType(LogicalType type) : type(std::move(type)) {
}

bool DuckDBPyType::Equals(const shared_ptr<DuckDBPyType> &other) const {
	if (!other) {
		return false;
	}
	return type == other->type;
}

bool DuckDBPyType::EqualsString(const string &type_str) const {
	auto other = TransformStringToLogicalType(type_str);
	return type == other;
}

void DuckDBPyType::Initialize(py::handle &m) {
	auto connection_module = py::class_<DuckDBPyType, shared_ptr<DuckDBPyType>>(m, "DuckDBPyType", py::module_local());

	connection_module.def("__repr__", &DuckDBPyType::ToString, "Stringified representation of the type object");
	connection_module.def("__eq__", &DuckDBPyType::Equals, "Compare two types for equality", py::arg("other"));
	connection_module.def("__eq__", &DuckDBPyType::EqualsString, "Compare two types for equality", py::arg("other"));
	connection_module.def(py::init<>(
	    [](const string &type_str) { return make_shared<DuckDBPyType>(TransformStringToLogicalType(type_str)); }));
}

string DuckDBPyType::ToString() const {
	return type.ToString();
}

const LogicalType &DuckDBPyType::Type() const {
	return type;
}

} // namespace duckdb
