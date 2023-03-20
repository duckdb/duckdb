#include "duckdb_python/pytype.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb_python/pyconnection.hpp"
#include "duckdb/main/connection.hpp"

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
	return StringUtil::CIEquals(type.ToString(), type_str);
}

static bool HasAttributeInternal(const LogicalType &type, const string &name, idx_t &index) {
	if (type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::UNION) {
		auto &children = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < children.size(); i++) {
			auto &child = children[i];
			if (StringUtil::CIEquals(child.first, name)) {
				index = i;
				return true;
			}
		}
	}
	return false;
}

bool DuckDBPyType::HasAttribute(const string &name) const {
	idx_t unused;
	return HasAttributeInternal(type, name, unused);
}

shared_ptr<DuckDBPyType> DuckDBPyType::GetAttribute(const string &name) const {
	idx_t child_index;
	if (!HasAttributeInternal(type, name, child_index)) {
		throw py::attribute_error(
		    StringUtil::Format("Tried to get child type by the name of '%s', but this type either isn't nested, "
		                       "or it doesn't have a child by that name",
		                       name));
	}
	return make_shared<DuckDBPyType>(StructType::GetChildType(type, child_index));
}

void DuckDBPyType::Initialize(py::handle &m) {
	auto connection_module = py::class_<DuckDBPyType, shared_ptr<DuckDBPyType>>(m, "DuckDBPyType", py::module_local());

	connection_module.def("__repr__", &DuckDBPyType::ToString, "Stringified representation of the type object");
	connection_module.def("__eq__", &DuckDBPyType::Equals, "Compare two types for equality", py::arg("other"));
	connection_module.def("__eq__", &DuckDBPyType::EqualsString, "Compare two types for equality", py::arg("other"));
	connection_module.def(py::init<>([](const string &type_str, shared_ptr<DuckDBPyConnection> connection = nullptr) {
		                      if (!connection) {
			                      connection = DuckDBPyConnection::DefaultConnection();
		                      }
		                      return make_shared<DuckDBPyType>(
		                          TransformStringToLogicalType(type_str, *connection->connection->context));
	                      }),
	                      py::arg("type_str"), py::arg("connection") = py::none());
	connection_module.def("__getattr__", &DuckDBPyType::GetAttribute, "Get the child type by 'name'", py::arg("name"));
	connection_module.def("__getitem__", &DuckDBPyType::GetAttribute, "Get the child type by 'name'", py::arg("name"));
}

string DuckDBPyType::ToString() const {
	return type.ToString();
}

const LogicalType &DuckDBPyType::Type() const {
	return type;
}

} // namespace duckdb
