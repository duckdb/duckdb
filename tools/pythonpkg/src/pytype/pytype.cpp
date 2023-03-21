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

shared_ptr<DuckDBPyType> DuckDBPyType::GetAttribute(const string &name) const {
	if (type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::UNION) {
		auto &children = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < children.size(); i++) {
			auto &child = children[i];
			if (StringUtil::CIEquals(child.first, name)) {
				return make_shared<DuckDBPyType>(StructType::GetChildType(type, i));
			}
		}
	}
	if (type.id() == LogicalTypeId::LIST && StringUtil::CIEquals(name, "child")) {
		return make_shared<DuckDBPyType>(ListType::GetChildType(type));
	}
	throw py::attribute_error(
	    StringUtil::Format("Tried to get child type by the name of '%s', but this type either isn't nested, "
	                       "or it doesn't have a child by that name",
	                       name));
}

static LogicalType FromObject(const py::object &object);

namespace {
enum class PythonTypeObject : uint8_t { INVALID, BASE, UNION, COMPOSITE };
}

static PythonTypeObject GetTypeObjectType(const py::handle &type_object) {
	if (py::isinstance<py::type>(type_object)) {
		return PythonTypeObject::BASE;
	}
	if (py::isinstance<PyGenericAlias>(type_object)) {
		return PythonTypeObject::COMPOSITE;
	}
	if (py::isinstance<PyUnionType>(type_object)) {
		return PythonTypeObject::UNION;
	}
	return PythonTypeObject::INVALID;
}

static LogicalType FromString(const string &type_str, shared_ptr<DuckDBPyConnection> connection) {
	if (!connection) {
		connection = DuckDBPyConnection::DefaultConnection();
	}
	return TransformStringToLogicalType(type_str, *connection->connection->context);
}

static LogicalType FromType(const py::type &obj) {
	py::module_ builtins = py::module_::import("builtins");
	if (obj.is(builtins.attr("str"))) {
		return LogicalType::VARCHAR;
	}
	if (obj.is(builtins.attr("int"))) {
		return LogicalType::BIGINT;
	}
	if (obj.is(builtins.attr("bytearray"))) {
		return LogicalType::BLOB;
	}
	if (obj.is(builtins.attr("bytes"))) {
		return LogicalType::BLOB;
	}
	if (obj.is(builtins.attr("float"))) {
		return LogicalType::DOUBLE;
	}
	if (obj.is(builtins.attr("bool"))) {
		return LogicalType::BOOLEAN;
	}
	throw py::type_error("Could not convert from unknown 'type' to DuckDBPyType");
}

static bool IsMapType(const py::tuple &args) {
	if (args.size() != 2) {
		return false;
	}
	for (auto &arg : args) {
		if (GetTypeObjectType(arg) == PythonTypeObject::INVALID) {
			return false;
		}
	}
	return true;
}

// This allows us to create structs from things like: dict['a': str, 'b': int]
static bool IsStructType(const py::tuple &args) {
	py::module_ builtins = py::module_::import("builtins");
	auto slice = builtins.attr("slice");
	for (auto &field : args) {
		if (!py::isinstance(field, slice)) {
			return false;
		}
		auto name = field.attr("start");
		auto type = field.attr("stop");
		if (!py::isinstance<py::str>(name)) {
			return false;
		}
		if (GetTypeObjectType(type) == PythonTypeObject::INVALID) {
			return false;
		}
	}
	return true;
}

static child_list_t<LogicalType> ToFields(const py::tuple &fields_p) {
	child_list_t<LogicalType> fields;

	D_ASSERT(IsStructType(fields_p));
	for (auto &field : fields_p) {
		auto name_p = field.attr("start");
		auto type = field.attr("stop");
		D_ASSERT(py::isinstance<py::str>(name_p));
		fields.push_back(make_pair(string(py::str(name_p)), FromObject(type)));
	}

	return fields;
}

static LogicalType FromUnionType(const py::object &obj) {
	idx_t index = 1;
	child_list_t<LogicalType> members;
	py::tuple args = obj.attr("__args__");

	for (const auto &arg : args) {
		auto name = StringUtil::Format("u%d", index++);
		py::object object = py::object(arg.ptr(), true);
		members.push_back(make_pair(name, FromObject(object)));
	}

	return LogicalType::UNION(std::move(members));
};

static LogicalType FromGenericAlias(const py::object &obj) {
	py::module_ builtins = py::module_::import("builtins");
	py::module_ types = py::module_::import("types");
	auto generic_alias = types.attr("GenericAlias");
	D_ASSERT(py::isinstance(obj, generic_alias));
	auto origin = obj.attr("__origin__");
	py::tuple args = obj.attr("__args__");

	if (origin.is(builtins.attr("list"))) {
		if (args.size() != 1) {
			throw NotImplementedException("Can only create a LIST from a single type");
		}
		return LogicalType::LIST(FromObject(args[0]));
	}
	if (origin.is(builtins.attr("dict"))) {
		if (IsMapType(args)) {
			return LogicalType::MAP(FromObject(args[0]), FromObject(args[1]));
		} else if (IsStructType(args)) {
			auto children = ToFields(args);
			return LogicalType::STRUCT(std::move(children));
		} else {
			throw NotImplementedException("Can only create a MAP or STRUCT from a dict if args is formed correctly");
		}
	}
	string origin_type = py::str(origin);
	throw InvalidInputException("Could not convert from '%s' to DuckDBPyType", origin_type);
}

static LogicalType FromObject(const py::object &object) {
	auto object_type = GetTypeObjectType(object);
	switch (object_type) {
	case PythonTypeObject::BASE: {
		return FromType(object);
	}
	case PythonTypeObject::COMPOSITE: {
		return FromGenericAlias(object);
	}
	case PythonTypeObject::UNION: {
		return FromUnionType(object);
	}
	default: {
		string actual_type = py::str(object.get_type());
		throw NotImplementedException("Could not convert from object of type '%s' to DuckDBPyType", actual_type);
	}
	}
}

void DuckDBPyType::Initialize(py::handle &m) {
	auto connection_module = py::class_<DuckDBPyType, shared_ptr<DuckDBPyType>>(m, "DuckDBPyType", py::module_local());

	connection_module.def("__repr__", &DuckDBPyType::ToString, "Stringified representation of the type object");
	connection_module.def("__eq__", &DuckDBPyType::Equals, "Compare two types for equality", py::arg("other"));
	connection_module.def("__eq__", &DuckDBPyType::EqualsString, "Compare two types for equality", py::arg("other"));
	connection_module.def(py::init<>([](const string &type_str, shared_ptr<DuckDBPyConnection> connection = nullptr) {
		                      auto ltype = FromString(type_str, connection);
		                      return make_shared<DuckDBPyType>(ltype);
	                      }),
	                      py::arg("type_str"), py::arg("connection") = py::none());
	connection_module.def(py::init<>([](const py::type &obj) {
		auto ltype = FromType(obj);
		return make_shared<DuckDBPyType>(ltype);
	}));
	connection_module.def(py::init<>([](const PyGenericAlias &obj) {
		auto ltype = FromGenericAlias(obj);
		return make_shared<DuckDBPyType>(ltype);
	}));
	connection_module.def(py::init<>([](const PyUnionType &obj) {
		auto ltype = FromUnionType(obj);
		return make_shared<DuckDBPyType>(ltype);
	}));
	connection_module.def("__getattr__", &DuckDBPyType::GetAttribute, "Get the child type by 'name'", py::arg("name"));
	connection_module.def("__getitem__", &DuckDBPyType::GetAttribute, "Get the child type by 'name'", py::arg("name"));

	py::implicitly_convertible<py::str, DuckDBPyType>();
	py::implicitly_convertible<py::type, DuckDBPyType>();
	py::implicitly_convertible<py::object, DuckDBPyType>();
	py::implicitly_convertible<PyGenericAlias, DuckDBPyType>();
	py::implicitly_convertible<PyUnionType, DuckDBPyType>();
}

string DuckDBPyType::ToString() const {
	return type.ToString();
}

const LogicalType &DuckDBPyType::Type() const {
	return type;
}

} // namespace duckdb
