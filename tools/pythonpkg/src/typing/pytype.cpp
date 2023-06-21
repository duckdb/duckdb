#include "duckdb_python/pytype.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

// NOLINTNEXTLINE(readability-identifier-naming)
bool PyGenericAlias::check_(const py::handle &object) {
	if (!ModuleIsLoaded<TypesCacheItem>()) {
		return false;
	}
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return py::isinstance(object, import_cache.types().GenericAlias());
}

// NOLINTNEXTLINE(readability-identifier-naming)
bool PyUnionType::check_(const py::handle &object) {
	auto types_loaded = ModuleIsLoaded<TypesCacheItem>();
	auto typing_loaded = ModuleIsLoaded<TypingCacheItem>();

	if (!types_loaded && !typing_loaded) {
		return false;
	}

	auto &import_cache = *DuckDBPyConnection::ImportCache();
	if (types_loaded && py::isinstance(object, import_cache.types().UnionType())) {
		return true;
	}
	if (typing_loaded && py::isinstance(object, import_cache.typing()._UnionGenericAlias())) {
		return true;
	}
	return false;
}

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
enum class PythonTypeObject : uint8_t {
	INVALID,   // not convertible to our type
	BASE,      // 'builtin' type objects
	UNION,     // typing.UnionType
	COMPOSITE, // list|dict types
	STRUCT,    // dictionary
	STRING,    // string value
};
}

static PythonTypeObject GetTypeObjectType(const py::handle &type_object) {
	if (py::isinstance<py::type>(type_object)) {
		return PythonTypeObject::BASE;
	}
	if (py::isinstance<py::str>(type_object)) {
		return PythonTypeObject::STRING;
	}
	if (py::isinstance<PyGenericAlias>(type_object)) {
		return PythonTypeObject::COMPOSITE;
	}
	if (py::isinstance<py::dict>(type_object)) {
		return PythonTypeObject::STRUCT;
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

static bool FromNumpyType(const py::object &type, LogicalType &result) {
	// Since this is a type, we have to create an instance from it first.
	auto obj = type();
	// We convert these to string because the underlying physical
	// types of a numpy type aren't consistent on every platform
	string type_str = py::str(obj.attr("dtype"));
	if (type_str == "bool") {
		result = LogicalType::BOOLEAN;
	} else if (type_str == "int8") {
		result = LogicalType::TINYINT;
	} else if (type_str == "uint8") {
		result = LogicalType::UTINYINT;
	} else if (type_str == "int16") {
		result = LogicalType::SMALLINT;
	} else if (type_str == "uint16") {
		result = LogicalType::USMALLINT;
	} else if (type_str == "int32") {
		result = LogicalType::INTEGER;
	} else if (type_str == "uint32") {
		result = LogicalType::UINTEGER;
	} else if (type_str == "int64") {
		result = LogicalType::BIGINT;
	} else if (type_str == "uint64") {
		result = LogicalType::UBIGINT;
	} else if (type_str == "float16") {
		// FIXME: should we even support this?
		result = LogicalType::FLOAT;
	} else if (type_str == "float32") {
		result = LogicalType::FLOAT;
	} else if (type_str == "float64") {
		result = LogicalType::DOUBLE;
	} else {
		return false;
	}
	return true;
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

	LogicalType result;
	if (FromNumpyType(obj, result)) {
		return result;
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

static LogicalType FromUnionType(const py::object &obj) {
	idx_t index = 1;
	child_list_t<LogicalType> members;
	py::tuple args = obj.attr("__args__");

	for (const auto &arg : args) {
		auto name = StringUtil::Format("u%d", index++);
		py::object object = py::reinterpret_borrow<py::object>(arg);
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
		} else {
			throw NotImplementedException("Can only create a MAP from a dict if args is formed correctly");
		}
	}
	string origin_type = py::str(origin);
	throw InvalidInputException("Could not convert from '%s' to DuckDBPyType", origin_type);
}

static LogicalType FromDictionary(const py::object &obj) {
	auto dict = py::reinterpret_steal<py::dict>(obj);
	child_list_t<LogicalType> children;
	children.reserve(dict.size());
	for (auto &item : dict) {
		auto &name_p = item.first;
		auto type_p = py::reinterpret_borrow<py::object>(item.second);
		string name = py::str(name_p);
		auto type = FromObject(type_p);
		children.push_back(std::make_pair(name, std::move(type)));
	}
	return LogicalType::STRUCT(std::move(children));
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
	case PythonTypeObject::STRUCT: {
		return FromDictionary(object);
	}
	case PythonTypeObject::UNION: {
		return FromUnionType(object);
	}
	case PythonTypeObject::STRING: {
		auto string_value = std::string(py::str(object));
		return FromString(string_value, nullptr);
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
		auto ltype = FromString(type_str, std::move(connection));
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
	connection_module.def(py::init<>([](const py::object &obj) {
		auto ltype = FromObject(obj);
		return make_shared<DuckDBPyType>(ltype);
	}));
	connection_module.def("__getattr__", &DuckDBPyType::GetAttribute, "Get the child type by 'name'", py::arg("name"));
	connection_module.def("__getitem__", &DuckDBPyType::GetAttribute, "Get the child type by 'name'", py::arg("name"));

	py::implicitly_convertible<py::object, DuckDBPyType>();
	py::implicitly_convertible<py::str, DuckDBPyType>();
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
