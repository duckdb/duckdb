#include "duckdb_python/pyconnection/pyconnection.hpp"

namespace duckdb {

shared_ptr<DuckDBPyType> DuckDBPyConnection::MapType(const shared_ptr<DuckDBPyType> &key_type,
                                                     const shared_ptr<DuckDBPyType> &value_type) {
	auto map_type = LogicalType::MAP(key_type->Type(), value_type->Type());
	return make_shared<DuckDBPyType>(map_type);
}

shared_ptr<DuckDBPyType> DuckDBPyConnection::ArrayType(const shared_ptr<DuckDBPyType> &type) {
	auto array_type = LogicalType::LIST(type->Type());
	return make_shared<DuckDBPyType>(array_type);
}

static child_list_t<LogicalType> GetChildList(const py::object &container) {
	child_list_t<LogicalType> types;
	if (py::isinstance<py::list>(container)) {
		const py::list &fields = container;
		idx_t i = 1;
		for (auto &item : fields) {
			shared_ptr<DuckDBPyType> pytype;
			if (!py::try_cast<shared_ptr<DuckDBPyType>>(item, pytype)) {
				string actual_type = py::str(item.get_type());
				throw InvalidInputException("object has to be a list of DuckDBPyType's, not '%s'", actual_type);
			}
			types.push_back(std::make_pair(StringUtil::Format("v%d", i++), pytype->Type()));
		}
		return types;
	} else if (py::isinstance<py::dict>(container)) {
		const py::dict &fields = container;
		for (auto &item : fields) {
			auto &name_p = item.first;
			auto &type_p = item.second;
			string name = py::str(name_p);
			shared_ptr<DuckDBPyType> pytype;
			if (!py::try_cast<shared_ptr<DuckDBPyType>>(type_p, pytype)) {
				string actual_type = py::str(type_p.get_type());
				throw InvalidInputException("object has to be a list of DuckDBPyType's, not '%s'", actual_type);
			}
			types.push_back(std::make_pair(name, pytype->Type()));
		}
		return types;
	} else {
		string actual_type = py::str(container.get_type());
		throw InvalidInputException(
		    "Can not construct a child list from object of type '%s', only dict/list is supported", actual_type);
	}
}

shared_ptr<DuckDBPyType> DuckDBPyConnection::StructType(const py::object &fields) {
	child_list_t<LogicalType> types = GetChildList(fields);
	if (types.empty()) {
		throw InvalidInputException("Can not create an empty struct type!");
	}
	auto struct_type = LogicalType::STRUCT(std::move(types));
	return make_shared<DuckDBPyType>(struct_type);
}

shared_ptr<DuckDBPyType> DuckDBPyConnection::UnionType(const py::object &members) {
	child_list_t<LogicalType> types = GetChildList(members);

	if (types.empty()) {
		throw InvalidInputException("Can not create an empty union type!");
	}
	auto union_type = LogicalType::UNION(std::move(types));
	return make_shared<DuckDBPyType>(union_type);
}

shared_ptr<DuckDBPyType> DuckDBPyConnection::EnumType(const string &name, const shared_ptr<DuckDBPyType> &type,
                                                      const py::list &values_p) {
	throw NotImplementedException("enum_type creation method is not implemented yet");
}

shared_ptr<DuckDBPyType> DuckDBPyConnection::DecimalType(int width, int scale) {
	auto decimal_type = LogicalType::DECIMAL(width, scale);
	return make_shared<DuckDBPyType>(decimal_type);
}

shared_ptr<DuckDBPyType> DuckDBPyConnection::StringType(const string &collation) {
	LogicalType type;
	if (collation.empty()) {
		type = LogicalType::VARCHAR;
	} else {
		type = LogicalType::VARCHAR_COLLATION(collation);
	}
	return make_shared<DuckDBPyType>(type);
}

shared_ptr<DuckDBPyType> DuckDBPyConnection::Type(const string &type_str) {
	if (!connection) {
		throw ConnectionException("Connection already closed!");
	}
	return make_shared<DuckDBPyType>(TransformStringToLogicalType(type_str, *connection->context));
}

} // namespace duckdb
