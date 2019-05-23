#include "common/exception.hpp"
#include "common/string_util.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

static SQLType TransformStringToSQLType(char *str) {
	string lower_str = StringUtil::Lower(string(str));
	// Transform column type
	if (lower_str == "int" || lower_str == "int4" || lower_str == "signed") {
		return SQLType(SQLTypeId::INTEGER);
	} else if (lower_str == "varchar" || lower_str == "bpchar" || lower_str == "text" || lower_str == "string") {
		return SQLType(SQLTypeId::VARCHAR);
	} else if (lower_str == "int8") {
		return SQLType(SQLTypeId::BIGINT);
	} else if (lower_str == "int2") {
		return SQLType(SQLTypeId::SMALLINT);
	} else if (lower_str == "timestamp" || lower_str == "datetime") {
		return SQLType(SQLTypeId::TIMESTAMP);
	} else if (lower_str == "bool") {
		return SQLType(SQLTypeId::BOOLEAN);
	} else if (lower_str == "real" || lower_str == "float4") {
		return SQLType(SQLTypeId::FLOAT);
	} else if (lower_str == "double" || lower_str == "numeric" || lower_str == "float8") {
		return SQLType(SQLTypeId::DOUBLE);
	} else if (lower_str == "tinyint") {
		return SQLType(SQLTypeId::TINYINT);
	} else if (lower_str == "varbinary") {
		return SQLType(SQLTypeId::VARBINARY);
	} else if (lower_str == "date") {
		return SQLType(SQLTypeId::DATE);
	} else {
		throw NotImplementedException("DataType %s not supported yet...\n", str);
	}
}

SQLType Transformer::TransformTypeName(TypeName *type_name) {
	char *name = (reinterpret_cast<postgres::Value *>(type_name->names->tail->data.ptr_value)->val.str);
	// transform it to the SQL type
	return TransformStringToSQLType(name);
}
