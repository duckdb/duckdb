#include "common/exception.hpp"
#include "common/string_util.hpp"
#include "parser/transformer.hpp"
#include <iostream>
#include <nodes/pg_list.h>

using namespace duckdb;
using namespace std;

static SQLType TransformStringToSQLType(char *str, postgres::TypeName *type_name) {
	auto lower_str = StringUtil::Lower(string(str));
	// Transform column type
	if (lower_str == "int" || lower_str == "int4" || lower_str == "signed") {
		return SQLType::INTEGER;
	} else if (lower_str == "varchar" || lower_str == "bpchar" || lower_str == "text" || lower_str == "string") {
		return SQLType::VARCHAR;
	} else if (lower_str == "int8") {
		return SQLType::BIGINT;
	} else if (lower_str == "int2") {
		return SQLType::SMALLINT;
	} else if (lower_str == "timestamp" || lower_str == "datetime") {
		return SQLType::TIMESTAMP;
	} else if (lower_str == "bool") {
		return SQLType(SQLTypeId::BOOLEAN);
	} else if (lower_str == "real" || lower_str == "float4") {
		return SQLType::FLOAT;
	} else if (lower_str == "double" || lower_str == "numeric" || lower_str == "float8") {
		return SQLType::DOUBLE;
	} else if (lower_str == "tinyint") {
		return SQLType::TINYINT;
	} else if (lower_str == "varbinary") {
		return SQLType(SQLTypeId::VARBINARY);
	} else if (lower_str == "date") {
		return SQLType::DATE;
	} else if (lower_str == "time") {
		return SQLType::TIME;
    } else if (lower_str == "interval") {
        assert(type_name->typmods);
        long period = reinterpret_cast<postgres::A_Const *>(type_name->typmods->head->data.ptr_value)->val.val.ival;
        return SQLType(SQLTypeId::INTERVAL, 0 , period);
	} else {
		throw NotImplementedException("DataType %s not supported yet...\n", str);
	}
}

SQLType Transformer::TransformTypeName(postgres::TypeName *type_name) {
	auto name = (reinterpret_cast<postgres::Value *>(type_name->names->tail->data.ptr_value)->val.str);
	// transform it to the SQL type
	return TransformStringToSQLType(name, type_name);
}
