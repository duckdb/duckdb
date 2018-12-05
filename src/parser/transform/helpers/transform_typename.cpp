#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

TypeId Transformer::TransformStringToTypeId(char *str) {
	string lower_str = StringUtil::Lower(string(str));
	// Transform column type
	if (lower_str == "int" || lower_str == "int4" || lower_str == "signed") {
		return TypeId::INTEGER;
	} else if (lower_str == "varchar" || lower_str == "bpchar" || lower_str == "text" || lower_str == "string") {
		return TypeId::VARCHAR;
	} else if (lower_str == "int8") {
		return TypeId::BIGINT;
	} else if (lower_str == "int2") {
		return TypeId::SMALLINT;
	} else if (lower_str == "timestamp") {
		return TypeId::TIMESTAMP;
	} else if (lower_str == "bool") {
		return TypeId::BOOLEAN;
	} else if (lower_str == "double" || lower_str == "float8" || lower_str == "real" || lower_str == "float4" ||
	           lower_str == "numeric") {
		return TypeId::DECIMAL;
	} else if (lower_str == "tinyint") {
		return TypeId::TINYINT;
	} else if (lower_str == "varbinary") {
		return TypeId::VARBINARY;
	} else if (lower_str == "date") {
		return TypeId::DATE;
	} else {
		throw NotImplementedException("DataType %s not supported yet...\n", str);
	}
}
