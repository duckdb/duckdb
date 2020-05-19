#include "duckdb/common/types.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/string_type.hpp"

#include <cmath>

using namespace std;

namespace duckdb {

const SQLType SQLType::SQLNULL = SQLType(SQLTypeId::SQLNULL);
const SQLType SQLType::BOOLEAN = SQLType(SQLTypeId::BOOLEAN);
const SQLType SQLType::TINYINT = SQLType(SQLTypeId::TINYINT);
const SQLType SQLType::SMALLINT = SQLType(SQLTypeId::SMALLINT);
const SQLType SQLType::INTEGER = SQLType(SQLTypeId::INTEGER);
const SQLType SQLType::BIGINT = SQLType(SQLTypeId::BIGINT);
const SQLType SQLType::FLOAT = SQLType(SQLTypeId::FLOAT);
const SQLType SQLType::DOUBLE = SQLType(SQLTypeId::DOUBLE);
const SQLType SQLType::DATE = SQLType(SQLTypeId::DATE);
const SQLType SQLType::TIMESTAMP = SQLType(SQLTypeId::TIMESTAMP);
const SQLType SQLType::TIME = SQLType(SQLTypeId::TIME);

const SQLType SQLType::VARCHAR = SQLType(SQLTypeId::VARCHAR);
const SQLType SQLType::VARBINARY = SQLType(SQLTypeId::VARBINARY);

const SQLType SQLType::BLOB = SQLType(SQLTypeId::BLOB);

// TODO these are incomplete and should maybe not exist as such
const SQLType SQLType::STRUCT = SQLType(SQLTypeId::STRUCT);
const SQLType SQLType::LIST = SQLType(SQLTypeId::LIST);

const SQLType SQLType::ANY = SQLType(SQLTypeId::ANY);

const vector<SQLType> SQLType::NUMERIC = {
    SQLType::TINYINT, SQLType::SMALLINT, SQLType::INTEGER,           SQLType::BIGINT,
    SQLType::FLOAT,   SQLType::DOUBLE,   SQLType(SQLTypeId::DECIMAL)};

const vector<SQLType> SQLType::INTEGRAL = {SQLType::TINYINT, SQLType::SMALLINT, SQLType::INTEGER, SQLType::BIGINT};

const vector<SQLType> SQLType::ALL_TYPES = {
    SQLType::BOOLEAN, SQLType::TINYINT,   SQLType::SMALLINT, SQLType::INTEGER, SQLType::BIGINT,
    SQLType::DATE,    SQLType::TIMESTAMP, SQLType::DOUBLE,   SQLType::FLOAT,   SQLType(SQLTypeId::DECIMAL),
    SQLType::VARCHAR, SQLType::BLOB};
// TODO add LIST/STRUCT here

const TypeId ROW_TYPE = TypeId::INT64;

string TypeIdToString(TypeId type) {
	switch (type) {
	case TypeId::BOOL:
		return "BOOL";
	case TypeId::INT8:
		return "INT8";
	case TypeId::INT16:
		return "INT16";
	case TypeId::INT32:
		return "INT32";
	case TypeId::INT64:
		return "INT64";
	case TypeId::HASH:
		return "HASH";
	case TypeId::POINTER:
		return "POINTER";
	case TypeId::FLOAT:
		return "FLOAT";
	case TypeId::DOUBLE:
		return "DOUBLE";
	case TypeId::VARCHAR:
		return "VARCHAR";
	case TypeId::VARBINARY:
		return "VARBINARY";
	case TypeId::STRUCT:
		return "STRUCT<?>";
	case TypeId::LIST:
		return "LIST<?>";
	default:
		throw ConversionException("Invalid TypeId %d", type);
	}
}

idx_t GetTypeIdSize(TypeId type) {
	switch (type) {
	case TypeId::BOOL:
		return sizeof(bool);
	case TypeId::INT8:
		return sizeof(int8_t);
	case TypeId::INT16:
		return sizeof(int16_t);
	case TypeId::INT32:
		return sizeof(int32_t);
	case TypeId::INT64:
		return sizeof(int64_t);
	case TypeId::FLOAT:
		return sizeof(float);
	case TypeId::DOUBLE:
		return sizeof(double);
	case TypeId::HASH:
		return sizeof(hash_t);
	case TypeId::POINTER:
		return sizeof(uintptr_t);
	case TypeId::VARCHAR:
		return sizeof(string_t);
	case TypeId::STRUCT:
		return 0; // no own payload
	case TypeId::LIST:
		return 16; // offset + len
	case TypeId::VARBINARY:
		return sizeof(blob_t);
	default:
		throw ConversionException("Invalid TypeId %d", type);
	}
}

SQLType SQLTypeFromInternalType(TypeId type) {
	switch (type) {
	case TypeId::BOOL:
		return SQLType(SQLTypeId::BOOLEAN);
	case TypeId::INT8:
		return SQLType::TINYINT;
	case TypeId::INT16:
		return SQLType::SMALLINT;
	case TypeId::INT32:
		return SQLType::INTEGER;
	case TypeId::INT64:
		return SQLType::BIGINT;
	case TypeId::FLOAT:
		return SQLType::FLOAT;
	case TypeId::DOUBLE:
		return SQLType::DOUBLE;
	case TypeId::VARCHAR:
		return SQLType::VARCHAR;
	case TypeId::VARBINARY:
		return SQLType(SQLTypeId::VARBINARY);
	case TypeId::STRUCT:
		return SQLType(SQLTypeId::STRUCT); // TODO we do not know the child types here
	case TypeId::LIST:
		return SQLType(SQLTypeId::LIST);
	default:
		throw ConversionException("Invalid TypeId %d", type);
	}
}

bool TypeIsConstantSize(TypeId type) {
	return (type >= TypeId::BOOL && type <= TypeId::DOUBLE) ||
	       (type >= TypeId::FIXED_SIZE_BINARY && type <= TypeId::DECIMAL) || type == TypeId::HASH ||
	       type == TypeId::POINTER;
}
bool TypeIsIntegral(TypeId type) {
	return (type >= TypeId::UINT8 && type <= TypeId::INT64) || type == TypeId::HASH || type == TypeId::POINTER;
}
bool TypeIsNumeric(TypeId type) {
	return type >= TypeId::UINT8 && type <= TypeId::DOUBLE;
}
bool TypeIsInteger(TypeId type) {
	return type >= TypeId::UINT8 && type <= TypeId::INT64;
}

void SQLType::Serialize(Serializer &serializer) {
	serializer.Write(id);
	serializer.Write(width);
	serializer.Write(scale);
	serializer.WriteString(collation);
}

SQLType SQLType::Deserialize(Deserializer &source) {
	auto id = source.Read<SQLTypeId>();
	auto width = source.Read<uint16_t>();
	auto scale = source.Read<uint8_t>();
	auto collation = source.Read<string>();
	return SQLType(id, width, scale, collation);
}

string SQLTypeIdToString(SQLTypeId id) {
	switch (id) {
	case SQLTypeId::BOOLEAN:
		return "BOOLEAN";
	case SQLTypeId::TINYINT:
		return "TINYINT";
	case SQLTypeId::SMALLINT:
		return "SMALLINT";
	case SQLTypeId::INTEGER:
		return "INTEGER";
	case SQLTypeId::BIGINT:
		return "BIGINT";
	case SQLTypeId::DATE:
		return "DATE";
	case SQLTypeId::TIME:
		return "TIME";
	case SQLTypeId::TIMESTAMP:
		return "TIMESTAMP";
	case SQLTypeId::FLOAT:
		return "FLOAT";
	case SQLTypeId::DOUBLE:
		return "DOUBLE";
	case SQLTypeId::DECIMAL:
		return "DECIMAL";
	case SQLTypeId::VARCHAR:
		return "VARCHAR";
	case SQLTypeId::BLOB:
		return "BLOB";
	case SQLTypeId::VARBINARY:
		return "VARBINARY";
	case SQLTypeId::CHAR:
		return "CHAR";
	case SQLTypeId::SQLNULL:
		return "NULL";
	case SQLTypeId::ANY:
		return "ANY";
	case SQLTypeId::STRUCT:
		return "STRUCT<?>";
	case SQLTypeId::LIST:
		return "LIST<?>";
	default:
		return "INVALID";
	}
}

string SQLTypeToString(SQLType type) {
	// FIXME: display width/scale
	switch (type.id) {
	case SQLTypeId::STRUCT: {
		string ret = "STRUCT<";
		for (size_t i = 0; i < type.child_type.size(); i++) {
			ret += type.child_type[i].first + ": " + SQLTypeToString(type.child_type[i].second);
			if (i < type.child_type.size() - 1) {
				ret += ", ";
			}
		}
		ret += ">";
		return ret;
	}
	case SQLTypeId::LIST: {
		if (type.child_type.size() == 0) {
			return "LIST<?>";
		}
		if (type.child_type.size() != 1) {
			throw Exception("List needs a single child element");
		}
		return "LIST<" + SQLTypeToString(type.child_type[0].second) + ">";
	}
	default:
		return SQLTypeIdToString(type.id);
	}
}

SQLType TransformStringToSQLType(string str) {
	auto lower_str = StringUtil::Lower(str);
	// Transform column type
	if (lower_str == "int" || lower_str == "int4" || lower_str == "signed" || lower_str == "integer" ||
	    lower_str == "integral" || lower_str == "int32") {
		return SQLType::INTEGER;
	} else if (lower_str == "varchar" || lower_str == "bpchar" || lower_str == "text" || lower_str == "string" ||
	           lower_str == "char") {
		return SQLType::VARCHAR;
	} else if (lower_str == "bytea" || lower_str == "blob") {
		return SQLType::BLOB;
	} else if (lower_str == "int8" || lower_str == "bigint" || lower_str == "int64" || lower_str == "long") {
		return SQLType::BIGINT;
	} else if (lower_str == "int2" || lower_str == "smallint" || lower_str == "short" || lower_str == "int16") {
		return SQLType::SMALLINT;
	} else if (lower_str == "timestamp" || lower_str == "datetime") {
		return SQLType::TIMESTAMP;
	} else if (lower_str == "bool" || lower_str == "boolean" || lower_str == "logical") {
		return SQLType(SQLTypeId::BOOLEAN);
	} else if (lower_str == "real" || lower_str == "float4" || lower_str == "float") {
		return SQLType::FLOAT;
	} else if (lower_str == "double" || lower_str == "numeric" || lower_str == "float8") {
		return SQLType::DOUBLE;
	} else if (lower_str == "tinyint" || lower_str == "int1") {
		return SQLType::TINYINT;
	} else if (lower_str == "varbinary") {
		return SQLType(SQLTypeId::VARBINARY);
	} else if (lower_str == "date") {
		return SQLType::DATE;
	} else if (lower_str == "time") {
		return SQLType::TIME;
	} else {
		throw NotImplementedException("DataType %s not supported yet...\n", str.c_str());
	}
}

bool SQLType::IsIntegral() const {
	switch (id) {
	case SQLTypeId::TINYINT:
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
		return true;
	default:
		return false;
	}
}

bool SQLType::IsNumeric() const {
	switch (id) {
	case SQLTypeId::TINYINT:
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
	case SQLTypeId::FLOAT:
	case SQLTypeId::DOUBLE:
	case SQLTypeId::DECIMAL:
		return true;
	default:
		return false;
	}
}

bool SQLType::IsMoreGenericThan(SQLType &other) const {
	if (other.id == id) {
		return false;
	}

	if (other.id == SQLTypeId::SQLNULL) {
		return true;
	}

	switch (id) {
	case SQLTypeId::SMALLINT:
		switch (other.id) {
		case SQLTypeId::TINYINT:
			return true;
		default:
			return false;
		}
	case SQLTypeId::INTEGER:
		switch (other.id) {
		case SQLTypeId::TINYINT:
		case SQLTypeId::SMALLINT:
			return true;
		default:
			return false;
		}
	case SQLTypeId::BIGINT:
		switch (other.id) {
		case SQLTypeId::TINYINT:
		case SQLTypeId::SMALLINT:
		case SQLTypeId::INTEGER:
			return true;
		default:
			return false;
		}
	case SQLTypeId::DOUBLE:
		switch (other.id) {
		case SQLTypeId::TINYINT:
		case SQLTypeId::SMALLINT:
		case SQLTypeId::INTEGER:
		case SQLTypeId::BIGINT:
			return true;
		default:
			return false;
		}
		return false;
	case SQLTypeId::DATE:
		return false;
	case SQLTypeId::TIMESTAMP:
		switch (other.id) {
		case SQLTypeId::TIME:
		case SQLTypeId::DATE:
			return true;
		default:
			return false;
		}
	case SQLTypeId::VARCHAR:
		return true;
	default:
		return false;
	}

	return true;
}

TypeId GetInternalType(SQLType type) {
	switch (type.id) {
	case SQLTypeId::BOOLEAN:
		return TypeId::BOOL;
	case SQLTypeId::TINYINT:
		return TypeId::INT8;
	case SQLTypeId::SMALLINT:
		return TypeId::INT16;
	case SQLTypeId::SQLNULL:
	case SQLTypeId::DATE:
	case SQLTypeId::TIME:
	case SQLTypeId::INTEGER:
		return TypeId::INT32;
	case SQLTypeId::BIGINT:
	case SQLTypeId::TIMESTAMP:
		return TypeId::INT64;
	case SQLTypeId::FLOAT:
		return TypeId::FLOAT;
	case SQLTypeId::DOUBLE:
		return TypeId::DOUBLE;
	case SQLTypeId::DECIMAL:
		// FIXME: for now
		return TypeId::DOUBLE;
	case SQLTypeId::VARCHAR:
	case SQLTypeId::CHAR:
	case SQLTypeId::BLOB:
		return TypeId::VARCHAR;
	case SQLTypeId::VARBINARY:
		return TypeId::VARBINARY;
	case SQLTypeId::STRUCT:
		return TypeId::STRUCT;
	case SQLTypeId::LIST:
		return TypeId::LIST;
	case SQLTypeId::ANY:
		return TypeId::INVALID;
	default:
		throw ConversionException("Invalid SQLType %s", SQLTypeToString(type).c_str());
	}
}

SQLType MaxSQLType(SQLType left, SQLType right) {
	if (left.id < right.id) {
		return right;
	} else if (right.id < left.id) {
		return left;
	} else if (left.width > right.width || left.collation > right.collation) {
		return left;
	} else {
		return right;
	}
}

bool ApproxEqual(float ldecimal, float rdecimal) {
	float epsilon = fabs(rdecimal) * 0.01;
	return fabs(ldecimal - rdecimal) <= epsilon;
}

bool ApproxEqual(double ldecimal, double rdecimal) {
	double epsilon = fabs(rdecimal) * 0.01;
	return fabs(ldecimal - rdecimal) <= epsilon;
}

} // namespace duckdb
