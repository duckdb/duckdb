#include "duckdb/common/types.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"

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

const vector<SQLType> SQLType::NUMERIC = {
    SQLType::TINYINT, SQLType::SMALLINT, SQLType::INTEGER,           SQLType::BIGINT,
    SQLType::FLOAT,   SQLType::DOUBLE,   SQLType(SQLTypeId::DECIMAL)};

const vector<SQLType> SQLType::INTEGRAL = {SQLType::TINYINT, SQLType::SMALLINT, SQLType::INTEGER, SQLType::BIGINT};

const vector<SQLType> SQLType::ALL_TYPES = {
    SQLType::BOOLEAN, SQLType::TINYINT,   SQLType::SMALLINT, SQLType::INTEGER, SQLType::BIGINT,
    SQLType::DATE,    SQLType::TIMESTAMP, SQLType::DOUBLE,   SQLType::FLOAT,   SQLType(SQLTypeId::DECIMAL),
    SQLType::VARCHAR};

const TypeId ROW_TYPE = TypeId::BIGINT;

string TypeIdToString(TypeId type) {
	switch (type) {
	case TypeId::BOOLEAN:
		return "BOOLEAN";
	case TypeId::TINYINT:
		return "TINYINT";
	case TypeId::SMALLINT:
		return "SMALLINT";
	case TypeId::INTEGER:
		return "INTEGER";
	case TypeId::BIGINT:
		return "BIGINT";
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
	default:
		throw ConversionException("Invalid TypeId %d", type);
	}
}

index_t GetTypeIdSize(TypeId type) {
	switch (type) {
	case TypeId::BOOLEAN:
		return sizeof(bool);
	case TypeId::TINYINT:
		return sizeof(int8_t);
	case TypeId::SMALLINT:
		return sizeof(int16_t);
	case TypeId::INTEGER:
		return sizeof(int32_t);
	case TypeId::BIGINT:
		return sizeof(int64_t);
	case TypeId::FLOAT:
		return sizeof(float);
	case TypeId::DOUBLE:
		return sizeof(double);
	case TypeId::HASH:
		return sizeof(uint64_t);
	case TypeId::POINTER:
		return sizeof(uintptr_t);
	case TypeId::VARCHAR:
		return sizeof(void *);
	case TypeId::VARBINARY:
		return sizeof(blob_t);
	default:
		throw ConversionException("Invalid TypeId %d", type);
	}
}

SQLType SQLTypeFromInternalType(TypeId type) {
	switch (type) {
	case TypeId::BOOLEAN:
		return SQLType(SQLTypeId::BOOLEAN);
	case TypeId::TINYINT:
		return SQLType::TINYINT;
	case TypeId::SMALLINT:
		return SQLType::SMALLINT;
	case TypeId::INTEGER:
		return SQLType::INTEGER;
	case TypeId::BIGINT:
		return SQLType::BIGINT;
	case TypeId::FLOAT:
		return SQLType::FLOAT;
	case TypeId::DOUBLE:
		return SQLType::DOUBLE;
	case TypeId::VARCHAR:
		return SQLType::VARCHAR;
	case TypeId::VARBINARY:
		return SQLType(SQLTypeId::VARBINARY);
	default:
		throw ConversionException("Invalid TypeId %d", type);
	}
}

bool TypeIsConstantSize(TypeId type) {
	return type < TypeId::VARCHAR;
}
bool TypeIsIntegral(TypeId type) {
	return type >= TypeId::TINYINT && type <= TypeId::POINTER;
}
bool TypeIsNumeric(TypeId type) {
	return type >= TypeId::TINYINT && type <= TypeId::DOUBLE;
}
bool TypeIsInteger(TypeId type) {
	return type >= TypeId::TINYINT && type <= TypeId::BIGINT;
}

void SQLType::Serialize(Serializer &serializer) {
	serializer.Write(id);
	serializer.Write(width);
	serializer.Write(scale);
}

SQLType SQLType::Deserialize(Deserializer &source) {
	auto id = source.Read<SQLTypeId>();
	auto width = source.Read<uint16_t>();
	auto scale = source.Read<uint8_t>();
	return SQLType(id, width, scale);
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
	case SQLTypeId::VARBINARY:
		return "VARBINARY";
	case SQLTypeId::CHAR:
		return "CHAR";
	case SQLTypeId::SQLNULL:
		return "NULL";
	case SQLTypeId::ANY:
		return "ANY";
	default:
		return "INVALID";
	}
}

string SQLTypeToString(SQLType type) {
	// FIXME: display width/scale
	return SQLTypeIdToString(type.id);
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

TypeId GetInternalType(SQLType type) {
	switch (type.id) {
	case SQLTypeId::BOOLEAN:
		return TypeId::BOOLEAN;
	case SQLTypeId::TINYINT:
		return TypeId::TINYINT;
	case SQLTypeId::SMALLINT:
		return TypeId::SMALLINT;
	case SQLTypeId::SQLNULL:
	case SQLTypeId::DATE:
	case SQLTypeId::TIME:
	case SQLTypeId::INTEGER:
		return TypeId::INTEGER;
	case SQLTypeId::BIGINT:
	case SQLTypeId::TIMESTAMP:
		return TypeId::BIGINT;
	case SQLTypeId::FLOAT:
		return TypeId::FLOAT;
	case SQLTypeId::DOUBLE:
		return TypeId::DOUBLE;
	case SQLTypeId::DECIMAL:
		// FIXME: for now
		return TypeId::DOUBLE;
	case SQLTypeId::VARCHAR:
	case SQLTypeId::CHAR:
		return TypeId::VARCHAR;
	case SQLTypeId::VARBINARY:
		return TypeId::VARBINARY;
	default:
		throw ConversionException("Invalid SQLType %d", type);
	}
}

SQLType MaxSQLType(SQLType left, SQLType right) {
	if (left.id < right.id) {
		return right;
	} else if (right.id < left.id) {
		return left;
	} else if (left.width > right.width) {
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
