#include "common/types.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"

using namespace std;

namespace duckdb {

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
	case TypeId::POINTER:
		return "POINTER";
	case TypeId::FLOAT:
		return "FLOAT";
	case TypeId::DOUBLE:
		return "DOUBLE";
	case TypeId::VARCHAR:
		return "VARCHAR";
	default:
		assert(type == TypeId::VARBINARY);
		return "VARBINARY";
	}
}

size_t GetTypeIdSize(TypeId type) {
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
	case TypeId::POINTER:
		return sizeof(uint64_t);
	case TypeId::VARCHAR:
		return sizeof(void *);
	default:
		assert(type == TypeId::VARBINARY);
		return sizeof(blob_t);
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
	case SQLTypeId::TIMESTAMP:
		return "TIMESTAMP";
	case SQLTypeId::REAL:
		return "REAL";
	case SQLTypeId::DOUBLE:
		return "DOUBLE";
	case SQLTypeId::DECIMAL:
		return "DECIMAL";
	case SQLTypeId::VARCHAR:
		return "VARCHAR";
	case SQLTypeId::CHAR:
		return "CHAR";
	default:
		return "VARBINARY";
	}
}

string SQLTypeToString(SQLType type) {
	// FIXME: display width/scale
	return SQLTypeIdToString(type.id);
}

bool IsNumericType(SQLTypeId type) {
	switch (type) {
	case SQLTypeId::TINYINT:
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
	case SQLTypeId::REAL:
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
	case SQLTypeId::DATE:
	case SQLTypeId::INTEGER:
		return TypeId::INTEGER;
	case SQLTypeId::BIGINT:
	case SQLTypeId::TIMESTAMP:
		return TypeId::BIGINT;
	case SQLTypeId::REAL:
		return TypeId::FLOAT;
	case SQLTypeId::DOUBLE:
		return TypeId::DOUBLE;
	case SQLTypeId::DECIMAL:
		// FIXME: for now
		return TypeId::DOUBLE;
	case SQLTypeId::VARCHAR:
	case SQLTypeId::CHAR:
		return TypeId::VARCHAR;
	default:
		assert(type.id == SQLTypeId::VARBINARY);
		return TypeId::VARBINARY;
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

} // namespace duckdb
