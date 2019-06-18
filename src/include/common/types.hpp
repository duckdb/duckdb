//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/assert.hpp"
#include "common/constants.hpp"

#include <type_traits>

namespace duckdb {

class Serializer;
class Deserializer;

struct blob_t {
	data_ptr_t data;
	index_t size;
};

//===--------------------------------------------------------------------===//
// Internal Types
//===--------------------------------------------------------------------===//
enum class TypeId : uint8_t {
	INVALID = 0,
	BOOLEAN = 1,   /* bool */
	TINYINT = 2,   /* int8_t */
	SMALLINT = 3,  /* int16_t */
	INTEGER = 4,   /* int32_t */
	BIGINT = 5,    /* int64_t */
	HASH = 6,      /* uint64_t */
	POINTER = 7,   /* uintptr_t */
	FLOAT = 8,     /* float32_t */
	DOUBLE = 9,    /* float64_t */
	VARCHAR = 10,  /* char*, representing a null-terminated UTF-8 string */
	VARBINARY = 11 /* blob_t, representing arbitrary bytes */
};

//===--------------------------------------------------------------------===//
// SQL Types
//===--------------------------------------------------------------------===//
enum class SQLTypeId : uint8_t {
	INVALID = 0,
	SQLNULL = 1,
	BOOLEAN = 2,
	TINYINT = 3,
	SMALLINT = 4,
	INTEGER = 5,
	BIGINT = 6,
	DATE = 7,
	TIMESTAMP = 8,
	REAL = 9,
	DOUBLE = 10,
	FLOAT = 11,
	DECIMAL = 12,
	CHAR = 13,
	VARCHAR = 14,
	VARBINARY = 15
};

struct SQLType {
	SQLTypeId id;
	uint16_t width;
	uint8_t scale;

	SQLType(SQLTypeId id = SQLTypeId::INVALID, uint16_t width = 0, uint8_t scale = 0)
	    : id(id), width(width), scale(scale) {
	}

	bool operator==(const SQLType &rhs) const {
		return id == rhs.id && width == rhs.width && scale == rhs.scale;
	}
	bool operator!=(const SQLType &rhs) const {
		return !(*this == rhs);
	}

	//! Serializes a SQLType to a stand-alone binary blob
	void Serialize(Serializer &serializer);
	//! Deserializes a blob back into an SQLType
	static SQLType Deserialize(Deserializer &source);
};

bool IsNumericType(SQLTypeId type);

string SQLTypeIdToString(SQLTypeId type);
string SQLTypeToString(SQLType type);

SQLType MaxSQLType(SQLType left, SQLType right);

//! Gets the internal type associated with the given SQL type
TypeId GetInternalType(SQLType type);
//! Returns the "simplest" SQL type corresponding to the given type id (e.g. TypeId::INTEGER -> SQLTypeId::INTEGER)
SQLType SQLTypeFromInternalType(TypeId type);

//! Returns the TypeId for the given type
template <class T> TypeId GetTypeId() {
	if (std::is_same<T, bool>()) {
		return TypeId::BOOLEAN;
	} else if (std::is_same<T, int8_t>()) {
		return TypeId::TINYINT;
	} else if (std::is_same<T, int16_t>()) {
		return TypeId::SMALLINT;
	} else if (std::is_same<T, int32_t>()) {
		return TypeId::INTEGER;
	} else if (std::is_same<T, int64_t>()) {
		return TypeId::BIGINT;
	} else if (std::is_same<T, uint64_t>()) {
		return TypeId::HASH;
	} else if (std::is_same<T, uintptr_t>()) {
		return TypeId::POINTER;
	} else if (std::is_same<T, double>()) {
		return TypeId::DOUBLE;
	} else if (std::is_same<T, const char *>() || std::is_same<T, char *>()) {
		return TypeId::VARCHAR;
	} else {
		return TypeId::INVALID;
	}
}

template <class T> bool IsValidType() {
	return GetTypeId<T>() != TypeId::INVALID;
}

//! The TypeId used by the row identifiers column
extern const TypeId ROW_TYPE;

string TypeIdToString(TypeId type);
string TypeIdToString(TypeId type);
index_t GetTypeIdSize(TypeId type);
bool TypeIsConstantSize(TypeId type);
bool TypeIsIntegral(TypeId type);
bool TypeIsNumeric(TypeId type);
bool TypeIsInteger(TypeId type);

template <class T> bool IsIntegerType() {
	return TypeIsIntegral(GetTypeId<T>());
}

bool ApproxEqual(float l, float r);
bool ApproxEqual(double l, double r);

}; // namespace duckdb
