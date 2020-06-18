//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/constants.hpp"

#include <type_traits>

namespace duckdb {

class Serializer;
class Deserializer;

struct blob_t {
	data_ptr_t data;
	idx_t size;
};

struct string_t;

template <class T> using child_list_t = std::vector<std::pair<std::string, T>>;
template <class T> using buffer_ptr = std::shared_ptr<T>;

template <class T, typename... Args> buffer_ptr<T> make_buffer(Args &&... args) {
	return std::make_shared<T>(std::forward<Args>(args)...);
}

struct list_entry_t {
	list_entry_t() = default;
	list_entry_t(uint64_t offset, uint64_t length) : offset(offset), length(length) {
	}

	uint64_t offset;
	uint64_t length;
};

//===--------------------------------------------------------------------===//
// Internal Types
//===--------------------------------------------------------------------===//

// taken from arrow's type.h
enum class TypeId : uint8_t {
	/// A NULL type having no physical storage
	NA = 0,

	/// Boolean as 1 bit, LSB bit-packed ordering
	BOOL = 1,

	/// Unsigned 8-bit little-endian integer
	UINT8 = 2,

	/// Signed 8-bit little-endian integer
	INT8 = 3,

	/// Unsigned 16-bit little-endian integer
	UINT16 = 4,

	/// Signed 16-bit little-endian integer
	INT16 = 5,

	/// Unsigned 32-bit little-endian integer
	UINT32 = 6,

	/// Signed 32-bit little-endian integer
	INT32 = 7,

	/// Unsigned 64-bit little-endian integer
	UINT64 = 8,

	/// Signed 64-bit little-endian integer
	INT64 = 9,

	/// 2-byte floating point value
	HALF_FLOAT = 10,

	/// 4-byte floating point value
	FLOAT = 11,

	/// 8-byte floating point value
	DOUBLE = 12,

	/// UTF8 variable-length string as List<Char>
	STRING = 13,

	/// Variable-length bytes (no guarantee of UTF8-ness)
	BINARY = 14,

	/// Fixed-size binary. Each value occupies the same number of bytes
	FIXED_SIZE_BINARY = 15,

	/// int32_t days since the UNIX epoch
	DATE32 = 16,

	/// int64_t milliseconds since the UNIX epoch
	DATE64 = 17,

	/// Exact timestamp encoded with int64 since UNIX epoch
	/// Default unit millisecond
	TIMESTAMP = 18,

	/// Time as signed 32-bit integer, representing either seconds or
	/// milliseconds since midnight
	TIME32 = 19,

	/// Time as signed 64-bit integer, representing either microseconds or
	/// nanoseconds since midnight
	TIME64 = 20,

	/// YEAR_MONTH or DAY_TIME interval in SQL style
	INTERVAL = 21,

	/// Precision- and scale-based decimal type. Storage type depends on the
	/// parameters.
	DECIMAL = 22,

	/// A list of some logical data type
	LIST = 23,

	/// Struct of logical types
	STRUCT = 24,

	/// Unions of logical types
	UNION = 25,

	/// Dictionary-encoded type, also called "categorical" or "factor"
	/// in other programming languages. Holds the dictionary value
	/// type but not the dictionary itself, which is part of the
	/// ArrayData struct
	DICTIONARY = 26,

	/// Map, a repeated struct logical type
	MAP = 27,

	/// Custom data type, implemented by user
	EXTENSION = 28,

	/// Fixed size list of some logical type
	FIXED_SIZE_LIST = 29,

	/// Measure of elapsed time in either seconds, milliseconds, microseconds
	/// or nanoseconds.
	DURATION = 30,

	/// Like STRING, but with 64-bit offsets
	LARGE_STRING = 31,

	/// Like BINARY, but with 64-bit offsets
	LARGE_BINARY = 32,

	/// Like LIST, but with 64-bit offsets
	LARGE_LIST = 33,

	// DuckDB Extensions
	VARCHAR = 200, // our own string representation, different from STRING and LARGE_STRING above
	VARBINARY = 201,
	POINTER = 202,
	HASH = 203,

	INVALID = 255
};

//===--------------------------------------------------------------------===//
// SQL Types
//===--------------------------------------------------------------------===//
enum class SQLTypeId : uint8_t {
	INVALID = 0,
	SQLNULL = 1, /* NULL type, used for constant NULL */
	UNKNOWN = 2, /* unknown type, used for parameter expressions */
	ANY = 3,     /* ANY type, used for functions that accept any type as parameter */

	BOOLEAN = 10,
	TINYINT = 11,
	SMALLINT = 12,
	INTEGER = 13,
	BIGINT = 14,
	DATE = 15,
	TIME = 16,
	TIMESTAMP = 17,
	FLOAT = 18,
	DOUBLE = 19,
	DECIMAL = 20,
	CHAR = 21,
	VARCHAR = 22,
	VARBINARY = 23,
	BLOB = 24,

	STRUCT = 100,
	LIST = 101
};

struct SQLType {
	SQLTypeId id;
	uint16_t width;
	uint8_t scale;
	string collation;

	// TODO serialize this
	child_list_t<SQLType> child_type;

	SQLType(SQLTypeId id = SQLTypeId::INVALID, uint16_t width = 0, uint8_t scale = 0, string collation = string())
	    : id(id), width(width), scale(scale), collation(move(collation)) {
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

	bool IsIntegral() const;
	bool IsNumeric() const;
	bool IsMoreGenericThan(SQLType &other) const;

public:
	static const SQLType SQLNULL;
	static const SQLType BOOLEAN;
	static const SQLType TINYINT;
	static const SQLType SMALLINT;
	static const SQLType INTEGER;
	static const SQLType BIGINT;
	static const SQLType FLOAT;
	static const SQLType DOUBLE;
	static const SQLType DATE;
	static const SQLType TIMESTAMP;
	static const SQLType TIME;
	static const SQLType VARCHAR;
	static const SQLType VARBINARY;
	static const SQLType STRUCT;
	static const SQLType LIST;
	static const SQLType ANY;
	static const SQLType BLOB;

	//! A list of all NUMERIC types (integral and floating point types)
	static const vector<SQLType> NUMERIC;
	//! A list of all INTEGRAL types
	static const vector<SQLType> INTEGRAL;
	//! A list of ALL SQL types
	static const vector<SQLType> ALL_TYPES;
};

string SQLTypeIdToString(SQLTypeId type);
string SQLTypeToString(SQLType type);

SQLType MaxSQLType(SQLType left, SQLType right);
SQLType TransformStringToSQLType(string str);

//! Gets the internal type associated with the given SQL type
TypeId GetInternalType(SQLType type);
//! Returns the "simplest" SQL type corresponding to the given type id (e.g. TypeId::INT32 -> SQLTypeId::INTEGER)
SQLType SQLTypeFromInternalType(TypeId type);

//! Returns the TypeId for the given type
template <class T> TypeId GetTypeId() {
	if (std::is_same<T, bool>()) {
		return TypeId::BOOL;
	} else if (std::is_same<T, int8_t>()) {
		return TypeId::INT8;
	} else if (std::is_same<T, int16_t>()) {
		return TypeId::INT16;
	} else if (std::is_same<T, int32_t>()) {
		return TypeId::INT32;
	} else if (std::is_same<T, int64_t>()) {
		return TypeId::INT64;
	} else if (std::is_same<T, uint64_t>()) {
		return TypeId::HASH;
	} else if (std::is_same<T, uintptr_t>()) {
		return TypeId::POINTER;
	} else if (std::is_same<T, float>()) {
		return TypeId::FLOAT;
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
idx_t GetTypeIdSize(TypeId type);
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
