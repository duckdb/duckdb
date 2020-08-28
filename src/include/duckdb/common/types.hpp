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
#include "duckdb/common/vector.hpp"

namespace duckdb {

class Serializer;
class Deserializer;

struct blob_t {
	data_ptr_t data;
	idx_t size;
};

struct interval_t {
	int32_t months;
	int32_t days;
	int64_t msecs;
};

struct hugeint_t {
public:
	uint64_t lower;
	int64_t upper;

public:
	hugeint_t() = default;
	hugeint_t(int64_t value);
	hugeint_t(const hugeint_t &rhs) = default;
	hugeint_t(hugeint_t &&rhs) = default;
	hugeint_t &operator=(const hugeint_t &rhs) = default;
	hugeint_t &operator=(hugeint_t &&rhs) = default;

	string ToString() const;

	// comparison operators
	bool operator==(const hugeint_t &rhs) const;
	bool operator!=(const hugeint_t &rhs) const;
	bool operator<=(const hugeint_t &rhs) const;
	bool operator<(const hugeint_t &rhs) const;
	bool operator>(const hugeint_t &rhs) const;
	bool operator>=(const hugeint_t &rhs) const;

	// arithmetic operators
	hugeint_t operator+(const hugeint_t &rhs) const;
	hugeint_t operator-(const hugeint_t &rhs) const;
	hugeint_t operator*(const hugeint_t &rhs) const;
	hugeint_t operator/(const hugeint_t &rhs) const;
	hugeint_t operator%(const hugeint_t &rhs) const;
	hugeint_t operator-() const;

	// bitwise operators
	hugeint_t operator>>(const hugeint_t &rhs) const;
	hugeint_t operator<<(const hugeint_t &rhs) const;
	hugeint_t operator&(const hugeint_t &rhs) const;
	hugeint_t operator|(const hugeint_t &rhs) const;
	hugeint_t operator^(const hugeint_t &rhs) const;
	hugeint_t operator~() const;

	// in-place operators
	hugeint_t &operator+=(const hugeint_t &rhs);
	hugeint_t &operator-=(const hugeint_t &rhs);
	hugeint_t &operator*=(const hugeint_t &rhs);
	hugeint_t &operator/=(const hugeint_t &rhs);
	hugeint_t &operator%=(const hugeint_t &rhs);
	hugeint_t &operator>>=(const hugeint_t &rhs);
	hugeint_t &operator<<=(const hugeint_t &rhs);
	hugeint_t &operator&=(const hugeint_t &rhs);
	hugeint_t &operator|=(const hugeint_t &rhs);
	hugeint_t &operator^=(const hugeint_t &rhs);
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
enum class PhysicalType : uint8_t {
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
	// DECIMAL = 22,

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
	INT128 = 204, // 128-bit integers

	INVALID = 255
};

//===--------------------------------------------------------------------===//
// SQL Types
//===--------------------------------------------------------------------===//
enum class LogicalTypeId : uint8_t {
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
	DECIMAL = 18,
	FLOAT = 19,
	DOUBLE = 20,
	CHAR = 21,
	VARCHAR = 22,
	VARBINARY = 23,
	BLOB = 24,
	INTERVAL = 25,

	HUGEINT = 50,
	POINTER = 51,
	HASH = 52,

	STRUCT = 100,
	LIST = 101
};

struct LogicalType {
	LogicalType();
	LogicalType(LogicalTypeId id);
	LogicalType(LogicalTypeId id, string collation);
	LogicalType(LogicalTypeId id, uint8_t width, uint8_t scale);
	LogicalType(LogicalTypeId id, child_list_t<LogicalType> child_types);
	LogicalType(LogicalTypeId id, uint8_t width, uint8_t scale, string collation,
	            child_list_t<LogicalType> child_types);

	LogicalTypeId id() const {
		return id_;
	}
	uint8_t width() const {
		return width_;
	}
	uint8_t scale() const {
		return scale_;
	}
	const string &collation() const {
		return collation_;
	}
	const child_list_t<LogicalType> &child_types() const {
		return child_types_;
	}
	PhysicalType InternalType() const {
		return physical_type_;
	}

	bool operator==(const LogicalType &rhs) const {
		return id_ == rhs.id_ && width_ == rhs.width_ && scale_ == rhs.scale_;
	}
	bool operator!=(const LogicalType &rhs) const {
		return !(*this == rhs);
	}

	//! Serializes a LogicalType to a stand-alone binary blob
	void Serialize(Serializer &serializer);
	//! Deserializes a blob back into an LogicalType
	static LogicalType Deserialize(Deserializer &source);

	string ToString() const;
	bool IsIntegral() const;
	bool IsNumeric() const;
	bool IsMoreGenericThan(LogicalType &other) const;
	hash_t Hash() const;

	static LogicalType MaxLogicalType(LogicalType left, LogicalType right);

	//! Gets the decimal properties of a numeric type. Fails if the type is not numeric.
	bool GetDecimalProperties(int &width, int &scale) const;

	void Verify() const;

private:
	LogicalTypeId id_;
	uint8_t width_;
	uint8_t scale_;
	string collation_;

	child_list_t<LogicalType> child_types_;
	PhysicalType physical_type_;

private:
	PhysicalType GetInternalType();

public:
	static const LogicalType SQLNULL;
	static const LogicalType BOOLEAN;
	static const LogicalType TINYINT;
	static const LogicalType SMALLINT;
	static const LogicalType INTEGER;
	static const LogicalType BIGINT;
	static const LogicalType FLOAT;
	static const LogicalType DOUBLE;
	static const LogicalType DECIMAL;
	static const LogicalType DATE;
	static const LogicalType TIMESTAMP;
	static const LogicalType TIME;
	static const LogicalType VARCHAR;
	static const LogicalType VARBINARY;
	static const LogicalType STRUCT;
	static const LogicalType LIST;
	static const LogicalType ANY;
	static const LogicalType BLOB;
	static const LogicalType INTERVAL;
	static const LogicalType HUGEINT;
	static const LogicalType HASH;
	static const LogicalType POINTER;
	static const LogicalType INVALID;

	//! A list of all NUMERIC types (integral and floating point types)
	static const vector<LogicalType> NUMERIC;
	//! A list of all INTEGRAL types
	static const vector<LogicalType> INTEGRAL;
	//! A list of ALL SQL types
	static const vector<LogicalType> ALL_TYPES;
};

string LogicalTypeIdToString(LogicalTypeId type);

LogicalType TransformStringToLogicalType(string str);

//! Returns the PhysicalType for the given type
template <class T> PhysicalType GetTypeId() {
	if (std::is_same<T, bool>()) {
		return PhysicalType::BOOL;
	} else if (std::is_same<T, int8_t>()) {
		return PhysicalType::INT8;
	} else if (std::is_same<T, int16_t>()) {
		return PhysicalType::INT16;
	} else if (std::is_same<T, int32_t>()) {
		return PhysicalType::INT32;
	} else if (std::is_same<T, int64_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<T, hugeint_t>()) {
		return PhysicalType::INT128;
	} else if (std::is_same<T, uint64_t>()) {
		return PhysicalType::HASH;
	} else if (std::is_same<T, uintptr_t>()) {
		return PhysicalType::POINTER;
	} else if (std::is_same<T, float>()) {
		return PhysicalType::FLOAT;
	} else if (std::is_same<T, double>()) {
		return PhysicalType::DOUBLE;
	} else if (std::is_same<T, const char *>() || std::is_same<T, char *>()) {
		return PhysicalType::VARCHAR;
	} else if (std::is_same<T, interval_t>()) {
		return PhysicalType::INTERVAL;
	} else {
		return PhysicalType::INVALID;
	}
}

template <class T> bool IsValidType() {
	return GetTypeId<T>() != PhysicalType::INVALID;
}

//! The PhysicalType used by the row identifiers column
extern const LogicalType LOGICAL_ROW_TYPE;
extern const PhysicalType ROW_TYPE;

string TypeIdToString(PhysicalType type);
idx_t GetTypeIdSize(PhysicalType type);
bool TypeIsConstantSize(PhysicalType type);
bool TypeIsIntegral(PhysicalType type);
bool TypeIsNumeric(PhysicalType type);
bool TypeIsInteger(PhysicalType type);

template <class T> bool IsIntegerType() {
	return TypeIsIntegral(GetTypeId<T>());
}

bool ApproxEqual(float l, float r);
bool ApproxEqual(double l, double r);

} // namespace duckdb
