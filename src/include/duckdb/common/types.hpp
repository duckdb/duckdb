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
#include "duckdb/common/single_thread_ptr.hpp"
#include "duckdb/common/vector.hpp"

#include <limits>

namespace duckdb {

class Serializer;
class Deserializer;
class Value;
class TypeCatalogEntry;
class Vector;
class ClientContext;

struct hugeint_t {
public:
	uint64_t lower;
	int64_t upper;

public:
	DUCKDB_API hugeint_t() = default;
	DUCKDB_API hugeint_t(int64_t value); // NOLINT: Allow implicit conversion from `int64_t`
	DUCKDB_API hugeint_t(const hugeint_t &rhs) = default;
	DUCKDB_API hugeint_t(hugeint_t &&rhs) = default;
	DUCKDB_API hugeint_t &operator=(const hugeint_t &rhs) = default;
	DUCKDB_API hugeint_t &operator=(hugeint_t &&rhs) = default;

	DUCKDB_API string ToString() const;

	// comparison operators
	DUCKDB_API bool operator==(const hugeint_t &rhs) const;
	DUCKDB_API bool operator!=(const hugeint_t &rhs) const;
	DUCKDB_API bool operator<=(const hugeint_t &rhs) const;
	DUCKDB_API bool operator<(const hugeint_t &rhs) const;
	DUCKDB_API bool operator>(const hugeint_t &rhs) const;
	DUCKDB_API bool operator>=(const hugeint_t &rhs) const;

	// arithmetic operators
	DUCKDB_API hugeint_t operator+(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator-(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator*(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator/(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator%(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator-() const;

	// bitwise operators
	DUCKDB_API hugeint_t operator>>(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator<<(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator&(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator|(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator^(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator~() const;

	// in-place operators
	DUCKDB_API hugeint_t &operator+=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator-=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator*=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator/=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator%=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator>>=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator<<=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator&=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator|=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator^=(const hugeint_t &rhs);
};

struct string_t;

template <class T>
using child_list_t = std::vector<std::pair<std::string, T>>;
//! FIXME: this should be a single_thread_ptr
template <class T>
using buffer_ptr = shared_ptr<T>;

template <class T, typename... Args>
buffer_ptr<T> make_buffer(Args &&...args) {
	return make_shared<T>(std::forward<Args>(args)...);
}

struct list_entry_t {
	list_entry_t() = default;
	list_entry_t(uint64_t offset, uint64_t length) : offset(offset), length(length) {
	}

	uint64_t offset;
	uint64_t length;
};

using union_tag_t = uint8_t; 

//===--------------------------------------------------------------------===//
// Internal Types
//===--------------------------------------------------------------------===//

// taken from arrow's type.h
enum class PhysicalType : uint8_t {
	///// A NULL type having no physical storage
	//NA = 0,

	/// Boolean as 8 bit "bool" value
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

	///// 2-byte floating point value
	//HALF_FLOAT = 10,

	/// 4-byte floating point value
	FLOAT = 11,

	/// 8-byte floating point value
	DOUBLE = 12,

	///// UTF8 variable-length string as List<Char>
	//STRING = 13,

	///// Variable-length bytes (no guarantee of UTF8-ness)
	//BINARY = 14,

	///// Fixed-size binary. Each value occupies the same number of bytes
	//FIXED_SIZE_BINARY = 15,

	///// int32_t days since the UNIX epoch
	//DATE32 = 16,

	///// int64_t milliseconds since the UNIX epoch
	//DATE64 = 17,

	///// Exact timestamp encoded with int64 since UNIX epoch
	///// Default unit millisecond
	//TIMESTAMP = 18,

	///// Time as signed 32-bit integer, representing either seconds or
	///// milliseconds since midnight
	//TIME32 = 19,

	///// Time as signed 64-bit integer, representing either microseconds or
	///// nanoseconds since midnight
	//TIME64 = 20,

	/// YEAR_MONTH or DAY_TIME interval in SQL style
	INTERVAL = 21,

	/// Precision- and scale-based decimal type. Storage type depends on the
	/// parameters.
	// DECIMAL = 22,

	/// A list of some logical data type
	LIST = 23,

	/// Struct of logical types
	STRUCT = 24,

	///// Unions of logical types
	//UNION = 25,

	///// Dictionary-encoded type, also called "categorical" or "factor"
	///// in other programming languages. Holds the dictionary value
	///// type but not the dictionary itself, which is part of the
	///// ArrayData struct
	//DICTIONARY = 26,

	/// Map, a repeated struct logical type
	MAP = 27,

	///// Custom data type, implemented by user
	//EXTENSION = 28,

	///// Fixed size list of some logical type
	//FIXED_SIZE_LIST = 29,

	///// Measure of elapsed time in either seconds, milliseconds, microseconds
	///// or nanoseconds.
	//DURATION = 30,

	///// Like STRING, but with 64-bit offsets
	//LARGE_STRING = 31,

	///// Like BINARY, but with 64-bit offsets
	//LARGE_BINARY = 32,

	///// Like LIST, but with 64-bit offsets
	//LARGE_LIST = 33,

	/// DuckDB Extensions
	VARCHAR = 200, // our own string representation, different from STRING and LARGE_STRING above
	INT128 = 204, // 128-bit integers
	UNKNOWN = 205, // Unknown physical type of user defined types
	/// Boolean as 1 bit, LSB bit-packed ordering
	BIT = 206,

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
	USER = 4, /* A User Defined Type (e.g., ENUMs before the binder) */
	BOOLEAN = 10,
	TINYINT = 11,
	SMALLINT = 12,
	INTEGER = 13,
	BIGINT = 14,
	DATE = 15,
	TIME = 16,
	TIMESTAMP_SEC = 17,
	TIMESTAMP_MS = 18,
	TIMESTAMP = 19, //! us
	TIMESTAMP_NS = 20,
	DECIMAL = 21,
	FLOAT = 22,
	DOUBLE = 23,
	CHAR = 24,
	VARCHAR = 25,
	BLOB = 26,
	INTERVAL = 27,
	UTINYINT = 28,
	USMALLINT = 29,
	UINTEGER = 30,
	UBIGINT = 31,
	TIMESTAMP_TZ = 32,
	TIME_TZ = 34,

	HUGEINT = 50,
	POINTER = 51,
	// HASH = 52, // deprecated, uses UBIGINT instead
	VALIDITY = 53,
	UUID = 54,

	STRUCT = 100,
	LIST = 101,
	MAP = 102,
	TABLE = 103,
	ENUM = 104,
	AGGREGATE_STATE = 105,
	LAMBDA = 106,
	UNION = 107
};

struct ExtraTypeInfo;


struct aggregate_state_t;

struct LogicalType {
	DUCKDB_API LogicalType();
	DUCKDB_API LogicalType(LogicalTypeId id); // NOLINT: Allow implicit conversion from `LogicalTypeId`
	DUCKDB_API LogicalType(LogicalTypeId id, shared_ptr<ExtraTypeInfo> type_info);
	DUCKDB_API LogicalType(const LogicalType &other);
	DUCKDB_API LogicalType(LogicalType &&other) noexcept;

	DUCKDB_API ~LogicalType();

	inline LogicalTypeId id() const {
		return id_;
	}
	inline PhysicalType InternalType() const {
		return physical_type_;
	}
	inline const ExtraTypeInfo *AuxInfo() const {
		return type_info_.get();
	}
	inline void CopyAuxInfo(const LogicalType& other) {
		type_info_ = other.type_info_;
	}
	bool EqualTypeInfo(const LogicalType& rhs) const;

	// copy assignment
	inline LogicalType& operator=(const LogicalType &other) {
		id_ = other.id_;
		physical_type_ = other.physical_type_;
		type_info_ = other.type_info_;
		return *this;
	}
	// move assignment
	inline LogicalType& operator=(LogicalType&& other) noexcept {
		id_ = other.id_;
		physical_type_ = other.physical_type_;
		type_info_ = move(other.type_info_);
		return *this;
	}

	DUCKDB_API bool operator==(const LogicalType &rhs) const;
	inline bool operator!=(const LogicalType &rhs) const {
		return !(*this == rhs);
	}

	//! Serializes a LogicalType to a stand-alone binary blob
	DUCKDB_API void Serialize(Serializer &serializer) const;
	//! Deserializes a blob back into an LogicalType
	DUCKDB_API static LogicalType Deserialize(Deserializer &source);

	DUCKDB_API static bool TypeIsTimestamp(LogicalTypeId id) {
		return (id == LogicalTypeId::TIMESTAMP ||
				id == LogicalTypeId::TIMESTAMP_MS ||
				id == LogicalTypeId::TIMESTAMP_NS ||
				id == LogicalTypeId::TIMESTAMP_SEC ||
				id == LogicalTypeId::TIMESTAMP_TZ);
	}
	DUCKDB_API static bool TypeIsTimestamp(const LogicalType& type) {
		return TypeIsTimestamp(type.id());
	}
	DUCKDB_API string ToString() const;
	DUCKDB_API bool IsIntegral() const;
	DUCKDB_API bool IsNumeric() const;
	DUCKDB_API hash_t Hash() const;
	DUCKDB_API void SetAlias(string alias);
	DUCKDB_API bool HasAlias() const;
	DUCKDB_API string GetAlias() const;

	DUCKDB_API static LogicalType MaxLogicalType(const LogicalType &left, const LogicalType &right);
	DUCKDB_API static void SetCatalog(LogicalType &type, TypeCatalogEntry* catalog_entry);
	DUCKDB_API static TypeCatalogEntry* GetCatalog(const LogicalType &type);

	//! Gets the decimal properties of a numeric type. Fails if the type is not numeric.
	DUCKDB_API bool GetDecimalProperties(uint8_t &width, uint8_t &scale) const;

	DUCKDB_API void Verify() const;

	DUCKDB_API bool IsValid() const;

private:
	LogicalTypeId id_;
	PhysicalType physical_type_;
	shared_ptr<ExtraTypeInfo> type_info_;

private:
	PhysicalType GetInternalType();

public:
	static constexpr const LogicalTypeId SQLNULL = LogicalTypeId::SQLNULL;
	static constexpr const LogicalTypeId UNKNOWN = LogicalTypeId::UNKNOWN;
	static constexpr const LogicalTypeId BOOLEAN = LogicalTypeId::BOOLEAN;
	static constexpr const LogicalTypeId TINYINT = LogicalTypeId::TINYINT;
	static constexpr const LogicalTypeId UTINYINT = LogicalTypeId::UTINYINT;
	static constexpr const LogicalTypeId SMALLINT = LogicalTypeId::SMALLINT;
	static constexpr const LogicalTypeId USMALLINT = LogicalTypeId::USMALLINT;
	static constexpr const LogicalTypeId INTEGER = LogicalTypeId::INTEGER;
	static constexpr const LogicalTypeId UINTEGER = LogicalTypeId::UINTEGER;
	static constexpr const LogicalTypeId BIGINT = LogicalTypeId::BIGINT;
	static constexpr const LogicalTypeId UBIGINT = LogicalTypeId::UBIGINT;
	static constexpr const LogicalTypeId FLOAT = LogicalTypeId::FLOAT;
	static constexpr const LogicalTypeId DOUBLE = LogicalTypeId::DOUBLE;
	static constexpr const LogicalTypeId DATE = LogicalTypeId::DATE;
	static constexpr const LogicalTypeId TIMESTAMP = LogicalTypeId::TIMESTAMP;
	static constexpr const LogicalTypeId TIMESTAMP_S = LogicalTypeId::TIMESTAMP_SEC;
	static constexpr const LogicalTypeId TIMESTAMP_MS = LogicalTypeId::TIMESTAMP_MS;
	static constexpr const LogicalTypeId TIMESTAMP_NS = LogicalTypeId::TIMESTAMP_NS;
	static constexpr const LogicalTypeId TIME = LogicalTypeId::TIME;
	static constexpr const LogicalTypeId TIMESTAMP_TZ = LogicalTypeId::TIMESTAMP_TZ;
	static constexpr const LogicalTypeId TIME_TZ = LogicalTypeId::TIME_TZ;
	static constexpr const LogicalTypeId VARCHAR = LogicalTypeId::VARCHAR;
	static constexpr const LogicalTypeId ANY = LogicalTypeId::ANY;
	static constexpr const LogicalTypeId BLOB = LogicalTypeId::BLOB;
	static constexpr const LogicalTypeId INTERVAL = LogicalTypeId::INTERVAL;
	static constexpr const LogicalTypeId HUGEINT = LogicalTypeId::HUGEINT;
	static constexpr const LogicalTypeId UUID = LogicalTypeId::UUID;
	static constexpr const LogicalTypeId HASH = LogicalTypeId::UBIGINT;
	static constexpr const LogicalTypeId POINTER = LogicalTypeId::POINTER;
	static constexpr const LogicalTypeId TABLE = LogicalTypeId::TABLE;
	static constexpr const LogicalTypeId LAMBDA = LogicalTypeId::LAMBDA;
	static constexpr const LogicalTypeId INVALID = LogicalTypeId::INVALID;
	static constexpr const LogicalTypeId ROW_TYPE = LogicalTypeId::BIGINT;

	// explicitly allowing these functions to be capitalized to be in-line with the remaining functions
	DUCKDB_API static LogicalType DECIMAL(int width, int scale);                 // NOLINT
	DUCKDB_API static LogicalType VARCHAR_COLLATION(string collation);           // NOLINT
	DUCKDB_API static LogicalType LIST( LogicalType child);                       // NOLINT
	DUCKDB_API static LogicalType STRUCT( child_list_t<LogicalType> children);    // NOLINT
	DUCKDB_API static LogicalType AGGREGATE_STATE(aggregate_state_t state_type);    // NOLINT
	DUCKDB_API static LogicalType MAP( child_list_t<LogicalType> children);       // NOLINT
	DUCKDB_API static LogicalType MAP(LogicalType key, LogicalType value); // NOLINT
	DUCKDB_API static LogicalType UNION( child_list_t<LogicalType> members);     // NOLINT
	DUCKDB_API static LogicalType ENUM(const string &enum_name, Vector &ordered_data, idx_t size); // NOLINT
	DUCKDB_API static LogicalType DEDUP_POINTER_ENUM(); // NOLINT
	DUCKDB_API static LogicalType USER(const string &user_type_name); // NOLINT
	//! A list of all NUMERIC types (integral and floating point types)
	DUCKDB_API static const vector<LogicalType> Numeric();
	//! A list of all INTEGRAL types
	DUCKDB_API static const vector<LogicalType> Integral();
	//! A list of ALL SQL types
	DUCKDB_API static const vector<LogicalType> AllTypes();
};

struct DecimalType {
	DUCKDB_API static uint8_t GetWidth(const LogicalType &type);
	DUCKDB_API static uint8_t GetScale(const LogicalType &type);
	DUCKDB_API static uint8_t MaxWidth();
};

struct StringType {
	DUCKDB_API static string GetCollation(const LogicalType &type);
};

struct ListType {
	DUCKDB_API static const LogicalType &GetChildType(const LogicalType &type);
};

struct UserType{
	DUCKDB_API static const string &GetTypeName(const LogicalType &type);
};

struct EnumType{
	DUCKDB_API static const string &GetTypeName(const LogicalType &type);
	DUCKDB_API static int64_t GetPos(const LogicalType &type, const string_t& key);
	DUCKDB_API static Vector &GetValuesInsertOrder(const LogicalType &type);
	DUCKDB_API static idx_t GetSize(const LogicalType &type);
	DUCKDB_API static const string GetValue(const Value &val);
	DUCKDB_API static void SetCatalog(LogicalType &type, TypeCatalogEntry* catalog_entry);
	DUCKDB_API static TypeCatalogEntry* GetCatalog(const LogicalType &type);
	DUCKDB_API static PhysicalType GetPhysicalType(const LogicalType &type);
};

struct StructType {
	DUCKDB_API static const child_list_t<LogicalType> &GetChildTypes(const LogicalType &type);
	DUCKDB_API static const LogicalType &GetChildType(const LogicalType &type, idx_t index);
	DUCKDB_API static const string &GetChildName(const LogicalType &type, idx_t index);
	DUCKDB_API static idx_t GetChildCount(const LogicalType &type);
};

struct MapType {
	DUCKDB_API static const LogicalType &KeyType(const LogicalType &type);
	DUCKDB_API static const LogicalType &ValueType(const LogicalType &type);
};

struct UnionType {
	DUCKDB_API static const idx_t MAX_UNION_MEMBERS = 256;
	DUCKDB_API static idx_t GetMemberCount(const LogicalType &type);
	DUCKDB_API static const LogicalType &GetMemberType(const LogicalType &type, idx_t index);
	DUCKDB_API static const string &GetMemberName(const LogicalType &type, idx_t index);
	DUCKDB_API static const child_list_t<LogicalType> CopyMemberTypes(const LogicalType &type);
};

struct AggregateStateType {
	DUCKDB_API static const string GetTypeName(const LogicalType &type);
	DUCKDB_API static const aggregate_state_t &GetStateType(const LogicalType &type);
};

DUCKDB_API string LogicalTypeIdToString(LogicalTypeId type);

DUCKDB_API LogicalTypeId TransformStringToLogicalTypeId(const string &str);

DUCKDB_API LogicalType TransformStringToLogicalType(const string &str);

DUCKDB_API LogicalType TransformStringToLogicalType(const string &str, ClientContext &context);

//! The PhysicalType used by the row identifiers column
extern const PhysicalType ROW_TYPE;

DUCKDB_API string TypeIdToString(PhysicalType type);
idx_t GetTypeIdSize(PhysicalType type);
bool TypeIsConstantSize(PhysicalType type);
bool TypeIsIntegral(PhysicalType type);
bool TypeIsNumeric(PhysicalType type);
bool TypeIsInteger(PhysicalType type);

bool ApproxEqual(float l, float r);
bool ApproxEqual(double l, double r);

struct aggregate_state_t {
	aggregate_state_t(string function_name_p, LogicalType return_type_p, vector<LogicalType> bound_argument_types_p) : function_name(move(function_name_p)), return_type(move(return_type_p)), bound_argument_types(move(bound_argument_types_p)) {
	}

	string function_name;
	LogicalType return_type;
	vector<LogicalType> bound_argument_types;
};

} // namespace duckdb
