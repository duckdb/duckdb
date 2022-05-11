#include "duckdb/common/types.hpp"

#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"

#include <cmath>

namespace duckdb {

LogicalType::LogicalType() : LogicalType(LogicalTypeId::INVALID) {
}

LogicalType::LogicalType(LogicalTypeId id) : id_(id) {
	physical_type_ = GetInternalType();
}
LogicalType::LogicalType(LogicalTypeId id, shared_ptr<ExtraTypeInfo> type_info_p)
    : id_(id), type_info_(move(type_info_p)) {
	physical_type_ = GetInternalType();
}

LogicalType::LogicalType(const LogicalType &other)
    : id_(other.id_), physical_type_(other.physical_type_), type_info_(other.type_info_) {
}

LogicalType::LogicalType(LogicalType &&other) noexcept
    : id_(other.id_), physical_type_(other.physical_type_), type_info_(move(other.type_info_)) {
}

hash_t LogicalType::Hash() const {
	return duckdb::Hash<uint8_t>((uint8_t)id_);
}

PhysicalType LogicalType::GetInternalType() {
	switch (id_) {
	case LogicalTypeId::BOOLEAN:
		return PhysicalType::BOOL;
	case LogicalTypeId::TINYINT:
		return PhysicalType::INT8;
	case LogicalTypeId::UTINYINT:
		return PhysicalType::UINT8;
	case LogicalTypeId::SMALLINT:
		return PhysicalType::INT16;
	case LogicalTypeId::USMALLINT:
		return PhysicalType::UINT16;
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTEGER:
		return PhysicalType::INT32;
	case LogicalTypeId::UINTEGER:
		return PhysicalType::UINT32;
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
		return PhysicalType::INT64;
	case LogicalTypeId::UBIGINT:
		return PhysicalType::UINT64;
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UUID:
		return PhysicalType::INT128;
	case LogicalTypeId::FLOAT:
		return PhysicalType::FLOAT;
	case LogicalTypeId::DOUBLE:
		return PhysicalType::DOUBLE;
	case LogicalTypeId::DECIMAL: {
		if (!type_info_) {
			return PhysicalType::INVALID;
		}
		auto width = DecimalType::GetWidth(*this);
		if (width <= Decimal::MAX_WIDTH_INT16) {
			return PhysicalType::INT16;
		} else if (width <= Decimal::MAX_WIDTH_INT32) {
			return PhysicalType::INT32;
		} else if (width <= Decimal::MAX_WIDTH_INT64) {
			return PhysicalType::INT64;
		} else if (width <= Decimal::MAX_WIDTH_INT128) {
			return PhysicalType::INT128;
		} else {
			throw InternalException("Widths bigger than 38 are not supported");
		}
	}
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::CHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::JSON:
		return PhysicalType::VARCHAR;
	case LogicalTypeId::INTERVAL:
		return PhysicalType::INTERVAL;
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT:
		return PhysicalType::STRUCT;
	case LogicalTypeId::LIST:
		return PhysicalType::LIST;
	case LogicalTypeId::HASH:
		static_assert(sizeof(hash_t) == sizeof(uint64_t), "Hash must be uint64_t");
		return PhysicalType::UINT64;
	case LogicalTypeId::POINTER:
		// LCOV_EXCL_START
		if (sizeof(uintptr_t) == sizeof(uint32_t)) {
			return PhysicalType::UINT32;
		} else if (sizeof(uintptr_t) == sizeof(uint64_t)) {
			return PhysicalType::UINT64;
		} else {
			throw InternalException("Unsupported pointer size");
		}
		// LCOV_EXCL_STOP
	case LogicalTypeId::VALIDITY:
		return PhysicalType::BIT;
	case LogicalTypeId::ENUM: {
		D_ASSERT(type_info_);
		auto size = EnumType::GetSize(*this);
		return EnumType::GetPhysicalType(size);
	}
	case LogicalTypeId::TABLE:
	case LogicalTypeId::ANY:
	case LogicalTypeId::INVALID:
	case LogicalTypeId::UNKNOWN:
		return PhysicalType::INVALID;
	case LogicalTypeId::USER:
		return PhysicalType::UNKNOWN;
	case LogicalTypeId::AGGREGATE_STATE:
		return PhysicalType::VARCHAR;
	default:
		throw InternalException("Invalid LogicalType %s", ToString());
	}
}

constexpr const LogicalTypeId LogicalType::INVALID;
constexpr const LogicalTypeId LogicalType::SQLNULL;
constexpr const LogicalTypeId LogicalType::BOOLEAN;
constexpr const LogicalTypeId LogicalType::TINYINT;
constexpr const LogicalTypeId LogicalType::UTINYINT;
constexpr const LogicalTypeId LogicalType::SMALLINT;
constexpr const LogicalTypeId LogicalType::USMALLINT;
constexpr const LogicalTypeId LogicalType::INTEGER;
constexpr const LogicalTypeId LogicalType::UINTEGER;
constexpr const LogicalTypeId LogicalType::BIGINT;
constexpr const LogicalTypeId LogicalType::UBIGINT;
constexpr const LogicalTypeId LogicalType::HUGEINT;
constexpr const LogicalTypeId LogicalType::UUID;
constexpr const LogicalTypeId LogicalType::FLOAT;
constexpr const LogicalTypeId LogicalType::DOUBLE;
constexpr const LogicalTypeId LogicalType::DATE;

constexpr const LogicalTypeId LogicalType::TIMESTAMP;
constexpr const LogicalTypeId LogicalType::TIMESTAMP_MS;
constexpr const LogicalTypeId LogicalType::TIMESTAMP_NS;
constexpr const LogicalTypeId LogicalType::TIMESTAMP_S;

constexpr const LogicalTypeId LogicalType::TIME;

constexpr const LogicalTypeId LogicalType::TIME_TZ;
constexpr const LogicalTypeId LogicalType::TIMESTAMP_TZ;

constexpr const LogicalTypeId LogicalType::HASH;
constexpr const LogicalTypeId LogicalType::POINTER;

constexpr const LogicalTypeId LogicalType::VARCHAR;
constexpr const LogicalTypeId LogicalType::JSON;

constexpr const LogicalTypeId LogicalType::BLOB;
constexpr const LogicalTypeId LogicalType::INTERVAL;
constexpr const LogicalTypeId LogicalType::ROW_TYPE;

// TODO these are incomplete and should maybe not exist as such
constexpr const LogicalTypeId LogicalType::TABLE;

constexpr const LogicalTypeId LogicalType::ANY;

const vector<LogicalType> LogicalType::Numeric() {
	vector<LogicalType> types = {LogicalType::TINYINT,   LogicalType::SMALLINT,  LogicalType::INTEGER,
	                             LogicalType::BIGINT,    LogicalType::HUGEINT,   LogicalType::FLOAT,
	                             LogicalType::DOUBLE,    LogicalTypeId::DECIMAL, LogicalType::UTINYINT,
	                             LogicalType::USMALLINT, LogicalType::UINTEGER,  LogicalType::UBIGINT};
	return types;
}

const vector<LogicalType> LogicalType::Integral() {
	vector<LogicalType> types = {LogicalType::TINYINT,   LogicalType::SMALLINT, LogicalType::INTEGER,
	                             LogicalType::BIGINT,    LogicalType::HUGEINT,  LogicalType::UTINYINT,
	                             LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT};
	return types;
}

const vector<LogicalType> LogicalType::AllTypes() {
	vector<LogicalType> types = {
	    LogicalType::BOOLEAN,  LogicalType::TINYINT,   LogicalType::SMALLINT,     LogicalType::INTEGER,
	    LogicalType::BIGINT,   LogicalType::DATE,      LogicalType::TIMESTAMP,    LogicalType::DOUBLE,
	    LogicalType::FLOAT,    LogicalType::VARCHAR,   LogicalType::BLOB,         LogicalType::INTERVAL,
	    LogicalType::HUGEINT,  LogicalTypeId::DECIMAL, LogicalType::UTINYINT,     LogicalType::USMALLINT,
	    LogicalType::UINTEGER, LogicalType::UBIGINT,   LogicalType::TIME,         LogicalTypeId::LIST,
	    LogicalTypeId::STRUCT, LogicalType::TIME_TZ,   LogicalType::TIMESTAMP_TZ, LogicalTypeId::MAP,
	    LogicalType::UUID,     LogicalType::JSON};
	return types;
}

const PhysicalType ROW_TYPE = PhysicalType::INT64;

// LCOV_EXCL_START
string TypeIdToString(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
		return "BOOL";
	case PhysicalType::INT8:
		return "INT8";
	case PhysicalType::INT16:
		return "INT16";
	case PhysicalType::INT32:
		return "INT32";
	case PhysicalType::INT64:
		return "INT64";
	case PhysicalType::UINT8:
		return "UINT8";
	case PhysicalType::UINT16:
		return "UINT16";
	case PhysicalType::UINT32:
		return "UINT32";
	case PhysicalType::UINT64:
		return "UINT64";
	case PhysicalType::INT128:
		return "INT128";
	case PhysicalType::FLOAT:
		return "FLOAT";
	case PhysicalType::DOUBLE:
		return "DOUBLE";
	case PhysicalType::VARCHAR:
		return "VARCHAR";
	case PhysicalType::INTERVAL:
		return "INTERVAL";
	case PhysicalType::STRUCT:
		return "STRUCT<?>";
	case PhysicalType::LIST:
		return "LIST<?>";
	case PhysicalType::INVALID:
		return "INVALID";
	case PhysicalType::BIT:
		return "BIT";
	case PhysicalType::NA:
		return "NA";
	case PhysicalType::HALF_FLOAT:
		return "HALF_FLOAT";
	case PhysicalType::STRING:
		return "ARROW_STRING";
	case PhysicalType::BINARY:
		return "BINARY";
	case PhysicalType::FIXED_SIZE_BINARY:
		return "FIXED_SIZE_BINARY";
	case PhysicalType::DATE32:
		return "DATE32";
	case PhysicalType::DATE64:
		return "DATE64";
	case PhysicalType::TIMESTAMP:
		return "TIMESTAMP";
	case PhysicalType::TIME32:
		return "TIME32";
	case PhysicalType::TIME64:
		return "TIME64";
	case PhysicalType::UNION:
		return "UNION";
	case PhysicalType::DICTIONARY:
		return "DICTIONARY";
	case PhysicalType::MAP:
		return "MAP";
	case PhysicalType::EXTENSION:
		return "EXTENSION";
	case PhysicalType::FIXED_SIZE_LIST:
		return "FIXED_SIZE_LIST";
	case PhysicalType::DURATION:
		return "DURATION";
	case PhysicalType::LARGE_STRING:
		return "LARGE_STRING";
	case PhysicalType::LARGE_BINARY:
		return "LARGE_BINARY";
	case PhysicalType::LARGE_LIST:
		return "LARGE_LIST";
	case PhysicalType::UNKNOWN:
		return "UNKNOWN";
	}
	return "INVALID";
}
// LCOV_EXCL_STOP

idx_t GetTypeIdSize(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL:
		return sizeof(bool);
	case PhysicalType::INT8:
		return sizeof(int8_t);
	case PhysicalType::INT16:
		return sizeof(int16_t);
	case PhysicalType::INT32:
		return sizeof(int32_t);
	case PhysicalType::INT64:
		return sizeof(int64_t);
	case PhysicalType::UINT8:
		return sizeof(uint8_t);
	case PhysicalType::UINT16:
		return sizeof(uint16_t);
	case PhysicalType::UINT32:
		return sizeof(uint32_t);
	case PhysicalType::UINT64:
		return sizeof(uint64_t);
	case PhysicalType::INT128:
		return sizeof(hugeint_t);
	case PhysicalType::FLOAT:
		return sizeof(float);
	case PhysicalType::DOUBLE:
		return sizeof(double);
	case PhysicalType::VARCHAR:
		return sizeof(string_t);
	case PhysicalType::INTERVAL:
		return sizeof(interval_t);
	case PhysicalType::STRUCT:
	case PhysicalType::UNKNOWN:
		return 0; // no own payload
	case PhysicalType::LIST:
		return sizeof(list_entry_t); // offset + len
	default:
		throw InternalException("Invalid PhysicalType for GetTypeIdSize");
	}
}

bool TypeIsConstantSize(PhysicalType type) {
	return (type >= PhysicalType::BOOL && type <= PhysicalType::DOUBLE) ||
	       (type >= PhysicalType::FIXED_SIZE_BINARY && type <= PhysicalType::INTERVAL) ||
	       type == PhysicalType::INTERVAL || type == PhysicalType::INT128;
}
bool TypeIsIntegral(PhysicalType type) {
	return (type >= PhysicalType::UINT8 && type <= PhysicalType::INT64) || type == PhysicalType::INT128;
}
bool TypeIsNumeric(PhysicalType type) {
	return (type >= PhysicalType::UINT8 && type <= PhysicalType::DOUBLE) || type == PhysicalType::INT128;
}
bool TypeIsInteger(PhysicalType type) {
	return (type >= PhysicalType::UINT8 && type <= PhysicalType::INT64) || type == PhysicalType::INT128;
}

// LCOV_EXCL_START
string LogicalTypeIdToString(LogicalTypeId id) {
	switch (id) {
	case LogicalTypeId::BOOLEAN:
		return "BOOLEAN";
	case LogicalTypeId::TINYINT:
		return "TINYINT";
	case LogicalTypeId::SMALLINT:
		return "SMALLINT";
	case LogicalTypeId::INTEGER:
		return "INTEGER";
	case LogicalTypeId::BIGINT:
		return "BIGINT";
	case LogicalTypeId::HUGEINT:
		return "HUGEINT";
	case LogicalTypeId::UUID:
		return "UUID";
	case LogicalTypeId::UTINYINT:
		return "UTINYINT";
	case LogicalTypeId::USMALLINT:
		return "USMALLINT";
	case LogicalTypeId::UINTEGER:
		return "UINTEGER";
	case LogicalTypeId::UBIGINT:
		return "UBIGINT";
	case LogicalTypeId::DATE:
		return "DATE";
	case LogicalTypeId::TIME:
		return "TIME";
	case LogicalTypeId::TIMESTAMP:
		return "TIMESTAMP";
	case LogicalTypeId::TIMESTAMP_MS:
		return "TIMESTAMP_MS";
	case LogicalTypeId::TIMESTAMP_NS:
		return "TIMESTAMP_NS";
	case LogicalTypeId::TIMESTAMP_SEC:
		return "TIMESTAMP_S";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "TIMESTAMP WITH TIME ZONE";
	case LogicalTypeId::TIME_TZ:
		return "TIME WITH TIME ZONE";
	case LogicalTypeId::FLOAT:
		return "FLOAT";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE";
	case LogicalTypeId::DECIMAL:
		return "DECIMAL";
	case LogicalTypeId::VARCHAR:
		return "VARCHAR";
	case LogicalTypeId::BLOB:
		return "BLOB";
	case LogicalTypeId::CHAR:
		return "CHAR";
	case LogicalTypeId::INTERVAL:
		return "INTERVAL";
	case LogicalTypeId::SQLNULL:
		return "NULL";
	case LogicalTypeId::ANY:
		return "ANY";
	case LogicalTypeId::VALIDITY:
		return "VALIDITY";
	case LogicalTypeId::STRUCT:
		return "STRUCT<?>";
	case LogicalTypeId::LIST:
		return "LIST<?>";
	case LogicalTypeId::MAP:
		return "MAP<?>";
	case LogicalTypeId::HASH:
		return "HASH";
	case LogicalTypeId::POINTER:
		return "POINTER";
	case LogicalTypeId::TABLE:
		return "TABLE";
	case LogicalTypeId::INVALID:
		return "INVALID";
	case LogicalTypeId::UNKNOWN:
		return "UNKNOWN";
	case LogicalTypeId::ENUM:
		return "ENUM";
	case LogicalTypeId::AGGREGATE_STATE:
		return "AGGREGATE_STATE<?>";
	case LogicalTypeId::USER:
		return "USER";
	case LogicalTypeId::JSON:
		return "JSON";
	}
	return "UNDEFINED";
}

string LogicalType::ToString() const {
	switch (id_) {
	case LogicalTypeId::STRUCT: {
		if (!type_info_) {
			return "STRUCT";
		}
		auto &child_types = StructType::GetChildTypes(*this);
		string ret = "STRUCT(";
		for (size_t i = 0; i < child_types.size(); i++) {
			ret += child_types[i].first + " " + child_types[i].second.ToString();
			if (i < child_types.size() - 1) {
				ret += ", ";
			}
		}
		ret += ")";
		return ret;
	}
	case LogicalTypeId::LIST: {
		if (!type_info_) {
			return "LIST";
		}
		return ListType::GetChildType(*this).ToString() + "[]";
	}
	case LogicalTypeId::MAP: {
		if (!type_info_) {
			return "MAP";
		}
		auto &child_types = StructType::GetChildTypes(*this);
		if (child_types.empty()) {
			return "MAP(?)";
		}
		if (child_types.size() != 2) {
			throw InternalException("Map needs exactly two child elements");
		}
		return "MAP(" + ListType::GetChildType(child_types[0].second).ToString() + ", " +
		       ListType::GetChildType(child_types[1].second).ToString() + ")";
	}
	case LogicalTypeId::DECIMAL: {
		if (!type_info_) {
			return "DECIMAL";
		}
		auto width = DecimalType::GetWidth(*this);
		auto scale = DecimalType::GetScale(*this);
		if (width == 0) {
			return "DECIMAL";
		}
		return StringUtil::Format("DECIMAL(%d,%d)", width, scale);
	}
	case LogicalTypeId::ENUM: {
		return KeywordHelper::WriteOptionallyQuoted(EnumType::GetTypeName(*this));
	}
	case LogicalTypeId::USER: {
		return KeywordHelper::WriteOptionallyQuoted(UserType::GetTypeName(*this));
	}
	case LogicalTypeId::AGGREGATE_STATE: {
		return AggregateStateType::GetTypeName(*this);
	}
	default:
		return LogicalTypeIdToString(id_);
	}
}
// LCOV_EXCL_STOP

LogicalTypeId TransformStringToLogicalTypeId(const string &str) {
	auto lower_str = StringUtil::Lower(str);
	// Transform column type
	if (lower_str == "int" || lower_str == "int4" || lower_str == "signed" || lower_str == "integer" ||
	    lower_str == "integral" || lower_str == "int32") {
		return LogicalTypeId::INTEGER;
	} else if (lower_str == "varchar" || lower_str == "bpchar" || lower_str == "text" || lower_str == "string" ||
	           lower_str == "char" || lower_str == "nvarchar") {
		return LogicalTypeId::VARCHAR;
	} else if (lower_str == "bytea" || lower_str == "blob" || lower_str == "varbinary" || lower_str == "binary") {
		return LogicalTypeId::BLOB;
	} else if (lower_str == "int8" || lower_str == "bigint" || lower_str == "int64" || lower_str == "long" ||
	           lower_str == "oid") {
		return LogicalTypeId::BIGINT;
	} else if (lower_str == "int2" || lower_str == "smallint" || lower_str == "short" || lower_str == "int16") {
		return LogicalTypeId::SMALLINT;
	} else if (lower_str == "timestamp" || lower_str == "datetime" || lower_str == "timestamp_us") {
		return LogicalTypeId::TIMESTAMP;
	} else if (lower_str == "timestamp_ms") {
		return LogicalTypeId::TIMESTAMP_MS;
	} else if (lower_str == "timestamp_ns") {
		return LogicalTypeId::TIMESTAMP_NS;
	} else if (lower_str == "timestamp_s") {
		return LogicalTypeId::TIMESTAMP_SEC;
	} else if (lower_str == "bool" || lower_str == "boolean" || lower_str == "logical") {
		return LogicalTypeId::BOOLEAN;
	} else if (lower_str == "decimal" || lower_str == "dec" || lower_str == "numeric") {
		return LogicalTypeId::DECIMAL;
	} else if (lower_str == "real" || lower_str == "float4" || lower_str == "float") {
		return LogicalTypeId::FLOAT;
	} else if (lower_str == "double" || lower_str == "float8") {
		return LogicalTypeId::DOUBLE;
	} else if (lower_str == "tinyint" || lower_str == "int1") {
		return LogicalTypeId::TINYINT;
	} else if (lower_str == "date") {
		return LogicalTypeId::DATE;
	} else if (lower_str == "time") {
		return LogicalTypeId::TIME;
	} else if (lower_str == "interval") {
		return LogicalTypeId::INTERVAL;
	} else if (lower_str == "hugeint" || lower_str == "int128") {
		return LogicalTypeId::HUGEINT;
	} else if (lower_str == "uuid" || lower_str == "guid") {
		return LogicalTypeId::UUID;
	} else if (lower_str == "struct" || lower_str == "row") {
		return LogicalTypeId::STRUCT;
	} else if (lower_str == "map") {
		return LogicalTypeId::MAP;
	} else if (lower_str == "utinyint" || lower_str == "uint8") {
		return LogicalTypeId::UTINYINT;
	} else if (lower_str == "usmallint" || lower_str == "uint16") {
		return LogicalTypeId::USMALLINT;
	} else if (lower_str == "uinteger" || lower_str == "uint32") {
		return LogicalTypeId::UINTEGER;
	} else if (lower_str == "ubigint" || lower_str == "uint64") {
		return LogicalTypeId::UBIGINT;
	} else if (lower_str == "timestamptz") {
		return LogicalTypeId::TIMESTAMP_TZ;
	} else if (lower_str == "timetz") {
		return LogicalTypeId::TIME_TZ;
	} else if (lower_str == "json") {
		return LogicalTypeId::JSON;
	} else if (lower_str == "null") {
		return LogicalTypeId::SQLNULL;
	} else {
		// This is a User Type, at this point we don't know if its one of the User Defined Types or an error
		// It is checked in the binder
		return LogicalTypeId::USER;
	}
}

LogicalType TransformStringToLogicalType(const string &str) {
	if (StringUtil::Lower(str) == "null") {
		return LogicalType::SQLNULL;
	}
	return Parser::ParseColumnList("dummy " + str)[0].type;
}

bool LogicalType::IsIntegral() const {
	switch (id_) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
		return true;
	default:
		return false;
	}
}

bool LogicalType::IsNumeric() const {
	switch (id_) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return true;
	default:
		return false;
	}
}

bool LogicalType::GetDecimalProperties(uint8_t &width, uint8_t &scale) const {
	switch (id_) {
	case LogicalTypeId::SQLNULL:
		width = 0;
		scale = 0;
		break;
	case LogicalTypeId::BOOLEAN:
		width = 1;
		scale = 0;
		break;
	case LogicalTypeId::TINYINT:
		// tinyint: [-127, 127] = DECIMAL(3,0)
		width = 3;
		scale = 0;
		break;
	case LogicalTypeId::SMALLINT:
		// smallint: [-32767, 32767] = DECIMAL(5,0)
		width = 5;
		scale = 0;
		break;
	case LogicalTypeId::INTEGER:
		// integer: [-2147483647, 2147483647] = DECIMAL(10,0)
		width = 10;
		scale = 0;
		break;
	case LogicalTypeId::BIGINT:
		// bigint: [-9223372036854775807, 9223372036854775807] = DECIMAL(19,0)
		width = 19;
		scale = 0;
		break;
	case LogicalTypeId::UTINYINT:
		// UInt8 — [0 : 255]
		width = 3;
		scale = 0;
		break;
	case LogicalTypeId::USMALLINT:
		// UInt16 — [0 : 65535]
		width = 5;
		scale = 0;
		break;
	case LogicalTypeId::UINTEGER:
		// UInt32 — [0 : 4294967295]
		width = 10;
		scale = 0;
		break;
	case LogicalTypeId::UBIGINT:
		// UInt64 — [0 : 18446744073709551615]
		width = 20;
		scale = 0;
		break;
	case LogicalTypeId::HUGEINT:
		// hugeint: max size decimal (38, 0)
		// note that a hugeint is not guaranteed to fit in this
		width = 38;
		scale = 0;
		break;
	case LogicalTypeId::DECIMAL:
		width = DecimalType::GetWidth(*this);
		scale = DecimalType::GetScale(*this);
		break;
	default:
		return false;
	}
	return true;
}

LogicalType LogicalType::MaxLogicalType(const LogicalType &left, const LogicalType &right) {
	if (left.id() < right.id()) {
		return right;
	} else if (right.id() < left.id()) {
		return left;
	} else {
		// Since both left and right are equal we get the left type as our type_id for checks
		auto type_id = left.id();
		if (type_id == LogicalTypeId::ENUM) {
			// If both types are different ENUMs we do a string comparison.
			return left == right ? left : LogicalType::VARCHAR;
		}
		if (type_id == LogicalTypeId::VARCHAR) {
			// varchar: use type that has collation (if any)
			if (StringType::GetCollation(right).empty()) {
				return left;
			} else {
				return right;
			}
		} else if (type_id == LogicalTypeId::DECIMAL) {
			// use max width/scale of the two types
			auto width = MaxValue<uint8_t>(DecimalType::GetWidth(left), DecimalType::GetWidth(right));
			auto scale = MaxValue<uint8_t>(DecimalType::GetScale(left), DecimalType::GetScale(right));
			return LogicalType::DECIMAL(width, scale);
		} else if (type_id == LogicalTypeId::LIST) {
			// list: perform max recursively on child type
			auto new_child = MaxLogicalType(ListType::GetChildType(left), ListType::GetChildType(right));
			return LogicalType::LIST(move(new_child));
		} else if (type_id == LogicalTypeId::STRUCT) {
			// struct: perform recursively
			auto &left_child_types = StructType::GetChildTypes(left);
			auto &right_child_types = StructType::GetChildTypes(right);
			if (left_child_types.size() != right_child_types.size()) {
				// child types are not of equal size, we can't cast anyway
				// just return the left child
				return left;
			}
			child_list_t<LogicalType> child_types;
			for (idx_t i = 0; i < left_child_types.size(); i++) {
				auto child_type = MaxLogicalType(left_child_types[i].second, right_child_types[i].second);
				child_types.push_back(make_pair(left_child_types[i].first, move(child_type)));
			}
			return LogicalType::STRUCT(move(child_types));
		} else {
			// types are equal but no extra specifier: just return the type
			return left;
		}
	}
}

void LogicalType::Verify() const {
#ifdef DEBUG
	if (id_ == LogicalTypeId::DECIMAL) {
		D_ASSERT(DecimalType::GetWidth(*this) >= 1 && DecimalType::GetWidth(*this) <= Decimal::MAX_WIDTH_DECIMAL);
		D_ASSERT(DecimalType::GetScale(*this) >= 0 && DecimalType::GetScale(*this) <= DecimalType::GetWidth(*this));
	}
#endif
}

bool ApproxEqual(float ldecimal, float rdecimal) {
	if (Value::IsNan(ldecimal) && Value::IsNan(rdecimal)) {
		return true;
	}
	if (!Value::FloatIsFinite(ldecimal) || !Value::FloatIsFinite(rdecimal)) {
		return ldecimal == rdecimal;
	}
	float epsilon = std::fabs(rdecimal) * 0.01;
	return std::fabs(ldecimal - rdecimal) <= epsilon;
}

bool ApproxEqual(double ldecimal, double rdecimal) {
	if (Value::IsNan(ldecimal) && Value::IsNan(rdecimal)) {
		return true;
	}
	if (!Value::DoubleIsFinite(ldecimal) || !Value::DoubleIsFinite(rdecimal)) {
		return ldecimal == rdecimal;
	}
	double epsilon = std::fabs(rdecimal) * 0.01;
	return std::fabs(ldecimal - rdecimal) <= epsilon;
}

//===--------------------------------------------------------------------===//
// Extra Type Info
//===--------------------------------------------------------------------===//
enum class ExtraTypeInfoType : uint8_t {
	INVALID_TYPE_INFO = 0,
	DECIMAL_TYPE_INFO = 1,
	STRING_TYPE_INFO = 2,
	LIST_TYPE_INFO = 3,
	STRUCT_TYPE_INFO = 4,
	ENUM_TYPE_INFO = 5,
	USER_TYPE_INFO = 6,
	AGGREGATE_STATE_TYPE_INFO = 7
};

struct ExtraTypeInfo {
	explicit ExtraTypeInfo(ExtraTypeInfoType type) : type(type) {
	}
	virtual ~ExtraTypeInfo() {
	}

	ExtraTypeInfoType type;

public:
	virtual bool Equals(ExtraTypeInfo *other) const = 0;
	//! Serializes a ExtraTypeInfo to a stand-alone binary blob
	virtual void Serialize(FieldWriter &writer) const = 0;
	//! Serializes a ExtraTypeInfo to a stand-alone binary blob
	static void Serialize(ExtraTypeInfo *info, FieldWriter &writer);
	//! Deserializes a blob back into an ExtraTypeInfo
	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader);
};

//===--------------------------------------------------------------------===//
// Decimal Type
//===--------------------------------------------------------------------===//
struct DecimalTypeInfo : public ExtraTypeInfo {
	DecimalTypeInfo(uint8_t width_p, uint8_t scale_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::DECIMAL_TYPE_INFO), width(width_p), scale(scale_p) {
	}

	uint8_t width;
	uint8_t scale;

public:
	bool Equals(ExtraTypeInfo *other_p) const override {
		if (!other_p) {
			return false;
		}
		if (type != other_p->type) {
			return false;
		}
		auto &other = (DecimalTypeInfo &)*other_p;
		return width == other.width && scale == other.scale;
	}

	void Serialize(FieldWriter &writer) const override {
		writer.WriteField<uint8_t>(width);
		writer.WriteField<uint8_t>(scale);
	}

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader) {
		auto width = reader.ReadRequired<uint8_t>();
		auto scale = reader.ReadRequired<uint8_t>();
		return make_shared<DecimalTypeInfo>(width, scale);
	}
};

uint8_t DecimalType::GetWidth(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((DecimalTypeInfo &)*info).width;
}

uint8_t DecimalType::GetScale(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((DecimalTypeInfo &)*info).scale;
}

LogicalType LogicalType::DECIMAL(int width, int scale) {
	auto type_info = make_shared<DecimalTypeInfo>(width, scale);
	return LogicalType(LogicalTypeId::DECIMAL, move(type_info));
}

//===--------------------------------------------------------------------===//
// String Type
//===--------------------------------------------------------------------===//
struct StringTypeInfo : public ExtraTypeInfo {
	explicit StringTypeInfo(string collation_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::STRING_TYPE_INFO), collation(move(collation_p)) {
	}

	string collation;

public:
	bool Equals(ExtraTypeInfo *other_p) const override {
		// collation info has no impact on equality
		return true;
	}

	void Serialize(FieldWriter &writer) const override {
		writer.WriteString(collation);
	}

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader) {
		auto collation = reader.ReadRequired<string>();
		return make_shared<StringTypeInfo>(move(collation));
	}
};

string StringType::GetCollation(const LogicalType &type) {
	if (type.id() != LogicalTypeId::VARCHAR) {
		return string();
	}
	auto info = type.AuxInfo();
	if (!info) {
		return string();
	}
	return ((StringTypeInfo &)*info).collation;
}

LogicalType LogicalType::VARCHAR_COLLATION(string collation) { // NOLINT
	auto string_info = make_shared<StringTypeInfo>(move(collation));
	return LogicalType(LogicalTypeId::VARCHAR, move(string_info));
}

//===--------------------------------------------------------------------===//
// List Type
//===--------------------------------------------------------------------===//
struct ListTypeInfo : public ExtraTypeInfo {
	explicit ListTypeInfo(LogicalType child_type_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::LIST_TYPE_INFO), child_type(move(child_type_p)) {
	}

	LogicalType child_type;

public:
	bool Equals(ExtraTypeInfo *other_p) const override {
		if (!other_p) {
			return false;
		}
		if (type != other_p->type) {
			return false;
		}
		auto &other = (ListTypeInfo &)*other_p;
		return child_type == other.child_type;
	}

	void Serialize(FieldWriter &writer) const override {
		writer.WriteSerializable(child_type);
	}

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader) {
		auto child_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
		return make_shared<ListTypeInfo>(move(child_type));
	}
};

const LogicalType &ListType::GetChildType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::LIST);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((ListTypeInfo &)*info).child_type;
}

LogicalType LogicalType::LIST(LogicalType child) {
	auto info = make_shared<ListTypeInfo>(move(child));
	return LogicalType(LogicalTypeId::LIST, move(info));
}

//===--------------------------------------------------------------------===//
// Struct Type
//===--------------------------------------------------------------------===//
struct StructTypeInfo : public ExtraTypeInfo {
	explicit StructTypeInfo(child_list_t<LogicalType> child_types_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::STRUCT_TYPE_INFO), child_types(move(child_types_p)) {
	}

	child_list_t<LogicalType> child_types;

public:
	bool Equals(ExtraTypeInfo *other_p) const override {
		if (!other_p) {
			return false;
		}
		if (type != other_p->type) {
			return false;
		}
		auto &other = (StructTypeInfo &)*other_p;
		return child_types == other.child_types;
	}

	void Serialize(FieldWriter &writer) const override {
		writer.WriteField<uint32_t>(child_types.size());
		auto &serializer = writer.GetSerializer();
		for (idx_t i = 0; i < child_types.size(); i++) {
			serializer.WriteString(child_types[i].first);
			child_types[i].second.Serialize(serializer);
		}
	}

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader) {
		child_list_t<LogicalType> child_list;
		auto child_types_size = reader.ReadRequired<uint32_t>();
		auto &source = reader.GetSource();
		for (uint32_t i = 0; i < child_types_size; i++) {
			auto name = source.Read<string>();
			auto type = LogicalType::Deserialize(source);
			child_list.push_back(make_pair(move(name), move(type)));
		}
		return make_shared<StructTypeInfo>(move(child_list));
	}
};

struct AggregateStateTypeInfo : public ExtraTypeInfo {
	explicit AggregateStateTypeInfo(aggregate_state_t state_type_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::AGGREGATE_STATE_TYPE_INFO), state_type(move(state_type_p)) {
	}

	aggregate_state_t state_type;

public:
	bool Equals(ExtraTypeInfo *other_p) const override {
		if (!other_p) {
			return false;
		}
		if (type != other_p->type) {
			return false;
		}
		auto &other = (AggregateStateTypeInfo &)*other_p;
		return state_type.function_name == other.state_type.function_name &&
		       state_type.return_type == other.state_type.return_type &&
		       state_type.bound_argument_types == other.state_type.bound_argument_types;
	}

	void Serialize(FieldWriter &writer) const override {
		auto &serializer = writer.GetSerializer();
		writer.WriteString(state_type.function_name);
		state_type.return_type.Serialize(serializer);
		writer.WriteField<uint32_t>(state_type.bound_argument_types.size());
		for (idx_t i = 0; i < state_type.bound_argument_types.size(); i++) {
			state_type.bound_argument_types[i].Serialize(serializer);
		}
	}

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader) {
		auto &source = reader.GetSource();

		auto function_name = reader.ReadRequired<string>();
		auto return_type = LogicalType::Deserialize(source);
		auto bound_argument_types_size = reader.ReadRequired<uint32_t>();
		vector<LogicalType> bound_argument_types;

		for (uint32_t i = 0; i < bound_argument_types_size; i++) {
			auto type = LogicalType::Deserialize(source);
			bound_argument_types.push_back(move(type));
		}
		return make_shared<AggregateStateTypeInfo>(
		    aggregate_state_t(move(function_name), move(return_type), move(bound_argument_types)));
	}
};

const aggregate_state_t &AggregateStateType::GetStateType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::AGGREGATE_STATE);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((AggregateStateTypeInfo &)*info).state_type;
}

const string AggregateStateType::GetTypeName(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::AGGREGATE_STATE);
	auto info = type.AuxInfo();
	if (!info) {
		return "AGGREGATE_STATE<?>";
	}
	auto aggr_state = ((AggregateStateTypeInfo &)*info).state_type;
	return "AGGREGATE_STATE<" + aggr_state.function_name + "(" +
	       StringUtil::Join(aggr_state.bound_argument_types, aggr_state.bound_argument_types.size(), ", ",
	                        [](const LogicalType &arg_type) { return arg_type.ToString(); }) +
	       ")" + "::" + aggr_state.return_type.ToString() + ">";
}

const child_list_t<LogicalType> &StructType::GetChildTypes(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::MAP);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((StructTypeInfo &)*info).child_types;
}

const LogicalType &StructType::GetChildType(const LogicalType &type, idx_t index) {
	auto &child_types = StructType::GetChildTypes(type);
	D_ASSERT(index < child_types.size());
	return child_types[index].second;
}

const string &StructType::GetChildName(const LogicalType &type, idx_t index) {
	auto &child_types = StructType::GetChildTypes(type);
	D_ASSERT(index < child_types.size());
	return child_types[index].first;
}

idx_t StructType::GetChildCount(const LogicalType &type) {
	return StructType::GetChildTypes(type).size();
}

LogicalType LogicalType::STRUCT(child_list_t<LogicalType> children) {
	auto info = make_shared<StructTypeInfo>(move(children));
	return LogicalType(LogicalTypeId::STRUCT, move(info));
}

LogicalType LogicalType::AGGREGATE_STATE(aggregate_state_t state_type) { // NOLINT
	auto info = make_shared<AggregateStateTypeInfo>(move(state_type));
	return LogicalType(LogicalTypeId::AGGREGATE_STATE, move(info));
}

//===--------------------------------------------------------------------===//
// Map Type
//===--------------------------------------------------------------------===//
LogicalType LogicalType::MAP(child_list_t<LogicalType> children) {
	auto info = make_shared<StructTypeInfo>(move(children));
	return LogicalType(LogicalTypeId::MAP, move(info));
}

LogicalType LogicalType::MAP(LogicalType key, LogicalType value) {
	child_list_t<LogicalType> child_types;
	child_types.push_back({"key", LogicalType::LIST(move(key))});
	child_types.push_back({"value", LogicalType::LIST(move(value))});
	return LogicalType::MAP(move(child_types));
}

const LogicalType &MapType::KeyType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::MAP);
	return ListType::GetChildType(StructType::GetChildTypes(type)[0].second);
}

const LogicalType &MapType::ValueType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::MAP);
	return ListType::GetChildType(StructType::GetChildTypes(type)[1].second);
}

//===--------------------------------------------------------------------===//
// User Type
//===--------------------------------------------------------------------===//
struct UserTypeInfo : public ExtraTypeInfo {
	explicit UserTypeInfo(string name_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::USER_TYPE_INFO), user_type_name(move(name_p)) {
	}

	string user_type_name;

public:
	bool Equals(ExtraTypeInfo *other_p) const override {
		if (!other_p) {
			return false;
		}
		if (type != other_p->type) {
			return false;
		}
		auto &other = (UserTypeInfo &)*other_p;
		return other.user_type_name == user_type_name;
	}

	void Serialize(FieldWriter &writer) const override {
		writer.WriteString(user_type_name);
	}

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader) {
		auto enum_name = reader.ReadRequired<string>();
		return make_shared<UserTypeInfo>(move(enum_name));
	}
};

const string &UserType::GetTypeName(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::USER);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((UserTypeInfo &)*info).user_type_name;
}

LogicalType LogicalType::USER(const string &user_type_name) {
	auto info = make_shared<UserTypeInfo>(user_type_name);
	return LogicalType(LogicalTypeId::USER, move(info));
}

//===--------------------------------------------------------------------===//
// Enum Type
//===--------------------------------------------------------------------===//
struct EnumTypeInfo : public ExtraTypeInfo {
	explicit EnumTypeInfo(string enum_name_p, Vector &values_insert_order_p, idx_t size)
	    : ExtraTypeInfo(ExtraTypeInfoType::ENUM_TYPE_INFO), enum_name(move(enum_name_p)),
	      values_insert_order(values_insert_order_p), size(size) {
	}
	string enum_name;
	Vector values_insert_order;
	idx_t size;
	TypeCatalogEntry *catalog_entry = nullptr;

public:
	// Equalities are only used in enums with different catalog entries
	bool Equals(ExtraTypeInfo *other_p) const override {
		if (!other_p) {
			return false;
		}
		if (type != other_p->type) {
			return false;
		}
		auto &other = (EnumTypeInfo &)*other_p;

		// We must check if both enums have the same size
		if (other.size != size) {
			return false;
		}
		auto other_vector_ptr = FlatVector::GetData<string_t>(other.values_insert_order);
		auto this_vector_ptr = FlatVector::GetData<string_t>(values_insert_order);

		// Now we must check if all strings are the same
		for (idx_t i = 0; i < size; i++) {
			if (!Equals::Operation(other_vector_ptr[i], this_vector_ptr[i])) {
				return false;
			}
		}
		return true;
	}

	void Serialize(FieldWriter &writer) const override {
		writer.WriteField<uint32_t>(size);
		writer.WriteString(enum_name);
		((Vector &)values_insert_order).Serialize(size, writer.GetSerializer());
	}
};

template <class T>
struct EnumTypeInfoTemplated : public EnumTypeInfo {
	explicit EnumTypeInfoTemplated(const string &enum_name_p, Vector &values_insert_order_p, idx_t size_p)
	    : EnumTypeInfo(enum_name_p, values_insert_order_p, size_p) {
		for (idx_t count = 0; count < size_p; count++) {
			values[values_insert_order_p.GetValue(count).ToString()] = count;
		}
	}

	static shared_ptr<EnumTypeInfoTemplated> Deserialize(FieldReader &reader, uint32_t size) {
		auto enum_name = reader.ReadRequired<string>();
		Vector values_insert_order(LogicalType::VARCHAR, size);
		values_insert_order.Deserialize(size, reader.GetSource());
		return make_shared<EnumTypeInfoTemplated>(move(enum_name), values_insert_order, size);
	}
	unordered_map<string, T> values;
};

const string &EnumType::GetTypeName(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((EnumTypeInfo &)*info).enum_name;
}

LogicalType LogicalType::ENUM(const string &enum_name, Vector &ordered_data, idx_t size) {
	// Generate EnumTypeInfo
	shared_ptr<ExtraTypeInfo> info;
	auto enum_internal_type = EnumType::GetPhysicalType(size);
	switch (enum_internal_type) {
	case PhysicalType::UINT8:
		info = make_shared<EnumTypeInfoTemplated<uint8_t>>(enum_name, ordered_data, size);
		break;
	case PhysicalType::UINT16:
		info = make_shared<EnumTypeInfoTemplated<uint16_t>>(enum_name, ordered_data, size);
		break;
	case PhysicalType::UINT32:
		info = make_shared<EnumTypeInfoTemplated<uint32_t>>(enum_name, ordered_data, size);
		break;
	default:
		throw InternalException("Invalid Physical Type for ENUMs");
	}
	// Generate Actual Enum Type
	return LogicalType(LogicalTypeId::ENUM, info);
}

template <class T>
int64_t TemplatedGetPos(unordered_map<string, T> &map, const string &key) {
	auto it = map.find(key);
	if (it == map.end()) {
		return -1;
	}
	return it->second;
}
int64_t EnumType::GetPos(const LogicalType &type, const string &key) {
	auto info = type.AuxInfo();
	switch (type.InternalType()) {
	case PhysicalType::UINT8:
		return TemplatedGetPos(((EnumTypeInfoTemplated<uint8_t> &)*info).values, key);
	case PhysicalType::UINT16:
		return TemplatedGetPos(((EnumTypeInfoTemplated<uint16_t> &)*info).values, key);
	case PhysicalType::UINT32:
		return TemplatedGetPos(((EnumTypeInfoTemplated<uint32_t> &)*info).values, key);
	default:
		throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
	}
}

const string EnumType::GetValue(const Value &val) {
	auto info = val.type().AuxInfo();
	auto &values_insert_order = ((EnumTypeInfo &)*info).values_insert_order;
	return StringValue::Get(values_insert_order.GetValue(val.GetValue<uint32_t>()));
}

Vector &EnumType::GetValuesInsertOrder(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((EnumTypeInfo &)*info).values_insert_order;
}

idx_t EnumType::GetSize(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((EnumTypeInfo &)*info).size;
}

void EnumType::SetCatalog(LogicalType &type, TypeCatalogEntry *catalog_entry) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	((EnumTypeInfo &)*info).catalog_entry = catalog_entry;
}
TypeCatalogEntry *EnumType::GetCatalog(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((EnumTypeInfo &)*info).catalog_entry;
}

PhysicalType EnumType::GetPhysicalType(idx_t size) {
	if (size <= NumericLimits<uint8_t>::Maximum()) {
		return PhysicalType::UINT8;
	} else if (size <= NumericLimits<uint16_t>::Maximum()) {
		return PhysicalType::UINT16;
	} else if (size <= NumericLimits<uint32_t>::Maximum()) {
		return PhysicalType::UINT32;
	} else {
		throw InternalException("Enum size must be lower than " + std::to_string(NumericLimits<uint32_t>::Maximum()));
	}
}

//===--------------------------------------------------------------------===//
// Extra Type Info
//===--------------------------------------------------------------------===//
void ExtraTypeInfo::Serialize(ExtraTypeInfo *info, FieldWriter &writer) {
	if (!info) {
		writer.WriteField<ExtraTypeInfoType>(ExtraTypeInfoType::INVALID_TYPE_INFO);
	} else {
		writer.WriteField<ExtraTypeInfoType>(info->type);
		info->Serialize(writer);
	}
}
shared_ptr<ExtraTypeInfo> ExtraTypeInfo::Deserialize(FieldReader &reader) {
	auto type = reader.ReadRequired<ExtraTypeInfoType>();
	switch (type) {
	case ExtraTypeInfoType::INVALID_TYPE_INFO:
		return nullptr;
	case ExtraTypeInfoType::DECIMAL_TYPE_INFO:
		return DecimalTypeInfo::Deserialize(reader);
	case ExtraTypeInfoType::STRING_TYPE_INFO:
		return StringTypeInfo::Deserialize(reader);
	case ExtraTypeInfoType::LIST_TYPE_INFO:
		return ListTypeInfo::Deserialize(reader);
	case ExtraTypeInfoType::STRUCT_TYPE_INFO:
		return StructTypeInfo::Deserialize(reader);
	case ExtraTypeInfoType::USER_TYPE_INFO:
		return UserTypeInfo::Deserialize(reader);
	case ExtraTypeInfoType::ENUM_TYPE_INFO: {
		auto enum_size = reader.ReadRequired<uint32_t>();
		auto enum_internal_type = EnumType::GetPhysicalType(enum_size);
		switch (enum_internal_type) {
		case PhysicalType::UINT8:
			return EnumTypeInfoTemplated<uint8_t>::Deserialize(reader, enum_size);
		case PhysicalType::UINT16:
			return EnumTypeInfoTemplated<uint16_t>::Deserialize(reader, enum_size);
		case PhysicalType::UINT32:
			return EnumTypeInfoTemplated<uint32_t>::Deserialize(reader, enum_size);
		default:
			throw InternalException("Invalid Physical Type for ENUMs");
		}
	}
	case ExtraTypeInfoType::AGGREGATE_STATE_TYPE_INFO:
		return AggregateStateTypeInfo::Deserialize(reader);

	default:
		throw InternalException("Unimplemented type info in ExtraTypeInfo::Deserialize");
	}
}

//===--------------------------------------------------------------------===//
// Logical Type
//===--------------------------------------------------------------------===//

// the destructor needs to know about the extra type info
LogicalType::~LogicalType() {
}

void LogicalType::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<LogicalTypeId>(id_);
	ExtraTypeInfo::Serialize(type_info_.get(), writer);
	writer.Finalize();
}

LogicalType LogicalType::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto id = reader.ReadRequired<LogicalTypeId>();
	auto info = ExtraTypeInfo::Deserialize(reader);
	reader.Finalize();

	return LogicalType(id, move(info));
}

bool LogicalType::operator==(const LogicalType &rhs) const {
	if (id_ != rhs.id_) {
		return false;
	}
	if (type_info_.get() == rhs.type_info_.get()) {
		return true;
	}
	if (type_info_) {
		return type_info_->Equals(rhs.type_info_.get());
	} else {
		D_ASSERT(rhs.type_info_);
		return rhs.type_info_->Equals(type_info_.get());
	}
}

} // namespace duckdb
