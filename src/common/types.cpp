#include "duckdb/common/types.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/function/cast_rules.hpp"
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
    : id_(id), type_info_(std::move(type_info_p)) {
	physical_type_ = GetInternalType();
}

LogicalType::LogicalType(const LogicalType &other)
    : id_(other.id_), physical_type_(other.physical_type_), type_info_(other.type_info_) {
}

LogicalType::LogicalType(LogicalType &&other) noexcept
    : id_(other.id_), physical_type_(other.physical_type_), type_info_(std::move(other.type_info_)) {
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
			throw InternalException("Decimal has a width of %d which is bigger than the maximum supported width of %d",
			                        width, DecimalType::MaxWidth());
		}
	}
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::CHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIT:
		return PhysicalType::VARCHAR;
	case LogicalTypeId::INTERVAL:
		return PhysicalType::INTERVAL;
	case LogicalTypeId::UNION:
	case LogicalTypeId::STRUCT:
		return PhysicalType::STRUCT;
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		return PhysicalType::LIST;
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
		if (!type_info_) {
			return PhysicalType::INVALID;
		}
		return EnumType::GetPhysicalType(*this);
	}
	case LogicalTypeId::TABLE:
	case LogicalTypeId::LAMBDA:
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

constexpr const LogicalTypeId LogicalType::BLOB;
constexpr const LogicalTypeId LogicalType::BIT;
constexpr const LogicalTypeId LogicalType::INTERVAL;
constexpr const LogicalTypeId LogicalType::ROW_TYPE;

// TODO these are incomplete and should maybe not exist as such
constexpr const LogicalTypeId LogicalType::TABLE;
constexpr const LogicalTypeId LogicalType::LAMBDA;

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
	    LogicalType::BOOLEAN,   LogicalType::TINYINT,  LogicalType::SMALLINT,  LogicalType::INTEGER,
	    LogicalType::BIGINT,    LogicalType::DATE,     LogicalType::TIMESTAMP, LogicalType::DOUBLE,
	    LogicalType::FLOAT,     LogicalType::VARCHAR,  LogicalType::BLOB,      LogicalType::BIT,
	    LogicalType::INTERVAL,  LogicalType::HUGEINT,  LogicalTypeId::DECIMAL, LogicalType::UTINYINT,
	    LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT,   LogicalType::TIME,
	    LogicalTypeId::LIST,    LogicalTypeId::STRUCT, LogicalType::TIME_TZ,   LogicalType::TIMESTAMP_TZ,
	    LogicalTypeId::MAP,     LogicalTypeId::UNION,  LogicalType::UUID};
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
		return "STRUCT";
	case PhysicalType::LIST:
		return "LIST";
	case PhysicalType::INVALID:
		return "INVALID";
	case PhysicalType::BIT:
		return "BIT";
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
	return (type >= PhysicalType::BOOL && type <= PhysicalType::DOUBLE) || type == PhysicalType::INTERVAL ||
	       type == PhysicalType::INT128;
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
		return "STRUCT";
	case LogicalTypeId::LIST:
		return "LIST";
	case LogicalTypeId::MAP:
		return "MAP";
	case LogicalTypeId::POINTER:
		return "POINTER";
	case LogicalTypeId::TABLE:
		return "TABLE";
	case LogicalTypeId::LAMBDA:
		return "LAMBDA";
	case LogicalTypeId::INVALID:
		return "INVALID";
	case LogicalTypeId::UNION:
		return "UNION";
	case LogicalTypeId::UNKNOWN:
		return "UNKNOWN";
	case LogicalTypeId::ENUM:
		return "ENUM";
	case LogicalTypeId::AGGREGATE_STATE:
		return "AGGREGATE_STATE";
	case LogicalTypeId::USER:
		return "USER";
	case LogicalTypeId::BIT:
		return "BIT";
	}
	return "UNDEFINED";
}

string LogicalType::ToString() const {
	auto alias = GetAlias();
	if (!alias.empty()) {
		return alias;
	}
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
		auto &key_type = MapType::KeyType(*this);
		auto &value_type = MapType::ValueType(*this);
		return "MAP(" + key_type.ToString() + ", " + value_type.ToString() + ")";
	}
	case LogicalTypeId::UNION: {
		if (!type_info_) {
			return "UNION";
		}
		string ret = "UNION(";
		size_t count = UnionType::GetMemberCount(*this);
		for (size_t i = 0; i < count; i++) {
			ret += UnionType::GetMemberName(*this, i) + " " + UnionType::GetMemberType(*this, i).ToString();
			if (i < count - 1) {
				ret += ", ";
			}
		}
		ret += ")";
		return ret;
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
	auto type = DefaultTypeGenerator::GetDefaultType(str);
	if (type == LogicalTypeId::INVALID) {
		// This is a User Type, at this point we don't know if its one of the User Defined Types or an error
		// It is checked in the binder
		type = LogicalTypeId::USER;
	}
	return type;
}

LogicalType TransformStringToLogicalType(const string &str) {
	if (StringUtil::Lower(str) == "null") {
		return LogicalType::SQLNULL;
	}
	return Parser::ParseColumnList("dummy " + str).GetColumn(LogicalIndex(0)).Type();
}

LogicalType GetUserTypeRecursive(const LogicalType &type, ClientContext &context) {
	if (type.id() == LogicalTypeId::USER && type.HasAlias()) {
		return Catalog::GetSystemCatalog(context).GetType(context, SYSTEM_CATALOG, DEFAULT_SCHEMA, type.GetAlias());
	}
	// Look for LogicalTypeId::USER in nested types
	if (type.id() == LogicalTypeId::STRUCT) {
		child_list_t<LogicalType> children;
		children.reserve(StructType::GetChildCount(type));
		for (auto &child : StructType::GetChildTypes(type)) {
			children.emplace_back(child.first, GetUserTypeRecursive(child.second, context));
		}
		return LogicalType::STRUCT(std::move(children));
	}
	if (type.id() == LogicalTypeId::LIST) {
		return LogicalType::LIST(GetUserTypeRecursive(ListType::GetChildType(type), context));
	}
	if (type.id() == LogicalTypeId::MAP) {
		return LogicalType::MAP(GetUserTypeRecursive(MapType::KeyType(type), context),
		                        GetUserTypeRecursive(MapType::ValueType(type), context));
	}
	// Not LogicalTypeId::USER or a nested type
	return type;
}

LogicalType TransformStringToLogicalType(const string &str, ClientContext &context) {
	return GetUserTypeRecursive(TransformStringToLogicalType(str), context);
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

bool LogicalType::IsValid() const {
	return id() != LogicalTypeId::INVALID && id() != LogicalTypeId::UNKNOWN;
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

//! Grows Decimal width/scale when appropriate
static LogicalType DecimalSizeCheck(const LogicalType &left, const LogicalType &right) {
	D_ASSERT(left.id() == LogicalTypeId::DECIMAL || right.id() == LogicalTypeId::DECIMAL);
	D_ASSERT(left.id() != right.id());

	//! Make sure the 'right' is the DECIMAL type
	if (left.id() == LogicalTypeId::DECIMAL) {
		return DecimalSizeCheck(right, left);
	}
	auto width = DecimalType::GetWidth(right);
	auto scale = DecimalType::GetScale(right);

	uint8_t other_width;
	uint8_t other_scale;
	bool success = left.GetDecimalProperties(other_width, other_scale);
	if (!success) {
		throw InternalException("Type provided to DecimalSizeCheck was not a numeric type");
	}
	D_ASSERT(other_scale == 0);
	const auto effective_width = width - scale;
	if (other_width > effective_width) {
		auto new_width = other_width + scale;
		//! Cap the width at max, if an actual value exceeds this, an exception will be thrown later
		if (new_width > DecimalType::MaxWidth()) {
			new_width = DecimalType::MaxWidth();
		}
		return LogicalType::DECIMAL(new_width, scale);
	}
	return right;
}

static LogicalType CombineNumericTypes(const LogicalType &left, const LogicalType &right) {
	D_ASSERT(left.id() != right.id());
	if (left.id() > right.id()) {
		// this method is symmetric
		// arrange it so the left type is smaller to limit the number of options we need to check
		return CombineNumericTypes(right, left);
	}
	if (CastRules::ImplicitCast(left, right) >= 0) {
		// we can implicitly cast left to right, return right
		//! Depending on the type, we might need to grow the `width` of the DECIMAL type
		if (right.id() == LogicalTypeId::DECIMAL) {
			return DecimalSizeCheck(left, right);
		}
		return right;
	}
	if (CastRules::ImplicitCast(right, left) >= 0) {
		// we can implicitly cast right to left, return left
		//! Depending on the type, we might need to grow the `width` of the DECIMAL type
		if (left.id() == LogicalTypeId::DECIMAL) {
			return DecimalSizeCheck(right, left);
		}
		return left;
	}
	// we can't cast implicitly either way and types are not equal
	// this happens when left is signed and right is unsigned
	// e.g. INTEGER and UINTEGER
	// in this case we need to upcast to make sure the types fit

	if (left.id() == LogicalTypeId::BIGINT || right.id() == LogicalTypeId::UBIGINT) {
		return LogicalType::HUGEINT;
	}
	if (left.id() == LogicalTypeId::INTEGER || right.id() == LogicalTypeId::UINTEGER) {
		return LogicalType::BIGINT;
	}
	if (left.id() == LogicalTypeId::SMALLINT || right.id() == LogicalTypeId::USMALLINT) {
		return LogicalType::INTEGER;
	}
	if (left.id() == LogicalTypeId::TINYINT || right.id() == LogicalTypeId::UTINYINT) {
		return LogicalType::SMALLINT;
	}
	throw InternalException("Cannot combine these numeric types!?");
}

LogicalType LogicalType::MaxLogicalType(const LogicalType &left, const LogicalType &right) {
	// we always prefer aliased types
	if (!left.GetAlias().empty()) {
		return left;
	}
	if (!right.GetAlias().empty()) {
		return right;
	}
	if (left.id() != right.id() && left.IsNumeric() && right.IsNumeric()) {
		return CombineNumericTypes(left, right);
	} else if (left.id() == LogicalTypeId::UNKNOWN) {
		return right;
	} else if (right.id() == LogicalTypeId::UNKNOWN) {
		return left;
	} else if (left.id() < right.id()) {
		return right;
	}
	if (right.id() < left.id()) {
		return left;
	}
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
		}
		return right;
	}
	if (type_id == LogicalTypeId::DECIMAL) {
		// unify the width/scale so that the resulting decimal always fits
		// "width - scale" gives us the number of digits on the left side of the decimal point
		// "scale" gives us the number of digits allowed on the right of the decimal point
		// using the max of these of the two types gives us the new decimal size
		auto extra_width_left = DecimalType::GetWidth(left) - DecimalType::GetScale(left);
		auto extra_width_right = DecimalType::GetWidth(right) - DecimalType::GetScale(right);
		auto extra_width = MaxValue<uint8_t>(extra_width_left, extra_width_right);
		auto scale = MaxValue<uint8_t>(DecimalType::GetScale(left), DecimalType::GetScale(right));
		auto width = extra_width + scale;
		if (width > DecimalType::MaxWidth()) {
			// if the resulting decimal does not fit, we truncate the scale
			width = DecimalType::MaxWidth();
			scale = width - extra_width;
		}
		return LogicalType::DECIMAL(width, scale);
	}
	if (type_id == LogicalTypeId::LIST) {
		// list: perform max recursively on child type
		auto new_child = MaxLogicalType(ListType::GetChildType(left), ListType::GetChildType(right));
		return LogicalType::LIST(std::move(new_child));
	}
	if (type_id == LogicalTypeId::MAP) {
		// list: perform max recursively on child type
		auto new_child = MaxLogicalType(ListType::GetChildType(left), ListType::GetChildType(right));
		return LogicalType::MAP(std::move(new_child));
	}
	if (type_id == LogicalTypeId::STRUCT) {
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
			child_types.push_back(make_pair(left_child_types[i].first, std::move(child_type)));
		}

		return LogicalType::STRUCT(std::move(child_types));
	}
	if (type_id == LogicalTypeId::UNION) {
		auto left_member_count = UnionType::GetMemberCount(left);
		auto right_member_count = UnionType::GetMemberCount(right);
		if (left_member_count != right_member_count) {
			// return the "larger" type, with the most members
			return left_member_count > right_member_count ? left : right;
		}
		// otherwise, keep left, dont try to meld the two together.
		return left;
	}
	// types are equal but no extra specifier: just return the type
	return left;
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
	float epsilon = std::fabs(rdecimal) * 0.01 + 0.00000001;
	return std::fabs(ldecimal - rdecimal) <= epsilon;
}

bool ApproxEqual(double ldecimal, double rdecimal) {
	if (Value::IsNan(ldecimal) && Value::IsNan(rdecimal)) {
		return true;
	}
	if (!Value::DoubleIsFinite(ldecimal) || !Value::DoubleIsFinite(rdecimal)) {
		return ldecimal == rdecimal;
	}
	double epsilon = std::fabs(rdecimal) * 0.01 + 0.00000001;
	return std::fabs(ldecimal - rdecimal) <= epsilon;
}

//===--------------------------------------------------------------------===//
// Extra Type Info
//===--------------------------------------------------------------------===//
enum class ExtraTypeInfoType : uint8_t {
	INVALID_TYPE_INFO = 0,
	GENERIC_TYPE_INFO = 1,
	DECIMAL_TYPE_INFO = 2,
	STRING_TYPE_INFO = 3,
	LIST_TYPE_INFO = 4,
	STRUCT_TYPE_INFO = 5,
	ENUM_TYPE_INFO = 6,
	USER_TYPE_INFO = 7,
	AGGREGATE_STATE_TYPE_INFO = 8
};

struct ExtraTypeInfo {
	explicit ExtraTypeInfo(ExtraTypeInfoType type) : type(type) {
	}
	explicit ExtraTypeInfo(ExtraTypeInfoType type, string alias) : type(type), alias(std::move(alias)) {
	}
	virtual ~ExtraTypeInfo() {
	}

	ExtraTypeInfoType type;
	string alias;
	TypeCatalogEntry *catalog_entry = nullptr;

public:
	bool Equals(ExtraTypeInfo *other_p) const {
		if (type == ExtraTypeInfoType::INVALID_TYPE_INFO || type == ExtraTypeInfoType::STRING_TYPE_INFO ||
		    type == ExtraTypeInfoType::GENERIC_TYPE_INFO) {
			if (!other_p) {
				if (!alias.empty()) {
					return false;
				}
				//! We only need to compare aliases when both types have them in this case
				return true;
			}
			if (alias != other_p->alias) {
				return false;
			}
			return true;
		}
		if (!other_p) {
			return false;
		}
		if (type != other_p->type) {
			return false;
		}
		return alias == other_p->alias && EqualsInternal(other_p);
	}
	//! Serializes a ExtraTypeInfo to a stand-alone binary blob
	virtual void Serialize(FieldWriter &writer) const {};
	//! Serializes a ExtraTypeInfo to a stand-alone binary blob
	static void Serialize(ExtraTypeInfo *info, FieldWriter &writer);
	//! Deserializes a blob back into an ExtraTypeInfo
	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader);

protected:
	virtual bool EqualsInternal(ExtraTypeInfo *other_p) const {
		// Do nothing
		return true;
	}
};

void LogicalType::SetAlias(string alias) {
	if (!type_info_) {
		type_info_ = make_shared<ExtraTypeInfo>(ExtraTypeInfoType::GENERIC_TYPE_INFO, std::move(alias));
	} else {
		type_info_->alias = std::move(alias);
	}
}

string LogicalType::GetAlias() const {
	if (id() == LogicalTypeId::USER) {
		return UserType::GetTypeName(*this);
	}
	if (type_info_) {
		return type_info_->alias;
	}
	return string();
}

bool LogicalType::HasAlias() const {
	if (id() == LogicalTypeId::USER) {
		return !UserType::GetTypeName(*this).empty();
	}
	if (type_info_ && !type_info_->alias.empty()) {
		return true;
	}
	return false;
}

void LogicalType::SetCatalog(LogicalType &type, TypeCatalogEntry *catalog_entry) {
	auto info = type.AuxInfo();
	D_ASSERT(info);
	((ExtraTypeInfo &)*info).catalog_entry = catalog_entry;
}
TypeCatalogEntry *LogicalType::GetCatalog(const LogicalType &type) {
	auto info = type.AuxInfo();
	if (!info) {
		return nullptr;
	}
	return ((ExtraTypeInfo &)*info).catalog_entry;
}

//===--------------------------------------------------------------------===//
// Decimal Type
//===--------------------------------------------------------------------===//
struct DecimalTypeInfo : public ExtraTypeInfo {
	DecimalTypeInfo(uint8_t width_p, uint8_t scale_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::DECIMAL_TYPE_INFO), width(width_p), scale(scale_p) {
		D_ASSERT(width_p >= scale_p);
	}

	uint8_t width;
	uint8_t scale;

public:
	void Serialize(FieldWriter &writer) const override {
		writer.WriteField<uint8_t>(width);
		writer.WriteField<uint8_t>(scale);
	}

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader) {
		auto width = reader.ReadRequired<uint8_t>();
		auto scale = reader.ReadRequired<uint8_t>();
		return make_shared<DecimalTypeInfo>(width, scale);
	}

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override {
		auto &other = (DecimalTypeInfo &)*other_p;
		return width == other.width && scale == other.scale;
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

uint8_t DecimalType::MaxWidth() {
	return DecimalWidth<hugeint_t>::max;
}

LogicalType LogicalType::DECIMAL(int width, int scale) {
	D_ASSERT(width >= scale);
	auto type_info = make_shared<DecimalTypeInfo>(width, scale);
	return LogicalType(LogicalTypeId::DECIMAL, std::move(type_info));
}

//===--------------------------------------------------------------------===//
// String Type
//===--------------------------------------------------------------------===//
struct StringTypeInfo : public ExtraTypeInfo {
	explicit StringTypeInfo(string collation_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::STRING_TYPE_INFO), collation(std::move(collation_p)) {
	}

	string collation;

public:
	void Serialize(FieldWriter &writer) const override {
		writer.WriteString(collation);
	}

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader) {
		auto collation = reader.ReadRequired<string>();
		return make_shared<StringTypeInfo>(std::move(collation));
	}

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override {
		// collation info has no impact on equality
		return true;
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
	if (info->type == ExtraTypeInfoType::GENERIC_TYPE_INFO) {
		return string();
	}
	return ((StringTypeInfo &)*info).collation;
}

LogicalType LogicalType::VARCHAR_COLLATION(string collation) { // NOLINT
	auto string_info = make_shared<StringTypeInfo>(std::move(collation));
	return LogicalType(LogicalTypeId::VARCHAR, std::move(string_info));
}

//===--------------------------------------------------------------------===//
// List Type
//===--------------------------------------------------------------------===//
struct ListTypeInfo : public ExtraTypeInfo {
	explicit ListTypeInfo(LogicalType child_type_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::LIST_TYPE_INFO), child_type(std::move(child_type_p)) {
	}

	LogicalType child_type;

public:
	void Serialize(FieldWriter &writer) const override {
		writer.WriteSerializable(child_type);
	}

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader) {
		auto child_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
		return make_shared<ListTypeInfo>(std::move(child_type));
	}

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override {
		auto &other = (ListTypeInfo &)*other_p;
		return child_type == other.child_type;
	}
};

const LogicalType &ListType::GetChildType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::LIST || type.id() == LogicalTypeId::MAP);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((ListTypeInfo &)*info).child_type;
}

LogicalType LogicalType::LIST(LogicalType child) {
	auto info = make_shared<ListTypeInfo>(std::move(child));
	return LogicalType(LogicalTypeId::LIST, std::move(info));
}

//===--------------------------------------------------------------------===//
// Struct Type
//===--------------------------------------------------------------------===//
struct StructTypeInfo : public ExtraTypeInfo {
	explicit StructTypeInfo(child_list_t<LogicalType> child_types_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::STRUCT_TYPE_INFO), child_types(std::move(child_types_p)) {
	}

	child_list_t<LogicalType> child_types;

public:
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
			child_list.push_back(make_pair(std::move(name), std::move(type)));
		}
		return make_shared<StructTypeInfo>(std::move(child_list));
	}

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override {
		auto &other = (StructTypeInfo &)*other_p;
		return child_types == other.child_types;
	}
};

struct AggregateStateTypeInfo : public ExtraTypeInfo {
	explicit AggregateStateTypeInfo(aggregate_state_t state_type_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::AGGREGATE_STATE_TYPE_INFO), state_type(std::move(state_type_p)) {
	}

	aggregate_state_t state_type;

public:
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
			bound_argument_types.push_back(std::move(type));
		}
		return make_shared<AggregateStateTypeInfo>(
		    aggregate_state_t(std::move(function_name), std::move(return_type), std::move(bound_argument_types)));
	}

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override {
		auto &other = (AggregateStateTypeInfo &)*other_p;
		return state_type.function_name == other.state_type.function_name &&
		       state_type.return_type == other.state_type.return_type &&
		       state_type.bound_argument_types == other.state_type.bound_argument_types;
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
	D_ASSERT(type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::UNION);

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
	auto info = make_shared<StructTypeInfo>(std::move(children));
	return LogicalType(LogicalTypeId::STRUCT, std::move(info));
}

LogicalType LogicalType::AGGREGATE_STATE(aggregate_state_t state_type) { // NOLINT
	auto info = make_shared<AggregateStateTypeInfo>(std::move(state_type));
	return LogicalType(LogicalTypeId::AGGREGATE_STATE, std::move(info));
}

//===--------------------------------------------------------------------===//
// Map Type
//===--------------------------------------------------------------------===//
LogicalType LogicalType::MAP(LogicalType child) {
	auto info = make_shared<ListTypeInfo>(std::move(child));
	return LogicalType(LogicalTypeId::MAP, std::move(info));
}

LogicalType LogicalType::MAP(LogicalType key, LogicalType value) {
	child_list_t<LogicalType> child_types;
	child_types.push_back({"key", std::move(key)});
	child_types.push_back({"value", std::move(value)});
	return LogicalType::MAP(LogicalType::STRUCT(std::move(child_types)));
}

const LogicalType &MapType::KeyType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::MAP);
	return StructType::GetChildTypes(ListType::GetChildType(type))[0].second;
}

const LogicalType &MapType::ValueType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::MAP);
	return StructType::GetChildTypes(ListType::GetChildType(type))[1].second;
}

//===--------------------------------------------------------------------===//
// Union Type
//===--------------------------------------------------------------------===//

LogicalType LogicalType::UNION(child_list_t<LogicalType> members) {
	D_ASSERT(members.size() > 0);
	D_ASSERT(members.size() <= UnionType::MAX_UNION_MEMBERS);
	// union types always have a hidden "tag" field in front
	members.insert(members.begin(), {"", LogicalType::TINYINT});
	auto info = make_shared<StructTypeInfo>(std::move(members));
	return LogicalType(LogicalTypeId::UNION, std::move(info));
}

const LogicalType &UnionType::GetMemberType(const LogicalType &type, idx_t index) {
	auto &child_types = StructType::GetChildTypes(type);
	D_ASSERT(index < child_types.size());
	// skip the "tag" field
	return child_types[index + 1].second;
}

const string &UnionType::GetMemberName(const LogicalType &type, idx_t index) {
	auto &child_types = StructType::GetChildTypes(type);
	D_ASSERT(index < child_types.size());
	// skip the "tag" field
	return child_types[index + 1].first;
}

idx_t UnionType::GetMemberCount(const LogicalType &type) {
	// dont count the "tag" field
	return StructType::GetChildTypes(type).size() - 1;
}
const child_list_t<LogicalType> UnionType::CopyMemberTypes(const LogicalType &type) {
	auto child_types = StructType::GetChildTypes(type);
	child_types.erase(child_types.begin());
	return child_types;
}

//===--------------------------------------------------------------------===//
// User Type
//===--------------------------------------------------------------------===//
struct UserTypeInfo : public ExtraTypeInfo {
	explicit UserTypeInfo(string name_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::USER_TYPE_INFO), user_type_name(std::move(name_p)) {
	}

	string user_type_name;

public:
	void Serialize(FieldWriter &writer) const override {
		writer.WriteString(user_type_name);
	}

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader) {
		auto enum_name = reader.ReadRequired<string>();
		return make_shared<UserTypeInfo>(std::move(enum_name));
	}

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override {
		auto &other = (UserTypeInfo &)*other_p;
		return other.user_type_name == user_type_name;
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
	return LogicalType(LogicalTypeId::USER, std::move(info));
}

//===--------------------------------------------------------------------===//
// Enum Type
//===--------------------------------------------------------------------===//

enum EnumDictType : uint8_t { INVALID = 0, VECTOR_DICT = 1 };

struct EnumTypeInfo : public ExtraTypeInfo {
	explicit EnumTypeInfo(string enum_name_p, Vector &values_insert_order_p, idx_t dict_size_p)
	    : ExtraTypeInfo(ExtraTypeInfoType::ENUM_TYPE_INFO), dict_type(EnumDictType::VECTOR_DICT),
	      enum_name(std::move(enum_name_p)), values_insert_order(values_insert_order_p), dict_size(dict_size_p) {
	}
	EnumDictType dict_type;
	string enum_name;
	Vector values_insert_order;
	idx_t dict_size;

protected:
	// Equalities are only used in enums with different catalog entries
	bool EqualsInternal(ExtraTypeInfo *other_p) const override {
		auto &other = (EnumTypeInfo &)*other_p;
		if (dict_type != other.dict_type) {
			return false;
		}
		D_ASSERT(dict_type == EnumDictType::VECTOR_DICT);
		// We must check if both enums have the same size
		if (other.dict_size != dict_size) {
			return false;
		}
		auto other_vector_ptr = FlatVector::GetData<string_t>(other.values_insert_order);
		auto this_vector_ptr = FlatVector::GetData<string_t>(values_insert_order);

		// Now we must check if all strings are the same
		for (idx_t i = 0; i < dict_size; i++) {
			if (!Equals::Operation(other_vector_ptr[i], this_vector_ptr[i])) {
				return false;
			}
		}
		return true;
	}

	void Serialize(FieldWriter &writer) const override {
		if (dict_type != EnumDictType::VECTOR_DICT) {
			throw InternalException("Cannot serialize non-vector dictionary ENUM types");
		}
		writer.WriteField<uint32_t>(dict_size);
		writer.WriteString(enum_name);
		((Vector &)values_insert_order).Serialize(dict_size, writer.GetSerializer());
	}
};

template <class T>
struct EnumTypeInfoTemplated : public EnumTypeInfo {
	explicit EnumTypeInfoTemplated(const string &enum_name_p, Vector &values_insert_order_p, idx_t size_p)
	    : EnumTypeInfo(enum_name_p, values_insert_order_p, size_p) {
		D_ASSERT(values_insert_order_p.GetType().InternalType() == PhysicalType::VARCHAR);

		UnifiedVectorFormat vdata;
		values_insert_order.ToUnifiedFormat(size_p, vdata);

		auto data = (string_t *)vdata.data;
		for (idx_t i = 0; i < size_p; i++) {
			auto idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(idx)) {
				throw InternalException("Attempted to create ENUM type with NULL value");
			}
			if (values.count(data[idx]) > 0) {
				throw InvalidInputException("Attempted to create ENUM type with duplicate value %s",
				                            data[idx].GetString());
			}
			values[data[idx]] = i;
		}
	}

	static shared_ptr<EnumTypeInfoTemplated> Deserialize(FieldReader &reader, uint32_t size) {
		auto enum_name = reader.ReadRequired<string>();
		Vector values_insert_order(LogicalType::VARCHAR, size);
		values_insert_order.Deserialize(size, reader.GetSource());
		return make_shared<EnumTypeInfoTemplated>(std::move(enum_name), values_insert_order, size);
	}

	string_map_t<T> values;
};

const string &EnumType::GetTypeName(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return ((EnumTypeInfo &)*info).enum_name;
}

static PhysicalType EnumVectorDictType(idx_t size) {
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

LogicalType LogicalType::ENUM(const string &enum_name, Vector &ordered_data, idx_t size) {
	// Generate EnumTypeInfo
	shared_ptr<ExtraTypeInfo> info;
	auto enum_internal_type = EnumVectorDictType(size);
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
int64_t TemplatedGetPos(string_map_t<T> &map, const string_t &key) {
	auto it = map.find(key);
	if (it == map.end()) {
		return -1;
	}
	return it->second;
}

int64_t EnumType::GetPos(const LogicalType &type, const string_t &key) {
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
	return ((EnumTypeInfo &)*info).dict_size;
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

PhysicalType EnumType::GetPhysicalType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto aux_info = type.AuxInfo();
	D_ASSERT(aux_info);
	auto &info = (EnumTypeInfo &)*aux_info;
	D_ASSERT(info.dict_type == EnumDictType::VECTOR_DICT);
	return EnumVectorDictType(info.dict_size);
}

//===--------------------------------------------------------------------===//
// Extra Type Info
//===--------------------------------------------------------------------===//
void ExtraTypeInfo::Serialize(ExtraTypeInfo *info, FieldWriter &writer) {
	if (!info) {
		writer.WriteField<ExtraTypeInfoType>(ExtraTypeInfoType::INVALID_TYPE_INFO);
		writer.WriteString(string());
	} else {
		writer.WriteField<ExtraTypeInfoType>(info->type);
		info->Serialize(writer);
		writer.WriteString(info->alias);
	}
}
shared_ptr<ExtraTypeInfo> ExtraTypeInfo::Deserialize(FieldReader &reader) {
	auto type = reader.ReadRequired<ExtraTypeInfoType>();
	shared_ptr<ExtraTypeInfo> extra_info;
	switch (type) {
	case ExtraTypeInfoType::INVALID_TYPE_INFO: {
		auto alias = reader.ReadField<string>(string());
		if (!alias.empty()) {
			return make_shared<ExtraTypeInfo>(type, alias);
		}
		return nullptr;
	}
	case ExtraTypeInfoType::GENERIC_TYPE_INFO: {
		extra_info = make_shared<ExtraTypeInfo>(type);
	} break;
	case ExtraTypeInfoType::DECIMAL_TYPE_INFO:
		extra_info = DecimalTypeInfo::Deserialize(reader);
		break;
	case ExtraTypeInfoType::STRING_TYPE_INFO:
		extra_info = StringTypeInfo::Deserialize(reader);
		break;
	case ExtraTypeInfoType::LIST_TYPE_INFO:
		extra_info = ListTypeInfo::Deserialize(reader);
		break;
	case ExtraTypeInfoType::STRUCT_TYPE_INFO:
		extra_info = StructTypeInfo::Deserialize(reader);
		break;
	case ExtraTypeInfoType::USER_TYPE_INFO:
		extra_info = UserTypeInfo::Deserialize(reader);
		break;
	case ExtraTypeInfoType::ENUM_TYPE_INFO: {
		auto enum_size = reader.ReadRequired<uint32_t>();
		auto enum_internal_type = EnumVectorDictType(enum_size);
		switch (enum_internal_type) {
		case PhysicalType::UINT8:
			extra_info = EnumTypeInfoTemplated<uint8_t>::Deserialize(reader, enum_size);
			break;
		case PhysicalType::UINT16:
			extra_info = EnumTypeInfoTemplated<uint16_t>::Deserialize(reader, enum_size);
			break;
		case PhysicalType::UINT32:
			extra_info = EnumTypeInfoTemplated<uint32_t>::Deserialize(reader, enum_size);
			break;
		default:
			throw InternalException("Invalid Physical Type for ENUMs");
		}
	} break;
	case ExtraTypeInfoType::AGGREGATE_STATE_TYPE_INFO:
		extra_info = AggregateStateTypeInfo::Deserialize(reader);
		break;

	default:
		throw InternalException("Unimplemented type info in ExtraTypeInfo::Deserialize");
	}
	auto alias = reader.ReadField<string>(string());
	extra_info->alias = alias;
	return extra_info;
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

	return LogicalType(id, std::move(info));
}

bool LogicalType::EqualTypeInfo(const LogicalType &rhs) const {
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

bool LogicalType::operator==(const LogicalType &rhs) const {
	if (id_ != rhs.id_) {
		return false;
	}
	return EqualTypeInfo(rhs);
}

} // namespace duckdb
