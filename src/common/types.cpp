#include "duckdb/common/types.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/main/config.hpp"
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
	case LogicalTypeId::UHUGEINT:
		return PhysicalType::UINT128;
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
	case LogicalTypeId::VARINT:
		return PhysicalType::VARCHAR;
	case LogicalTypeId::INTERVAL:
		return PhysicalType::INTERVAL;
	case LogicalTypeId::UNION:
	case LogicalTypeId::STRUCT:
		return PhysicalType::STRUCT;
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		return PhysicalType::LIST;
	case LogicalTypeId::ARRAY:
		return PhysicalType::ARRAY;
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
	case LogicalTypeId::STRING_LITERAL:
	case LogicalTypeId::INTEGER_LITERAL:
		return PhysicalType::INVALID;
	case LogicalTypeId::USER:
		return PhysicalType::UNKNOWN;
	case LogicalTypeId::AGGREGATE_STATE:
		return PhysicalType::VARCHAR;
	default:
		throw InternalException("Invalid LogicalType %s", ToString());
	}
}

// **DEPRECATED**: Use EnumUtil directly instead.
string LogicalTypeIdToString(LogicalTypeId type) {
	return EnumUtil::ToString(type);
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
constexpr const LogicalTypeId LogicalType::UHUGEINT;
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
constexpr const LogicalTypeId LogicalType::VARINT;

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
	                             LogicalType::USMALLINT, LogicalType::UINTEGER,  LogicalType::UBIGINT,
	                             LogicalType::UHUGEINT};
	return types;
}

const vector<LogicalType> LogicalType::Integral() {
	vector<LogicalType> types = {LogicalType::TINYINT,   LogicalType::SMALLINT, LogicalType::INTEGER,
	                             LogicalType::BIGINT,    LogicalType::HUGEINT,  LogicalType::UTINYINT,
	                             LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT,
	                             LogicalType::UHUGEINT};
	return types;
}

const vector<LogicalType> LogicalType::Real() {
	vector<LogicalType> types = {LogicalType::FLOAT, LogicalType::DOUBLE};
	return types;
}

const vector<LogicalType> LogicalType::AllTypes() {
	vector<LogicalType> types = {
	    LogicalType::BOOLEAN,  LogicalType::TINYINT,      LogicalType::SMALLINT,  LogicalType::INTEGER,
	    LogicalType::BIGINT,   LogicalType::DATE,         LogicalType::TIMESTAMP, LogicalType::DOUBLE,
	    LogicalType::FLOAT,    LogicalType::VARCHAR,      LogicalType::BLOB,      LogicalType::BIT,
	    LogicalType::VARINT,   LogicalType::INTERVAL,     LogicalType::HUGEINT,   LogicalTypeId::DECIMAL,
	    LogicalType::UTINYINT, LogicalType::USMALLINT,    LogicalType::UINTEGER,  LogicalType::UBIGINT,
	    LogicalType::UHUGEINT, LogicalType::TIME,         LogicalTypeId::LIST,    LogicalTypeId::STRUCT,
	    LogicalType::TIME_TZ,  LogicalType::TIMESTAMP_TZ, LogicalTypeId::MAP,     LogicalTypeId::UNION,
	    LogicalType::UUID,     LogicalTypeId::ARRAY};
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
	case PhysicalType::UINT128:
		return "UINT128";
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
	case PhysicalType::ARRAY:
		return "ARRAY";
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
	case PhysicalType::UINT128:
		return sizeof(uhugeint_t);
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
	case PhysicalType::ARRAY:
		return 0; // no own payload
	case PhysicalType::LIST:
		return sizeof(list_entry_t); // offset + len
	default:
		throw InternalException("Invalid PhysicalType for GetTypeIdSize");
	}
}

bool TypeIsConstantSize(PhysicalType type) {
	return (type >= PhysicalType::BOOL && type <= PhysicalType::DOUBLE) || type == PhysicalType::INTERVAL ||
	       type == PhysicalType::INT128 || type == PhysicalType::UINT128;
}
bool TypeIsIntegral(PhysicalType type) {
	return (type >= PhysicalType::UINT8 && type <= PhysicalType::INT64) || type == PhysicalType::INT128 ||
	       type == PhysicalType::UINT128;
}
bool TypeIsNumeric(PhysicalType type) {
	return (type >= PhysicalType::UINT8 && type <= PhysicalType::DOUBLE) || type == PhysicalType::INT128 ||
	       type == PhysicalType::UINT128;
}
bool TypeIsInteger(PhysicalType type) {
	return (type >= PhysicalType::UINT8 && type <= PhysicalType::INT64) || type == PhysicalType::INT128 ||
	       type == PhysicalType::UINT128;
}

string LogicalType::ToString() const {
	if (id_ != LogicalTypeId::USER) {
		auto alias = GetAlias();
		if (!alias.empty()) {
			auto mods_ptr = GetModifiers();
			if (mods_ptr && !mods_ptr->empty()) {
				auto &mods = *mods_ptr;
				alias += "(";
				for (idx_t i = 0; i < mods.size(); i++) {
					alias += mods[i].ToString();
					if (i < mods.size() - 1) {
						alias += ", ";
					}
				}
				alias += ")";
			}
			return alias;
		}
	}
	switch (id_) {
	case LogicalTypeId::STRUCT: {
		if (!type_info_) {
			return "STRUCT";
		}
		auto is_unnamed = StructType::IsUnnamed(*this);
		auto &child_types = StructType::GetChildTypes(*this);
		string ret = "STRUCT(";
		for (size_t i = 0; i < child_types.size(); i++) {
			if (is_unnamed) {
				ret += child_types[i].second.ToString();
			} else {
				ret += StringUtil::Format("%s %s", SQLIdentifier(child_types[i].first), child_types[i].second);
			}
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
			auto member_name = UnionType::GetMemberName(*this, i);
			auto member_type = UnionType::GetMemberType(*this, i).ToString();
			ret += StringUtil::Format("%s %s", SQLIdentifier(member_name), member_type);
			if (i < count - 1) {
				ret += ", ";
			}
		}
		ret += ")";
		return ret;
	}
	case LogicalTypeId::ARRAY: {
		if (!type_info_) {
			return "ARRAY";
		}
		auto size = ArrayType::GetSize(*this);
		if (size == 0) {
			return ArrayType::GetChildType(*this).ToString() + "[ANY]";
		} else {
			return ArrayType::GetChildType(*this).ToString() + "[" + to_string(size) + "]";
		}
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
		string ret = "ENUM(";
		for (idx_t i = 0; i < EnumType::GetSize(*this); i++) {
			if (i > 0) {
				ret += ", ";
			}
			ret += KeywordHelper::WriteQuoted(EnumType::GetString(*this, i).GetString(), '\'');
		}
		ret += ")";
		return ret;
	}
	case LogicalTypeId::USER: {
		string result;
		auto &catalog = UserType::GetCatalog(*this);
		auto &schema = UserType::GetSchema(*this);
		auto &type = UserType::GetTypeName(*this);
		auto &mods = UserType::GetTypeModifiers(*this);

		if (!catalog.empty()) {
			result = KeywordHelper::WriteOptionallyQuoted(catalog);
		}
		if (!schema.empty()) {
			if (!result.empty()) {
				result += ".";
			}
			result += KeywordHelper::WriteOptionallyQuoted(schema);
		}
		if (!result.empty()) {
			result += ".";
		}
		result += KeywordHelper::WriteOptionallyQuoted(type);

		if (!mods.empty()) {
			result += "(";
			for (idx_t i = 0; i < mods.size(); i++) {
				result += mods[i].ToString();
				if (i < mods.size() - 1) {
					result += ", ";
				}
			}
			result += ")";
		}

		return result;
	}
	case LogicalTypeId::AGGREGATE_STATE: {
		return AggregateStateType::GetTypeName(*this);
	}
	case LogicalTypeId::SQLNULL: {
		return "\"NULL\"";
	}
	default:
		return EnumUtil::ToString(id_);
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
	ColumnList column_list;
	try {
		column_list = Parser::ParseColumnList("dummy " + str);
	} catch (const std::runtime_error &e) {
		const vector<string> suggested_types {"BIGINT",
		                                      "INT8",
		                                      "LONG",
		                                      "BIT",
		                                      "BITSTRING",
		                                      "BLOB",
		                                      "BYTEA",
		                                      "BINARY,",
		                                      "VARBINARY",
		                                      "BOOLEAN",
		                                      "BOOL",
		                                      "LOGICAL",
		                                      "DATE",
		                                      "DECIMAL(prec, scale)",
		                                      "DOUBLE",
		                                      "FLOAT8",
		                                      "FLOAT",
		                                      "FLOAT4",
		                                      "REAL",
		                                      "HUGEINT",
		                                      "INTEGER",
		                                      "INT4",
		                                      "INT",
		                                      "SIGNED",
		                                      "INTERVAL",
		                                      "SMALLINT",
		                                      "INT2",
		                                      "SHORT",
		                                      "TIME",
		                                      "TIMESTAMPTZ	",
		                                      "TIMESTAMP",
		                                      "DATETIME",
		                                      "TINYINT",
		                                      "INT1",
		                                      "UBIGINT",
		                                      "UHUGEINT",
		                                      "UINTEGER",
		                                      "USMALLINT",
		                                      "UTINYINT",
		                                      "UUID",
		                                      "VARCHAR",
		                                      "CHAR",
		                                      "BPCHAR",
		                                      "TEXT",
		                                      "STRING",
		                                      "MAP(INTEGER, VARCHAR)",
		                                      "UNION(num INTEGER, text VARCHAR)"};
		std::ostringstream error;
		error << "Value \"" << str << "\" can not be converted to a DuckDB Type." << '\n';
		error << "Possible examples as suggestions: " << '\n';
		auto suggestions = StringUtil::TopNJaroWinkler(suggested_types, str);
		for (auto &suggestion : suggestions) {
			error << "* " << suggestion << '\n';
		}
		throw InvalidInputException(error.str());
	}
	return column_list.GetColumn(LogicalIndex(0)).Type();
}

LogicalType GetUserTypeRecursive(const LogicalType &type, ClientContext &context) {
	if (type.id() == LogicalTypeId::USER && type.HasAlias()) {
		auto &type_entry =
		    Catalog::GetEntry<TypeCatalogEntry>(context, INVALID_CATALOG, INVALID_SCHEMA, type.GetAlias());
		return type_entry.user_type;
	}
	// Look for LogicalTypeId::USER in nested types
	if (type.id() == LogicalTypeId::STRUCT) {
		child_list_t<LogicalType> children;
		children.reserve(StructType::GetChildCount(type));
		for (auto &child : StructType::GetChildTypes(type)) {
			children.emplace_back(child.first, GetUserTypeRecursive(child.second, context));
		}
		return LogicalType::STRUCT(children);
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
	case LogicalTypeId::UHUGEINT:
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
	case LogicalTypeId::UHUGEINT:
		return true;
	default:
		return false;
	}
}

bool LogicalType::IsTemporal() const {
	switch (id_) {
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
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
	case LogicalTypeId::UHUGEINT:
		// hugeint: max size decimal (38, 0)
		// note that a uhugeint is not guaranteed to fit in this
		width = 38;
		scale = 0;
		break;
	case LogicalTypeId::DECIMAL:
		width = DecimalType::GetWidth(*this);
		scale = DecimalType::GetScale(*this);
		break;
	case LogicalTypeId::INTEGER_LITERAL:
		return IntegerLiteral::GetType(*this).GetDecimalProperties(width, scale);
	default:
		// Nonsense values to ensure initialization
		width = 255u;
		scale = 255u;
		// FIXME(carlo): This should be probably a throw, requires checkign the various call-sites
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
		auto new_width = NumericCast<uint8_t>(other_width + scale);
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

	// No type is larger than (u)hugeint, so casting to double is required
	// UHUGEINT is on the left because the enum is lower
	if (left.id() == LogicalTypeId::UHUGEINT || right.id() == LogicalTypeId::HUGEINT) {
		return LogicalType::DOUBLE;
	}
	throw InternalException("Cannot combine these numeric types (%s & %s)", left.ToString(), right.ToString());
}

LogicalType LogicalType::NormalizeType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::STRING_LITERAL:
		return LogicalType::VARCHAR;
	case LogicalTypeId::INTEGER_LITERAL:
		return IntegerLiteral::GetType(type);
	case LogicalTypeId::UNKNOWN:
		throw ParameterNotResolvedException();
	default:
		return type;
	}
}

template <class OP>
static bool CombineUnequalTypes(const LogicalType &left, const LogicalType &right, LogicalType &result) {
	// left and right are not equal
	// for enums, match the varchar rules
	if (left.id() == LogicalTypeId::ENUM) {
		return OP::Operation(LogicalType::VARCHAR, right, result);
	} else if (right.id() == LogicalTypeId::ENUM) {
		return OP::Operation(left, LogicalType::VARCHAR, result);
	}
	// NULL/string literals/unknown (parameter) types always take the other type
	LogicalTypeId other_types[] = {LogicalTypeId::SQLNULL, LogicalTypeId::UNKNOWN, LogicalTypeId::STRING_LITERAL};
	for (auto &other_type : other_types) {
		if (left.id() == other_type) {
			result = LogicalType::NormalizeType(right);
			return true;
		} else if (right.id() == other_type) {
			result = LogicalType::NormalizeType(left);
			return true;
		}
	}

	// for other types - use implicit cast rules to check if we can combine the types
	auto left_to_right_cost = CastRules::ImplicitCast(left, right);
	auto right_to_left_cost = CastRules::ImplicitCast(right, left);
	if (left_to_right_cost >= 0 && (left_to_right_cost < right_to_left_cost || right_to_left_cost < 0)) {
		// we can implicitly cast left to right, return right
		//! Depending on the type, we might need to grow the `width` of the DECIMAL type
		if (right.id() == LogicalTypeId::DECIMAL) {
			result = DecimalSizeCheck(left, right);
		} else {
			result = right;
		}
		return true;
	}
	if (right_to_left_cost >= 0) {
		// we can implicitly cast right to left, return left
		//! Depending on the type, we might need to grow the `width` of the DECIMAL type
		if (left.id() == LogicalTypeId::DECIMAL) {
			result = DecimalSizeCheck(right, left);
		} else {
			result = left;
		}
		return true;
	}
	// for integer literals - rerun the operation with the underlying type
	if (left.id() == LogicalTypeId::INTEGER_LITERAL) {
		return OP::Operation(IntegerLiteral::GetType(left), right, result);
	}
	if (right.id() == LogicalTypeId::INTEGER_LITERAL) {
		return OP::Operation(left, IntegerLiteral::GetType(right), result);
	}
	// for unsigned/signed comparisons we have a few fallbacks
	if (left.IsNumeric() && right.IsNumeric()) {
		result = CombineNumericTypes(left, right);
		return true;
	}
	if (left.id() == LogicalTypeId::BOOLEAN && right.IsIntegral()) {
		result = right;
		return true;
	}
	if (right.id() == LogicalTypeId::BOOLEAN && left.IsIntegral()) {
		result = left;
		return true;
	}
	return false;
}

template <class OP>
static bool CombineEqualTypes(const LogicalType &left, const LogicalType &right, LogicalType &result) {
	// Since both left and right are equal we get the left type as our type_id for checks
	auto type_id = left.id();
	switch (type_id) {
	case LogicalTypeId::STRING_LITERAL:
		// two string literals convert to varchar
		result = LogicalType::VARCHAR;
		return true;
	case LogicalTypeId::INTEGER_LITERAL:
		// for two integer literals we unify the underlying types
		return OP::Operation(IntegerLiteral::GetType(left), IntegerLiteral::GetType(right), result);
	case LogicalTypeId::ENUM:
		// If both types are different ENUMs we do a string comparison.
		result = left == right ? left : LogicalType::VARCHAR;
		return true;
	case LogicalTypeId::VARCHAR:
		// varchar: use type that has collation (if any)
		if (StringType::GetCollation(right).empty()) {
			result = left;
		} else {
			result = right;
		}
		return true;
	case LogicalTypeId::DECIMAL: {
		// unify the width/scale so that the resulting decimal always fits
		// "width - scale" gives us the number of digits on the left side of the decimal point
		// "scale" gives us the number of digits allowed on the right of the decimal point
		// using the max of these of the two types gives us the new decimal size
		auto extra_width_left = DecimalType::GetWidth(left) - DecimalType::GetScale(left);
		auto extra_width_right = DecimalType::GetWidth(right) - DecimalType::GetScale(right);
		auto extra_width =
		    MaxValue<uint8_t>(NumericCast<uint8_t>(extra_width_left), NumericCast<uint8_t>(extra_width_right));
		auto scale = MaxValue<uint8_t>(DecimalType::GetScale(left), DecimalType::GetScale(right));
		auto width = NumericCast<uint8_t>(extra_width + scale);
		if (width > DecimalType::MaxWidth()) {
			// if the resulting decimal does not fit, we truncate the scale
			width = DecimalType::MaxWidth();
			scale = NumericCast<uint8_t>(width - extra_width);
		}
		result = LogicalType::DECIMAL(width, scale);
		return true;
	}
	case LogicalTypeId::LIST: {
		// list: perform max recursively on child type
		LogicalType new_child;
		if (!OP::Operation(ListType::GetChildType(left), ListType::GetChildType(right), new_child)) {
			return false;
		}
		result = LogicalType::LIST(new_child);
		return true;
	}
	case LogicalTypeId::ARRAY: {
		LogicalType new_child;
		if (!OP::Operation(ArrayType::GetChildType(left), ArrayType::GetChildType(right), new_child)) {
			return false;
		}
		auto new_size = MaxValue(ArrayType::GetSize(left), ArrayType::GetSize(right));
		result = LogicalType::ARRAY(new_child, new_size);
		return true;
	}
	case LogicalTypeId::MAP: {
		// map: perform max recursively on child type
		LogicalType new_child;
		if (!OP::Operation(ListType::GetChildType(left), ListType::GetChildType(right), new_child)) {
			return false;
		}
		result = LogicalType::MAP(new_child);
		return true;
	}
	case LogicalTypeId::STRUCT: {
		// struct: perform recursively on each child
		auto &left_child_types = StructType::GetChildTypes(left);
		auto &right_child_types = StructType::GetChildTypes(right);
		bool left_unnamed = StructType::IsUnnamed(left);
		auto any_unnamed = left_unnamed || StructType::IsUnnamed(right);
		if (left_child_types.size() != right_child_types.size()) {
			// child types are not of equal size, we can't cast
			// return false
			return false;
		}
		child_list_t<LogicalType> child_types;
		for (idx_t i = 0; i < left_child_types.size(); i++) {
			LogicalType child_type;
			// Child names must be in the same order OR either one of the structs must be unnamed
			if (!any_unnamed && !StringUtil::CIEquals(left_child_types[i].first, right_child_types[i].first)) {
				return false;
			}
			if (!OP::Operation(left_child_types[i].second, right_child_types[i].second, child_type)) {
				return false;
			}
			auto &child_name = left_unnamed ? right_child_types[i].first : left_child_types[i].first;
			child_types.emplace_back(child_name, std::move(child_type));
		}
		result = LogicalType::STRUCT(child_types);
		return true;
	}
	case LogicalTypeId::UNION: {
		auto left_member_count = UnionType::GetMemberCount(left);
		auto right_member_count = UnionType::GetMemberCount(right);
		if (left_member_count != right_member_count) {
			// return the "larger" type, with the most members
			result = left_member_count > right_member_count ? left : right;
			return true;
		}
		// otherwise, keep left, don't try to meld the two together.
		result = left;
		return true;
	}
	default:
		result = left;
		return true;
	}
}

template <class OP>
bool TryGetMaxLogicalTypeInternal(const LogicalType &left, const LogicalType &right, LogicalType &result) {
	// we always prefer aliased types
	if (!left.GetAlias().empty()) {
		result = left;
		return true;
	}
	if (!right.GetAlias().empty()) {
		result = right;
		return true;
	}
	if (left.id() != right.id()) {
		return CombineUnequalTypes<OP>(left, right, result);
	} else {
		return CombineEqualTypes<OP>(left, right, result);
	}
}

struct TryGetTypeOperation {
	static bool Operation(const LogicalType &left, const LogicalType &right, LogicalType &result) {
		return TryGetMaxLogicalTypeInternal<TryGetTypeOperation>(left, right, result);
	}
};

struct ForceGetTypeOperation {
	static bool Operation(const LogicalType &left, const LogicalType &right, LogicalType &result) {
		result = LogicalType::ForceMaxLogicalType(left, right);
		return true;
	}
};

bool LogicalType::TryGetMaxLogicalType(ClientContext &context, const LogicalType &left, const LogicalType &right,
                                       LogicalType &result) {
	if (DBConfig::GetConfig(context).options.old_implicit_casting) {
		result = LogicalType::ForceMaxLogicalType(left, right);
		return true;
	}
	return TryGetMaxLogicalTypeInternal<TryGetTypeOperation>(left, right, result);
}

static idx_t GetLogicalTypeScore(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::INVALID:
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::UNKNOWN:
	case LogicalTypeId::ANY:
	case LogicalTypeId::STRING_LITERAL:
	case LogicalTypeId::INTEGER_LITERAL:
		return 0;
	// numerics
	case LogicalTypeId::BOOLEAN:
		return 10;
	case LogicalTypeId::UTINYINT:
		return 11;
	case LogicalTypeId::TINYINT:
		return 12;
	case LogicalTypeId::USMALLINT:
		return 13;
	case LogicalTypeId::SMALLINT:
		return 14;
	case LogicalTypeId::UINTEGER:
		return 15;
	case LogicalTypeId::INTEGER:
		return 16;
	case LogicalTypeId::UBIGINT:
		return 17;
	case LogicalTypeId::BIGINT:
		return 18;
	case LogicalTypeId::UHUGEINT:
		return 19;
	case LogicalTypeId::HUGEINT:
		return 20;
	case LogicalTypeId::DECIMAL:
		return 21;
	case LogicalTypeId::FLOAT:
		return 22;
	case LogicalTypeId::DOUBLE:
		return 23;
	// date/time/timestamp
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return 50;
	case LogicalTypeId::DATE:
		return 51;
	case LogicalTypeId::TIMESTAMP_SEC:
		return 52;
	case LogicalTypeId::TIMESTAMP_MS:
		return 53;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return 54;
	case LogicalTypeId::TIMESTAMP_NS:
		return 55;
	case LogicalTypeId::INTERVAL:
		return 56;
	// text/character strings
	case LogicalTypeId::CHAR:
		return 75;
	case LogicalTypeId::VARCHAR:
		return 77;
	case LogicalTypeId::ENUM:
		return 78;
	// blob/complex types
	case LogicalTypeId::BIT:
		return 100;
	case LogicalTypeId::BLOB:
		return 101;
	case LogicalTypeId::UUID:
		return 102;
	case LogicalTypeId::VARINT:
		return 103;
	// nested types
	case LogicalTypeId::STRUCT:
		return 125;
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY:
		return 126;
	case LogicalTypeId::MAP:
		return 127;
	case LogicalTypeId::UNION:
	case LogicalTypeId::TABLE:
		return 150;
	// weirdo types
	case LogicalTypeId::LAMBDA:
	case LogicalTypeId::AGGREGATE_STATE:
	case LogicalTypeId::POINTER:
	case LogicalTypeId::VALIDITY:
	case LogicalTypeId::USER:
		break;
	}
	return 1000;
}

LogicalType LogicalType::ForceMaxLogicalType(const LogicalType &left, const LogicalType &right) {
	LogicalType result;
	if (TryGetMaxLogicalTypeInternal<ForceGetTypeOperation>(left, right, result)) {
		return result;
	}
	// we prefer the type with the highest score
	auto left_score = GetLogicalTypeScore(left);
	auto right_score = GetLogicalTypeScore(right);
	if (left_score < right_score) {
		return right;
	} else {
		return left;
	}
}

LogicalType LogicalType::MaxLogicalType(ClientContext &context, const LogicalType &left, const LogicalType &right) {
	LogicalType result;
	if (!TryGetMaxLogicalType(context, left, right, result)) {
		throw NotImplementedException("Cannot combine types %s and %s - an explicit cast is required", left.ToString(),
		                              right.ToString());
	}
	return result;
}

void LogicalType::Verify() const {
#ifdef DEBUG
	switch (id_) {
	case LogicalTypeId::DECIMAL:
		D_ASSERT(DecimalType::GetWidth(*this) >= 1 && DecimalType::GetWidth(*this) <= Decimal::MAX_WIDTH_DECIMAL);
		D_ASSERT(DecimalType::GetScale(*this) >= 0 && DecimalType::GetScale(*this) <= DecimalType::GetWidth(*this));
		break;
	case LogicalTypeId::STRUCT: {
		// verify child types
		case_insensitive_set_t child_names;
		bool all_empty = true;
		for (auto &entry : StructType::GetChildTypes(*this)) {
			if (entry.first.empty()) {
				D_ASSERT(all_empty);
			} else {
				// check for duplicate struct names
				all_empty = false;
				auto existing_entry = child_names.find(entry.first);
				D_ASSERT(existing_entry == child_names.end());
				child_names.insert(entry.first);
			}
			entry.second.Verify();
		}
		break;
	}
	case LogicalTypeId::LIST:
		ListType::GetChildType(*this).Verify();
		break;
	case LogicalTypeId::MAP: {
		MapType::KeyType(*this).Verify();
		MapType::ValueType(*this).Verify();
		break;
	}
	default:
		break;
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
	float epsilon = static_cast<float>(std::fabs(rdecimal) * 0.01 + 0.00000001);
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

LogicalType LogicalType::DeepCopy() const {
	LogicalType copy = *this;
	if (type_info_) {
		copy.type_info_ = type_info_->Copy();
	}
	return copy;
}

void LogicalType::SetAlias(string alias) {
	if (!type_info_) {
		type_info_ = make_shared_ptr<ExtraTypeInfo>(ExtraTypeInfoType::GENERIC_TYPE_INFO, std::move(alias));
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

void LogicalType::SetModifiers(vector<Value> modifiers) {
	if (!type_info_ && !modifiers.empty()) {
		type_info_ = make_shared_ptr<ExtraTypeInfo>(ExtraTypeInfoType::GENERIC_TYPE_INFO);
	}
	type_info_->modifiers = std::move(modifiers);
}

bool LogicalType::HasModifiers() const {
	if (id() == LogicalTypeId::USER) {
		return !UserType::GetTypeModifiers(*this).empty();
	}
	if (type_info_) {
		return !type_info_->modifiers.empty();
	}
	return false;
}

vector<Value> LogicalType::GetModifiersCopy() const {
	if (id() == LogicalTypeId::USER) {
		return UserType::GetTypeModifiers(*this);
	}
	if (type_info_) {
		return type_info_->modifiers;
	}
	return {};
}

optional_ptr<vector<Value>> LogicalType::GetModifiers() {
	if (id() == LogicalTypeId::USER) {
		return UserType::GetTypeModifiers(*this);
	}
	if (type_info_) {
		return type_info_->modifiers;
	}
	return nullptr;
}

optional_ptr<const vector<Value>> LogicalType::GetModifiers() const {
	if (id() == LogicalTypeId::USER) {
		return UserType::GetTypeModifiers(*this);
	}
	if (type_info_) {
		return type_info_->modifiers;
	}
	return nullptr;
}

//===--------------------------------------------------------------------===//
// Decimal Type
//===--------------------------------------------------------------------===//
uint8_t DecimalType::GetWidth(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<DecimalTypeInfo>().width;
}

uint8_t DecimalType::GetScale(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<DecimalTypeInfo>().scale;
}

uint8_t DecimalType::MaxWidth() {
	return DecimalWidth<hugeint_t>::max;
}

LogicalType LogicalType::DECIMAL(uint8_t width, uint8_t scale) {
	D_ASSERT(width >= scale);
	auto type_info = make_shared_ptr<DecimalTypeInfo>(width, scale);
	return LogicalType(LogicalTypeId::DECIMAL, std::move(type_info));
}

//===--------------------------------------------------------------------===//
// String Type
//===--------------------------------------------------------------------===//
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
	return info->Cast<StringTypeInfo>().collation;
}

LogicalType LogicalType::VARCHAR_COLLATION(string collation) { // NOLINT
	auto string_info = make_shared_ptr<StringTypeInfo>(std::move(collation));
	return LogicalType(LogicalTypeId::VARCHAR, std::move(string_info));
}

//===--------------------------------------------------------------------===//
// List Type
//===--------------------------------------------------------------------===//
const LogicalType &ListType::GetChildType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::LIST || type.id() == LogicalTypeId::MAP);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<ListTypeInfo>().child_type;
}

LogicalType LogicalType::LIST(const LogicalType &child) {
	auto info = make_shared_ptr<ListTypeInfo>(child);
	return LogicalType(LogicalTypeId::LIST, std::move(info));
}

//===--------------------------------------------------------------------===//
// Aggregate State Type
//===--------------------------------------------------------------------===//
const aggregate_state_t &AggregateStateType::GetStateType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::AGGREGATE_STATE);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<AggregateStateTypeInfo>().state_type;
}

const string AggregateStateType::GetTypeName(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::AGGREGATE_STATE);
	auto info = type.AuxInfo();
	if (!info) {
		return "AGGREGATE_STATE<?>";
	}
	auto aggr_state = info->Cast<AggregateStateTypeInfo>().state_type;
	return "AGGREGATE_STATE<" + aggr_state.function_name + "(" +
	       StringUtil::Join(aggr_state.bound_argument_types, aggr_state.bound_argument_types.size(), ", ",
	                        [](const LogicalType &arg_type) { return arg_type.ToString(); }) +
	       ")" + "::" + aggr_state.return_type.ToString() + ">";
}

//===--------------------------------------------------------------------===//
// Struct Type
//===--------------------------------------------------------------------===//
const child_list_t<LogicalType> &StructType::GetChildTypes(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::UNION);

	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<StructTypeInfo>().child_types;
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

idx_t StructType::GetChildIndexUnsafe(const LogicalType &type, const string &name) {
	auto &child_types = StructType::GetChildTypes(type);
	for (idx_t i = 0; i < child_types.size(); i++) {
		if (StringUtil::CIEquals(child_types[i].first, name)) {
			return i;
		}
	}
	throw InternalException("Could not find child with name \"%s\" in struct type \"%s\"", name, type.ToString());
}

idx_t StructType::GetChildCount(const LogicalType &type) {
	return StructType::GetChildTypes(type).size();
}
bool StructType::IsUnnamed(const LogicalType &type) {
	auto &child_types = StructType::GetChildTypes(type);
	if (child_types.empty()) {
		return false;
	}
	return child_types[0].first.empty(); // NOLINT
}

LogicalType LogicalType::STRUCT(child_list_t<LogicalType> children) {
	auto info = make_shared_ptr<StructTypeInfo>(std::move(children));
	return LogicalType(LogicalTypeId::STRUCT, std::move(info));
}

LogicalType LogicalType::AGGREGATE_STATE(aggregate_state_t state_type) { // NOLINT
	auto info = make_shared_ptr<AggregateStateTypeInfo>(std::move(state_type));
	return LogicalType(LogicalTypeId::AGGREGATE_STATE, std::move(info));
}

//===--------------------------------------------------------------------===//
// Map Type
//===--------------------------------------------------------------------===//
LogicalType LogicalType::MAP(const LogicalType &child_p) {
	D_ASSERT(child_p.id() == LogicalTypeId::STRUCT);
	auto &children = StructType::GetChildTypes(child_p);
	D_ASSERT(children.size() == 2);

	// We do this to enforce that for every MAP created, the keys are called "key"
	// and the values are called "value"

	// This is done because for Vector the keys of the STRUCT are used in equality checks.
	// Vector::Reference will throw if the types don't match
	child_list_t<LogicalType> new_children(2);
	new_children[0] = children[0];
	new_children[0].first = "key";

	new_children[1] = children[1];
	new_children[1].first = "value";

	auto child = LogicalType::STRUCT(std::move(new_children));
	auto info = make_shared_ptr<ListTypeInfo>(child);
	return LogicalType(LogicalTypeId::MAP, std::move(info));
}

LogicalType LogicalType::MAP(LogicalType key, LogicalType value) {
	child_list_t<LogicalType> child_types;
	child_types.emplace_back("key", std::move(key));
	child_types.emplace_back("value", std::move(value));
	return LogicalType::MAP(LogicalType::STRUCT(child_types));
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
	D_ASSERT(!members.empty());
	D_ASSERT(members.size() <= UnionType::MAX_UNION_MEMBERS);
	// union types always have a hidden "tag" field in front
	members.insert(members.begin(), {"", LogicalType::UTINYINT});
	auto info = make_shared_ptr<StructTypeInfo>(std::move(members));
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
	// don't count the "tag" field
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
const string &UserType::GetCatalog(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::USER);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<UserTypeInfo>().catalog;
}

const string &UserType::GetSchema(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::USER);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<UserTypeInfo>().schema;
}

const string &UserType::GetTypeName(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::USER);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<UserTypeInfo>().user_type_name;
}

const vector<Value> &UserType::GetTypeModifiers(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::USER);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<UserTypeInfo>().user_type_modifiers;
}

vector<Value> &UserType::GetTypeModifiers(LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::USER);
	auto info = type.GetAuxInfoShrPtr();
	D_ASSERT(info);
	return info->Cast<UserTypeInfo>().user_type_modifiers;
}

LogicalType LogicalType::USER(const string &user_type_name) {
	auto info = make_shared_ptr<UserTypeInfo>(user_type_name);
	return LogicalType(LogicalTypeId::USER, std::move(info));
}

LogicalType LogicalType::USER(const string &user_type_name, const vector<Value> &user_type_mods) {
	auto info = make_shared_ptr<UserTypeInfo>(user_type_name, user_type_mods);
	return LogicalType(LogicalTypeId::USER, std::move(info));
}

LogicalType LogicalType::USER(string catalog, string schema, string name, vector<Value> user_type_mods) {
	auto info = make_shared_ptr<UserTypeInfo>(std::move(catalog), std::move(schema), std::move(name),
	                                          std::move(user_type_mods));
	return LogicalType(LogicalTypeId::USER, std::move(info));
}

//===--------------------------------------------------------------------===//
// Enum Type
//===--------------------------------------------------------------------===//
LogicalType LogicalType::ENUM(Vector &ordered_data, idx_t size) {
	return EnumTypeInfo::CreateType(ordered_data, size);
}

LogicalType LogicalType::ENUM(const string &enum_name, Vector &ordered_data, idx_t size) {
	return LogicalType::ENUM(ordered_data, size);
}

const string EnumType::GetValue(const Value &val) {
	auto info = val.type().AuxInfo();
	auto &values_insert_order = info->Cast<EnumTypeInfo>().GetValuesInsertOrder();
	return StringValue::Get(values_insert_order.GetValue(val.GetValue<uint32_t>()));
}

const Vector &EnumType::GetValuesInsertOrder(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<EnumTypeInfo>().GetValuesInsertOrder();
}

idx_t EnumType::GetSize(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<EnumTypeInfo>().GetDictSize();
}

PhysicalType EnumType::GetPhysicalType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto aux_info = type.AuxInfo();
	D_ASSERT(aux_info);
	auto &info = aux_info->Cast<EnumTypeInfo>();
	D_ASSERT(info.GetEnumDictType() == EnumDictType::VECTOR_DICT);
	return EnumTypeInfo::DictType(info.GetDictSize());
}

//===--------------------------------------------------------------------===//
// JSON Type
//===--------------------------------------------------------------------===//
LogicalType LogicalType::JSON() {
	auto json_type = LogicalType(LogicalTypeId::VARCHAR);
	json_type.SetAlias(JSON_TYPE_NAME);
	return json_type;
}

bool LogicalType::IsJSONType() const {
	return id() == LogicalTypeId::VARCHAR && HasAlias() && GetAlias() == JSON_TYPE_NAME;
}

//===--------------------------------------------------------------------===//
// Array Type
//===--------------------------------------------------------------------===//

const LogicalType &ArrayType::GetChildType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ARRAY);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<ArrayTypeInfo>().child_type;
}

idx_t ArrayType::GetSize(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ARRAY);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<ArrayTypeInfo>().size;
}

bool ArrayType::IsAnySize(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ARRAY);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<ArrayTypeInfo>().size == 0;
}

LogicalType ArrayType::ConvertToList(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::ARRAY: {
		return LogicalType::LIST(ConvertToList(ArrayType::GetChildType(type)));
	}
	case LogicalTypeId::LIST:
		return LogicalType::LIST(ConvertToList(ListType::GetChildType(type)));
	case LogicalTypeId::STRUCT: {
		auto children = StructType::GetChildTypes(type);
		for (auto &child : children) {
			child.second = ConvertToList(child.second);
		}
		return LogicalType::STRUCT(children);
	}
	case LogicalTypeId::MAP: {
		auto key_type = ConvertToList(MapType::KeyType(type));
		auto value_type = ConvertToList(MapType::ValueType(type));
		return LogicalType::MAP(key_type, value_type);
	}
	case LogicalTypeId::UNION: {
		auto children = UnionType::CopyMemberTypes(type);
		for (auto &child : children) {
			child.second = ConvertToList(child.second);
		}
		return LogicalType::UNION(children);
	}
	default:
		return type;
	}
}

LogicalType LogicalType::ARRAY(const LogicalType &child, optional_idx size) {
	if (!size.IsValid()) {
		// Create an incomplete ARRAY type, used for binding
		auto info = make_shared_ptr<ArrayTypeInfo>(child, 0);
		return LogicalType(LogicalTypeId::ARRAY, std::move(info));
	} else {
		auto array_size = size.GetIndex();
		D_ASSERT(array_size > 0);
		D_ASSERT(array_size <= ArrayType::MAX_ARRAY_SIZE);
		auto info = make_shared_ptr<ArrayTypeInfo>(child, array_size);
		return LogicalType(LogicalTypeId::ARRAY, std::move(info));
	}
}

//===--------------------------------------------------------------------===//
// Any Type
//===--------------------------------------------------------------------===//
LogicalType LogicalType::ANY_PARAMS(LogicalType target, idx_t cast_score) { // NOLINT
	auto type_info = make_shared_ptr<AnyTypeInfo>(std::move(target), cast_score);
	return LogicalType(LogicalTypeId::ANY, std::move(type_info));
}

LogicalType AnyType::GetTargetType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ANY);
	auto info = type.AuxInfo();
	if (!info) {
		return LogicalType::ANY;
	}
	return info->Cast<AnyTypeInfo>().target_type;
}

idx_t AnyType::GetCastScore(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ANY);
	auto info = type.AuxInfo();
	if (!info) {
		return 5;
	}
	return info->Cast<AnyTypeInfo>().cast_score;
}

//===--------------------------------------------------------------------===//
// Integer Literal Type
//===--------------------------------------------------------------------===//
LogicalType IntegerLiteral::GetType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::INTEGER_LITERAL);
	auto info = type.AuxInfo();
	D_ASSERT(info && info->type == ExtraTypeInfoType::INTEGER_LITERAL_TYPE_INFO);
	return info->Cast<IntegerLiteralTypeInfo>().constant_value.type();
}

bool IntegerLiteral::FitsInType(const LogicalType &type, const LogicalType &target) {
	D_ASSERT(type.id() == LogicalTypeId::INTEGER_LITERAL);
	// we can always cast integer literals to float and double
	if (target.id() == LogicalTypeId::FLOAT || target.id() == LogicalTypeId::DOUBLE) {
		return true;
	}
	if (!target.IsIntegral()) {
		return false;
	}
	// we can cast to integral types if the constant value fits within that type
	auto info = type.AuxInfo();
	D_ASSERT(info && info->type == ExtraTypeInfoType::INTEGER_LITERAL_TYPE_INFO);
	auto &literal_info = info->Cast<IntegerLiteralTypeInfo>();
	Value copy = literal_info.constant_value;
	return copy.DefaultTryCastAs(target);
}

LogicalType LogicalType::INTEGER_LITERAL(const Value &constant) { // NOLINT
	if (!constant.type().IsIntegral()) {
		throw InternalException("INTEGER_LITERAL can only be made from literals of integer types");
	}
	auto type_info = make_shared_ptr<IntegerLiteralTypeInfo>(constant);
	return LogicalType(LogicalTypeId::INTEGER_LITERAL, std::move(type_info));
}

//===--------------------------------------------------------------------===//
// Logical Type
//===--------------------------------------------------------------------===//

// the destructor needs to know about the extra type info
LogicalType::~LogicalType() {
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
