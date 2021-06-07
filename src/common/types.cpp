#include "duckdb/common/types.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/decimal.hpp"

#include <cmath>

namespace duckdb {

LogicalType::LogicalType() : id_(LogicalTypeId::INVALID), width_(0), scale_(0), collation_(string()) {
	physical_type_ = GetInternalType();
}
LogicalType::LogicalType(LogicalTypeId id) : id_(id), width_(0), scale_(0), collation_(string()) {
	physical_type_ = GetInternalType();
}

LogicalType::LogicalType(LogicalTypeId id, string collation)
    : id_(id), width_(0), scale_(0), collation_(move(collation)) {
	physical_type_ = GetInternalType();
}
LogicalType::LogicalType(LogicalTypeId id, uint8_t width, uint8_t scale)
    : id_(id), width_(width), scale_(scale), collation_(string()) {
	physical_type_ = GetInternalType();
}
LogicalType::LogicalType(LogicalTypeId id, child_list_t<LogicalType> child_types)
    : id_(id), width_(0), scale_(0), collation_(string()), child_types_(move(child_types)) {
	physical_type_ = GetInternalType();
}
LogicalType::LogicalType(LogicalTypeId id, uint8_t width, uint8_t scale, string collation,
                         child_list_t<LogicalType> child_types)
    : id_(id), width_(width), scale_(scale), collation_(move(collation)), child_types_(move(child_types)) {
	physical_type_ = GetInternalType();
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
		return PhysicalType::INT64;
	case LogicalTypeId::UBIGINT:
		return PhysicalType::UINT64;
	case LogicalTypeId::HUGEINT:
		return PhysicalType::INT128;
	case LogicalTypeId::FLOAT:
		return PhysicalType::FLOAT;
	case LogicalTypeId::DOUBLE:
		return PhysicalType::DOUBLE;
	case LogicalTypeId::DECIMAL:
		if (width_ <= Decimal::MAX_WIDTH_INT16) {
			return PhysicalType::INT16;
		} else if (width_ <= Decimal::MAX_WIDTH_INT32) {
			return PhysicalType::INT32;
		} else if (width_ <= Decimal::MAX_WIDTH_INT64) {
			return PhysicalType::INT64;
		} else if (width_ <= Decimal::MAX_WIDTH_INT128) {
			return PhysicalType::INT128;
		} else {
			throw NotImplementedException("Widths bigger than 38 are not supported");
		}
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::CHAR:
	case LogicalTypeId::BLOB:
		return PhysicalType::VARCHAR;
	case LogicalTypeId::INTERVAL:
		return PhysicalType::INTERVAL;
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT:
		return PhysicalType::STRUCT;
	case LogicalTypeId::LIST:
		return PhysicalType::LIST;
	case LogicalTypeId::HASH:
		return PhysicalType::HASH;
	case LogicalTypeId::POINTER:
		return PhysicalType::POINTER;
	case LogicalTypeId::VALIDITY:
		return PhysicalType::BIT;
	case LogicalTypeId::TABLE:
	case LogicalTypeId::ANY:
	case LogicalTypeId::INVALID:
	case LogicalTypeId::UNKNOWN:
		return PhysicalType::INVALID;
	default:
		throw ConversionException("Invalid LogicalType %s", ToString());
	}
}

const LogicalType LogicalType::INVALID = LogicalType(LogicalTypeId::INVALID);
const LogicalType LogicalType::SQLNULL = LogicalType(LogicalTypeId::SQLNULL);
const LogicalType LogicalType::BOOLEAN = LogicalType(LogicalTypeId::BOOLEAN);
const LogicalType LogicalType::TINYINT = LogicalType(LogicalTypeId::TINYINT);
const LogicalType LogicalType::UTINYINT = LogicalType(LogicalTypeId::UTINYINT);
const LogicalType LogicalType::SMALLINT = LogicalType(LogicalTypeId::SMALLINT);
const LogicalType LogicalType::USMALLINT = LogicalType(LogicalTypeId::USMALLINT);
const LogicalType LogicalType::INTEGER = LogicalType(LogicalTypeId::INTEGER);
const LogicalType LogicalType::UINTEGER = LogicalType(LogicalTypeId::UINTEGER);
const LogicalType LogicalType::BIGINT = LogicalType(LogicalTypeId::BIGINT);
const LogicalType LogicalType::UBIGINT = LogicalType(LogicalTypeId::UBIGINT);
const LogicalType LogicalType::HUGEINT = LogicalType(LogicalTypeId::HUGEINT);
const LogicalType LogicalType::FLOAT = LogicalType(LogicalTypeId::FLOAT);
const LogicalType LogicalType::DECIMAL = LogicalType(LogicalTypeId::DECIMAL);
const LogicalType LogicalType::DOUBLE = LogicalType(LogicalTypeId::DOUBLE);
const LogicalType LogicalType::DATE = LogicalType(LogicalTypeId::DATE);

const LogicalType LogicalType::TIMESTAMP = LogicalType(LogicalTypeId::TIMESTAMP);
const LogicalType LogicalType::TIMESTAMP_MS = LogicalType(LogicalTypeId::TIMESTAMP_MS);
const LogicalType LogicalType::TIMESTAMP_NS = LogicalType(LogicalTypeId::TIMESTAMP_NS);
const LogicalType LogicalType::TIMESTAMP_S = LogicalType(LogicalTypeId::TIMESTAMP_SEC);

const LogicalType LogicalType::TIME = LogicalType(LogicalTypeId::TIME);
const LogicalType LogicalType::HASH = LogicalType(LogicalTypeId::HASH);
const LogicalType LogicalType::POINTER = LogicalType(LogicalTypeId::POINTER);

const LogicalType LogicalType::VARCHAR = LogicalType(LogicalTypeId::VARCHAR);

const LogicalType LogicalType::BLOB = LogicalType(LogicalTypeId::BLOB);
const LogicalType LogicalType::INTERVAL = LogicalType(LogicalTypeId::INTERVAL);

// TODO these are incomplete and should maybe not exist as such
const LogicalType LogicalType::STRUCT = LogicalType(LogicalTypeId::STRUCT);
const LogicalType LogicalType::LIST = LogicalType(LogicalTypeId::LIST);
const LogicalType LogicalType::MAP = LogicalType(LogicalTypeId::MAP);
const LogicalType LogicalType::TABLE = LogicalType(LogicalTypeId::TABLE);

const LogicalType LogicalType::ANY = LogicalType(LogicalTypeId::ANY);

const vector<LogicalType> LogicalType::NUMERIC = {LogicalType::TINYINT,   LogicalType::SMALLINT, LogicalType::INTEGER,
                                                  LogicalType::BIGINT,    LogicalType::HUGEINT,  LogicalType::FLOAT,
                                                  LogicalType::DOUBLE,    LogicalType::DECIMAL,  LogicalType::UTINYINT,
                                                  LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT};

const vector<LogicalType> LogicalType::INTEGRAL = {LogicalType::TINYINT,   LogicalType::SMALLINT, LogicalType::INTEGER,
                                                   LogicalType::BIGINT,    LogicalType::HUGEINT,  LogicalType::UTINYINT,
                                                   LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT};

const vector<LogicalType> LogicalType::ALL_TYPES = {
    LogicalType::BOOLEAN,   LogicalType::TINYINT,   LogicalType::SMALLINT, LogicalType::INTEGER, LogicalType::BIGINT,
    LogicalType::DATE,      LogicalType::TIMESTAMP, LogicalType::DOUBLE,   LogicalType::FLOAT,   LogicalType::VARCHAR,
    LogicalType::BLOB,      LogicalType::INTERVAL,  LogicalType::HUGEINT,  LogicalType::DECIMAL, LogicalType::UTINYINT,
    LogicalType::USMALLINT, LogicalType::UINTEGER,  LogicalType::UBIGINT, LogicalType::TIME};
// TODO add LIST/STRUCT here

const LogicalType LOGICAL_ROW_TYPE = LogicalType::BIGINT;
const PhysicalType ROW_TYPE = PhysicalType::INT64;

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
	case PhysicalType::HASH:
		return "HASH";
	case PhysicalType::POINTER:
		return "POINTER";
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
	default:
		throw ConversionException("Invalid PhysicalType %s", type);
	}
}

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
	case PhysicalType::HASH:
		return sizeof(hash_t);
	case PhysicalType::POINTER:
		return sizeof(uintptr_t);
	case PhysicalType::VARCHAR:
		return sizeof(string_t);
	case PhysicalType::INTERVAL:
		return sizeof(interval_t);
	case PhysicalType::STRUCT:
		return 0; // no own payload
	case PhysicalType::LIST:
		return 16; // offset + len

	default:
		throw ConversionException("Invalid PhysicalType %s", type);
	}
}

bool TypeIsConstantSize(PhysicalType type) {
	return (type >= PhysicalType::BOOL && type <= PhysicalType::DOUBLE) ||
	       (type >= PhysicalType::FIXED_SIZE_BINARY && type <= PhysicalType::INTERVAL) || type == PhysicalType::HASH ||
	       type == PhysicalType::POINTER || type == PhysicalType::INTERVAL || type == PhysicalType::INT128;
}
bool TypeIsIntegral(PhysicalType type) {
	return (type >= PhysicalType::UINT8 && type <= PhysicalType::INT64) || type == PhysicalType::HASH ||
	       type == PhysicalType::POINTER || type == PhysicalType::INT128;
}
bool TypeIsNumeric(PhysicalType type) {
	return (type >= PhysicalType::UINT8 && type <= PhysicalType::DOUBLE) || type == PhysicalType::INT128;
}
bool TypeIsInteger(PhysicalType type) {
	return (type >= PhysicalType::UINT8 && type <= PhysicalType::INT64) || type == PhysicalType::INT128;
}

void LogicalType::Serialize(Serializer &serializer) const {
	serializer.Write<LogicalTypeId>(id_);
	serializer.Write<uint8_t>(width_);
	serializer.Write<uint8_t>(scale_);
	serializer.WriteString(collation_);
	serializer.Write<uint16_t>(child_types_.size());
	for (auto &entry : child_types_) {
		serializer.WriteString(entry.first);
		entry.second.Serialize(serializer);
	}
}

LogicalType LogicalType::Deserialize(Deserializer &source) {
	auto id = source.Read<LogicalTypeId>();
	auto width = source.Read<uint8_t>();
	auto scale = source.Read<uint8_t>();
	auto collation = source.Read<string>();
	child_list_t<LogicalType> children;
	auto child_count = source.Read<uint16_t>();
	for (uint16_t i = 0; i < child_count; i++) {
		string name = source.Read<string>();
		LogicalType child_type = LogicalType::Deserialize(source);
		children.push_back(make_pair(move(name), move(child_type)));
	}
	return LogicalType(id, width, scale, collation, move(children));
}

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
		return "TIMESTAMP (MS)";
	case LogicalTypeId::TIMESTAMP_NS:
		return "TIMESTAMP (NS)";
	case LogicalTypeId::TIMESTAMP_SEC:
		return "TIMESTAMP (SEC)";
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
	}
	return "UNDEFINED";
}

string LogicalType::ToString() const {
	switch (id_) {
	case LogicalTypeId::STRUCT: {
		string ret = "STRUCT<";
		for (size_t i = 0; i < child_types_.size(); i++) {
			ret += child_types_[i].first + ": " + child_types_[i].second.ToString();
			if (i < child_types_.size() - 1) {
				ret += ", ";
			}
		}
		ret += ">";
		return ret;
	}
	case LogicalTypeId::LIST: {
		if (child_types_.empty()) {
			return "LIST<?>";
		}
		if (child_types_.size() != 1) {
			throw Exception("List needs a single child element");
		}
		return "LIST<" + child_types_[0].second.ToString() + ">";
	}
	case LogicalTypeId::MAP: {
		if (child_types_.empty()) {
			return "MAP<?>";
		}
		if (child_types_.size() != 2) {
			throw Exception("Map needs exactly two child elements");
		}
		return "MAP<" + child_types_[0].second.child_types()[0].second.ToString() + ", " +
		       child_types_[1].second.child_types()[0].second.ToString() + ">";
	}
	case LogicalTypeId::DECIMAL: {
		if (width_ == 0) {
			return "DECIMAL";
		}
		return StringUtil::Format("DECIMAL(%d,%d)", width_, scale_);
	}
	default:
		return LogicalTypeIdToString(id_);
	}
}

LogicalType TransformStringToLogicalType(const string &str) {
	auto lower_str = StringUtil::Lower(str);
	// Transform column type
	if (lower_str == "int" || lower_str == "int4" || lower_str == "signed" || lower_str == "integer" ||
	    lower_str == "integral" || lower_str == "int32") {
		return LogicalType::INTEGER;
	} else if (lower_str == "varchar" || lower_str == "bpchar" || lower_str == "text" || lower_str == "string" ||
	           lower_str == "char") {
		return LogicalType::VARCHAR;
	} else if (lower_str == "bytea" || lower_str == "blob" || lower_str == "varbinary" || lower_str == "binary") {
		return LogicalType::BLOB;
	} else if (lower_str == "int8" || lower_str == "bigint" || lower_str == "int64" || lower_str == "long" || lower_str == "oid") {
		return LogicalType::BIGINT;
	} else if (lower_str == "int2" || lower_str == "smallint" || lower_str == "short" || lower_str == "int16") {
		return LogicalType::SMALLINT;
	} else if (lower_str == "timestamp" || lower_str == "datetime" || lower_str == "timestamp_us") {
		return LogicalType::TIMESTAMP;
	} else if (lower_str == "timestamp_ms") {
		return LogicalType::TIMESTAMP_MS;
	} else if (lower_str == "timestamp_ns") {
		return LogicalType::TIMESTAMP_NS;
	} else if (lower_str == "timestamp_s") {
		return LogicalType::TIMESTAMP_S;
	} else if (lower_str == "bool" || lower_str == "boolean" || lower_str == "logical") {
		return LogicalType(LogicalTypeId::BOOLEAN);
	} else if (lower_str == "real" || lower_str == "float4" || lower_str == "float") {
		return LogicalType::FLOAT;
	} else if (lower_str == "decimal" || lower_str == "dec" || lower_str == "numeric") {
		return LogicalType(LogicalTypeId::DECIMAL, 18, 3);
	} else if (lower_str == "double" || lower_str == "float8" || lower_str == "decimal") {
		return LogicalType::DOUBLE;
	} else if (lower_str == "tinyint" || lower_str == "int1") {
		return LogicalType::TINYINT;
	} else if (lower_str == "date") {
		return LogicalType::DATE;
	} else if (lower_str == "time") {
		return LogicalType::TIME;
	} else if (lower_str == "interval") {
		return LogicalType::INTERVAL;
	} else if (lower_str == "hugeint" || lower_str == "int128") {
		return LogicalType::HUGEINT;
	} else if (lower_str == "struct" || lower_str == "row") {
		return LogicalType::STRUCT;
	} else if (lower_str == "map") {
		return LogicalType::MAP;
	} else if (lower_str == "utinyint") {
		return LogicalType::UTINYINT;
	} else if (lower_str == "usmallint") {
		return LogicalType::USMALLINT;
	} else if (lower_str == "uinteger") {
		return LogicalType::UINTEGER;
	} else if (lower_str == "ubigint") {
		return LogicalType::UBIGINT;
	} else {
		throw NotImplementedException("DataType %s not supported yet...\n", str);
	}
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
		width = width_;
		scale = scale_;
		break;
	default:
		return false;
	}
	return true;
}

bool LogicalType::IsMoreGenericThan(LogicalType &other) const {
	if (other.id() == id_) {
		return false;
	}

	if (other.id() == LogicalTypeId::SQLNULL) {
		return true;
	}

	// all integer types can cast from INTEGER
	// this is because INTEGER is the smallest type considered by the automatic csv sniffer
	switch (id_) {
	case LogicalTypeId::SMALLINT:
		switch (other.id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
			return true;
		default:
			return false;
		}
	case LogicalTypeId::INTEGER:
		switch (other.id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
			return true;
		default:
			return false;
		}
	case LogicalTypeId::BIGINT:
		switch (other.id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
			return true;
		default:
			return false;
		}
	case LogicalTypeId::HUGEINT:
		switch (other.id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
			return true;
		default:
			return false;
		}
	case LogicalTypeId::FLOAT:
		switch (other.id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
			return true;
		default:
			return false;
		}
	case LogicalTypeId::DOUBLE:
		switch (other.id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::FLOAT:
			return true;
		default:
			return false;
		}
	case LogicalTypeId::DATE:
		return false;
	case LogicalTypeId::TIMESTAMP: {
		switch (other.id()) {
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::TIME:
		case LogicalTypeId::DATE:
			return true;
		default:
			return false;
		}
	}
	case LogicalTypeId::TIMESTAMP_NS: {
		switch (other.id()) {
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIME:
		case LogicalTypeId::DATE:
			return true;
		default:
			return false;
		}
	}
	case LogicalTypeId::TIMESTAMP_MS: {
		switch (other.id()) {
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIME:
		case LogicalTypeId::DATE:
			return true;
		default:
			return false;
		}
	}
	case LogicalTypeId::TIMESTAMP_SEC: {
		switch (other.id()) {
		case LogicalTypeId::TIME:
		case LogicalTypeId::DATE:
			return true;
		default:
			return false;
		}
	}

	case LogicalTypeId::VARCHAR:
		return true;
	default:
		return false;
	}
}

LogicalType LogicalType::MaxLogicalType(const LogicalType &left, const LogicalType &right) {
	if (left.id() < right.id()) {
		return right;
	} else if (right.id() < left.id()) {
		return left;
	} else {
		if (left.id() == LogicalTypeId::VARCHAR) {
			// varchar: use type that has collation (if any)
			if (right.collation().empty()) {
				return left;
			} else {
				return right;
			}
		} else if (left.id() == LogicalTypeId::DECIMAL) {
			// use max width/scale of the two types
			return LogicalType(LogicalTypeId::DECIMAL, MaxValue<uint8_t>(left.width(), right.width()),
			                   MaxValue<uint8_t>(left.scale(), right.scale()));
		} else if (left.id() == LogicalTypeId::LIST) {
			// list: perform max recursively on child type
			child_list_t<LogicalType> child_types;
			child_types.push_back(
			    make_pair(left.child_types()[0].first,
			              MaxLogicalType(left.child_types()[0].second, right.child_types()[0].second)));
			return LogicalType(LogicalTypeId::LIST, move(child_types));
		} else if (left.id() == LogicalTypeId::STRUCT) {
			// struct: perform recursively
			auto &left_child_types = left.child_types();
			auto &right_child_types = right.child_types();
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
			return LogicalType(LogicalTypeId::STRUCT, move(child_types));
		} else {
			// types are equal but no extra specifier: just return the type
			// FIXME: LIST and STRUCT?
			return left;
		}
	}
}

void LogicalType::Verify() const {
#ifdef DEBUG
	if (id_ == LogicalTypeId::DECIMAL) {
		D_ASSERT(width_ >= 1 && width_ <= Decimal::MAX_WIDTH_DECIMAL);
		D_ASSERT(scale_ >= 0 && scale_ <= width_);
	}
#endif
}

bool ApproxEqual(float ldecimal, float rdecimal) {
	float epsilon = std::fabs(rdecimal) * 0.01;
	return std::fabs(ldecimal - rdecimal) <= epsilon;
}

bool ApproxEqual(double ldecimal, double rdecimal) {
	double epsilon = std::fabs(rdecimal) * 0.01;
	return std::fabs(ldecimal - rdecimal) <= epsilon;
}

} // namespace duckdb
