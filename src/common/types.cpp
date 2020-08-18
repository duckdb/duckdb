#include "duckdb/common/types.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/decimal.hpp"

#include <cmath>

using namespace std;

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
	case LogicalTypeId::SMALLINT:
		return PhysicalType::INT16;
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::INTEGER:
		return PhysicalType::INT32;
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIMESTAMP:
		return PhysicalType::INT64;
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
	case LogicalTypeId::VARBINARY:
		return PhysicalType::VARBINARY;
	case LogicalTypeId::INTERVAL:
		return PhysicalType::INTERVAL;
	case LogicalTypeId::STRUCT:
		return PhysicalType::STRUCT;
	case LogicalTypeId::LIST:
		return PhysicalType::LIST;
	case LogicalTypeId::HASH:
		return PhysicalType::HASH;
	case LogicalTypeId::POINTER:
		return PhysicalType::POINTER;
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
const LogicalType LogicalType::SMALLINT = LogicalType(LogicalTypeId::SMALLINT);
const LogicalType LogicalType::INTEGER = LogicalType(LogicalTypeId::INTEGER);
const LogicalType LogicalType::BIGINT = LogicalType(LogicalTypeId::BIGINT);
const LogicalType LogicalType::HUGEINT = LogicalType(LogicalTypeId::HUGEINT);
const LogicalType LogicalType::FLOAT = LogicalType(LogicalTypeId::FLOAT);
const LogicalType LogicalType::DECIMAL = LogicalType(LogicalTypeId::DECIMAL);
const LogicalType LogicalType::DOUBLE = LogicalType(LogicalTypeId::DOUBLE);
const LogicalType LogicalType::DATE = LogicalType(LogicalTypeId::DATE);
const LogicalType LogicalType::TIMESTAMP = LogicalType(LogicalTypeId::TIMESTAMP);
const LogicalType LogicalType::TIME = LogicalType(LogicalTypeId::TIME);
const LogicalType LogicalType::HASH = LogicalType(LogicalTypeId::HASH);
const LogicalType LogicalType::POINTER = LogicalType(LogicalTypeId::POINTER);

const LogicalType LogicalType::VARCHAR = LogicalType(LogicalTypeId::VARCHAR);
const LogicalType LogicalType::VARBINARY = LogicalType(LogicalTypeId::VARBINARY);

const LogicalType LogicalType::BLOB = LogicalType(LogicalTypeId::BLOB);
const LogicalType LogicalType::INTERVAL = LogicalType(LogicalTypeId::INTERVAL);

// TODO these are incomplete and should maybe not exist as such
const LogicalType LogicalType::STRUCT = LogicalType(LogicalTypeId::STRUCT);
const LogicalType LogicalType::LIST = LogicalType(LogicalTypeId::LIST);

const LogicalType LogicalType::ANY = LogicalType(LogicalTypeId::ANY);

const vector<LogicalType> LogicalType::NUMERIC = {LogicalType::TINYINT, LogicalType::SMALLINT, LogicalType::INTEGER,
                                                  LogicalType::BIGINT,  LogicalType::HUGEINT,  LogicalType::FLOAT,
                                                  LogicalType::DOUBLE, LogicalType::DECIMAL};

const vector<LogicalType> LogicalType::INTEGRAL = {LogicalType::TINYINT, LogicalType::SMALLINT, LogicalType::INTEGER,
                                                   LogicalType::BIGINT, LogicalType::HUGEINT};

const vector<LogicalType> LogicalType::ALL_TYPES = {
    LogicalType::BOOLEAN, LogicalType::TINYINT,   LogicalType::SMALLINT, LogicalType::INTEGER, LogicalType::BIGINT,
    LogicalType::DATE,    LogicalType::TIMESTAMP, LogicalType::DOUBLE,   LogicalType::FLOAT,   LogicalType::VARCHAR,
    LogicalType::BLOB,    LogicalType::INTERVAL,  LogicalType::HUGEINT};
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
	case PhysicalType::VARBINARY:
		return "VARBINARY";
	case PhysicalType::INTERVAL:
		return "INTERVAL";
	case PhysicalType::STRUCT:
		return "STRUCT<?>";
	case PhysicalType::LIST:
		return "LIST<?>";
	default:
		throw ConversionException("Invalid PhysicalType %d", type);
	}
}

idx_t GetTypeIdSize(PhysicalType type) {
	switch (type) {
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
	case PhysicalType::VARBINARY:
		return sizeof(blob_t);
	default:
		throw ConversionException("Invalid PhysicalType %d", type);
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

void LogicalType::Serialize(Serializer &serializer) {
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
	case LogicalTypeId::DATE:
		return "DATE";
	case LogicalTypeId::TIME:
		return "TIME";
	case LogicalTypeId::TIMESTAMP:
		return "TIMESTAMP";
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
	case LogicalTypeId::VARBINARY:
		return "VARBINARY";
	case LogicalTypeId::CHAR:
		return "CHAR";
	case LogicalTypeId::INTERVAL:
		return "INTERVAL";
	case LogicalTypeId::SQLNULL:
		return "NULL";
	case LogicalTypeId::ANY:
		return "ANY";
	case LogicalTypeId::STRUCT:
		return "STRUCT<?>";
	case LogicalTypeId::LIST:
		return "LIST<?>";
	case LogicalTypeId::HASH:
		return "HASH";
	case LogicalTypeId::POINTER:
		return "POINTER";
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
		if (child_types_.size() == 0) {
			return "LIST<?>";
		}
		if (child_types_.size() != 1) {
			throw Exception("List needs a single child element");
		}
		return "LIST<" + child_types_[0].second.ToString() + ">";
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

LogicalType TransformStringToLogicalType(string str) {
	auto lower_str = StringUtil::Lower(str);
	// Transform column type
	if (lower_str == "int" || lower_str == "int4" || lower_str == "signed" || lower_str == "integer" ||
	    lower_str == "integral" || lower_str == "int32") {
		return LogicalType::INTEGER;
	} else if (lower_str == "varchar" || lower_str == "bpchar" || lower_str == "text" || lower_str == "string" ||
	           lower_str == "char") {
		return LogicalType::VARCHAR;
	} else if (lower_str == "bytea" || lower_str == "blob") {
		return LogicalType::BLOB;
	} else if (lower_str == "int8" || lower_str == "bigint" || lower_str == "int64" || lower_str == "long") {
		return LogicalType::BIGINT;
	} else if (lower_str == "int2" || lower_str == "smallint" || lower_str == "short" || lower_str == "int16") {
		return LogicalType::SMALLINT;
	} else if (lower_str == "timestamp" || lower_str == "datetime") {
		return LogicalType::TIMESTAMP;
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
	} else if (lower_str == "varbinary") {
		return LogicalType(LogicalTypeId::VARBINARY);
	} else if (lower_str == "date") {
		return LogicalType::DATE;
	} else if (lower_str == "time") {
		return LogicalType::TIME;
	} else if (lower_str == "interval") {
		return LogicalType::INTERVAL;
	} else if (lower_str == "hugeint" || lower_str == "int128") {
		return LogicalType::HUGEINT;
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
		return true;
	default:
		return false;
	}
}

int LogicalType::NumericTypeOrder() const {
	switch (InternalType()) {
	case PhysicalType::INT8:
		return 1;
	case PhysicalType::INT16:
		return 2;
	case PhysicalType::INT32:
		return 3;
	case PhysicalType::INT64:
		return 4;
	case PhysicalType::INT128:
		return 5;
	case PhysicalType::FLOAT:
		return 6;
	case PhysicalType::DOUBLE:
		return 7;
	default:
		throw NotImplementedException("Not a numeric type");
	}
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
		return false;
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
		return false;
	case LogicalTypeId::DATE:
		return false;
	case LogicalTypeId::TIMESTAMP:
		switch (other.id()) {
		case LogicalTypeId::TIME:
		case LogicalTypeId::DATE:
			return true;
		default:
			return false;
		}
	case LogicalTypeId::VARCHAR:
		return true;
	default:
		return false;
	}

	return true;
}

LogicalType MaxLogicalType(LogicalType left, LogicalType right) {
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
			return LogicalType(LogicalTypeId::DECIMAL, max<uint8_t>(left.width(), right.width()), max<uint8_t>(left.scale(), right.scale()));
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
		assert(width_ >= 1 && width_ <= Decimal::MAX_WIDTH_DECIMAL);
		assert(scale_ >= 0 && scale_ <= width_);
	}
#endif
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
