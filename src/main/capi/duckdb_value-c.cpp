#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/common/type_visitor.hpp"

static duckdb_value WrapValue(duckdb::Value *list_value) {
	return reinterpret_cast<duckdb_value>(list_value);
}

static duckdb::LogicalType &UnwrapType(duckdb_logical_type type) {
	return *(reinterpret_cast<duckdb::LogicalType *>(type));
}

static duckdb::Value &UnwrapValue(duckdb_value value) {
	return *(reinterpret_cast<duckdb::Value *>(value));
}
void duckdb_destroy_value(duckdb_value *value) {
	if (value && *value) {
		auto &unwrap_value = UnwrapValue(*value);
		delete &unwrap_value;
		*value = nullptr;
	}
}

duckdb_value duckdb_create_varchar_length(const char *text, idx_t length) {
	return WrapValue(new duckdb::Value(std::string(text, length)));
}

duckdb_value duckdb_create_varchar(const char *text) {
	return duckdb_create_varchar_length(text, strlen(text));
}

duckdb_value duckdb_create_int64(int64_t input) {
	auto val = duckdb::Value::BIGINT(input);
	return reinterpret_cast<duckdb_value>(new duckdb::Value(val));
}

duckdb_value duckdb_create_bool(bool input) {
	return WrapValue(new duckdb::Value(duckdb::Value::BOOLEAN(input)));
}
bool duckdb_get_bool(duckdb_value val) {
	return UnwrapValue(val).GetValue<bool>();
}
duckdb_value duckdb_create_tinyint(int8_t input) {
	return WrapValue(new duckdb::Value(duckdb::Value::TINYINT(input)));
}
int8_t duckdb_get_tinyint(duckdb_value val) {
	return UnwrapValue(val).GetValue<int8_t>();
}
duckdb_value duckdb_create_utinyint(uint8_t input) {
	return WrapValue(new duckdb::Value(duckdb::Value::UTINYINT(input)));
}
uint8_t duckdb_get_utinyint(duckdb_value val) {
	return UnwrapValue(val).GetValue<uint8_t>();
}
duckdb_value duckdb_create_smallint(int16_t input) {
	return WrapValue(new duckdb::Value(duckdb::Value::SMALLINT(input)));
}
int16_t duckdb_get_smallint(duckdb_value val) {
	return UnwrapValue(val).GetValue<int16_t>();
}
duckdb_value duckdb_create_usmallint(uint16_t input) {
	return WrapValue(new duckdb::Value(duckdb::Value::USMALLINT(input)));
}
uint16_t duckdb_get_usmallint(duckdb_value val) {
	return UnwrapValue(val).GetValue<uint16_t>();
}
duckdb_value duckdb_create_integer(int32_t input) {
	return WrapValue(new duckdb::Value(duckdb::Value::INTEGER(input)));
}
int32_t duckdb_get_integer(duckdb_value val) {
	return UnwrapValue(val).GetValue<int32_t>();
}
duckdb_value duckdb_create_uinteger(uint32_t input) {
	return WrapValue(new duckdb::Value(duckdb::Value::UINTEGER(input)));
}
uint32_t duckdb_get_uinteger(duckdb_value val) {
	return UnwrapValue(val).GetValue<uint32_t>();
}
duckdb_value duckdb_create_ubigint(uint64_t input) {
	return WrapValue(new duckdb::Value(duckdb::Value::UBIGINT(input)));
}
uint64_t duckdb_get_ubigint(duckdb_value val) {
	return UnwrapValue(val).GetValue<uint64_t>();
}
duckdb_value duckdb_create_bigint(int64_t input) {
	return WrapValue(new duckdb::Value(duckdb::Value::BIGINT(input)));
}
int64_t duckdb_get_bigint(duckdb_value val) {
	return UnwrapValue(val).GetValue<int64_t>();
}
duckdb_value duckdb_create_float(float input) {
	return WrapValue(new duckdb::Value(duckdb::Value::FLOAT(input)));
}
float duckdb_get_float(duckdb_value val) {
	return UnwrapValue(val).GetValue<float>();
}
duckdb_value duckdb_create_double(double input) {
	return WrapValue(new duckdb::Value(duckdb::Value::DOUBLE(input)));
}
double duckdb_get_double(duckdb_value val) {
	return UnwrapValue(val).GetValue<double>();
}
duckdb_value duckdb_create_date(duckdb_date input) {
	return WrapValue(new duckdb::Value(duckdb::Value::DATE(duckdb::date_t(input.days))));
}
duckdb_date duckdb_get_date(duckdb_value val) {
	auto res = UnwrapValue(val).GetValue<duckdb::date_t>();
	return {res.days};
}
duckdb_value duckdb_create_time(duckdb_time input) {
	return WrapValue(new duckdb::Value(duckdb::Value::TIME(duckdb::dtime_t(input.micros))));
}
duckdb_time duckdb_get_time(duckdb_value val) {
	auto dtime = UnwrapValue(val).GetValue<duckdb::dtime_t>();
	return {dtime.micros};
}
duckdb_value duckdb_create_time_tz_value(duckdb_time_tz value) {
	return WrapValue(new duckdb::Value(duckdb::Value::TIMETZ(duckdb::dtime_tz_t(value.bits))));
}
duckdb_time_tz duckdb_get_time_tz(duckdb_value val) {
	auto time_tz = UnwrapValue(val).GetValue<duckdb::dtime_tz_t>();
	return {time_tz.bits};
}
duckdb_value duckdb_create_timestamp(duckdb_timestamp input) {
	return WrapValue(new duckdb::Value(duckdb::Value::TIMESTAMP(duckdb::timestamp_t(input.micros))));
}
duckdb_timestamp duckdb_get_timestamp(duckdb_value val) {
	auto timestamp = UnwrapValue(val).GetValue<duckdb::timestamp_t>();
	return {timestamp.value};
}
duckdb_value duckdb_create_interval(duckdb_interval input) {
	return WrapValue(new duckdb::Value(duckdb::Value::INTERVAL(input.months, input.days, input.micros)));
}
duckdb_interval duckdb_get_interval(duckdb_value val) {
	auto interval = UnwrapValue(val).GetValue<duckdb::interval_t>();
	return {interval.months, interval.days, interval.micros};
}
duckdb_value duckdb_create_hugeint(duckdb_hugeint input) {
	return WrapValue(new duckdb::Value(duckdb::Value::HUGEINT(duckdb::hugeint_t(input.upper, input.lower))));
}
duckdb_hugeint duckdb_get_hugeint(duckdb_value val) {
	auto res = UnwrapValue(val).GetValue<duckdb::hugeint_t>();
	return {res.lower, res.upper};
}
duckdb_value duckdb_create_uhugeint(duckdb_uhugeint input) {
	return WrapValue(new duckdb::Value(duckdb::Value::UHUGEINT(duckdb::uhugeint_t(input.upper, input.lower))));
}
duckdb_uhugeint duckdb_get_uhugeint(duckdb_value val) {
	auto res = UnwrapValue(val).GetValue<duckdb::uhugeint_t>();
	return {res.lower, res.upper};
}
duckdb_value duckdb_create_blob(const uint8_t *data, idx_t length) {
	return WrapValue(new duckdb::Value(duckdb::Value::BLOB((const uint8_t *)data, length)));
}
duckdb_blob duckdb_get_blob(duckdb_value val) {
	auto res = UnwrapValue(val).DefaultCastAs(duckdb::LogicalType::BLOB);
	auto &str = duckdb::StringValue::Get(res);

	auto result = reinterpret_cast<void *>(malloc(sizeof(char) * str.size()));
	memcpy(result, str.c_str(), str.size());
	return {result, str.size()};
}
duckdb_logical_type duckdb_get_value_type(duckdb_value val) {
	auto &type = UnwrapValue(val).type();
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(type));
}

char *duckdb_get_varchar(duckdb_value value) {
	auto val = reinterpret_cast<duckdb::Value *>(value);
	auto str_val = val->DefaultCastAs(duckdb::LogicalType::VARCHAR);
	auto &str = duckdb::StringValue::Get(str_val);

	auto result = reinterpret_cast<char *>(malloc(sizeof(char) * (str.size() + 1)));
	memcpy(result, str.c_str(), str.size());
	result[str.size()] = '\0';
	return result;
}

int64_t duckdb_get_int64(duckdb_value value) {
	auto val = reinterpret_cast<duckdb::Value *>(value);
	if (!val->DefaultTryCastAs(duckdb::LogicalType::BIGINT)) {
		return 0;
	}
	return duckdb::BigIntValue::Get(*val);
}

duckdb_value duckdb_create_struct_value(duckdb_logical_type type, duckdb_value *values) {
	if (!type || !values) {
		return nullptr;
	}
	const auto &logical_type = UnwrapType(type);
	if (logical_type.id() != duckdb::LogicalTypeId::STRUCT) {
		return nullptr;
	}
	if (duckdb::TypeVisitor::Contains(logical_type, duckdb::LogicalTypeId::INVALID) ||
	    duckdb::TypeVisitor::Contains(logical_type, duckdb::LogicalTypeId::ANY)) {
		return nullptr;
	}

	auto count = duckdb::StructType::GetChildCount(logical_type);
	duckdb::vector<duckdb::Value> unwrapped_values;
	for (idx_t i = 0; i < count; i++) {
		auto value = values[i];
		if (!value) {
			return nullptr;
		}
		unwrapped_values.emplace_back(UnwrapValue(value));
	}
	duckdb::Value *struct_value = new duckdb::Value;
	try {
		*struct_value = duckdb::Value::STRUCT(logical_type, std::move(unwrapped_values));
	} catch (...) {
		delete struct_value;
		return nullptr;
	}
	return WrapValue(struct_value);
}

duckdb_value duckdb_create_list_value(duckdb_logical_type type, duckdb_value *values, idx_t value_count) {
	if (!type || !values) {
		return nullptr;
	}
	auto &logical_type = UnwrapType(type);
	duckdb::vector<duckdb::Value> unwrapped_values;
	if (duckdb::TypeVisitor::Contains(logical_type, duckdb::LogicalTypeId::INVALID) ||
	    duckdb::TypeVisitor::Contains(logical_type, duckdb::LogicalTypeId::ANY)) {
		return nullptr;
	}

	for (idx_t i = 0; i < value_count; i++) {
		auto value = values[i];
		if (!value) {
			return nullptr;
		}
		unwrapped_values.push_back(UnwrapValue(value));
	}
	duckdb::Value *list_value = new duckdb::Value;
	try {
		*list_value = duckdb::Value::LIST(logical_type, std::move(unwrapped_values));
	} catch (...) {
		delete list_value;
		return nullptr;
	}
	return WrapValue(list_value);
}

duckdb_value duckdb_create_array_value(duckdb_logical_type type, duckdb_value *values, idx_t value_count) {
	if (!type || !values) {
		return nullptr;
	}
	if (value_count >= duckdb::ArrayType::MAX_ARRAY_SIZE) {
		return nullptr;
	}
	auto &logical_type = UnwrapType(type);
	if (duckdb::TypeVisitor::Contains(logical_type, duckdb::LogicalTypeId::INVALID) ||
	    duckdb::TypeVisitor::Contains(logical_type, duckdb::LogicalTypeId::ANY)) {
		return nullptr;
	}
	duckdb::vector<duckdb::Value> unwrapped_values;

	for (idx_t i = 0; i < value_count; i++) {
		auto value = values[i];
		if (!value) {
			return nullptr;
		}
		unwrapped_values.push_back(UnwrapValue(value));
	}
	duckdb::Value *array_value = new duckdb::Value;
	try {
		*array_value = duckdb::Value::ARRAY(logical_type, std::move(unwrapped_values));
	} catch (...) {
		delete array_value;
		return nullptr;
	}
	return WrapValue(array_value);
}
