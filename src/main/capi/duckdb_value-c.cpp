#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/capi/capi_internal.hpp"

using duckdb::LogicalTypeId;

static duckdb_value WrapValue(duckdb::Value *value) {
	return reinterpret_cast<duckdb_value>(value);
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

template <class T>
static duckdb_value CAPICreateValue(T input) {
	return WrapValue(new duckdb::Value(duckdb::Value::CreateValue<T>(input)));
}

template <class T, LogicalTypeId TYPE_ID>
static T CAPIGetValue(duckdb_value val) {
	auto &v = UnwrapValue(val);
	if (!v.DefaultTryCastAs(TYPE_ID)) {
		return duckdb::NullValue<T>();
	}
	return v.GetValue<T>();
}

duckdb_value duckdb_create_bool(bool input) {
	return CAPICreateValue(input);
}
bool duckdb_get_bool(duckdb_value val) {
	return CAPIGetValue<bool, LogicalTypeId::BOOLEAN>(val);
}
duckdb_value duckdb_create_int8(int8_t input) {
	return CAPICreateValue(input);
}
int8_t duckdb_get_int8(duckdb_value val) {
	return CAPIGetValue<int8_t, LogicalTypeId::TINYINT>(val);
}
duckdb_value duckdb_create_uint8(uint8_t input) {
	return CAPICreateValue(input);
}
uint8_t duckdb_get_uint8(duckdb_value val) {
	return CAPIGetValue<uint8_t, LogicalTypeId::UTINYINT>(val);
}
duckdb_value duckdb_create_int16(int16_t input) {
	return CAPICreateValue(input);
}
int16_t duckdb_get_int16(duckdb_value val) {
	return CAPIGetValue<int16_t, LogicalTypeId::SMALLINT>(val);
}
duckdb_value duckdb_create_uint16(uint16_t input) {
	return CAPICreateValue(input);
}
uint16_t duckdb_get_uint16(duckdb_value val) {
	return CAPIGetValue<uint16_t, LogicalTypeId::USMALLINT>(val);
}
duckdb_value duckdb_create_int32(int32_t input) {
	return CAPICreateValue(input);
}
int32_t duckdb_get_int32(duckdb_value val) {
	return CAPIGetValue<int32_t, LogicalTypeId::INTEGER>(val);
}
duckdb_value duckdb_create_uint32(uint32_t input) {
	return CAPICreateValue(input);
}
uint32_t duckdb_get_uint32(duckdb_value val) {
	return CAPIGetValue<uint32_t, LogicalTypeId::UINTEGER>(val);
}
duckdb_value duckdb_create_uint64(uint64_t input) {
	return CAPICreateValue(input);
}
uint64_t duckdb_get_uint64(duckdb_value val) {
	return CAPIGetValue<uint64_t, LogicalTypeId::UBIGINT>(val);
}
duckdb_value duckdb_create_int64(int64_t input) {
	return CAPICreateValue(input);
}
int64_t duckdb_get_int64(duckdb_value val) {
	return CAPIGetValue<int64_t, LogicalTypeId::BIGINT>(val);
}
duckdb_value duckdb_create_hugeint(duckdb_hugeint input) {
	return WrapValue(new duckdb::Value(duckdb::Value::HUGEINT(duckdb::hugeint_t(input.upper, input.lower))));
}
duckdb_hugeint duckdb_get_hugeint(duckdb_value val) {
	auto res = CAPIGetValue<duckdb::hugeint_t, LogicalTypeId::HUGEINT>(val);
	return {res.lower, res.upper};
}
duckdb_value duckdb_create_uhugeint(duckdb_uhugeint input) {
	return WrapValue(new duckdb::Value(duckdb::Value::UHUGEINT(duckdb::uhugeint_t(input.upper, input.lower))));
}
duckdb_uhugeint duckdb_get_uhugeint(duckdb_value val) {
	auto res = CAPIGetValue<duckdb::uhugeint_t, LogicalTypeId::UHUGEINT>(val);
	return {res.lower, res.upper};
}
duckdb_value duckdb_create_float(float input) {
	return CAPICreateValue(input);
}
float duckdb_get_float(duckdb_value val) {
	return CAPIGetValue<float, LogicalTypeId::FLOAT>(val);
}
duckdb_value duckdb_create_double(double input) {
	return CAPICreateValue(input);
}
double duckdb_get_double(duckdb_value val) {
	return CAPIGetValue<double, LogicalTypeId::DOUBLE>(val);
}
duckdb_value duckdb_create_date(duckdb_date input) {
	return CAPICreateValue(duckdb::date_t(input.days));
}
duckdb_date duckdb_get_date(duckdb_value val) {
	return {CAPIGetValue<duckdb::date_t, LogicalTypeId::DATE>(val).days};
}
duckdb_value duckdb_create_time(duckdb_time input) {
	return CAPICreateValue(duckdb::dtime_t(input.micros));
}
duckdb_time duckdb_get_time(duckdb_value val) {
	return {CAPIGetValue<duckdb::dtime_t, LogicalTypeId::TIME>(val).micros};
}
duckdb_value duckdb_create_time_tz_value(duckdb_time_tz input) {
	return CAPICreateValue(duckdb::dtime_tz_t(input.bits));
}
duckdb_time_tz duckdb_get_time_tz(duckdb_value val) {
	return {CAPIGetValue<duckdb::dtime_tz_t, LogicalTypeId::TIME_TZ>(val).bits};
}
duckdb_value duckdb_create_timestamp(duckdb_timestamp input) {
	return CAPICreateValue(duckdb::timestamp_t(input.micros));
}
duckdb_timestamp duckdb_get_timestamp(duckdb_value val) {
	return {CAPIGetValue<duckdb::timestamp_t, LogicalTypeId::TIMESTAMP>(val).value};
}
duckdb_value duckdb_create_interval(duckdb_interval input) {
	return WrapValue(new duckdb::Value(duckdb::Value::INTERVAL(input.months, input.days, input.micros)));
}
duckdb_interval duckdb_get_interval(duckdb_value val) {
	auto interval = CAPIGetValue<duckdb::interval_t, LogicalTypeId::INTERVAL>(val);
	return {interval.months, interval.days, interval.micros};
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
	return (duckdb_logical_type)(&type);
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

idx_t duckdb_get_map_size(duckdb_value value) {
	if (!value) {
		return 0;
	}

	auto val = UnwrapValue(value);
	if (val.type().id() != LogicalTypeId::MAP) {
		return 0;
	}

	auto &children = duckdb::MapValue::GetChildren(val);
	return children.size();
}

duckdb_value duckdb_get_map_key(duckdb_value value, idx_t index) {
	if (!value) {
		return nullptr;
	}

	auto val = UnwrapValue(value);
	if (val.type().id() != LogicalTypeId::MAP) {
		return nullptr;
	}

	auto &children = duckdb::MapValue::GetChildren(val);
	if (index >= children.size()) {
		return nullptr;
	}

	auto &child = children[index];
	auto &child_struct = duckdb::StructValue::GetChildren(child);
	return WrapValue(new duckdb::Value(child_struct[0]));
}

duckdb_value duckdb_get_map_value(duckdb_value value, idx_t index) {
	if (!value) {
		return nullptr;
	}

	auto val = UnwrapValue(value);
	if (val.type().id() != LogicalTypeId::MAP) {
		return nullptr;
	}

	auto &children = duckdb::MapValue::GetChildren(val);
	if (index >= children.size()) {
		return nullptr;
	}

	auto &child = children[index];
	auto &child_struct = duckdb::StructValue::GetChildren(child);
	return WrapValue(new duckdb::Value(child_struct[1]));
}
