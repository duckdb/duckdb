#include "duckdb/main/capi/capi_internal.hpp"

Value *UnwrapValue(duckdb_value *value) {
	if (value && *value) {
		return reinterpret_cast<duckdb::Value *>(*value);
	}
	return nullptr;
}
void duckdb_destroy_value(duckdb_value *value) {
	auto val = UnwrapValue(value);
	if (val) {
		delete val;
		*value = nullptr;
	}
}

duckdb_value duckdb_create_varchar_length(const char *text, idx_t length) {
	return reinterpret_cast<duckdb_value>(new duckdb::Value(std::string(text, length)));
}

duckdb_value duckdb_create_varchar(const char *text) {
	return duckdb_create_varchar_length(text, strlen(text));
}

duckdb_value duckdb_create_int64(int64_t input) {
	auto val = duckdb::Value::BIGINT(input);
	return reinterpret_cast<duckdb_value>(new duckdb::Value(val));
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
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	if (ltype.id() != duckdb::LogicalTypeId::STRUCT) {
		return nullptr;
	}
	auto &struct_type = duckdb::StructType::GetChildTypes(ltype);
	auto count = struct_type.size();

	duckdb::child_list_t<Value> unwrapped_values;
	for (idx_t i = 0; i < count; i++) {
		auto val = UnwrapValue(&values[i]);
		if (!val) {
			return nullptr;
		}
		unwrapped_values.emplace_back(struct_type.get(i).first, *val);
	}
	duckdb::Value *struct_value = new duckdb::Value;
	*struct_value = duckdb::Value::STRUCT(unwrapped_values);
	return reinterpret_cast<duckdb_value>(struct_value);
}
