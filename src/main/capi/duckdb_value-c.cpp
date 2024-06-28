#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types.hpp"

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
	const auto &ltype = UnwrapType(type);
	if (ltype.id() != duckdb::LogicalTypeId::STRUCT) {
		return nullptr;
	}
	auto count = duckdb::StructType::GetChildCount(ltype);

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
		*struct_value = duckdb::Value::STRUCT(ltype, std::move(unwrapped_values));
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
	auto &ltype = UnwrapType(type);
	duckdb::vector<duckdb::Value> unwrapped_values;

	for (idx_t i = 0; i < value_count; i++) {
		auto value = values[i];
		if (!value) {
			return nullptr;
		}
		unwrapped_values.push_back(UnwrapValue(value));
	}
	duckdb::Value *list_value = new duckdb::Value;
	try {
		*list_value = duckdb::Value::LIST(ltype, std::move(unwrapped_values));
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
	auto &ltype = UnwrapType(type);
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
		*array_value = duckdb::Value::ARRAY(ltype, std::move(unwrapped_values));
	} catch (...) {
		delete array_value;
		return nullptr;
	}
	return WrapValue(array_value);
}
