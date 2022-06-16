#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include <cmath>
#include <limits>

namespace duckdb {

struct TestAllTypesData : public GlobalTableFunctionState {
	TestAllTypesData() : offset(0) {
	}

	vector<vector<Value>> entries;
	idx_t offset;
};

vector<TestType> TestAllTypesFun::GetTestTypes() {
	vector<TestType> result;
	// scalar types/numerics
	result.emplace_back(LogicalType::BOOLEAN, "bool");
	result.emplace_back(LogicalType::TINYINT, "tinyint");
	result.emplace_back(LogicalType::SMALLINT, "smallint");
	result.emplace_back(LogicalType::INTEGER, "int");
	result.emplace_back(LogicalType::BIGINT, "bigint");
	result.emplace_back(LogicalType::HUGEINT, "hugeint");
	result.emplace_back(LogicalType::UTINYINT, "utinyint");
	result.emplace_back(LogicalType::USMALLINT, "usmallint");
	result.emplace_back(LogicalType::UINTEGER, "uint");
	result.emplace_back(LogicalType::UBIGINT, "ubigint");
	result.emplace_back(LogicalType::DATE, "date");
	result.emplace_back(LogicalType::TIME, "time");
	result.emplace_back(LogicalType::TIMESTAMP, "timestamp");
	result.emplace_back(LogicalType::TIMESTAMP_S, "timestamp_s");
	result.emplace_back(LogicalType::TIMESTAMP_MS, "timestamp_ms");
	result.emplace_back(LogicalType::TIMESTAMP_NS, "timestamp_ns");
	result.emplace_back(LogicalType::TIME_TZ, "time_tz");
	result.emplace_back(LogicalType::TIMESTAMP_TZ, "timestamp_tz");
	result.emplace_back(LogicalType::FLOAT, "float");
	result.emplace_back(LogicalType::DOUBLE, "double");
	result.emplace_back(LogicalType::DECIMAL(4, 1), "dec_4_1");
	result.emplace_back(LogicalType::DECIMAL(9, 4), "dec_9_4");
	result.emplace_back(LogicalType::DECIMAL(18, 6), "dec_18_6");
	result.emplace_back(LogicalType::DECIMAL(38, 10), "dec38_10");
	result.emplace_back(LogicalType::UUID, "uuid");

	// interval
	interval_t min_interval;
	min_interval.months = 0;
	min_interval.days = 0;
	min_interval.micros = 0;

	interval_t max_interval;
	max_interval.months = 999;
	max_interval.days = 999;
	max_interval.micros = 999999999;
	result.emplace_back(LogicalType::INTERVAL, "interval", Value::INTERVAL(min_interval),
	                    Value::INTERVAL(max_interval));
	// strings/blobs
	result.emplace_back(LogicalType::VARCHAR, "varchar", Value("🦆🦆🦆🦆🦆🦆"), Value("goose"));
	result.emplace_back(LogicalType::JSON, "json", Value("🦆🦆🦆🦆🦆🦆"), Value("goose"));
	result.emplace_back(LogicalType::BLOB, "blob", Value::BLOB("thisisalongblob\\x00withnullbytes"),
	                    Value("\\x00\\x00\\x00a"));

	// enums
	Vector small_enum(LogicalType::VARCHAR, 2);
	auto small_enum_ptr = FlatVector::GetData<string_t>(small_enum);
	small_enum_ptr[0] = StringVector::AddStringOrBlob(small_enum, "DUCK_DUCK_ENUM");
	small_enum_ptr[1] = StringVector::AddStringOrBlob(small_enum, "GOOSE");
	result.emplace_back(LogicalType::ENUM("small_enum", small_enum, 2), "small_enum");

	Vector medium_enum(LogicalType::VARCHAR, 300);
	auto medium_enum_ptr = FlatVector::GetData<string_t>(medium_enum);
	for (idx_t i = 0; i < 300; i++) {
		medium_enum_ptr[i] = StringVector::AddStringOrBlob(medium_enum, string("enum_") + to_string(i));
	}
	result.emplace_back(LogicalType::ENUM("medium_enum", medium_enum, 300), "medium_enum");

	// this is a big one... not sure if we should push this one here, but it's required for completeness
	Vector large_enum(LogicalType::VARCHAR, 70000);
	auto large_enum_ptr = FlatVector::GetData<string_t>(large_enum);
	for (idx_t i = 0; i < 70000; i++) {
		large_enum_ptr[i] = StringVector::AddStringOrBlob(large_enum, string("enum_") + to_string(i));
	}
	result.emplace_back(LogicalType::ENUM("large_enum", large_enum, 70000), "large_enum");

	// arrays
	auto int_list_type = LogicalType::LIST(LogicalType::INTEGER);
	auto empty_int_list = Value::EMPTYLIST(LogicalType::INTEGER);
	auto int_list = Value::LIST({Value::INTEGER(42), Value::INTEGER(999), Value(LogicalType::INTEGER),
	                             Value(LogicalType::INTEGER), Value::INTEGER(-42)});
	result.emplace_back(int_list_type, "int_array", empty_int_list, int_list);

	auto double_list_type = LogicalType::LIST(LogicalType::DOUBLE);
	auto empty_double_list = Value::EMPTYLIST(LogicalType::DOUBLE);
	auto double_list = Value::LIST(
	    {Value::DOUBLE(42), Value::DOUBLE(NAN), Value::DOUBLE(std::numeric_limits<double>::infinity()),
	     Value::DOUBLE(-std::numeric_limits<double>::infinity()), Value(LogicalType::DOUBLE), Value::DOUBLE(-42)});
	result.emplace_back(double_list_type, "double_array", empty_double_list, double_list);

	auto date_list_type = LogicalType::LIST(LogicalType::DATE);
	auto empty_date_list = Value::EMPTYLIST(LogicalType::DATE);
	auto date_list =
	    Value::LIST({Value::DATE(date_t()), Value::DATE(date_t::infinity()), Value::DATE(date_t::ninfinity()),
	                 Value(LogicalType::DATE), Value::DATE(Date::FromString("2022-05-12"))});
	result.emplace_back(date_list_type, "date_array", empty_date_list, date_list);

	auto timestamp_list_type = LogicalType::LIST(LogicalType::TIMESTAMP);
	auto empty_timestamp_list = Value::EMPTYLIST(LogicalType::TIMESTAMP);
	auto timestamp_list = Value::LIST({Value::TIMESTAMP(timestamp_t()), Value::TIMESTAMP(timestamp_t::infinity()),
	                                   Value::TIMESTAMP(timestamp_t::ninfinity()), Value(LogicalType::TIMESTAMP),
	                                   Value::TIMESTAMP(Timestamp::FromString("2022-05-12 16:23:45"))});
	result.emplace_back(timestamp_list_type, "timestamp_array", empty_timestamp_list, timestamp_list);

	auto timestamptz_list_type = LogicalType::LIST(LogicalType::TIMESTAMP_TZ);
	auto empty_timestamptz_list = Value::EMPTYLIST(LogicalType::TIMESTAMP_TZ);
	auto timestamptz_list = Value::LIST({Value::TIMESTAMPTZ(timestamp_t()), Value::TIMESTAMPTZ(timestamp_t::infinity()),
	                                     Value::TIMESTAMPTZ(timestamp_t::ninfinity()), Value(LogicalType::TIMESTAMP_TZ),
	                                     Value::TIMESTAMPTZ(Timestamp::FromString("2022-05-12 16:23:45-07"))});
	result.emplace_back(timestamptz_list_type, "timestamptz_array", empty_timestamptz_list, timestamptz_list);

	auto varchar_list_type = LogicalType::LIST(LogicalType::VARCHAR);
	auto empty_varchar_list = Value::EMPTYLIST(LogicalType::VARCHAR);
	auto varchar_list =
	    Value::LIST({Value("🦆🦆🦆🦆🦆🦆"), Value("goose"), Value(LogicalType::VARCHAR), Value("")});
	result.emplace_back(varchar_list_type, "varchar_array", empty_varchar_list, varchar_list);

	// nested arrays
	auto nested_list_type = LogicalType::LIST(int_list_type);
	auto empty_nested_list = Value::EMPTYLIST(int_list_type);
	auto nested_int_list = Value::LIST({empty_int_list, int_list, Value(int_list_type), empty_int_list, int_list});
	result.emplace_back(nested_list_type, "nested_int_array", empty_nested_list, nested_int_list);

	// structs
	child_list_t<LogicalType> struct_type_list;
	struct_type_list.push_back(make_pair("a", LogicalType::INTEGER));
	struct_type_list.push_back(make_pair("b", LogicalType::VARCHAR));
	auto struct_type = LogicalType::STRUCT(move(struct_type_list));

	child_list_t<Value> min_struct_list;
	min_struct_list.push_back(make_pair("a", Value(LogicalType::INTEGER)));
	min_struct_list.push_back(make_pair("b", Value(LogicalType::VARCHAR)));
	auto min_struct_val = Value::STRUCT(move(min_struct_list));

	child_list_t<Value> max_struct_list;
	max_struct_list.push_back(make_pair("a", Value::INTEGER(42)));
	max_struct_list.push_back(make_pair("b", Value("🦆🦆🦆🦆🦆🦆")));
	auto max_struct_val = Value::STRUCT(move(max_struct_list));

	result.emplace_back(struct_type, "struct", min_struct_val, max_struct_val);

	// structs with lists
	child_list_t<LogicalType> struct_list_type_list;
	struct_list_type_list.push_back(make_pair("a", int_list_type));
	struct_list_type_list.push_back(make_pair("b", varchar_list_type));
	auto struct_list_type = LogicalType::STRUCT(move(struct_list_type_list));

	child_list_t<Value> min_struct_vl_list;
	min_struct_vl_list.push_back(make_pair("a", Value(int_list_type)));
	min_struct_vl_list.push_back(make_pair("b", Value(varchar_list_type)));
	auto min_struct_val_list = Value::STRUCT(move(min_struct_vl_list));

	child_list_t<Value> max_struct_vl_list;
	max_struct_vl_list.push_back(make_pair("a", int_list));
	max_struct_vl_list.push_back(make_pair("b", varchar_list));
	auto max_struct_val_list = Value::STRUCT(move(max_struct_vl_list));

	result.emplace_back(struct_list_type, "struct_of_arrays", move(min_struct_val_list), move(max_struct_val_list));

	// array of structs
	auto array_of_structs_type = LogicalType::LIST(struct_type);
	auto min_array_of_struct_val = Value::EMPTYLIST(struct_type);
	auto max_array_of_struct_val = Value::LIST({min_struct_val, max_struct_val, Value(struct_type)});
	result.emplace_back(array_of_structs_type, "array_of_structs", move(min_array_of_struct_val),
	                    move(max_array_of_struct_val));

	// map
	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	auto min_map_value = Value::MAP(Value::EMPTYLIST(LogicalType::VARCHAR), Value::EMPTYLIST(LogicalType::VARCHAR));
	auto max_map_value = Value::MAP(Value::LIST({Value("key1"), Value("key2")}),
	                                Value::LIST({Value("🦆🦆🦆🦆🦆🦆"), Value("goose")}));
	result.emplace_back(map_type, "map", move(min_map_value), move(max_map_value));

	return result;
}

static unique_ptr<FunctionData> TestAllTypesBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto test_types = TestAllTypesFun::GetTestTypes();
	for (auto &test_type : test_types) {
		return_types.push_back(move(test_type.type));
		names.push_back(move(test_type.name));
	}
	return nullptr;
}

unique_ptr<GlobalTableFunctionState> TestAllTypesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_unique<TestAllTypesData>();
	auto test_types = TestAllTypesFun::GetTestTypes();
	// 3 rows: min, max and NULL
	result->entries.resize(3);
	// initialize the values
	for (auto &test_type : test_types) {
		result->entries[0].push_back(move(test_type.min_value));
		result->entries[1].push_back(move(test_type.max_value));
		result->entries[2].emplace_back(move(test_type.type));
	}
	return move(result);
}

void TestAllTypesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (TestAllTypesData &)*data_p.global_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &vals = data.entries[data.offset++];
		for (idx_t col_idx = 0; col_idx < vals.size(); col_idx++) {
			output.SetValue(col_idx, count, vals[col_idx]);
		}
		count++;
	}
	output.SetCardinality(count);
}

void TestAllTypesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("test_all_types", {}, TestAllTypesFunction, TestAllTypesBind, TestAllTypesInit));
}

} // namespace duckdb
