#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test decimal types C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));

	// decimal types
	result =
	    tester.Query("SELECT 1.0::DECIMAL(4,1), 2.0::DECIMAL(9,2), 3.0::DECIMAL(18,3), 4.0::DECIMAL(38,4), 5::INTEGER");
	REQUIRE(NO_FAIL(*result));
	REQUIRE(result->ColumnCount() == 5);
	REQUIRE(result->ErrorMessage() == nullptr);

	if (duckdb_vector_size() < 64) {
		return;
	}

	// fetch the first chunk
	REQUIRE(result->ChunkCount() == 1);
	auto chunk = result->FetchChunk(0);
	REQUIRE(chunk);

	duckdb::vector<uint8_t> widths = {4, 9, 18, 38, 0};
	duckdb::vector<uint8_t> scales = {1, 2, 3, 4, 0};
	duckdb::vector<duckdb_type> types = {DUCKDB_TYPE_DECIMAL, DUCKDB_TYPE_DECIMAL, DUCKDB_TYPE_DECIMAL,
	                                     DUCKDB_TYPE_DECIMAL, DUCKDB_TYPE_INTEGER};
	duckdb::vector<duckdb_type> internal_types = {DUCKDB_TYPE_SMALLINT, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_BIGINT,
	                                              DUCKDB_TYPE_HUGEINT, DUCKDB_TYPE_INVALID};
	for (idx_t i = 0; i < result->ColumnCount(); i++) {
		auto logical_type = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(chunk->GetChunk(), i));
		REQUIRE(logical_type);
		REQUIRE(duckdb_get_type_id(logical_type) == types[i]);
		REQUIRE(duckdb_decimal_width(logical_type) == widths[i]);
		REQUIRE(duckdb_decimal_scale(logical_type) == scales[i]);
		REQUIRE(duckdb_decimal_internal_type(logical_type) == internal_types[i]);

		duckdb_destroy_logical_type(&logical_type);
	}
	REQUIRE(duckdb_decimal_width(nullptr) == 0);
	REQUIRE(duckdb_decimal_scale(nullptr) == 0);
	REQUIRE(duckdb_decimal_internal_type(nullptr) == DUCKDB_TYPE_INVALID);
}

TEST_CASE("Test enum types C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	if (duckdb_vector_size() < 64) {
		return;
	}

	REQUIRE(tester.OpenDatabase(nullptr));
	result =
	    tester.Query("select small_enum, medium_enum, large_enum, int from test_all_types(use_large_enum = true);");
	REQUIRE(NO_FAIL(*result));
	REQUIRE(result->ColumnCount() == 4);
	REQUIRE(result->ErrorMessage() == nullptr);

	// fetch the first chunk
	REQUIRE(result->ChunkCount() == 1);
	auto chunk = result->FetchChunk(0);
	REQUIRE(chunk);

	duckdb::vector<duckdb_type> types = {DUCKDB_TYPE_ENUM, DUCKDB_TYPE_ENUM, DUCKDB_TYPE_ENUM, DUCKDB_TYPE_INTEGER};
	duckdb::vector<duckdb_type> internal_types = {DUCKDB_TYPE_UTINYINT, DUCKDB_TYPE_USMALLINT, DUCKDB_TYPE_UINTEGER,
	                                              DUCKDB_TYPE_INVALID};
	duckdb::vector<uint32_t> dictionary_sizes = {2, 300, 70000, 0};
	duckdb::vector<string> dictionary_strings = {"DUCK_DUCK_ENUM", "enum_0", "enum_0", string()};
	for (idx_t i = 0; i < result->ColumnCount(); i++) {
		auto logical_type = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(chunk->GetChunk(), i));
		REQUIRE(logical_type);
		REQUIRE(duckdb_get_type_id(logical_type) == types[i]);
		REQUIRE(duckdb_enum_internal_type(logical_type) == internal_types[i]);
		REQUIRE(duckdb_enum_dictionary_size(logical_type) == dictionary_sizes[i]);

		auto value = duckdb_enum_dictionary_value(logical_type, 0);
		string str_val = value ? string(value) : string();
		duckdb_free(value);

		REQUIRE(str_val == dictionary_strings[i]);

		duckdb_destroy_logical_type(&logical_type);
	}
	REQUIRE(duckdb_enum_internal_type(nullptr) == DUCKDB_TYPE_INVALID);
	REQUIRE(duckdb_enum_dictionary_size(nullptr) == 0);
	REQUIRE(duckdb_enum_dictionary_value(nullptr, 0) == nullptr);
}

TEST_CASE("Test list types C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));
	result = tester.Query("select [1, 2, 3] l, ['hello', 'world'] s, [[1, 2, 3], [4, 5]] nested, 3::int");
	REQUIRE(NO_FAIL(*result));
	REQUIRE(result->ColumnCount() == 4);
	REQUIRE(result->ErrorMessage() == nullptr);

	// fetch the first chunk
	REQUIRE(result->ChunkCount() == 1);
	auto chunk = result->FetchChunk(0);
	REQUIRE(chunk);

	duckdb::vector<duckdb_type> types = {DUCKDB_TYPE_LIST, DUCKDB_TYPE_LIST, DUCKDB_TYPE_LIST, DUCKDB_TYPE_INTEGER};
	duckdb::vector<duckdb_type> child_types_1 = {DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_LIST,
	                                             DUCKDB_TYPE_INVALID};
	duckdb::vector<duckdb_type> child_types_2 = {DUCKDB_TYPE_INVALID, DUCKDB_TYPE_INVALID, DUCKDB_TYPE_INTEGER,
	                                             DUCKDB_TYPE_INVALID};
	for (idx_t i = 0; i < result->ColumnCount(); i++) {
		auto logical_type = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(chunk->GetChunk(), i));
		REQUIRE(logical_type);
		REQUIRE(duckdb_get_type_id(logical_type) == types[i]);

		auto child_type = duckdb_list_type_child_type(logical_type);
		auto child_type_2 = duckdb_list_type_child_type(child_type);

		REQUIRE(duckdb_get_type_id(child_type) == child_types_1[i]);
		REQUIRE(duckdb_get_type_id(child_type_2) == child_types_2[i]);

		duckdb_destroy_logical_type(&child_type);
		duckdb_destroy_logical_type(&child_type_2);
		duckdb_destroy_logical_type(&logical_type);
	}
	REQUIRE(duckdb_list_type_child_type(nullptr) == nullptr);
}

TEST_CASE("Test struct types C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));
	result = tester.Query("select {'a': 42::int}, {'b': 'hello', 'c': [1, 2, 3]}, {'d': {'e': 42}}, 3::int");
	REQUIRE(NO_FAIL(*result));
	REQUIRE(result->ColumnCount() == 4);
	REQUIRE(result->ErrorMessage() == nullptr);

	// fetch the first chunk
	REQUIRE(result->ChunkCount() == 1);
	auto chunk = result->FetchChunk(0);
	REQUIRE(chunk);

	duckdb::vector<duckdb_type> types = {DUCKDB_TYPE_STRUCT, DUCKDB_TYPE_STRUCT, DUCKDB_TYPE_STRUCT,
	                                     DUCKDB_TYPE_INTEGER};
	duckdb::vector<idx_t> child_count = {1, 2, 1, 0};
	duckdb::vector<duckdb::vector<string>> child_names = {{"a"}, {"b", "c"}, {"d"}, {}};
	duckdb::vector<duckdb::vector<duckdb_type>> child_types = {
	    {DUCKDB_TYPE_INTEGER}, {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_LIST}, {DUCKDB_TYPE_STRUCT}, {}};
	for (idx_t i = 0; i < result->ColumnCount(); i++) {
		auto logical_type = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(chunk->GetChunk(), i));
		REQUIRE(logical_type);
		REQUIRE(duckdb_get_type_id(logical_type) == types[i]);

		REQUIRE(duckdb_struct_type_child_count(logical_type) == child_count[i]);
		for (idx_t c_idx = 0; c_idx < child_count[i]; c_idx++) {
			auto val = duckdb_struct_type_child_name(logical_type, c_idx);
			string str_val(val);
			duckdb_free(val);

			REQUIRE(child_names[i][c_idx] == str_val);

			auto child_type = duckdb_struct_type_child_type(logical_type, c_idx);
			REQUIRE(duckdb_get_type_id(child_type) == child_types[i][c_idx]);
			duckdb_destroy_logical_type(&child_type);
		}

		duckdb_destroy_logical_type(&logical_type);
	}
	REQUIRE(duckdb_struct_type_child_count(nullptr) == 0);
	REQUIRE(duckdb_struct_type_child_name(nullptr, 0) == nullptr);
	REQUIRE(duckdb_struct_type_child_type(nullptr, 0) == nullptr);
}

TEST_CASE("Test struct types creation C API", "[capi]") {
	duckdb::vector<duckdb_logical_type> types = {duckdb_create_logical_type(DUCKDB_TYPE_INTEGER),
	                                             duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR)};
	duckdb::vector<const char *> names = {"a", "b"};

	auto logical_type = duckdb_create_struct_type(types.data(), names.data(), types.size());
	REQUIRE(duckdb_get_type_id(logical_type) == duckdb_type::DUCKDB_TYPE_STRUCT);
	REQUIRE(duckdb_struct_type_child_count(logical_type) == 2);

	for (idx_t i = 0; i < names.size(); i++) {
		auto name = duckdb_struct_type_child_name(logical_type, i);
		string str_name(name);
		duckdb_free(name);
		REQUIRE(str_name == names[i]);

		auto type = duckdb_struct_type_child_type(logical_type, i);
		REQUIRE(duckdb_get_type_id(type) == duckdb_get_type_id(types[i]));
		duckdb_destroy_logical_type(&type);
	}

	for (idx_t i = 0; i < types.size(); i++) {
		duckdb_destroy_logical_type(&types[i]);
	}
	duckdb_destroy_logical_type(&logical_type);
}

TEST_CASE("Binding values", "[capi]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));

	duckdb_prepared_statement prepared;
	REQUIRE_SUCCESS(duckdb_prepare(tester.connection, "SELECT ?, ?", &prepared));

	std::vector<const char *> member_names {"hello"};
	duckdb::vector<duckdb_type> child_types = {DUCKDB_TYPE_INTEGER};
	auto member_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	idx_t member_count = member_names.size();
	auto struct_type = duckdb_create_struct_type(&member_type, member_names.data(), member_count);

	auto value = duckdb_create_int64(42);
	auto struct_value = duckdb_create_struct_value(struct_type, &value);
	duckdb_destroy_logical_type(&struct_type);

	duckdb::vector<duckdb_value> list_values {value};
	auto list_value = duckdb_create_list_value(member_type, list_values.data(), member_count);

	duckdb_destroy_value(&value);
	duckdb_destroy_logical_type(&member_type);

	duckdb_bind_value(prepared, 1, struct_value);
	duckdb_bind_value(prepared, 2, list_value);

	duckdb_destroy_value(&struct_value);
	duckdb_destroy_value(&list_value);

	auto result = tester.QueryPrepared(prepared);
	duckdb_destroy_prepare(&prepared);

	REQUIRE(result->ErrorMessage() == nullptr);
	REQUIRE(result->ColumnCount() == 2);

	// fetch the first chunk
	REQUIRE(result->ChunkCount() == 1);
	auto chunk = result->FetchChunk(0);
	REQUIRE(chunk);
	for (idx_t i = 0; i < result->ColumnCount(); i++) {
		duckdb_data_chunk current_chunk = chunk->GetChunk();
		auto current_vector = duckdb_data_chunk_get_vector(current_chunk, i);
		auto logical_type = duckdb_vector_get_column_type(current_vector);
		REQUIRE(logical_type);
		auto type_id = duckdb_get_type_id(logical_type);

		for (idx_t c_idx = 0; c_idx < member_count; c_idx++) {
			if (type_id == DUCKDB_TYPE_STRUCT) {
				auto val = duckdb_struct_type_child_name(logical_type, c_idx);
				string str_val(val);
				duckdb_free(val);

				REQUIRE(member_names[c_idx] == str_val);
				auto child_type = duckdb_struct_type_child_type(logical_type, c_idx);
				REQUIRE(duckdb_get_type_id(child_type) == DUCKDB_TYPE_BIGINT);
				duckdb_destroy_logical_type(&child_type);

				auto int64_vector = duckdb_struct_vector_get_child(current_vector, i);

				auto int64_data = (int64_t *)duckdb_vector_get_data(int64_vector);

				REQUIRE(int64_data[0] == 42);

			} else if (type_id == DUCKDB_TYPE_LIST) {
				auto child_type = duckdb_list_type_child_type(logical_type);
				// TODO: why is this one an integer, the other a bigint?
				REQUIRE(duckdb_get_type_id(child_type) == DUCKDB_TYPE_INTEGER);
				duckdb_destroy_logical_type(&child_type);

				REQUIRE(duckdb_list_vector_get_size(current_vector) == 1);
				auto int32_vector = duckdb_list_vector_get_child(current_vector);

				auto int32_data = (int32_t *)duckdb_vector_get_data(int32_vector);

				REQUIRE(int32_data[0] == 42);

			} else {
				FAIL();
			}
		}

		duckdb_destroy_logical_type(&logical_type);
	}
}
