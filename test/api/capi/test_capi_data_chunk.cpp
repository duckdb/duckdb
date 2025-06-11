#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test table_info incorrect 'is_valid' value for 'dflt_value' column", "[capi]") {
	duckdb_database db;
	duckdb_connection con;
	duckdb_result result;

	REQUIRE(duckdb_open(NULL, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);
	//! Create a table with 40 columns
	REQUIRE(duckdb_query(con,
	                     "CREATE TABLE foo (c00 varchar, c01 varchar, c02 varchar, c03 varchar, c04 varchar, c05 "
	                     "varchar, c06 varchar, c07 varchar, c08 varchar, c09 varchar, c10 varchar, c11 varchar, c12 "
	                     "varchar, c13 varchar, c14 varchar, c15 varchar, c16 varchar, c17 varchar, c18 varchar, c19 "
	                     "varchar, c20 varchar, c21 varchar, c22 varchar, c23 varchar, c24 varchar, c25 varchar, c26 "
	                     "varchar, c27 varchar, c28 varchar, c29 varchar, c30 varchar, c31 varchar, c32 varchar, c33 "
	                     "varchar, c34 varchar, c35 varchar, c36 varchar, c37 varchar, c38 varchar, c39 varchar);",
	                     NULL) != DuckDBError);
	//! Get table info for the created table
	REQUIRE(duckdb_query(con, "PRAGMA table_info(foo);", &result) != DuckDBError);

	//! Columns ({cid, name, type, notnull, dflt_value, pk}}
	idx_t col_count = duckdb_column_count(&result);
	REQUIRE(col_count == 6);
	idx_t chunk_count = duckdb_result_chunk_count(result);

	// Loop over the produced chunks
	for (idx_t chunk_idx = 0; chunk_idx < chunk_count; chunk_idx++) {
		duckdb_data_chunk chunk = duckdb_result_get_chunk(result, chunk_idx);
		idx_t row_count = duckdb_data_chunk_get_size(chunk);

		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
				//! Get the column
				duckdb_vector vector = duckdb_data_chunk_get_vector(chunk, col_idx);
				uint64_t *validity = duckdb_vector_get_validity(vector);
				bool is_valid = duckdb_validity_row_is_valid(validity, row_idx);

				if (col_idx == 4) {
					//'dflt_value' column
					REQUIRE(is_valid == false);
				}
			}
		}
		duckdb_destroy_data_chunk(&chunk);
	}

	duckdb_destroy_result(&result);
	duckdb_disconnect(&con);
	duckdb_close(&db);
}

TEST_CASE("Test Logical Types C API", "[capi]") {
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	REQUIRE(type);
	REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_BIGINT);
	duckdb_destroy_logical_type(&type);
	duckdb_destroy_logical_type(&type);

	// list type
	duckdb_logical_type elem_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_logical_type list_type = duckdb_create_list_type(elem_type);
	REQUIRE(duckdb_get_type_id(list_type) == DUCKDB_TYPE_LIST);
	duckdb_logical_type elem_type_dup = duckdb_list_type_child_type(list_type);
	REQUIRE(elem_type_dup != elem_type);
	REQUIRE(duckdb_get_type_id(elem_type_dup) == duckdb_get_type_id(elem_type));
	duckdb_destroy_logical_type(&elem_type);
	duckdb_destroy_logical_type(&list_type);
	duckdb_destroy_logical_type(&elem_type_dup);

	// map type
	duckdb_logical_type key_type = duckdb_create_logical_type(DUCKDB_TYPE_SMALLINT);
	duckdb_logical_type value_type = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
	duckdb_logical_type map_type = duckdb_create_map_type(key_type, value_type);
	REQUIRE(duckdb_get_type_id(map_type) == DUCKDB_TYPE_MAP);
	duckdb_logical_type key_type_dup = duckdb_map_type_key_type(map_type);
	duckdb_logical_type value_type_dup = duckdb_map_type_value_type(map_type);
	REQUIRE(key_type_dup != key_type);
	REQUIRE(value_type_dup != value_type);
	REQUIRE(duckdb_get_type_id(key_type_dup) == duckdb_get_type_id(key_type));
	REQUIRE(duckdb_get_type_id(value_type_dup) == duckdb_get_type_id(value_type));
	duckdb_destroy_logical_type(&key_type);
	duckdb_destroy_logical_type(&value_type);
	duckdb_destroy_logical_type(&map_type);
	duckdb_destroy_logical_type(&key_type_dup);
	duckdb_destroy_logical_type(&value_type_dup);

	duckdb_destroy_logical_type(nullptr);
}

TEST_CASE("Test DataChunk C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	duckdb_state status;

	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE(duckdb_vector_size() == STANDARD_VECTOR_SIZE);

	// create column types
	const idx_t COLUMN_COUNT = 3;
	duckdb_type duckdbTypes[COLUMN_COUNT];
	duckdbTypes[0] = DUCKDB_TYPE_BIGINT;
	duckdbTypes[1] = DUCKDB_TYPE_SMALLINT;
	duckdbTypes[2] = DUCKDB_TYPE_BLOB;

	duckdb_logical_type types[COLUMN_COUNT];
	for (idx_t i = 0; i < COLUMN_COUNT; i++) {
		types[i] = duckdb_create_logical_type(duckdbTypes[i]);
	}

	// create data chunk
	auto data_chunk = duckdb_create_data_chunk(types, COLUMN_COUNT);
	REQUIRE(data_chunk);
	REQUIRE(duckdb_data_chunk_get_column_count(data_chunk) == COLUMN_COUNT);

	// test duckdb_vector_get_column_type
	for (idx_t i = 0; i < COLUMN_COUNT; i++) {
		auto vector = duckdb_data_chunk_get_vector(data_chunk, i);
		auto type = duckdb_vector_get_column_type(vector);
		REQUIRE(duckdb_get_type_id(type) == duckdbTypes[i]);
		duckdb_destroy_logical_type(&type);
	}

	REQUIRE(duckdb_data_chunk_get_vector(data_chunk, 999) == nullptr);
	REQUIRE(duckdb_data_chunk_get_vector(nullptr, 0) == nullptr);
	REQUIRE(duckdb_vector_get_column_type(nullptr) == nullptr);
	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 0);
	REQUIRE(duckdb_data_chunk_get_size(nullptr) == 0);

	// create table
	tester.Query("CREATE TABLE test(i BIGINT, j SMALLINT, k BLOB)");

	// use the appender to insert values using the data chunk API
	duckdb_appender appender;
	status = duckdb_appender_create(tester.connection, nullptr, "test", &appender);
	REQUIRE(status == DuckDBSuccess);

	// get the column types from the appender
	REQUIRE(duckdb_appender_column_count(appender) == COLUMN_COUNT);

	// test duckdb_appender_column_type
	for (idx_t i = 0; i < COLUMN_COUNT; i++) {
		auto type = duckdb_appender_column_type(appender, i);
		REQUIRE(duckdb_get_type_id(type) == duckdbTypes[i]);
		duckdb_destroy_logical_type(&type);
	}

	// append BIGINT
	auto bigint_vector = duckdb_data_chunk_get_vector(data_chunk, 0);
	auto int64_ptr = (int64_t *)duckdb_vector_get_data(bigint_vector);
	*int64_ptr = 42;

	// append SMALLINT
	auto smallint_vector = duckdb_data_chunk_get_vector(data_chunk, 1);
	auto int16_ptr = (int16_t *)duckdb_vector_get_data(smallint_vector);
	*int16_ptr = 84;

	// append BLOB
	string s = "this is my blob";
	auto blob_vector = duckdb_data_chunk_get_vector(data_chunk, 2);
	duckdb_vector_assign_string_element_len(blob_vector, 0, s.c_str(), s.length());

	REQUIRE(duckdb_vector_get_data(nullptr) == nullptr);

	duckdb_data_chunk_set_size(data_chunk, 1);
	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 1);

	REQUIRE(duckdb_append_data_chunk(appender, data_chunk) == DuckDBSuccess);
	REQUIRE(duckdb_append_data_chunk(appender, nullptr) == DuckDBError);
	REQUIRE(duckdb_append_data_chunk(nullptr, data_chunk) == DuckDBError);

	// append nulls
	duckdb_data_chunk_reset(data_chunk);
	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 0);

	for (idx_t i = 0; i < COLUMN_COUNT; i++) {
		auto vector = duckdb_data_chunk_get_vector(data_chunk, i);
		duckdb_vector_ensure_validity_writable(vector);
		auto validity = duckdb_vector_get_validity(vector);

		REQUIRE(duckdb_validity_row_is_valid(validity, 0));
		duckdb_validity_set_row_validity(validity, 0, false);
		REQUIRE(!duckdb_validity_row_is_valid(validity, 0));
	}

	duckdb_data_chunk_set_size(data_chunk, 1);
	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 1);
	REQUIRE(duckdb_append_data_chunk(appender, data_chunk) == DuckDBSuccess);
	REQUIRE(duckdb_vector_get_validity(nullptr) == nullptr);

	duckdb_appender_destroy(&appender);

	result = tester.Query("SELECT * FROM test");
	REQUIRE_NO_FAIL(*result);

	REQUIRE(result->Fetch<int64_t>(0, 0) == 42);
	REQUIRE(result->Fetch<int16_t>(1, 0) == 84);
	REQUIRE(result->Fetch<string>(2, 0) == "this is my blob");

	REQUIRE(result->IsNull(0, 1));
	REQUIRE(result->IsNull(1, 1));
	REQUIRE(result->IsNull(2, 1));

	duckdb_data_chunk_reset(data_chunk);
	duckdb_data_chunk_reset(nullptr);
	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 0);

	duckdb_destroy_data_chunk(&data_chunk);
	duckdb_destroy_data_chunk(&data_chunk);
	duckdb_destroy_data_chunk(nullptr);

	for (idx_t i = 0; i < COLUMN_COUNT; i++) {
		duckdb_destroy_logical_type(&types[i]);
	}
}

TEST_CASE("Test DataChunk varchar result fetch in C API", "[capi]") {
	if (duckdb_vector_size() < 64) {
		return;
	}

	duckdb_database database;
	duckdb_connection connection;
	duckdb_state state;

	state = duckdb_open(nullptr, &database);
	REQUIRE(state == DuckDBSuccess);
	state = duckdb_connect(database, &connection);
	REQUIRE(state == DuckDBSuccess);

	constexpr const char *VARCHAR_TEST_QUERY = "select case when i != 0 and i % 42 = 0 then NULL else repeat(chr((65 + "
	                                           "(i % 26))::INTEGER), (4 + (i % 12))) end from range(5000) tbl(i);";

	// fetch a small result set
	duckdb_result result;
	state = duckdb_query(connection, VARCHAR_TEST_QUERY, &result);
	REQUIRE(state == DuckDBSuccess);

	REQUIRE(duckdb_column_count(&result) == 1);
	REQUIRE(duckdb_row_count(&result) == 5000);
	REQUIRE(duckdb_result_error(&result) == nullptr);

	idx_t expected_chunk_count = (5000 / STANDARD_VECTOR_SIZE) + (5000 % STANDARD_VECTOR_SIZE != 0);

	REQUIRE(duckdb_result_chunk_count(result) == expected_chunk_count);

	auto chunk = duckdb_result_get_chunk(result, 0);

	REQUIRE(duckdb_data_chunk_get_column_count(chunk) == 1);
	REQUIRE(STANDARD_VECTOR_SIZE < 5000);
	REQUIRE(duckdb_data_chunk_get_size(chunk) == STANDARD_VECTOR_SIZE);
	duckdb_destroy_data_chunk(&chunk);

	idx_t tuple_index = 0;
	auto chunk_amount = duckdb_result_chunk_count(result);
	for (idx_t chunk_index = 0; chunk_index < chunk_amount; chunk_index++) {
		chunk = duckdb_result_get_chunk(result, chunk_index);
		// Our result only has one column
		auto vector = duckdb_data_chunk_get_vector(chunk, 0);
		auto validity = duckdb_vector_get_validity(vector);
		auto string_data = (duckdb_string_t *)duckdb_vector_get_data(vector);

		auto tuples_in_chunk = duckdb_data_chunk_get_size(chunk);
		for (idx_t i = 0; i < tuples_in_chunk; i++, tuple_index++) {
			if (!duckdb_validity_row_is_valid(validity, i)) {
				// This entry is NULL
				REQUIRE((tuple_index != 0 && tuple_index % 42 == 0));
				continue;
			}
			idx_t expected_length = (tuple_index % 12) + 4;
			char expected_character = (tuple_index % 26) + 'A';

			// TODO: how does the c-api handle non-flat vectors?
			auto tuple = string_data[i];
			auto length = tuple.value.inlined.length;
			REQUIRE(length == expected_length);
			if (duckdb_string_is_inlined(tuple)) {
				// The data is small enough to fit in the string_t, it does not have a separate allocation
				for (idx_t string_index = 0; string_index < length; string_index++) {
					REQUIRE(tuple.value.inlined.inlined[string_index] == expected_character);
				}
			} else {
				for (idx_t string_index = 0; string_index < length; string_index++) {
					REQUIRE(tuple.value.pointer.ptr[string_index] == expected_character);
				}
			}
		}
		duckdb_destroy_data_chunk(&chunk);
	}
	duckdb_destroy_result(&result);
	duckdb_disconnect(&connection);
	duckdb_close(&database);
}

TEST_CASE("Test DataChunk result fetch in C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	if (duckdb_vector_size() < 64) {
		return;
	}

	REQUIRE(tester.OpenDatabase(nullptr));

	// fetch a small result set
	result = tester.Query("SELECT CASE WHEN i=1 THEN NULL ELSE i::INTEGER END i FROM range(3) tbl(i)");
	REQUIRE(NO_FAIL(*result));
	REQUIRE(result->ColumnCount() == 1);
	REQUIRE(result->row_count() == 3);
	REQUIRE(result->ErrorMessage() == nullptr);

	// fetch the first chunk
	REQUIRE(result->ChunkCount() == 1);
	auto chunk = result->FetchChunk(0);
	REQUIRE(chunk);

	REQUIRE(chunk->ColumnCount() == 1);
	REQUIRE(chunk->size() == 3);

	auto data = (int32_t *)chunk->GetData(0);
	auto validity = chunk->GetValidity(0);
	REQUIRE(data[0] == 0);
	REQUIRE(data[2] == 2);
	REQUIRE(duckdb_validity_row_is_valid(validity, 0));
	REQUIRE(!duckdb_validity_row_is_valid(validity, 1));
	REQUIRE(duckdb_validity_row_is_valid(validity, 2));

	// after fetching a chunk, we cannot use the old API anymore
	REQUIRE(result->ColumnData<int32_t>(0) == nullptr);
	REQUIRE(result->Fetch<int32_t>(0, 1) == 0);

	// result set is exhausted!
	chunk = result->FetchChunk(1);
	REQUIRE(!chunk);
}

TEST_CASE("Test duckdb_result_return_type", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));

	result = tester.Query("CREATE TABLE t (id INT)");
	REQUIRE(duckdb_result_return_type(result->InternalResult()) == DUCKDB_RESULT_TYPE_NOTHING);

	result = tester.Query("INSERT INTO t VALUES (42)");
	REQUIRE(duckdb_result_return_type(result->InternalResult()) == DUCKDB_RESULT_TYPE_CHANGED_ROWS);

	result = tester.Query("FROM t");
	REQUIRE(duckdb_result_return_type(result->InternalResult()) == DUCKDB_RESULT_TYPE_QUERY_RESULT);
}

TEST_CASE("Test DataChunk populate ListVector in C API", "[capi]") {
	if (duckdb_vector_size() < 3) {
		return;
	}
	REQUIRE(duckdb_list_vector_reserve(nullptr, 100) == duckdb_state::DuckDBError);
	REQUIRE(duckdb_list_vector_set_size(nullptr, 200) == duckdb_state::DuckDBError);

	auto elem_type = duckdb_create_logical_type(duckdb_type::DUCKDB_TYPE_INTEGER);
	auto list_type = duckdb_create_list_type(elem_type);
	duckdb_logical_type schema[] = {list_type};
	auto chunk = duckdb_create_data_chunk(schema, 1);
	auto list_vector = duckdb_data_chunk_get_vector(chunk, 0);
	duckdb_data_chunk_set_size(chunk, 3);

	REQUIRE(duckdb_list_vector_reserve(list_vector, 123) == duckdb_state::DuckDBSuccess);
	REQUIRE(duckdb_list_vector_get_size(list_vector) == 0);
	auto child = duckdb_list_vector_get_child(list_vector);
	for (int i = 0; i < 123; i++) {
		((int *)duckdb_vector_get_data(child))[i] = i;
	}
	REQUIRE(duckdb_list_vector_set_size(list_vector, 123) == duckdb_state::DuckDBSuccess);
	REQUIRE(duckdb_list_vector_get_size(list_vector) == 123);

	auto entries = (duckdb_list_entry *)duckdb_vector_get_data(list_vector);
	entries[0].offset = 0;
	entries[0].length = 20;
	entries[1].offset = 20;
	entries[1].length = 80;
	entries[2].offset = 100;
	entries[2].length = 23;

	auto child_data = (int *)duckdb_vector_get_data(child);
	int count = 0;
	for (idx_t i = 0; i < duckdb_data_chunk_get_size(chunk); i++) {
		for (idx_t j = 0; j < entries[i].length; j++) {
			REQUIRE(child_data[entries[i].offset + j] == count);
			count++;
		}
	}
	auto &vector = (Vector &)(*list_vector);
	for (int i = 0; i < 123; i++) {
		REQUIRE(ListVector::GetEntry(vector).GetValue(i) == i);
	}

	duckdb_destroy_data_chunk(&chunk);
	duckdb_destroy_logical_type(&list_type);
	duckdb_destroy_logical_type(&elem_type);
}

TEST_CASE("Test DataChunk populate ArrayVector in C API", "[capi]") {

	auto elem_type = duckdb_create_logical_type(duckdb_type::DUCKDB_TYPE_INTEGER);
	auto array_type = duckdb_create_array_type(elem_type, 3);
	duckdb_logical_type schema[] = {array_type};
	auto chunk = duckdb_create_data_chunk(schema, 1);
	duckdb_data_chunk_set_size(chunk, 2);
	auto array_vector = duckdb_data_chunk_get_vector(chunk, 0);

	auto child = duckdb_array_vector_get_child(array_vector);
	for (int i = 0; i < 6; i++) {
		((int *)duckdb_vector_get_data(child))[i] = i;
	}

	auto vec = (Vector &)(*array_vector);
	for (int i = 0; i < 2; i++) {
		auto child_vals = ArrayValue::GetChildren(vec.GetValue(i));
		for (int j = 0; j < 3; j++) {
			REQUIRE(child_vals[j].GetValue<int>() == i * 3 + j);
		}
	}

	duckdb_destroy_data_chunk(&chunk);
	duckdb_destroy_logical_type(&array_type);
	duckdb_destroy_logical_type(&elem_type);
}

TEST_CASE("Test PK violation in the C API appender", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE(duckdb_vector_size() == STANDARD_VECTOR_SIZE);

	// Create column types.
	const idx_t COLUMN_COUNT = 1;
	duckdb_logical_type types[COLUMN_COUNT];
	types[0] = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);

	// Create data chunk.
	auto data_chunk = duckdb_create_data_chunk(types, COLUMN_COUNT);
	auto bigint_vector = duckdb_data_chunk_get_vector(data_chunk, 0);
	auto int64_ptr = reinterpret_cast<int64_t *>(duckdb_vector_get_data(bigint_vector));
	int64_ptr[0] = 42;
	int64_ptr[1] = 42;
	duckdb_data_chunk_set_size(data_chunk, 2);

	// Use the appender to append the data chunk.
	tester.Query("CREATE TABLE test(i BIGINT PRIMARY KEY)");
	duckdb_appender appender;
	REQUIRE(duckdb_appender_create(tester.connection, nullptr, "test", &appender) == DuckDBSuccess);

	// We only flush when destroying the appender. Thus, we expect this to succeed, as we only
	// detect constraint violations when flushing the results.
	REQUIRE(duckdb_append_data_chunk(appender, data_chunk) == DuckDBSuccess);

	// duckdb_appender_close attempts to flush the data and fails.
	auto state = duckdb_appender_close(appender);
	REQUIRE(state == DuckDBError);
	auto error = duckdb_appender_error(appender);
	REQUIRE(duckdb::StringUtil::Contains(error, "PRIMARY KEY or UNIQUE constraint violation"));

	// Destroy the appender despite the error to avoid leaks.
	state = duckdb_appender_destroy(&appender);
	REQUIRE(state == DuckDBError);

	// Clean-up.
	duckdb_destroy_data_chunk(&data_chunk);
	for (idx_t i = 0; i < COLUMN_COUNT; i++) {
		duckdb_destroy_logical_type(&types[i]);
	}

	// Ensure that no rows were appended.
	result = tester.Query("SELECT * FROM test;");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->row_count() == 0);

	// Try again by appending rows and flushing.
	REQUIRE(duckdb_appender_create(tester.connection, nullptr, "test", &appender) == DuckDBSuccess);
	REQUIRE(duckdb_appender_begin_row(appender) == DuckDBSuccess);
	REQUIRE(duckdb_append_int64(appender, 42) == DuckDBSuccess);
	REQUIRE(duckdb_appender_end_row(appender) == DuckDBSuccess);
	REQUIRE(duckdb_appender_begin_row(appender) == DuckDBSuccess);
	REQUIRE(duckdb_append_int64(appender, 42) == DuckDBSuccess);
	REQUIRE(duckdb_appender_end_row(appender) == DuckDBSuccess);

	state = duckdb_appender_flush(appender);
	REQUIRE(state == DuckDBError);
	error = duckdb_appender_error(appender);
	REQUIRE(duckdb::StringUtil::Contains(error, "PRIMARY KEY or UNIQUE constraint violation"));
	REQUIRE(duckdb_appender_destroy(&appender) == DuckDBError);

	// Ensure that only the last row was appended.
	result = tester.Query("SELECT * FROM test;");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->row_count() == 0);
}

TEST_CASE("Test DataChunk write BLOB", "[capi]") {
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
	REQUIRE(type);
	REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_BLOB);
	duckdb_logical_type types[] = {type};
	auto chunk = duckdb_create_data_chunk(types, 1);
	duckdb_data_chunk_set_size(chunk, 1);
	duckdb_vector vector = duckdb_data_chunk_get_vector(chunk, 0);
	auto column_type = duckdb_vector_get_column_type(vector);
	REQUIRE(duckdb_get_type_id(column_type) == DUCKDB_TYPE_BLOB);
	duckdb_destroy_logical_type(&column_type);
	uint8_t bytes[] = {0x80, 0x00, 0x01, 0x2a};
	duckdb_vector_assign_string_element_len(vector, 0, (const char *)bytes, 4);
	auto string_data = static_cast<duckdb_string_t *>(duckdb_vector_get_data(vector));
	auto string_value = duckdb_string_t_data(string_data);
	REQUIRE(duckdb_string_t_length(*string_data) == 4);
	REQUIRE(string_value[0] == (char)0x80);
	REQUIRE(string_value[1] == (char)0x00);
	REQUIRE(string_value[2] == (char)0x01);
	REQUIRE(string_value[3] == (char)0x2a);
	duckdb_destroy_data_chunk(&chunk);
	duckdb_destroy_logical_type(&type);
}

TEST_CASE("Test DataChunk write VARINT", "[capi]") {
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_VARINT);
	REQUIRE(type);
	REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_VARINT);
	duckdb_logical_type types[] = {type};
	auto chunk = duckdb_create_data_chunk(types, 1);
	duckdb_data_chunk_set_size(chunk, 1);
	duckdb_vector vector = duckdb_data_chunk_get_vector(chunk, 0);
	auto column_type = duckdb_vector_get_column_type(vector);
	REQUIRE(duckdb_get_type_id(column_type) == DUCKDB_TYPE_VARINT);
	duckdb_destroy_logical_type(&column_type);
	uint8_t bytes[] = {0x80, 0x00, 0x01, 0x2a}; // VARINT 42
	duckdb_vector_assign_string_element_len(vector, 0, (const char *)bytes, 4);
	auto string_data = static_cast<duckdb_string_t *>(duckdb_vector_get_data(vector));
	auto string_value = duckdb_string_t_data(string_data);
	REQUIRE(duckdb_string_t_length(*string_data) == 4);
	REQUIRE(string_value[0] == (char)0x80);
	REQUIRE(string_value[1] == (char)0x00);
	REQUIRE(string_value[2] == (char)0x01);
	REQUIRE(string_value[3] == (char)0x2a);
	duckdb_destroy_data_chunk(&chunk);
	duckdb_destroy_logical_type(&type);
}

TEST_CASE("Test duckdb_data_chunk_copy", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	REQUIRE(tester.OpenDatabase(nullptr));

	SECTION("Test basic data chunk copy") {
		duckdb_data_chunk src_chunk, dst_chunk;
		duckdb_logical_type types[] = {duckdb_create_logical_type(DUCKDB_TYPE_INTEGER),
		                               duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR)};

		src_chunk = duckdb_create_data_chunk(types, 2);
		dst_chunk = duckdb_create_data_chunk(types, 2);

		int32_t *int_data =
		    reinterpret_cast<int32_t *>(duckdb_vector_get_data(duckdb_data_chunk_get_vector(src_chunk, 0)));
		int_data[0] = 42;
		int_data[1] = 99;

		duckdb_vector varchar_vector = duckdb_data_chunk_get_vector(src_chunk, 1);
		duckdb_vector_assign_string_element(varchar_vector, 0, "hello");
		duckdb_vector_assign_string_element(varchar_vector, 1, "world");

		duckdb_data_chunk_set_size(src_chunk, 2);

		duckdb_data_chunk_copy(src_chunk, dst_chunk);

		REQUIRE(duckdb_data_chunk_get_size(dst_chunk) == 2);
		REQUIRE(duckdb_data_chunk_get_column_count(dst_chunk) == 2);

		int32_t *dst_int_data = (int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(dst_chunk, 0));
		CHECK(dst_int_data[0] == 42);
		CHECK(dst_int_data[1] == 99);

		auto ddst = reinterpret_cast<duckdb::DataChunk *>(dst_chunk);
		auto val1 = ddst->GetValue(1, 0);
		auto val2 = ddst->GetValue(1, 1);
		char *str1 = duckdb_get_varchar(reinterpret_cast<duckdb_value>(&val1));
		char *str2 = duckdb_get_varchar(reinterpret_cast<duckdb_value>(&val2));
		CHECK(strcmp(str1, "hello") == 0);
		CHECK(strcmp(str2, "world") == 0);
		free(str1);
		free(str2);

		duckdb_destroy_data_chunk(&src_chunk);
		duckdb_destroy_data_chunk(&dst_chunk);
		for (size_t i = 0; i < 2; i++) {
			duckdb_destroy_logical_type(&types[i]);
		}
	}

	SECTION("Test copying an empty data chunk") {
		duckdb_data_chunk src_chunk, dst_chunk;
		duckdb_logical_type types[] = {duckdb_create_logical_type(DUCKDB_TYPE_BIGINT)};

		src_chunk = duckdb_create_data_chunk(types, 1);
		dst_chunk = duckdb_create_data_chunk(types, 1);

		duckdb_data_chunk_copy(src_chunk, dst_chunk);

		REQUIRE(duckdb_data_chunk_get_size(dst_chunk) == 0);
		REQUIRE(duckdb_data_chunk_get_column_count(dst_chunk) == 1);

		duckdb_destroy_data_chunk(&src_chunk);
		duckdb_destroy_data_chunk(&dst_chunk);
		duckdb_destroy_logical_type(&types[0]);
	}

	SECTION("Test copying a data chunk with NULL values") {
		duckdb_data_chunk src_chunk, dst_chunk;
		duckdb_logical_type types[] = {duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE)};
		src_chunk = duckdb_create_data_chunk(types, 1);
		dst_chunk = duckdb_create_data_chunk(types, 1);

		// Populate the source chunk with a NULL value
		duckdb_vector vector = duckdb_data_chunk_get_vector(src_chunk, 0);
		duckdb_vector_ensure_validity_writable(vector);
		uint64_t *validity = duckdb_vector_get_validity(vector);
		validity[0] = validity[0] & ~0x02; // Set the second value as NULL

		double *data = (double *)duckdb_vector_get_data(vector);
		data[0] = 3.14;
		duckdb_data_chunk_set_size(src_chunk, 2);

		duckdb_data_chunk_copy(src_chunk, dst_chunk);

		REQUIRE(duckdb_data_chunk_get_size(dst_chunk) == 2);
		duckdb_vector dst_vector = duckdb_data_chunk_get_vector(dst_chunk, 0);
		uint64_t *dst_validity = duckdb_vector_get_validity(dst_vector);
		double *dst_data = (double *)duckdb_vector_get_data(dst_vector);

		CHECK((~dst_validity[0]) == 0x02);
		CHECK(dst_data[0] == 3.14);

		duckdb_destroy_data_chunk(&src_chunk);
		duckdb_destroy_data_chunk(&dst_chunk);
		duckdb_destroy_logical_type(&types[0]);
	}
}

TEST_CASE("Test duckdb_data_chunk_copy_sel", "[capi]") {
	if (STANDARD_VECTOR_SIZE < 16) {
		SKIP_TEST("require " + std::to_string(STANDARD_VECTOR_SIZE) + " >= 16");
		return;
	}

	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	REQUIRE(tester.OpenDatabase(nullptr));

	// Common setup
	duckdb_data_chunk src_chunk, dst_chunk;
	duckdb_logical_type types[] = {duckdb_create_logical_type(DUCKDB_TYPE_INTEGER),
	                               duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR)};

	src_chunk = duckdb_create_data_chunk(types, 2);
	dst_chunk = duckdb_create_data_chunk(types, 2);

	// Populate the source chunk
	idx_t src_size = 10;
	auto int_data = (int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(src_chunk, 0));
	auto varchar_vector = duckdb_data_chunk_get_vector(src_chunk, 1);
	for (idx_t i = 0; i < src_size; ++i) {
		int_data[i] = i * 10;
		string str_val = "value_" + std::to_string(i);
		duckdb_vector_assign_string_element(varchar_vector, i, str_val.c_str());
	}
	duckdb_data_chunk_set_size(src_chunk, src_size);

	SECTION("Test basic selection copy") {
		// Select rows 1, 3, 4 from the source chunk
		idx_t selection_data[] = {1, 3, 4};
		duckdb_selection_vector sel_vector = duckdb_create_selection_vector(3);
		auto sel = reinterpret_cast<duckdb::SelectionVector *>(sel_vector);
		sel->set_index(0, 1);
		sel->set_index(1, 3);
		sel->set_index(2, 4);

		duckdb_data_chunk_copy_sel(src_chunk, dst_chunk, sel_vector, 3, 0);

		REQUIRE(duckdb_data_chunk_get_size(dst_chunk) == 3);
		auto dst_int_data = (int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(dst_chunk, 0));

		auto ddst = reinterpret_cast<duckdb::DataChunk *>(dst_chunk);
		for (idx_t i = 0; i < 3; ++i) {
			CHECK(dst_int_data[i] == int32_t(selection_data[i] * 10));
			auto val = ddst->GetValue(1, i);
			char *str = duckdb_get_varchar(reinterpret_cast<duckdb_value>(&val));
			string expected = "value_" + std::to_string(selection_data[i]);
			CHECK(strcmp(str, expected.c_str()) == 0);
			free(str);
		}

		delete sel;
	}

	SECTION("Test selection copy with an offset and count") {
		idx_t selection_data[] = {0, 2, 4, 6, 8};
		duckdb_selection_vector sel_vector = duckdb_create_selection_vector(5);
		auto sel = reinterpret_cast<duckdb::SelectionVector *>(sel_vector);
		for (idx_t i = 0; i < 5; ++i) {
			sel->set_index(i, selection_data[i]);
		}

		// Copy the first 4 items from the selection vector, but start at the second offset.
		// This means only selection index 2, 3 will be selected (pointing to source array index
		// 4, 6).
		idx_t source_count = 4;
		idx_t offset = 2;
		duckdb_data_chunk_copy_sel(src_chunk, dst_chunk, sel_vector, source_count, offset);

		REQUIRE(duckdb_data_chunk_get_size(dst_chunk) == source_count - offset);
		auto dst_int_data = (int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(dst_chunk, 0));

		auto ddst = reinterpret_cast<duckdb::DataChunk *>(dst_chunk);
		for (idx_t i = 0; i < source_count - offset; ++i) {
			CHECK(dst_int_data[i] == int32_t(selection_data[i + offset] * 10));
			auto val = ddst->GetValue(1, i);
			char *str = duckdb_get_varchar(reinterpret_cast<duckdb_value>(&val));
			string expected = "value_" + std::to_string(selection_data[i + offset]);
			CHECK(strcmp(str, expected.c_str()) == 0);
			free(str);
		}

		delete sel;
	}

	SECTION("Test selection copy with zero count") {
		idx_t selection_data[] = {0, 2, 3, 4};
		duckdb_selection_vector sel_vector = duckdb_create_selection_vector(4);
		auto sel = reinterpret_cast<duckdb::SelectionVector *>(sel_vector);
		for (idx_t i = 0; i < 4; ++i) {
			sel->set_index(i, selection_data[i]);
		}

		// Attempt to copy 0 rows
		duckdb_data_chunk_copy_sel(src_chunk, dst_chunk, sel_vector, 0, 0);

		// Verify the destination chunk is empty
		REQUIRE(duckdb_data_chunk_get_size(dst_chunk) == 0);

		delete sel;
	}

	// Clean up common resources
	duckdb_destroy_data_chunk(&src_chunk);
	duckdb_destroy_data_chunk(&dst_chunk);
	for (size_t i = 0; i < 2; i++) {
		duckdb_destroy_logical_type(&types[i]);
	}
}

TEST_CASE("Test duckdb_data_chunk_reference_columns", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	REQUIRE(tester.OpenDatabase(nullptr));

	duckdb_data_chunk src_chunk;
	duckdb_logical_type src_types[] = {duckdb_create_logical_type(DUCKDB_TYPE_INTEGER),
	                                   duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE),
	                                   duckdb_create_logical_type(DUCKDB_TYPE_BIGINT)};
	src_chunk = duckdb_create_data_chunk(src_types, 3);

	auto src_int_vector = duckdb_data_chunk_get_vector(src_chunk, 0);
	auto src_double_vector = duckdb_data_chunk_get_vector(src_chunk, 1);
	auto src_bigint_vector = duckdb_data_chunk_get_vector(src_chunk, 2);

	auto src_int_data = (int32_t *)duckdb_vector_get_data(src_int_vector);
	auto src_double_data = (double *)duckdb_vector_get_data(src_double_vector);
	auto src_bigint_data = (int64_t *)duckdb_vector_get_data(src_bigint_vector);

	src_int_data[0] = 42;
	src_int_data[1] = 99;
	src_double_data[0] = 0.5;
	src_double_data[1] = 1.5;
	src_bigint_data[0] = 1000;
	src_bigint_data[1] = 2000;
	duckdb_data_chunk_set_size(src_chunk, 2);

	SECTION("Test basic column reference") {
		duckdb_data_chunk dst_chunk;
		duckdb_logical_type dst_types[] = {duckdb_create_logical_type(DUCKDB_TYPE_BIGINT),
		                                   duckdb_create_logical_type(DUCKDB_TYPE_INTEGER)};
		dst_chunk = duckdb_create_data_chunk(dst_types, 2);

		idx_t ref_indices[] = {2, 0};

		duckdb_data_chunk_reference_columns(src_chunk, dst_chunk, ref_indices, 2);

		REQUIRE(duckdb_data_chunk_get_column_count(dst_chunk) == 2);
		REQUIRE(duckdb_data_chunk_get_size(dst_chunk) == 2);

		auto dst_type_0 = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(dst_chunk, 0));
		auto dst_type_1 = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(dst_chunk, 1));
		REQUIRE(duckdb_get_type_id(dst_type_0) == DUCKDB_TYPE_BIGINT);
		REQUIRE(duckdb_get_type_id(dst_type_1) == DUCKDB_TYPE_INTEGER);
		duckdb_destroy_logical_type(&dst_type_0);
		duckdb_destroy_logical_type(&dst_type_1);

		// Verify that the data pointers are the same
		auto dst_bigint_vector = duckdb_data_chunk_get_vector(dst_chunk, 0);
		auto dst_int_vector = duckdb_data_chunk_get_vector(dst_chunk, 1);
		REQUIRE(duckdb_vector_get_data(dst_bigint_vector) == duckdb_vector_get_data(src_bigint_vector));
		REQUIRE(duckdb_vector_get_data(dst_int_vector) == duckdb_vector_get_data(src_int_vector));

		src_bigint_data[0] = 9999;
		auto dst_bigint_data = (int64_t *)duckdb_vector_get_data(dst_bigint_vector);
		REQUIRE(dst_bigint_data[0] == 9999);

		duckdb_destroy_data_chunk(&dst_chunk);
		for (size_t i = 0; i < 2; i++) {
			duckdb_destroy_logical_type(&dst_types[i]);
		}
	}

	duckdb_destroy_data_chunk(&src_chunk);
	for (size_t i = 0; i < 3; i++) {
		duckdb_destroy_logical_type(&src_types[i]);
	}
}
