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

TEST_CASE("Test DataChunk appending incorrect types in C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	duckdb_state status;

	REQUIRE(tester.OpenDatabase(nullptr));

	REQUIRE(duckdb_vector_size() == STANDARD_VECTOR_SIZE);

	tester.Query("CREATE TABLE test(i BIGINT, j SMALLINT)");

	duckdb_logical_type types[2];
	types[0] = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	types[1] = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);

	auto data_chunk = duckdb_create_data_chunk(types, 2);
	REQUIRE(data_chunk);

	auto col1_ptr = (int64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(data_chunk, 0));
	*col1_ptr = 42;
	auto col2_ptr = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(data_chunk, 1));
	*col2_ptr = false;

	duckdb_appender appender;
	status = duckdb_appender_create(tester.connection, nullptr, "test", &appender);
	REQUIRE(status == DuckDBSuccess);

	REQUIRE(duckdb_append_data_chunk(appender, data_chunk) == DuckDBError);

	auto error = duckdb_appender_error(appender);
	REQUIRE(duckdb::StringUtil::Contains(error, "expected SMALLINT but got BOOLEAN for column 2"));

	duckdb_appender_destroy(&appender);

	duckdb_destroy_data_chunk(&data_chunk);

	duckdb_destroy_logical_type(&types[0]);
	duckdb_destroy_logical_type(&types[1]);
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

#if STANDARD_VECTOR_SIZE > 2
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
#endif

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
	REQUIRE(duckdb::StringUtil::Contains(error, "PRIMARY KEY or UNIQUE constraint violated"));

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
	REQUIRE(duckdb::StringUtil::Contains(error, "PRIMARY KEY or UNIQUE constraint violated"));
	REQUIRE(duckdb_appender_destroy(&appender) == DuckDBError);

	// Ensure that only the last row was appended.
	result = tester.Query("SELECT * FROM test;");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->row_count() == 0);
}
