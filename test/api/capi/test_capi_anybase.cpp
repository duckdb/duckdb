#include "capi_tester.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb.h"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Insert version isolation", "[capi]") {

	duckdb_database db;
	duckdb_connection con;
	duckdb_connection con2;
	duckdb_result result;

	idx_t tableVersion = 0;
	idx_t vColumnVersion = 0;
	idx_t iColumnVersion = 0;
	idx_t xColumnVersion = 0;

	REQUIRE(duckdb_open(nullptr, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con2) != DuckDBError);

	// REQUIRE(duckdb_query(con, "CREATE SCHEMA FOO;", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "CREATE TABLE FOO(i INTEGER unique, v INTEGER DEFAULT 5, x INTEGER default 22);", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "Insert INTO FOO VALUES (1, 5, 22), (2, 5, 22);", NULL) != DuckDBError);

	REQUIRE(duckdb_query(con, "Begin Transaction", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "Insert INTO FOO VALUES (3, 5, 22), (4, 5, 22);", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "select * from FOO", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "INSERT INTO FOO values (1, 5, 22) on conflict do update set v = 10;", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "INSERT INTO FOO values (2, 5, 22) on conflict do update set x = 15;", NULL) != DuckDBError);

	tableVersion = duckdb_get_table_version(con, "", "FOO", nullptr);
	vColumnVersion = duckdb_get_column_version(con, "", "FOO", "v", nullptr);
	iColumnVersion = duckdb_get_column_version(con, "", "FOO", "i", nullptr);
	xColumnVersion = duckdb_get_column_version(con, "", "FOO", "x", nullptr);
	REQUIRE(tableVersion == 1);
	REQUIRE(iColumnVersion == 1);
	REQUIRE(vColumnVersion == 1);
	REQUIRE(xColumnVersion == 1);


	REQUIRE(duckdb_query(con, "commit", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "select * from FOO", &result) != DuckDBError);


	tableVersion = duckdb_get_table_version(con, "", "FOO", nullptr);
	vColumnVersion = duckdb_get_column_version(con, "", "FOO", "v", nullptr);
	iColumnVersion = duckdb_get_column_version(con, "", "FOO", "i", nullptr);
	xColumnVersion = duckdb_get_column_version(con, "", "FOO", "x", nullptr);
	REQUIRE(tableVersion == 2);
	REQUIRE(iColumnVersion == 2);
	REQUIRE(vColumnVersion == 2);
	REQUIRE(xColumnVersion == 2);

	tableVersion = duckdb_get_table_version(con, "", "FOO", nullptr);
	vColumnVersion = duckdb_get_column_version(con2, "", "FOO", "v", nullptr);
	iColumnVersion = duckdb_get_column_version(con, "", "FOO", "i", nullptr);
	xColumnVersion = duckdb_get_column_version(con, "", "FOO", "x", nullptr);
	REQUIRE(tableVersion == 2);
	REQUIRE(iColumnVersion == 2);
	REQUIRE(vColumnVersion == 2);
	REQUIRE(xColumnVersion == 2);

	duckdb_destroy_result(&result);
	duckdb_disconnect(&con);
	duckdb_disconnect(&con2);
	duckdb_close(&db);
}

TEST_CASE("Get table row count", "[capi]") {
	duckdb_database db;
	duckdb_connection con;

	idx_t row_count = 0;

	REQUIRE(duckdb_open(nullptr, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);

	REQUIRE(duckdb_query(con, "CREATE TABLE FOO(i INTEGER unique, v INTEGER DEFAULT 5, x INTEGER default 22);", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "Insert INTO FOO VALUES (1, 5, 22), (2, 5, 22);", NULL) != DuckDBError);

	row_count = duckdb_estimated_row_count(con,nullptr, nullptr, "FOO", nullptr);
	REQUIRE(row_count == 2);

	REQUIRE(duckdb_query(con, "Insert INTO FOO VALUES (3, 5, 22), (4, 5, 22);", NULL) != DuckDBError);

	row_count = duckdb_estimated_row_count(con,nullptr, nullptr, "FOO", nullptr);
	REQUIRE(row_count == 4);

	duckdb_disconnect(&con);
	duckdb_close(&db);
}

// TEST_CASE("Test Update version isolation", "[capi]") {
//
// 	duckdb_database db;
// 	duckdb_connection con;
// 	duckdb_connection con2;
// 	duckdb_result result;
//
// 	idx_t tableVersion = 0;
// 	idx_t vColumnVersion = 0;
// 	idx_t iColumnVersion = 0;
// 	idx_t xColumnVersion = 0;
//
// 	REQUIRE(duckdb_open("/Users/jeremyosterhoudt/Downloads/Foo_0.db", &db) != DuckDBError);
// 	REQUIRE(duckdb_connect(db, &con) != DuckDBError);
// 	REQUIRE(duckdb_connect(db, &con2) != DuckDBError);
//
// 	// REQUIRE(duckdb_query(con, "CREATE SCHEMA FOO;", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "CREATE TABLE FOO(i INTEGER unique, v INTEGER DEFAULT 5, x INTEGER default 22);", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "Insert INTO FOO VALUES (1, 5, 22), (2, 5, 22);", NULL) != DuckDBError);
//
// 	REQUIRE(duckdb_query(con, "Begin Transaction", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "update FOO set v = 22 where i = 1;", NULL) != DuckDBError);
//
// 	tableVersion = duckdb_get_table_version(con, "", "FOO", nullptr);
// 	vColumnVersion = duckdb_get_column_version(con, "", "FOO", "v", nullptr);
// 	iColumnVersion = duckdb_get_column_version(con, "", "FOO", "i", nullptr);
// 	xColumnVersion = duckdb_get_column_version(con, "", "FOO", "x", nullptr);
// 	REQUIRE(tableVersion == 1);
// 	REQUIRE(iColumnVersion == 1);
// 	REQUIRE(vColumnVersion == 1);
// 	REQUIRE(xColumnVersion == 1);
// 	REQUIRE(duckdb_query(con, "commit", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "select * from FOO", &result) != DuckDBError);
//
//
// 	tableVersion = duckdb_get_table_version(con, "", "FOO", nullptr);
// 	vColumnVersion = duckdb_get_column_version(con, "", "FOO", "v", nullptr);
// 	iColumnVersion = duckdb_get_column_version(con, "", "FOO", "i", nullptr);
// 	xColumnVersion = duckdb_get_column_version(con, "", "FOO", "x", nullptr);
// 	REQUIRE(tableVersion == 2);
// 	REQUIRE(iColumnVersion == 2);
// 	REQUIRE(vColumnVersion == 2);
// 	REQUIRE(xColumnVersion == 2);
//
// 	tableVersion = duckdb_get_table_version(con, "", "FOO", nullptr);
// 	vColumnVersion = duckdb_get_column_version(con2, "", "FOO", "v", nullptr);
// 	iColumnVersion = duckdb_get_column_version(con, "", "FOO", "i", nullptr);
// 	xColumnVersion = duckdb_get_column_version(con, "", "FOO", "x", nullptr);
// 	REQUIRE(tableVersion == 2);
// 	REQUIRE(iColumnVersion == 2);
// 	REQUIRE(vColumnVersion == 2);
// 	REQUIRE(xColumnVersion == 2);
// }
//
// TEST_CASE("Test rollback", "[capi]") {
// 	duckdb_database db;
// 	duckdb_connection con;
// 	duckdb_connection con2;
// 	duckdb_result result;
//
// 	idx_t tableVersion = 0;
// 	idx_t vColumnVersion = 0;
// 	idx_t iColumnVersion = 0;
// 	idx_t xColumnVersion = 0;
//
// 	REQUIRE(duckdb_open("/Users/jeremyosterhoudt/Downloads/Foo_0.db", &db) != DuckDBError);
// 	REQUIRE(duckdb_connect(db, &con) != DuckDBError);
// 	REQUIRE(duckdb_connect(db, &con2) != DuckDBError);
//
// 	REQUIRE(duckdb_query(con, "CREATE SCHEMA FOO;", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "CREATE TABLE FOO.FOO(i INTEGER unique, v INTEGER DEFAULT 5, x INTEGER default 22);", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "Insert INTO FOO.FOO VALUES (1, 5, 22), (2, 5, 22);", NULL) != DuckDBError);
//
// 	REQUIRE(duckdb_query(con, "Begin Transaction", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "Insert INTO FOO.FOO VALUES (3, 5, 22), (4, 5, 22);", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "select * from FOO.FOO", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "INSERT INTO FOO.FOO values (1, 5, 22) on conflict do update set v = 10;", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "INSERT INTO FOO.FOO values (2, 5, 22) on conflict do update set x = 15;", NULL) != DuckDBError);
//
//
//
//
// 	tableVersion = duckdb_get_table_version(con, "FOO", "FOO", nullptr);
// 	vColumnVersion = duckdb_get_column_version(con, "FOO", "FOO", "v", nullptr);
// 	iColumnVersion = duckdb_get_column_version(con, "FOO", "FOO", "i", nullptr);
// 	xColumnVersion = duckdb_get_column_version(con, "FOO", "FOO", "x", nullptr);
// 	REQUIRE(tableVersion == 1);
// 	REQUIRE(iColumnVersion == 1);
// 	REQUIRE(vColumnVersion == 1);
// 	REQUIRE(xColumnVersion == 1);
//
//
// 	REQUIRE(duckdb_query(con, "rollback", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "select * from FOO.FOO", &result) != DuckDBError);
//
//
// 	tableVersion = duckdb_get_table_version(con, "FOO", "FOO", nullptr);
// 	vColumnVersion = duckdb_get_column_version(con, "FOO", "FOO", "v", nullptr);
// 	iColumnVersion = duckdb_get_column_version(con, "FOO", "FOO", "i", nullptr);
// 	xColumnVersion = duckdb_get_column_version(con, "FOO", "FOO", "x", nullptr);
// 	REQUIRE(tableVersion == 1);
// 	REQUIRE(iColumnVersion == 1);
// 	REQUIRE(vColumnVersion == 1);
// 	REQUIRE(xColumnVersion == 1);
//
// 	tableVersion = duckdb_get_table_version(con, "FOO", "FOO", nullptr);
// 	vColumnVersion = duckdb_get_column_version(con2, "FOO", "FOO", "v", nullptr);
// 	iColumnVersion = duckdb_get_column_version(con, "FOO", "FOO", "i", nullptr);
// 	xColumnVersion = duckdb_get_column_version(con, "FOO", "FOO", "x", nullptr);
// 	REQUIRE(tableVersion == 1);
// 	REQUIRE(iColumnVersion == 1);
// 	REQUIRE(vColumnVersion == 1);
// 	REQUIRE(xColumnVersion == 1);
// 	REQUIRE(duckdb_query(con, "checkpoint", NULL) != DuckDBError);
// }
//
// TEST_CASE("Reload Version Info", "[capi]") {
//
// 	duckdb_database db;
// 	duckdb_connection con;
//
// 	idx_t tableVersion = 0;
// 	idx_t vColumnVersion = 0;
// 	idx_t iColumnVersion = 0;
// 	idx_t xColumnVersion = 0;
//
// 	REQUIRE(duckdb_open("/Users/jeremyosterhoudt/Development/apps/GE/ampere/data/ods-runtime-normal/storage/Foo/0/Foo_0.db", &db) != DuckDBError);
// 	REQUIRE(duckdb_connect(db, &con) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "checkpoint", NULL) != DuckDBError);
//
// 	tableVersion = duckdb_get_table_version(con, nullptr, "FOO", nullptr);
// 	iColumnVersion = duckdb_get_column_version(con, nullptr, "FOO", "i", nullptr);
// 	vColumnVersion = duckdb_get_column_version(con, nullptr, "FOO", "v", nullptr);
// 	xColumnVersion = duckdb_get_column_version(con, nullptr, "FOO", "x", nullptr);
// 	REQUIRE(tableVersion == 2);
// 	REQUIRE(iColumnVersion == 2);
// 	REQUIRE(vColumnVersion == 2);
// 	REQUIRE(xColumnVersion == 2);
// 	REQUIRE(duckdb_query(con, "checkpoint", NULL) != DuckDBError);
// }

TEST_CASE("Convert DuckDBResult to Arrow Array in C API", "[cAnybaseApi]") {
	duckdb_database db;
	duckdb_connection con;
	duckdb_result result;
	auto *arrow_array = new ArrowArray();

	REQUIRE(duckdb_open(NULL, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);

	REQUIRE(duckdb_query(con, "CREATE TABLE test(i INTEGER);", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "Insert INTO test VALUES (1), (2);", NULL) != DuckDBError);
	REQUIRE((duckdb_query(con, "SELECT * FROM test;", &result) != DuckDBError));

	REQUIRE(duckdb_result_to_arrow(result, (duckdb_arrow_array *)&arrow_array) == DuckDBSuccess);
	REQUIRE(arrow_array->length == 2);

	arrow_array->release(arrow_array);
	delete arrow_array;
	duckdb_destroy_result(&result); // segmentation failure happens here
	duckdb_disconnect(&con);
	duckdb_close(&db);
}

TEST_CASE("Convert DuckDB Chunks to Arrow Array in C API", "[cAnybaseApi]") {
	duckdb_database db;
	duckdb_connection con;
	duckdb_result result;
	auto *arrow_array = new ArrowArray();

	REQUIRE(duckdb_open(NULL, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);

	REQUIRE(duckdb_query(con, "CREATE TABLE test(i INTEGER);", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "Insert INTO test VALUES (1), (2);", NULL) != DuckDBError);
	REQUIRE((duckdb_query(con, "SELECT * FROM test;", &result) != DuckDBError));

	auto count = duckdb_result_chunk_count(result);
	auto chunks = new duckdb_data_chunk[count];

	for (auto i = 0UL; i < count; i++) {
		chunks[i] = duckdb_result_get_chunk(result, i);
	}

	REQUIRE(duckdb_data_chunks_to_arrow_array(con, chunks, count, (duckdb_arrow_array *)&arrow_array) == DuckDBSuccess);
	REQUIRE(arrow_array->length == 2);

	arrow_array->release(arrow_array);
	delete arrow_array;
	for (auto i = 0UL; i < count; i++) {
		duckdb_destroy_data_chunk(&chunks[i]);
	}
	delete [] chunks;
	duckdb_destroy_result(&result); // segmentation failure happens here
	duckdb_disconnect(&con);
	duckdb_close(&db);
}

TEST_CASE("Convert DuckDB Chunk column to Arrow Array in C API", "[cAnybaseApi]") {
	duckdb_database db;
	duckdb_connection con;
	duckdb_result result;
	auto *i_arrow_array = new ArrowArray();
	auto *s_arrow_array = new ArrowArray();

	REQUIRE(duckdb_open(NULL, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);

	REQUIRE(duckdb_query(con, "CREATE TABLE test(i INTEGER, s VARCHAR);", NULL) != DuckDBError);
	REQUIRE(duckdb_query(con, "Insert INTO test VALUES (1, 'a'), (2, 'b');", NULL) != DuckDBError);
	REQUIRE((duckdb_query(con, "SELECT * FROM test;", &result) != DuckDBError));

	auto count = duckdb_result_chunk_count(result);
	auto chunks = new duckdb_data_chunk[count];

	for (auto i = 0UL; i < count; i++) {
		chunks[i] = duckdb_result_get_chunk(result, i);
	}

	// Check column 0
	REQUIRE(duckdb_data_chunk_column_to_arrow_array(con, chunks, count, 0, (duckdb_arrow_array *)&i_arrow_array) == DuckDBSuccess);
	REQUIRE(i_arrow_array->length == 2);
	REQUIRE(i_arrow_array->n_buffers == 1);
	REQUIRE(i_arrow_array->n_children == 1);

	// Check column 1
	REQUIRE(duckdb_data_chunk_column_to_arrow_array(con, chunks, count, 1, (duckdb_arrow_array *)&s_arrow_array) == DuckDBSuccess);
	REQUIRE(s_arrow_array->length == 2);
	REQUIRE(s_arrow_array->n_buffers == 1);
	REQUIRE(s_arrow_array->n_children == 1);

	i_arrow_array->release(i_arrow_array);
	s_arrow_array->release(s_arrow_array);
	delete i_arrow_array;
	delete s_arrow_array;
	duckdb_destroy_result(&result); // segmentation failure happens here
	duckdb_disconnect(&con);
	duckdb_close(&db);
	for (auto i = 0UL; i < count; i++) {
		duckdb_destroy_data_chunk(&chunks[i]);
	}
	delete[] chunks;
}

TEST_CASE("Test DataChunk C API reference", "[cAnybaseApi]") {
	duckdb_logical_type types[2];
	types[0] = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	types[1] = duckdb_create_logical_type(DUCKDB_TYPE_SMALLINT);

	auto data_chunk = duckdb_create_data_chunk(types, 2);
	REQUIRE(data_chunk);
	duckdb_data_chunk_set_size(data_chunk, 1);

	// append standard primitive values
	auto col1_ptr = (int64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(data_chunk, 0));
	*col1_ptr = 42;
	auto col2_ptr = (int16_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(data_chunk, 1));
	*col2_ptr = 84;

	auto other_chunk = duckdb_create_data_chunk_copy(&data_chunk);
	REQUIRE(other_chunk);

	auto other_col1_ptr = (int64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(other_chunk, 0));
	auto other_col2_ptr = (int16_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(other_chunk, 1));
	*other_col1_ptr = 88;

	REQUIRE(*other_col1_ptr == 88);
	REQUIRE(*other_col2_ptr == 84);
	REQUIRE(*col1_ptr == 42);

	duckdb_data_chunk_set_size(other_chunk, 2);

	*(other_col1_ptr + 8) = 77;
	*(other_col2_ptr + 2) = 12;

	REQUIRE(*(other_col1_ptr + 8) == 77);
	REQUIRE(*(other_col2_ptr + 2) == 12);
	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 1);



	duckdb_destroy_data_chunk(&data_chunk);
	duckdb_destroy_data_chunk(&other_chunk);
	duckdb_destroy_logical_type(&types[0]);
	duckdb_destroy_logical_type(&types[1]);
    printf("Test DataChunk C API reference passed\n");
}

void some_func2(cdc_event_type type,
                idx_t transaction_id,
                idx_t column_count,
                idx_t table_version,
                idx_t *updated_column_index,
                const char *table_name,
                const char **column_names,
                idx_t *column_versions,
                duckdb_data_chunk values,
                duckdb_data_chunk previous_values) {
    auto c = column_count;

    duckdb_destroy_data_chunk(&values);
    duckdb_destroy_data_chunk(&previous_values);
}

TEST_CASE("Test WAL Generation", "[capi]") {
    duckdb_database db;
    duckdb_connection connection1;
    duckdb_connection connection2;
    duckdb_result result;
    duckdb_result errorMessage;

//     REQUIRE(duckdb_open("/Users/jeremyosterhoudt/Downloads/dbz_demo/test.db", &db) != DuckDBError);
    REQUIRE(duckdb_open(nullptr, &db) != DuckDBError);
    duckdb_set_cdc_callback(db, some_func2);
    REQUIRE(duckdb_connect(db, &connection1) != DuckDBError);
    REQUIRE(duckdb_connect(db, &connection2) != DuckDBError);

    REQUIRE(duckdb_query(connection1, "BEGIN TRANSACTION", nullptr) != DuckDBError);

    // REQUIRE(duckdb_query(connection1, "CREATE TABLE if not exists FOO(id BIGINT unique, val INTEGER default 22, xtra INTEGER default 11)", nullptr) != DuckDBError);
    // REQUIRE(duckdb_query(connection1, "INSERT INTO FOO (id) VALUES (1), (2)", nullptr) != DuckDBError);
    REQUIRE(duckdb_query(connection1, "CREATE TABLE if not exists FOO(id BIGINT, val INTEGER, xtra INTEGER, other integer)", nullptr) != DuckDBError);
    REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (1, 22, 11, 124), (2, 8, 31, 125), (3, 3, 3, 3)", nullptr) != DuckDBError);
    // REQUIRE(duckdb_query(connection1, "UPDATE FOO SET val = 5 where val = 8 or other = 125", nullptr) != DuckDBError);
    // REQUIRE(duckdb_query(connection1, "DELETE FROM FOO where id = 2", nullptr) != DuckDBError);
    // REQUIRE(duckdb_query(connection1, "INSERT INTO FOO VALUES (2, 8, 31, 125)", nullptr) != DuckDBError);
    REQUIRE(duckdb_query(connection1, "COMMIT", nullptr) != DuckDBError);
    // REQUIRE(duckdb_query(connection1, "BEGIN TRANSACTION", nullptr) != DuckDBError);



    REQUIRE(duckdb_query(connection1, "UPDATE FOO SET val = 5 where id = 2", nullptr) != DuckDBError);
    REQUIRE(duckdb_query(connection1, "UPDATE FOO SET val = 44 where id = 2", nullptr) != DuckDBError);
    // REQUIRE(duckdb_query(connection1, "UPDATE FOO SET val = 5 where val = 8 and other = 125", nullptr) != DuckDBError);
    // REQUIRE(duckdb_query(connection1, "UPDATE FOO SET val = 4 where val = 5", nullptr) != DuckDBError);
    // REQUIRE(duckdb_query(connection1, "UPDATE FOO SET val = 3 where val = 4", nullptr) != DuckDBError);
    // REQUIRE(duckdb_query(connection1, "COMMIT", nullptr) != DuckDBError);
    REQUIRE(duckdb_query(connection1, "DELETE FROM FOO where id = 1 or id = 3", nullptr) != DuckDBError);

    // duckdb_destroy_result(&errorMessage);
    //duckdb_destroy_result(&result);
    duckdb_disconnect(&connection1);
    duckdb_disconnect(&connection2);
    duckdb_close(&db);
}


// TEST_CASE("Test Snapshot in C API", "[cAnybaseApi]") {
// 	duckdb_database db;
// 	duckdb_connection con;
// 	duckdb_result result;
//
// 	REQUIRE(duckdb_open(NULL, &db) != DuckDBError);
// 	REQUIRE(duckdb_connect(db, &con) != DuckDBError);
//
// 	REQUIRE(duckdb_query(con, "CREATE TABLE test(i INTEGER);", NULL) != DuckDBError);
// 	REQUIRE(duckdb_query(con, "Insert INTO test VALUES (1), (2);", NULL) != DuckDBError);
//
// 	// For in memory databases, snapshot id == 0 and it is an error to create a snapshot
// 	REQUIRE(duckdb_get_snapshot_id(con) == 0);
//         char *snapshot_file_name;
// 	REQUIRE(duckdb_create_snapshot(con, &result, &snapshot_file_name) == DuckDBError);
//         duckdb_free(snapshot_file_name);
// 	duckdb_disconnect(&con);
// 	duckdb_close(&db);
// }
