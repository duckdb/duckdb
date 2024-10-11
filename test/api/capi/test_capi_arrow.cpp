#include "capi_tester.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test arrow in C API", "[capi][arrow]") {

	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	duckdb_prepared_statement stmt = nullptr;
	duckdb_arrow arrow_result = nullptr;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	SECTION("test rows changed") {

		REQUIRE_NO_FAIL(tester.Query("CREATE TABLE test(a INTEGER);"));
		auto state = duckdb_query_arrow(tester.connection, "INSERT INTO test VALUES (1), (2);", &arrow_result);
		REQUIRE(state == DuckDBSuccess);
		REQUIRE(duckdb_arrow_rows_changed(arrow_result) == 2);
		duckdb_destroy_arrow(&arrow_result);
		REQUIRE_NO_FAIL(tester.Query("DROP TABLE test;"));
	}

	SECTION("test query arrow") {

		auto state = duckdb_query_arrow(tester.connection, "SELECT 42 AS VALUE, [1,2,3,4,5] AS LST;", &arrow_result);
		REQUIRE(state == DuckDBSuccess);
		REQUIRE(duckdb_arrow_row_count(arrow_result) == 1);
		REQUIRE(duckdb_arrow_column_count(arrow_result) == 2);
		REQUIRE(duckdb_arrow_rows_changed(arrow_result) == 0);

		// query the arrow schema
		ArrowSchema arrow_schema;
		arrow_schema.Init();
		auto arrow_schema_ptr = &arrow_schema;

		state = duckdb_query_arrow_schema(arrow_result, reinterpret_cast<duckdb_arrow_schema *>(&arrow_schema_ptr));
		REQUIRE(state == DuckDBSuccess);
		REQUIRE(string(arrow_schema.name) == "duckdb_query_result");
		if (arrow_schema.release) {
			arrow_schema.release(arrow_schema_ptr);
		}

		// query array data
		ArrowArray arrow_array;
		arrow_array.Init();
		auto arrow_array_ptr = &arrow_array;

		state = duckdb_query_arrow_array(arrow_result, reinterpret_cast<duckdb_arrow_array *>(&arrow_array_ptr));
		REQUIRE(state == DuckDBSuccess);
		REQUIRE(arrow_array.length == 1);
		REQUIRE(arrow_array.release != nullptr);
		arrow_array.release(arrow_array_ptr);

		duckdb_arrow_array null_array = nullptr;
		state = duckdb_query_arrow_array(arrow_result, &null_array);
		REQUIRE(state == DuckDBSuccess);
		REQUIRE(null_array == nullptr);

		// destroy the arrow result
		duckdb_destroy_arrow(&arrow_result);
	}

	SECTION("test multiple chunks") {

		// create a table that consists of multiple chunks
		REQUIRE_NO_FAIL(tester.Query("CREATE TABLE test(a INTEGER);"));
		REQUIRE_NO_FAIL(
		    tester.Query("INSERT INTO test SELECT i FROM (VALUES (1), (2), (3), (4), (5)) t(i), range(500);"));
		auto state = duckdb_query_arrow(tester.connection, "SELECT CAST(a AS INTEGER) AS a FROM test ORDER BY a;",
		                                &arrow_result);
		REQUIRE(state == DuckDBSuccess);

		// query the arrow schema
		ArrowSchema arrow_schema;
		arrow_schema.Init();
		auto arrow_schema_ptr = &arrow_schema;

		state = duckdb_query_arrow_schema(arrow_result, reinterpret_cast<duckdb_arrow_schema *>(&arrow_schema_ptr));
		REQUIRE(state == DuckDBSuccess);
		REQUIRE(arrow_schema.release != nullptr);
		arrow_schema.release(arrow_schema_ptr);

		int total_count = 0;
		while (true) {

			// query array data
			ArrowArray arrow_array;
			arrow_array.Init();
			auto arrow_array_ptr = &arrow_array;

			state = duckdb_query_arrow_array(arrow_result, reinterpret_cast<duckdb_arrow_array *>(&arrow_array_ptr));
			REQUIRE(state == DuckDBSuccess);

			if (arrow_array.length == 0) {
				// nothing to release
				REQUIRE(total_count == 2500);
				break;
			}

			REQUIRE(arrow_array.length > 0);
			total_count += arrow_array.length;
			REQUIRE(arrow_array.release != nullptr);
			arrow_array.release(arrow_array_ptr);
		}

		// destroy the arrow result
		duckdb_destroy_arrow(&arrow_result);
		REQUIRE_NO_FAIL(tester.Query("DROP TABLE test;"));
	}

	SECTION("test prepare query arrow") {

		auto state = duckdb_prepare(tester.connection, "SELECT CAST($1 AS BIGINT)", &stmt);
		REQUIRE(state == DuckDBSuccess);
		REQUIRE(stmt != nullptr);
		REQUIRE(duckdb_bind_int64(stmt, 1, 42) == DuckDBSuccess);

		// prepare and execute the arrow schema
		ArrowSchema prepared_schema;
		prepared_schema.Init();
		auto prepared_schema_ptr = &prepared_schema;

		state = duckdb_prepared_arrow_schema(stmt, reinterpret_cast<duckdb_arrow_schema *>(&prepared_schema_ptr));
		REQUIRE(state == DuckDBSuccess);
		REQUIRE(string(prepared_schema.format) == "+s");
		REQUIRE(duckdb_execute_prepared_arrow(stmt, nullptr) == DuckDBError);
		REQUIRE(duckdb_execute_prepared_arrow(stmt, &arrow_result) == DuckDBSuccess);
		REQUIRE(prepared_schema.release != nullptr);
		prepared_schema.release(prepared_schema_ptr);

		// query the arrow schema
		ArrowSchema arrow_schema;
		arrow_schema.Init();
		auto arrow_schema_ptr = &arrow_schema;

		state = duckdb_query_arrow_schema(arrow_result, reinterpret_cast<duckdb_arrow_schema *>(&arrow_schema_ptr));
		REQUIRE(state == DuckDBSuccess);
		REQUIRE(string(arrow_schema.format) == "+s");
		REQUIRE(arrow_schema.release != nullptr);
		arrow_schema.release(arrow_schema_ptr);

		ArrowArray arrow_array;
		arrow_array.Init();
		auto arrow_array_ptr = &arrow_array;

		state = duckdb_query_arrow_array(arrow_result, reinterpret_cast<duckdb_arrow_array *>(&arrow_array_ptr));
		REQUIRE(state == DuckDBSuccess);
		REQUIRE(arrow_array.length == 1);
		REQUIRE(arrow_array.release);
		arrow_array.release(arrow_array_ptr);

		duckdb_destroy_arrow(&arrow_result);
		duckdb_destroy_prepare(&stmt);
	}

	SECTION("test scan") {

		const auto logical_types = duckdb::vector<LogicalType> {LogicalType(LogicalTypeId::INTEGER)};
		const auto column_names = duckdb::vector<string> {"value"};

		// arrow schema, release after use
		ArrowSchema arrow_schema;
		arrow_schema.Init();
		auto arrow_schema_ptr = &arrow_schema;

		ClientProperties options = (reinterpret_cast<Connection *>(tester.connection)->context->GetClientProperties());
		duckdb::ArrowConverter::ToArrowSchema(arrow_schema_ptr, logical_types, column_names, options);

		ArrowArray arrow_array;
		arrow_array.Init();
		auto arrow_array_ptr = &arrow_array;

		SECTION("empty array") {

			// Create an empty view with a `value` column.
			string view_name = "foo_empty_table";

			// arrow array scan, destroy out_stream after use
			ArrowArrayStream *arrow_array_stream;
			auto out_stream = reinterpret_cast<duckdb_arrow_stream *>(&arrow_array_stream);
			auto state = duckdb_arrow_array_scan(tester.connection, view_name.c_str(),
			                                     reinterpret_cast<duckdb_arrow_schema>(arrow_schema_ptr),
			                                     reinterpret_cast<duckdb_arrow_array>(arrow_array_ptr), out_stream);
			REQUIRE(state == DuckDBSuccess);

			// get the created view from the DB
			auto get_query = "SELECT * FROM " + view_name + ";";
			state = duckdb_prepare(tester.connection, get_query.c_str(), &stmt);
			REQUIRE(state == DuckDBSuccess);
			REQUIRE(stmt != nullptr);
			state = duckdb_execute_prepared_arrow(stmt, &arrow_result);
			REQUIRE(state == DuckDBSuccess);

			// recover the arrow array from the arrow result
			ArrowArray out_array;
			out_array.Init();
			auto out_array_ptr = &out_array;

			state = duckdb_query_arrow_array(arrow_result, reinterpret_cast<duckdb_arrow_array *>(&out_array_ptr));
			REQUIRE(state == DuckDBSuccess);
			REQUIRE(out_array.length == 0);
			REQUIRE(out_array.release == nullptr);

			duckdb_destroy_arrow_stream(out_stream);
			REQUIRE(arrow_array.release == nullptr);
		}

		SECTION("big array") {

			// Create a view with a `value` column containing 4096 values.
			int num_buffers = 2, size = STANDARD_VECTOR_SIZE * num_buffers;
			ArrowAppender appender(logical_types, size, options);
			Allocator allocator;

			auto data_chunks = std::vector<DataChunk>(num_buffers);
			for (int i = 0; i < num_buffers; i++) {
				auto data_chunk = &data_chunks[i];
				data_chunk->Initialize(allocator, logical_types, STANDARD_VECTOR_SIZE);
				data_chunk->SetCardinality(STANDARD_VECTOR_SIZE);
				for (idx_t row = 0; row < STANDARD_VECTOR_SIZE; row++) {
					data_chunk->SetValue(0, row, duckdb::Value(i));
				}
				appender.Append(*data_chunk, 0, data_chunk->size(), data_chunk->size());
			}

			*arrow_array_ptr = appender.Finalize();

			// Create the view.
			string view_name = "foo_table";

			// arrow array scan, destroy out_stream after use
			ArrowArrayStream *arrow_array_stream;
			auto out_stream = reinterpret_cast<duckdb_arrow_stream *>(&arrow_array_stream);
			auto state = duckdb_arrow_array_scan(tester.connection, view_name.c_str(),
			                                     reinterpret_cast<duckdb_arrow_schema>(arrow_schema_ptr),
			                                     reinterpret_cast<duckdb_arrow_array>(arrow_array_ptr), out_stream);
			REQUIRE(state == DuckDBSuccess);

			// Get the created view from the DB.
			auto get_query = "SELECT * FROM " + view_name + ";";
			state = duckdb_prepare(tester.connection, get_query.c_str(), &stmt);
			REQUIRE(state == DuckDBSuccess);
			REQUIRE(stmt != nullptr);
			state = duckdb_execute_prepared_arrow(stmt, &arrow_result);
			REQUIRE(state == DuckDBSuccess);

			// Recover the arrow array from the arrow result.
			ArrowArray out_array;
			out_array.Init();
			auto out_array_ptr = &out_array;
			state = duckdb_query_arrow_array(arrow_result, reinterpret_cast<duckdb_arrow_array *>(&out_array_ptr));
			REQUIRE(state == DuckDBSuccess);
			REQUIRE(out_array.length == STANDARD_VECTOR_SIZE);
			REQUIRE(out_array.release != nullptr);
			out_array.release(out_array_ptr);

			out_array.Init();
			state = duckdb_query_arrow_array(arrow_result, reinterpret_cast<duckdb_arrow_array *>(&out_array_ptr));
			REQUIRE(state == DuckDBSuccess);
			REQUIRE(out_array.length == STANDARD_VECTOR_SIZE);
			REQUIRE(out_array.release != nullptr);
			out_array.release(out_array_ptr);

			out_array.Init();
			state = duckdb_query_arrow_array(arrow_result, reinterpret_cast<duckdb_arrow_array *>(&out_array_ptr));
			REQUIRE(state == DuckDBSuccess);
			REQUIRE(out_array.length == 0);
			REQUIRE(out_array.release == nullptr);

			duckdb_destroy_arrow_stream(out_stream);
			REQUIRE(arrow_array.release != nullptr);
		}

		SECTION("null schema") {
			// Creating a view with a null schema should fail gracefully.
			string view_name = "foo_empty_table_null_schema";

			// arrow array scan, destroy out_stream after use
			ArrowArrayStream *arrow_array_stream;
			auto out_stream = reinterpret_cast<duckdb_arrow_stream *>(&arrow_array_stream);
			auto state = duckdb_arrow_array_scan(tester.connection, view_name.c_str(), nullptr,
			                                     reinterpret_cast<duckdb_arrow_array>(arrow_array_ptr), out_stream);
			REQUIRE(state == DuckDBError);
			duckdb_destroy_arrow_stream(out_stream);
		}

		if (arrow_schema.release) {
			arrow_schema.release(arrow_schema_ptr);
		}
		if (arrow_array.release) {
			arrow_array.release(arrow_array_ptr);
		}

		duckdb_destroy_arrow(&arrow_result);
		duckdb_destroy_prepare(&stmt);
	}

	// FIXME: needs test for scanning a fixed size list
	// this likely requires nanoarrow to create the array to scan
}
