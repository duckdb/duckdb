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
		REQUIRE_NO_FAIL(tester.Query("CREATE TABLE test(a INTEGER)"));
		REQUIRE(duckdb_query_arrow(tester.connection, "INSERT INTO test VALUES (1), (2);", &arrow_result) ==
		        DuckDBSuccess);
		REQUIRE(duckdb_arrow_rows_changed(arrow_result) == 2);
		duckdb_destroy_arrow(&arrow_result);
		REQUIRE_NO_FAIL(tester.Query("drop table test"));
	}

	SECTION("test query arrow") {
		REQUIRE(duckdb_query_arrow(tester.connection, "SELECT 42 AS VALUE, [1,2,3,4,5] AS LST", &arrow_result) ==
		        DuckDBSuccess);
		REQUIRE(duckdb_arrow_row_count(arrow_result) == 1);
		REQUIRE(duckdb_arrow_column_count(arrow_result) == 2);
		REQUIRE(duckdb_arrow_rows_changed(arrow_result) == 0);

		// query schema
		ArrowSchema *arrow_schema = new ArrowSchema();
		REQUIRE(duckdb_query_arrow_schema(arrow_result, (duckdb_arrow_schema *)&arrow_schema) == DuckDBSuccess);
		REQUIRE(string(arrow_schema->name) == "duckdb_query_result");
		// User need to release the data themselves
		if (arrow_schema->release) {
			arrow_schema->release(arrow_schema);
		}
		delete arrow_schema;

		// query array data
		ArrowArray *arrow_array = new ArrowArray();
		REQUIRE(duckdb_query_arrow_array(arrow_result, (duckdb_arrow_array *)&arrow_array) == DuckDBSuccess);
		REQUIRE(arrow_array->length == 1);
		arrow_array->release(arrow_array);
		delete arrow_array;

		duckdb_arrow_array null_array = nullptr;
		REQUIRE(duckdb_query_arrow_array(arrow_result, &null_array) == DuckDBSuccess);
		REQUIRE(null_array == nullptr);

		// destroy result
		duckdb_destroy_arrow(&arrow_result);
	}

	SECTION("test multiple chunks") {
		// create table that consists of multiple chunks
		REQUIRE_NO_FAIL(tester.Query("CREATE TABLE test(a INTEGER)"));
		REQUIRE_NO_FAIL(
		    tester.Query("INSERT INTO test SELECT i FROM (VALUES (1), (2), (3), (4), (5)) t(i), range(500);"));

		REQUIRE(duckdb_query_arrow(tester.connection, "SELECT CAST(a AS INTEGER) AS a FROM test ORDER BY a",
		                           &arrow_result) == DuckDBSuccess);

		ArrowSchema *arrow_schema = new ArrowSchema();
		REQUIRE(duckdb_query_arrow_schema(arrow_result, (duckdb_arrow_schema *)&arrow_schema) == DuckDBSuccess);
		REQUIRE(arrow_schema->release != nullptr);
		arrow_schema->release(arrow_schema);
		delete arrow_schema;

		int total_count = 0;
		while (true) {
			ArrowArray *arrow_array = new ArrowArray();
			REQUIRE(duckdb_query_arrow_array(arrow_result, (duckdb_arrow_array *)&arrow_array) == DuckDBSuccess);
			if (arrow_array->length == 0) {
				delete arrow_array;
				REQUIRE(total_count == 2500);
				break;
			}
			REQUIRE(arrow_array->length > 0);
			total_count += arrow_array->length;
			arrow_array->release(arrow_array);
			delete arrow_array;
		}
		duckdb_destroy_arrow(&arrow_result);
		REQUIRE_NO_FAIL(tester.Query("drop table test"));
	}

	SECTION("test prepare query arrow") {
		REQUIRE(duckdb_prepare(tester.connection, "SELECT CAST($1 AS BIGINT)", &stmt) == DuckDBSuccess);
		REQUIRE(stmt != nullptr);
		REQUIRE(duckdb_bind_int64(stmt, 1, 42) == DuckDBSuccess);
		ArrowSchema prepared_schema;
		prepared_schema.release = nullptr;
		auto prep_schema_p = &prepared_schema;
		REQUIRE(duckdb_prepared_arrow_schema(stmt, (duckdb_arrow_schema *)&prep_schema_p) == DuckDBSuccess);
		REQUIRE(string(prepared_schema.format) == "+s");
		REQUIRE(duckdb_execute_prepared_arrow(stmt, nullptr) == DuckDBError);
		REQUIRE(duckdb_execute_prepared_arrow(stmt, &arrow_result) == DuckDBSuccess);
		prepared_schema.release(&prepared_schema);

		ArrowSchema *arrow_schema = new ArrowSchema();
		REQUIRE(duckdb_query_arrow_schema(arrow_result, (duckdb_arrow_schema *)&arrow_schema) == DuckDBSuccess);
		REQUIRE(string(arrow_schema->format) == "+s");
		arrow_schema->release(arrow_schema);
		delete arrow_schema;

		ArrowArray *arrow_array = new ArrowArray();
		REQUIRE(duckdb_query_arrow_array(arrow_result, (duckdb_arrow_array *)&arrow_array) == DuckDBSuccess);
		REQUIRE(arrow_array->length == 1);
		arrow_array->release(arrow_array);
		delete arrow_array;

		duckdb_destroy_arrow(&arrow_result);
		duckdb_destroy_prepare(&stmt);
	}

	SECTION("test scan") {
		const auto logical_types = duckdb::vector<LogicalType> {LogicalType(LogicalTypeId::INTEGER)};
		const auto column_names = duckdb::vector<string> {"value"};

		ArrowSchema *arrow_schema = new ArrowSchema();

		ClientProperties options = ((Connection *)tester.connection)->context->GetClientProperties();
		duckdb::ArrowConverter::ToArrowSchema(arrow_schema, logical_types, column_names, options);

		ArrowArray *arrow_array = new ArrowArray();

		SECTION("empty array") {
			// Create an empty view with a `value` column.
			string view_name = "foo_empty_table";
			ArrowArrayStream *out_stream;
			REQUIRE(duckdb_arrow_array_scan(tester.connection, view_name.c_str(),
			                                reinterpret_cast<duckdb_arrow_schema>(arrow_schema),
			                                reinterpret_cast<duckdb_arrow_array>(arrow_array),
			                                reinterpret_cast<duckdb_arrow_stream *>(&out_stream)) == DuckDBSuccess);

			// Get created view from DB.
			auto get_query = "SELECT * FROM " + view_name + ";";
			REQUIRE(duckdb_prepare(tester.connection, get_query.c_str(), &stmt) == DuckDBSuccess);
			REQUIRE(stmt != nullptr);
			REQUIRE(duckdb_execute_prepared_arrow(stmt, &arrow_result) == DuckDBSuccess);

			// Recover array from results.
			ArrowArray *out_array = new ArrowArray();
			REQUIRE(duckdb_query_arrow_array(arrow_result, reinterpret_cast<duckdb_arrow_array *>(&out_array)) ==
			        DuckDBSuccess);
			REQUIRE(out_array->length == 0);
			REQUIRE(out_array->release == nullptr);
			delete out_array;

			out_stream->release(out_stream);
			delete out_stream;

			REQUIRE(arrow_array->release == nullptr);
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
				for (int row = 0; row < STANDARD_VECTOR_SIZE; row++) {
					data_chunk->SetValue(0, row, duckdb::Value(i));
				}

				appender.Append(*data_chunk, 0, data_chunk->size(), data_chunk->size());
			}

			*arrow_array = appender.Finalize();

			// Create view.
			string view_name = "foo_table";
			ArrowArrayStream *out_stream;
			REQUIRE(duckdb_arrow_array_scan(tester.connection, view_name.c_str(),
			                                reinterpret_cast<duckdb_arrow_schema>(arrow_schema),
			                                reinterpret_cast<duckdb_arrow_array>(arrow_array),
			                                reinterpret_cast<duckdb_arrow_stream *>(&out_stream)) == DuckDBSuccess);

			// Get created view from DB.
			auto get_query = "SELECT * FROM " + view_name + ";";
			REQUIRE(duckdb_prepare(tester.connection, get_query.c_str(), &stmt) == DuckDBSuccess);
			REQUIRE(stmt != nullptr);
			REQUIRE(duckdb_execute_prepared_arrow(stmt, &arrow_result) == DuckDBSuccess);

			// Recover array from results.
			ArrowArray *out_array = new ArrowArray();
			REQUIRE(duckdb_query_arrow_array(arrow_result, reinterpret_cast<duckdb_arrow_array *>(&out_array)) ==
			        DuckDBSuccess);
			REQUIRE(out_array->length == STANDARD_VECTOR_SIZE);
			out_array->release(out_array);
			delete out_array;

			out_array = new ArrowArray();
			REQUIRE(duckdb_query_arrow_array(arrow_result, reinterpret_cast<duckdb_arrow_array *>(&out_array)) ==
			        DuckDBSuccess);
			REQUIRE(out_array->length == STANDARD_VECTOR_SIZE);
			out_array->release(out_array);
			delete out_array;

			out_array = new ArrowArray();
			REQUIRE(duckdb_query_arrow_array(arrow_result, reinterpret_cast<duckdb_arrow_array *>(&out_array)) ==
			        DuckDBSuccess);
			REQUIRE(out_array->length == 0);
			REQUIRE(out_array->release == nullptr);
			delete out_array;

			out_stream->release(out_stream);
			delete out_stream;

			REQUIRE(arrow_array->release != nullptr);
		}

		SECTION("null schema") {
			// Creating a view with a null schema should fail gracefully.
			string view_name = "foo_empty_table_null_schema";
			ArrowArrayStream *out_stream;
			REQUIRE(duckdb_arrow_array_scan(tester.connection, view_name.c_str(), nullptr,
			                                reinterpret_cast<duckdb_arrow_array>(arrow_array),
			                                reinterpret_cast<duckdb_arrow_stream *>(&out_stream)) == DuckDBError);

			out_stream->release(out_stream);
			delete out_stream;
		}

		arrow_schema->release(arrow_schema);
		delete arrow_schema;

		if (arrow_array->release) {
			arrow_array->release(arrow_array);
		}

		delete arrow_array;

		duckdb_destroy_arrow(&arrow_result);
		duckdb_destroy_prepare(&stmt);
	}

	// FIXME: needs test for scanning a fixed size list
	// this likely requires nanoarrow to create the array to scan
}
