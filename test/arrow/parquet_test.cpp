#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow_test_factory.hpp"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow_check.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include <parquet/arrow/reader.h>
#include "arrow/io/file.h"
#include <arrow/type_traits.h>
#include "arrow/table.h"
#include "arrow/c/bridge.h"
#include <memory>
#include <iostream>
#include "parquet/exception.h"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/query_result.hpp"
#include "test_helpers.hpp"

std::shared_ptr<arrow::Table> ReadParquetFile(const duckdb::string &path) {
	std::shared_ptr<arrow::io::ReadableFile> infile;
	PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(path, arrow::default_memory_pool()));

	std::unique_ptr<parquet::arrow::FileReader> reader;

	auto status = parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader);
	REQUIRE(status.ok());
	std::shared_ptr<arrow::Table> table;
	status = reader->ReadTable(&table);
	REQUIRE(status.ok());
	return table;
}

std::unique_ptr<duckdb::QueryResult> ArrowToDuck(duckdb::Connection &conn, arrow::Table &table,
                                                 const std::string &query = "", const std::string &view_name = "") {
	std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

	auto batch_reader = arrow::TableBatchReader(table);
	auto status = batch_reader.ReadAll(&batches);

	REQUIRE(status.ok());
	SimpleFactory factory {batches, table.schema()};

	duckdb::vector<duckdb::Value> params;
	params.push_back(duckdb::Value::POINTER((uintptr_t)&factory));
	params.push_back(duckdb::Value::POINTER((uintptr_t)&SimpleFactory::CreateStream));
	params.push_back(duckdb::Value::UBIGINT(1000000));
	if (query.empty()) {
		return conn.TableFunction("arrow_scan", params)->Execute();
	}
	conn.TableFunction("arrow_scan", params)->CreateView(view_name);
	return conn.Query(query);
}

bool RoundTrip(std::string &path, std::vector<std::string> &skip, duckdb::Connection &conn) {
	bool skip_this = false;
	for (auto &non_sup : skip) {
		if (path.find(non_sup) != std::string::npos) {
			skip_this = true;
			break;
		}
	}
	if (skip_this) {
		return true;
	}

	auto table = ReadParquetFile(path);

	auto result = ArrowToDuck(conn, *table);
	ArrowSchema abi_arrow_schema;
	std::vector<std::shared_ptr<arrow::RecordBatch>> batches_result;
	result->ToArrowSchema(&abi_arrow_schema);
	auto result_schema = arrow::ImportSchema(&abi_arrow_schema);

	while (true) {
		auto data_chunk = result->Fetch();
		if (!data_chunk || data_chunk->size() == 0) {
			break;
		}
		ArrowArray arrow_array;
		data_chunk->ToArrowArray(&arrow_array);
		auto batch = arrow::ImportRecordBatch(&arrow_array, result_schema.ValueUnsafe());
		batches_result.push_back(batch.MoveValueUnsafe());
	}
	auto arrow_result_tbl = arrow::Table::FromRecordBatches(batches_result).ValueUnsafe();
	auto new_table = table->CombineChunks().ValueUnsafe();
	auto new_arrow_result_tbl = arrow_result_tbl->CombineChunks().ValueUnsafe();

	result = ArrowToDuck(conn, *new_arrow_result_tbl);
	while (true) {
		auto data_chunk = result->Fetch();
		if (!data_chunk || data_chunk->size() == 0) {
			break;
		}
		ArrowArray arrow_array;
		data_chunk->ToArrowArray(&arrow_array);
		auto batch = arrow::ImportRecordBatch(&arrow_array, result_schema.ValueUnsafe());
		batches_result.push_back(batch.MoveValueUnsafe());
	}
	return ArrowTableEquals(*new_table, *new_arrow_result_tbl);
}

TEST_CASE("Test Parquet File NaN", "[arrow]") {
	std::vector<std::string> skip;

	duckdb::DuckDB db;
	duckdb::Connection conn {db};

	//! Impossible to round-trip NaNs so we just validate that the duckdb table is correct-o
	std::string parquet_path = "data/parquet-testing/nan-float.parquet";
	auto table = ReadParquetFile(parquet_path);

	auto result = ArrowToDuck(conn, *table);
	REQUIRE(result->success);
	REQUIRE(CHECK_COLUMN(result, 0, {-1, nullptr, 2.5}));
	REQUIRE(CHECK_COLUMN(result, 1, {"foo", "bar", "baz"}));
	REQUIRE(CHECK_COLUMN(result, 2, {true, false, true}));
}

TEST_CASE("Test Parquet File Fixed Size Binary", "[arrow]") {
	std::vector<std::string> skip;

	duckdb::DuckDB db;
	duckdb::Connection conn {db};

	//! Impossible to round-trip Fixed Binaries so we just validate that the duckdb table is correct-o
	std::string parquet_path = "data/parquet-testing/fixed.parquet";
	auto table = ReadParquetFile(parquet_path);

	auto result = ArrowToDuck(conn, *table);
	REQUIRE(result->success);
	REQUIRE(
	    CHECK_COLUMN(result, 0, {"\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x09\\x0A\\x0B\\x0C\\x0D\\x0E\\x0F"}));
}

TEST_CASE("Test Parquet Long Files", "[arrow]") {
	std::vector<std::string> skip;

	duckdb::DuckDB db;
	duckdb::Connection conn {db};

	std::vector<std::string> run {"data/parquet-testing/leftdate3_192_loop_1.parquet"};
	run.emplace_back("data/parquet-testing/bug687_nulls.parquet");
	if (STANDARD_VECTOR_SIZE >= 1024) {
		for (auto &parquet_path : run) {
			REQUIRE(RoundTrip(parquet_path, skip, conn));
		}
	}
}

TEST_CASE("Test Parquet Files", "[arrow]") {

	std::vector<std::string> skip {"aws2.parquet"};    //! Not supported by arrow
	skip.emplace_back("broken-arrow.parquet");         //! Arrow can't read this
	skip.emplace_back("nan-float.parquet");            //! Can't roundtrip NaNs
	skip.emplace_back("fixed.parquet");                //! Can't roundtrip Fixed-size Binaries
	skip.emplace_back("leftdate3_192_loop_1.parquet"); //! This is just crazy slow
	skip.emplace_back("bug687_nulls.parquet");         //! This is just crazy slow
	skip.emplace_back("nullbyte.parquet");             //! Null byte in file
	skip.emplace_back("nullbyte_multiple.parquet");    //! Null byte in file

	duckdb::DuckDB db;
	duckdb::Connection conn {db};
	auto &fs = duckdb::FileSystem::GetFileSystem(*conn.context);

	auto parquet_files = fs.Glob("data/parquet-testing/*.parquet");
	for (auto &parquet_path : parquet_files) {
		REQUIRE(RoundTrip(parquet_path, skip, conn));
	}
}

TEST_CASE("Test Arrow Parquet Files", "[arrow]") {

	std::vector<std::string> skip {"datapage_v2.snappy.parquet"}; //! Not supported by arrow
	skip.emplace_back("lz4_raw_compressed.parquet");              //! Arrow can't read this
	skip.emplace_back("lz4_raw_compressed_larger.parquet");       //! Arrow can't read this

	duckdb::DuckDB db;
	duckdb::Connection conn {db};
	auto &fs = duckdb::FileSystem::GetFileSystem(*conn.context);

	auto parquet_files = fs.Glob("data/parquet-testing/arrow/*.parquet");
	for (auto &parquet_path : parquet_files) {
		REQUIRE(RoundTrip(parquet_path, skip, conn));
	}
}

TEST_CASE("Test Parquet Files Decimal", "[arrow]") {
	std::vector<std::string> skip;

	duckdb::DuckDB db;
	duckdb::Connection conn {db};
	auto &fs = duckdb::FileSystem::GetFileSystem(*conn.context);

	//! Decimal Files
	auto parquet_files = fs.Glob("data/parquet-testing/decimal/*.parquet");
	for (auto &parquet_path : parquet_files) {
		REQUIRE(RoundTrip(parquet_path, skip, conn));
	}
}

TEST_CASE("Test Parquet Files Glob", "[arrow]") {

	std::vector<std::string> skip;

	duckdb::DuckDB db;
	duckdb::Connection conn {db};
	auto &fs = duckdb::FileSystem::GetFileSystem(*conn.context);

	auto parquet_files = fs.Glob("data/parquet-testing/cache/*.parquet");
	for (auto &parquet_path : parquet_files) {
		REQUIRE(RoundTrip(parquet_path, skip, conn));
	}
	parquet_files = fs.Glob("data/parquet-testing/glob/*.parquet");
	for (auto &parquet_path : parquet_files) {
		REQUIRE(RoundTrip(parquet_path, skip, conn));
	}
}

TEST_CASE("Test Parquet Files H2O", "[arrow]") {

	std::vector<std::string> skip;

	duckdb::DuckDB db;
	duckdb::Connection conn {db};
	auto &fs = duckdb::FileSystem::GetFileSystem(*conn.context);

	//! Impossible to round-trip Dictionaries so we just validate a query result
	std::string parquet_path = "data/parquet-testing/h2oai/h2oai_group_small.parquet";
	auto table = ReadParquetFile(parquet_path);
	auto query = "SELECT COUNT(*), sum(v1) AS v1, sum(v3) AS v3 FROM (SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM x "
	             "GROUP BY id3) as t;";
	auto result = ArrowToDuck(conn, *table, query, "x");

	REQUIRE(result->success);
	REQUIRE(CHECK_COLUMN(result, 0, {9080}));
	REQUIRE(CHECK_COLUMN(result, 1, {28425.000000}));
	REQUIRE(CHECK_COLUMN(result, 2, {432406.286770}));
}