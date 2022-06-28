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
	if (!status.ok()) {
		fprintf(stderr, "Failed to open file \"%s\"\n", path.c_str());
		REQUIRE(false);
	}
	std::shared_ptr<arrow::Table> table;
	status = reader->ReadTable(&table);
	if (!status.ok()) {
		fprintf(stderr, "Arrow failed to read file \"%s\"\n", path.c_str());
		REQUIRE(false);
	}
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
	params.push_back(duckdb::Value::POINTER((uintptr_t)&SimpleFactory::GetSchema));
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
	auto timezone_config = duckdb::QueryResult::GetConfigTimezone(*result);
	duckdb::QueryResult::ToArrowSchema(&abi_arrow_schema, result->types, result->names, timezone_config);
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

	std::string parquet_path = "data/parquet-testing/nan-float.parquet";
	auto table = ReadParquetFile(parquet_path);

	auto result = ArrowToDuck(conn, *table);
	REQUIRE(result->success);
	REQUIRE(CHECK_COLUMN(result, 0, {-1, std::numeric_limits<double>::infinity(), 2.5}));
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
	std::vector<std::string> data;
	// data.emplace_back("data/parquet-testing/7-set.snappy.arrow2.parquet");
	data.emplace_back("data/parquet-testing/adam_genotypes.parquet");
	data.emplace_back("data/parquet-testing/apkwan.parquet");
	data.emplace_back("data/parquet-testing/aws1.snappy.parquet");
	// not supported by arrow
	// data.emplace_back("data/parquet-testing/aws2.parquet");
	data.emplace_back("data/parquet-testing/binary_string.parquet");
	data.emplace_back("data/parquet-testing/blob.parquet");
	data.emplace_back("data/parquet-testing/boolean_stats.parquet");
	// arrow can't read this
	// data.emplace_back("data/parquet-testing/broken-arrow.parquet");
	data.emplace_back("data/parquet-testing/bug1554.parquet");
	data.emplace_back("data/parquet-testing/bug1588.parquet");
	data.emplace_back("data/parquet-testing/bug1589.parquet");
	data.emplace_back("data/parquet-testing/bug1618_struct_strings.parquet");
	data.emplace_back("data/parquet-testing/bug2267.parquet");
	data.emplace_back("data/parquet-testing/bug2557.parquet");
	// slow
	// data.emplace_back("data/parquet-testing/bug687_nulls.parquet");
	// data.emplace_back("data/parquet-testing/complex.parquet");
	data.emplace_back("data/parquet-testing/data-types.parquet");
	data.emplace_back("data/parquet-testing/date.parquet");
	data.emplace_back("data/parquet-testing/date_stats.parquet");
	data.emplace_back("data/parquet-testing/decimal_stats.parquet");
	data.emplace_back("data/parquet-testing/decimals.parquet");
	data.emplace_back("data/parquet-testing/enum.parquet");
	data.emplace_back("data/parquet-testing/filter_bug1391.parquet");
	//	data.emplace_back("data/parquet-testing/fixed.parquet");
	// slow
	// data.emplace_back("data/parquet-testing/leftdate3_192_loop_1.parquet");
	data.emplace_back("data/parquet-testing/lineitem-top10000.gzip.parquet");
	data.emplace_back("data/parquet-testing/manyrowgroups.parquet");
	data.emplace_back("data/parquet-testing/manyrowgroups2.parquet");
	data.emplace_back("data/parquet-testing/map.parquet");
	// Can't roundtrip NaNs
	// data.emplace_back("data/parquet-testing/nan-float.parquet");
	// null byte in file
	// data.emplace_back("data/parquet-testing/nullbyte.parquet");
	// data.emplace_back("data/parquet-testing/nullbyte_multiple.parquet");
	// borked
	// data.emplace_back("data/parquet-testing/p2.parquet");
	// data.emplace_back("data/parquet-testing/p2strings.parquet");
	data.emplace_back("data/parquet-testing/pandas-date.parquet");
	data.emplace_back("data/parquet-testing/signed_stats.parquet");
	data.emplace_back("data/parquet-testing/silly-names.parquet");
	// borked
	// data.emplace_back("data/parquet-testing/simple.parquet");
	// data.emplace_back("data/parquet-testing/sorted.zstd_18_131072_small.parquet");
	data.emplace_back("data/parquet-testing/struct.parquet");
	data.emplace_back("data/parquet-testing/struct_skip_test.parquet");
	data.emplace_back("data/parquet-testing/timestamp-ms.parquet");
	data.emplace_back("data/parquet-testing/timestamp.parquet");
	data.emplace_back("data/parquet-testing/unsigned.parquet");
	data.emplace_back("data/parquet-testing/unsigned_stats.parquet");
	data.emplace_back("data/parquet-testing/userdata1.parquet");
	data.emplace_back("data/parquet-testing/varchar_stats.parquet");
	data.emplace_back("data/parquet-testing/zstd.parquet");

	std::vector<std::string> skip;

	duckdb::DuckDB db;
	duckdb::Connection conn {db};
	for (auto &parquet_path : data) {
		REQUIRE(RoundTrip(parquet_path, skip, conn));
	}
}

TEST_CASE("Test Arrow Parquet Files", "[arrow]") {

	std::vector<std::string> skip {"datapage_v2.snappy.parquet"}; //! Not supported by arrow
	skip.emplace_back("lz4_raw_compressed.parquet");              //! Arrow can't read this
	skip.emplace_back("lz4_raw_compressed_larger.parquet");       //! Arrow can't read this
	skip.emplace_back("uuid-arrow.parquet");                      //! Not supported by arrow

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
