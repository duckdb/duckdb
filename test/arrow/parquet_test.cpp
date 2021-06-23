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

std::unique_ptr<duckdb::QueryResult> ArrowToDuck(duckdb::Connection &conn, arrow::Table &table) {
	std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

	auto batch_reader = arrow::TableBatchReader(table);
	auto status = batch_reader.ReadAll(&batches);

	REQUIRE(status.ok());
	SimpleFactory factory {batches, table.schema()};

	duckdb::vector<duckdb::Value> params;
	params.push_back(duckdb::Value::POINTER((uintptr_t)&factory));
	params.push_back(duckdb::Value::POINTER((uintptr_t)&SimpleFactory::CreateStream));
	params.push_back(duckdb::Value::UBIGINT(1000000));
	return conn.TableFunction("arrow_scan", params)->Execute();
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
	return ArrowTableEquals(*new_table, *new_arrow_result_tbl);
}

TEST_CASE("Test Parquet File NaN", "[arrow]") {
	std::vector<std::string> skip;

	duckdb::DuckDB db;
	duckdb::Connection conn {db};

	//! Impossible to round-trip NaNs so we just validate that the duckdb table is correct-o
	std::string parquet_path = "test/sql/copy/parquet/data/nan-float.parquet";
	auto table = ReadParquetFile(parquet_path);

	auto result = ArrowToDuck(conn, *table);
	REQUIRE(result->success);
	REQUIRE(CHECK_COLUMN(result, 0, {-1, nullptr, 2.5}));
	REQUIRE(CHECK_COLUMN(result, 1, {"foo", "bar", "baz"}));
	REQUIRE(CHECK_COLUMN(result, 2, {true, false, true}));
}
//
//TEST_CASE("Test Parquet Files fix", "[arrow]") {
//
//	std::vector<std::string> skip {"aws2.parquet"};     //! Not supported by arrow
//	skip.emplace_back("datapage_v2.snappy.parquet");    //! Not supported by arrow
//	skip.emplace_back("broken-arrow.parquet");          //! Arrow can't read this
//	skip.emplace_back("nan-float.parquet");             //! Can't roundtrip NaNs
//	skip.emplace_back("alltypes_dictionary.parquet");   //! FIXME: Contains binary columns, we don't support those yet
//	skip.emplace_back("blob.parquet");                  //! FIXME: Contains binary columns, we don't support those yet
//	skip.emplace_back("alltypes_plain.parquet");        //! FIXME: Contains binary columns, we don't support those yet
//	skip.emplace_back("alltypes_plain.snappy.parquet"); //! FIXME: Contains binary columns, we don't support those yet
//	skip.emplace_back("data-types.parquet");            //! FIXME: Contains binary columns, we don't support those yet
//	skip.emplace_back("fixed.parquet"); //! FIXME: Contains fixed-width-binary columns, we don't support those yet
//
//	//! Breaking with vec2
//	//	skip.emplace_back("nested_maps.parquet");
//	//	skip.emplace_back("nested_maps.snappy.parquet");
//	//	skip.emplace_back("apkwan.parquet");
//	//	skip.emplace_back("nested_lists.snappy.parquet");
//	//    skip.emplace_back("leftdate3_192_loop_1.parquet"); //! This is just crazy slow
//	//	skip.emplace_back("list_columns.parquet");
//
//	duckdb::DuckDB db;
//	duckdb::Connection conn {db};
//	std::string parquet_path = "test/sql/copy/parquet/data/apkwan.parquet";
//	REQUIRE(RoundTrip(parquet_path, skip, conn));
//}
TEST_CASE("Test Parquet Files", "[arrow]") {

	std::vector<std::string> skip {"aws2.parquet"};     //! Not supported by arrow
	skip.emplace_back("datapage_v2.snappy.parquet");    //! Not supported by arrow
	skip.emplace_back("broken-arrow.parquet");          //! Arrow can't read this
	skip.emplace_back("nan-float.parquet");             //! Can't roundtrip NaNs
	skip.emplace_back("alltypes_dictionary.parquet");   //! FIXME: Contains binary columns, we don't support those yet
	skip.emplace_back("blob.parquet");                  //! FIXME: Contains binary columns, we don't support those yet
	skip.emplace_back("alltypes_plain.parquet");        //! FIXME: Contains binary columns, we don't support those yet
	skip.emplace_back("alltypes_plain.snappy.parquet"); //! FIXME: Contains binary columns, we don't support those yet
	skip.emplace_back("data-types.parquet");            //! FIXME: Contains binary columns, we don't support those yet
	skip.emplace_back("fixed.parquet"); //! FIXME: Contains fixed-width-binary columns, we don't support those yet

	duckdb::DuckDB db;
	duckdb::Connection conn {db};
	auto &fs = duckdb::FileSystem::GetFileSystem(*conn.context);

	auto parquet_files = fs.Glob("test/sql/copy/parquet/data/*.parquet");
	for (auto &parquet_path : parquet_files) {
		REQUIRE(RoundTrip(parquet_path, skip, conn));
	}
	skip.clear();

	//! Decimal Files
	parquet_files = fs.Glob("test/sql/copy/parquet/data/decimal/*.parquet");
	for (auto &parquet_path : parquet_files) {
		REQUIRE(RoundTrip(parquet_path, skip, conn));
	}

	skip.emplace_back("cache1.parquet"); //! FIXME: Contains binary columns, we don't support those yet

	//! FIXME: Cache Files
	parquet_files = fs.Glob("test/sql/copy/parquet/data/cache/*.parquet");
	for (auto &parquet_path : parquet_files) {
		REQUIRE(RoundTrip(parquet_path, skip, conn));
	}
	//! FIXME: GLOB Files (More binaries)
	//	parquet_files = fs.Glob("test/sql/copy/parquet/data/glob/*.parquet");
	//	for (auto &parquet_path : parquet_files) {
	//	    std::cout << parquet_path <<  std::endl;
	//        REQUIRE(RoundTrip(parquet_path,skip,conn));
	//	}
}