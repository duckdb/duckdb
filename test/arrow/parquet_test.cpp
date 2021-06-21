#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow_test_factory.hpp"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
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
#include "parquet/exception.h"
#include "duckdb/common/file_system.hpp"

std::shared_ptr<arrow::Table> ReadParquetFile(const duckdb::string &path) {
	std::shared_ptr<arrow::io::ReadableFile> infile;
	PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(path, arrow::default_memory_pool()));

	std::unique_ptr<parquet::arrow::FileReader> reader;

	parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader);
	std::shared_ptr<arrow::Table> table;
	reader->ReadTable(&table);
	return table;
}

bool ArrowChunkEquals(const arrow::ChunkedArray &left, const arrow::ChunkedArray &right) {
	if (left.length() != right.length()) {
		return false;
	}
	if (left.null_count() != right.null_count()) {
		return false;
	}
	arrow::internal::MultipleChunkIterator iterator(left, right);
	std::shared_ptr<arrow::Array> left_piece, right_piece;
	while (iterator.Next(&left_piece, &right_piece)) {
		if (!left_piece->Equals(right_piece)) {
			return false;
		}
	}
	return true;
}

bool ArrowTableEquals(const arrow::Table &left, const arrow::Table &right) {
	if (&left == &right) {
		return true;
	}
	if (!(left.schema()->Equals(*right.schema()), true)) {
		return false;
	}
	if (left.num_columns() != right.num_columns()) {
		return false;
	}

	for (int i = 0; i < left.num_columns(); i++) {
		if (!ArrowChunkEquals(*left.column(i), *right.column(i))) {
			return false;
		}
	}
	return true;
}

TEST_CASE("Test Parquet Files", "[arrow]") {
	duckdb::DuckDB db;
	duckdb::Connection conn {db};
	auto &fs = duckdb::FileSystem::GetFileSystem(*conn.context);

	auto parquet_files = fs.Glob("test/sql/copy/parquet/data/*.parquet");
	for (auto &parquet_path : parquet_files) {
		auto table = ReadParquetFile(fs.GetWorkingDirectory() + "/" + parquet_path);
		std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

		auto batch_reader = arrow::TableBatchReader(*table);
		batch_reader.ReadAll(&batches);

		SimpleFactory factory {batches, table->schema()};

		duckdb::vector<duckdb::Value> params;
		params.push_back(duckdb::Value::POINTER((uintptr_t)&factory));
		params.push_back(duckdb::Value::POINTER((uintptr_t)&SimpleFactory::CreateStream));
		params.push_back(duckdb::Value::UBIGINT(1000000));
		auto result = conn.TableFunction("arrow_scan", params)->Execute();

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
			auto batch = arrow::ImportRecordBatch(&arrow_array, table->schema());
			batches_result.push_back(batch.MoveValueUnsafe());
		}
		auto arrow_result_tbl = arrow::Table::FromRecordBatches(batches_result).ValueUnsafe();
		auto new_table = table->CombineChunks();
		auto new_arrow_result_tbl = arrow_result_tbl->CombineChunks();
		REQUIRE(ArrowTableEquals(*new_table.ValueUnsafe(), *new_table.ValueUnsafe()));
	}
}