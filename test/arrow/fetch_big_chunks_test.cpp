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

#include "duckdb/main/query_result.hpp"
#include "duckdb.h"
#include <arrow/c/bridge.h>
#include "duckdb/common/result_arrow_wrapper.hpp"

TEST_CASE("Test Fetch Bigger Than Vector Chunks", "[arrow]") {

	// Create the table in DuckDB
	duckdb::DuckDB db;
	duckdb::Connection conn {db};
	conn.Query("CREATE TABLE t AS SELECT RANGE a FROM RANGE(5000);");
	// Get a query result that selects the whole table
	auto result = conn.SendQuery("SELECT * FROM t");

	// Create the Record Batch Reader
	const idx_t vectors_per_chunk = 2048;
	duckdb::ResultArrowArrayStreamWrapper *result_stream =
	    new duckdb::ResultArrowArrayStreamWrapper(move(result), vectors_per_chunk);
	auto record_batch_reader_result = arrow::ImportRecordBatchReader(&result_stream->stream);

	// Read the whole Record Batch Reader
	auto record_batch_reader = record_batch_reader_result.ValueUnsafe();
	std::shared_ptr<RecordBatch> batch;

	record_batch_reader->ReadNext(&batch);
	REQUIRE(batch->num_rows() == 2048);

	record_batch_reader->ReadNext(&batch);
	REQUIRE(batch->num_rows() == 2048);

	record_batch_reader->ReadNext(&batch);
	REQUIRE(batch->num_rows() == 904);
}
