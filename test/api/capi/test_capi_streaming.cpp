#include "capi_tester.hpp"
#include "duckdb.h"

using namespace duckdb;
using namespace std;

TEST_CASE("Test streaming results in C API", "[capi]") {
	CAPITester tester;
	CAPIPrepared prepared;
	CAPIPending pending;
	unique_ptr<CAPIResult> result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE(prepared.Prepare(tester, "SELECT i::UINT32 FROM range(1000000) tbl(i)"));
	REQUIRE(pending.PendingStreaming(prepared));

	while (true) {
		auto state = pending.ExecuteTask();
		REQUIRE(state != DUCKDB_PENDING_ERROR);
		if (state == DUCKDB_PENDING_RESULT_READY) {
			break;
		}
	}

	result = pending.CreateStream();
	REQUIRE(result);
	REQUIRE(!result->HasError());
	auto chunk = result->StreamChunk();

	idx_t value = duckdb::DConstants::INVALID_INDEX;
	idx_t chunk_count = 0;
	while (chunk) {
		auto old_value = value;

		auto vector = chunk->GetVector(0);
		uint32_t *data = (uint32_t *)duckdb_vector_get_data(vector);
		value = data[0];
		if (old_value != duckdb::DConstants::INVALID_INDEX) {
			// We select from a range, so we can expect every starting value of a new chunk to be higher than the last
			// one.
			REQUIRE(value > old_value);
		}
		chunk_count++;
		chunk = result->StreamChunk();
	}
}

TEST_CASE("Test other methods on streaming results in C API", "[capi]") {
	CAPITester tester;
	CAPIPrepared prepared;
	CAPIPending pending;
	unique_ptr<CAPIResult> result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE(prepared.Prepare(tester, "SELECT i::UINT32 FROM range(1000000) tbl(i)"));
	REQUIRE(pending.PendingStreaming(prepared));

	while (true) {
		auto state = pending.ExecuteTask();
		REQUIRE(state != DUCKDB_PENDING_ERROR);
		if (state == DUCKDB_PENDING_RESULT_READY) {
			break;
		}
	}

	// Once we've done this, the StreamQueryResult is made
	result = pending.CreateStream();
	REQUIRE(result);
	REQUIRE(!result->HasError());
	REQUIRE(result->IsStreaming());

	// interrogate the result with various methods
	auto chunk_count = result->ChunkCount();
	REQUIRE(chunk_count == 0);
	auto column_count = result->ColumnCount();
	(void)column_count;
	auto column_name = result->ColumnName(0);
	(void)column_name;
	auto column_type = result->ColumnType(0);
	(void)column_type;
	auto error_message = result->ErrorMessage();
	REQUIRE(error_message == nullptr);
	auto fetched_chunk = result->FetchChunk(0);
	REQUIRE(fetched_chunk == nullptr);
	auto has_error = result->HasError();
	REQUIRE(has_error == false);
	auto row_count = result->row_count();
	REQUIRE(row_count == 0);
	auto rows_changed = result->rows_changed();
	REQUIRE(rows_changed == 0);

	// this succeeds because the result is materialized if a stream-result method hasn't being used yet
	auto column_data = result->ColumnData<uint32_t>(0);
	REQUIRE(column_data != nullptr);

	// this materializes the result
	auto is_null = result->IsNull(0, 0);
	REQUIRE(is_null == false);
}

TEST_CASE("Test materializing a streaming result in C API", "[capi]") {
	CAPITester tester;
	CAPIPrepared prepared;
	CAPIPending pending;
	unique_ptr<CAPIResult> result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE(prepared.Prepare(tester, "SELECT i::UINT32 FROM range(1000000) tbl(i)"));
	// Create a pending query that will turn into a streaming result
	REQUIRE(pending.PendingStreaming(prepared));

	while (true) {
		auto state = pending.ExecuteTask();
		REQUIRE(state != DUCKDB_PENDING_ERROR);
		if (state == DUCKDB_PENDING_RESULT_READY) {
			break;
		}
	}

	// This creates a streaming result, but gets materialized instantly into
	// a MaterializedQueryResult
	result = pending.Execute();
	REQUIRE(!result->IsStreaming());
}
