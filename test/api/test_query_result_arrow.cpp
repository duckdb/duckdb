#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/query_result_stream.hpp"

using namespace duckdb;

#ifndef DUCKDB_NO_THREADS

#include "arrow/arrow_test_helper.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_format.hpp"
#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/main/buffered_data/simple_buffered_data.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "result_wait_helpers.hpp"

namespace {

//! A submitted handle in the Arrow format, ready for a stream
unique_ptr<QueryResult> SubmitArrow(Connection &con, const string &query, idx_t batch_size) {
	auto handle = con.Submit(query);
	REQUIRE(!handle->HasError());
	handle->SetFormat(make_shared_ptr<ArrowFormat>(batch_size));
	return handle;
}

vector<unique_ptr<ArrowUnit>> DrainArrays(FormattedResultStream<ArrowFormat> &stream) {
	vector<unique_ptr<ArrowUnit>> arrays;
	while (auto unit = stream.Fetch()) {
		REQUIRE(unit->row_count > 0);
		REQUIRE(unit->array.arrow_array.release != nullptr);
		REQUIRE(NumericCast<idx_t>(unit->array.arrow_array.length) == unit->row_count);
		arrays.push_back(std::move(unit));
	}
	REQUIRE(!stream.HasError());
	return arrays;
}

idx_t TotalRows(const vector<unique_ptr<ArrowUnit>> &arrays) {
	idx_t rows = 0;
	for (auto &unit : arrays) {
		rows += unit->row_count;
	}
	return rows;
}

//! Serves record batches that are already in hand as an ArrowArrayStream, so arrow_scan can read them
//! back the way test/arrow does
class ServedArrays {
public:
	ServedArrays(vector<LogicalType> types_p, vector<string> names_p, ClientProperties properties_p,
	             vector<unique_ptr<ArrowUnit>> arrays_p)
	    : types(std::move(types_p)), names(std::move(names_p)), properties(std::move(properties_p)),
	      arrays(std::move(arrays_p)) {
		stream.private_data = this;
		stream.get_schema = GetSchema;
		stream.get_next = GetNext;
		stream.get_last_error = GetLastError;
		stream.release = Release;
	}

public:
	//! The rows of these arrays, scanned back through arrow_scan
	unique_ptr<QueryResult> Scan(Connection &con) {
		auto params = ArrowTestHelper::ConstructArrowScan(stream);
		return ArrowTestHelper::ScanArrowObject(con, params);
	}

private:
	static ServedArrays &Self(ArrowArrayStream *stream) {
		return *static_cast<ServedArrays *>(stream->private_data);
	}
	static int GetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
		auto &self = Self(stream);
		ArrowConverter::ToArrowSchema(out, self.types, self.names, self.properties);
		return 0;
	}
	static int GetNext(ArrowArrayStream *stream, ArrowArray *out) {
		auto &self = Self(stream);
		out->release = nullptr;
		if (self.next == self.arrays.size()) {
			return 0;
		}
		self.arrays[self.next++]->array.MoveTo(*out);
		return 0;
	}
	static const char *GetLastError(ArrowArrayStream *stream) {
		return "";
	}
	// The copies arrow_scan makes are released independently, so this frees nothing: the arrays and
	// their owner outlive every copy
	static void Release(ArrowArrayStream *stream) {
		stream->release = nullptr;
	}

private:
	vector<LogicalType> types;
	vector<string> names;
	ClientProperties properties;
	vector<unique_ptr<ArrowUnit>> arrays;
	idx_t next = 0;
	ArrowArrayStream stream {};
};

//! The record batches of a stream, scanned back into a DuckDB result
unique_ptr<QueryResult> ScanBack(Connection &con, FormattedResultStream<ArrowFormat> &stream,
                                 vector<unique_ptr<ArrowUnit>> arrays) {
	ServedArrays served(stream.GetTypes(), IdentifiersToStrings(stream.GetNames()), stream.GetClientProperties(),
	                    std::move(arrays));
	return served.Scan(con);
}

void RequireAscendingRows(QueryResult &result, idx_t expected_count) {
	REQUIRE(!result.HasError());
	auto &collection = result.Collection();
	REQUIRE(collection.Count() == expected_count);
	idx_t row = 0;
	for (auto &chunk_row : collection.Rows()) {
		auto value = chunk_row.GetValue(0);
		if (value.IsNull() || value.GetValue<int64_t>() != NumericCast<int64_t>(row)) {
			FAIL(StringUtil::Format("Out-of-order row %llu: %s", row, value.ToString()));
		}
		row++;
	}
	REQUIRE(row == expected_count);
}

} // namespace

TEST_CASE("A formatted Arrow stream drains an ordered plan in row order", "[api][query_result_arrow]") {
	constexpr idx_t ROWS = 50000;
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(50000)"));

	SECTION("through the simple store") {
		auto handle = SubmitArrow(con, "SELECT i FROM range(50000) t(i)", 4096);
		DrainWatchdog watchdog(con);
		FormattedResultStream<ArrowFormat> stream(std::move(handle));
		REQUIRE_NOTHROW(stream.GetBufferedData().Cast<SimpleBufferedData>());
		auto arrays = DrainArrays(stream);
		REQUIRE(TotalRows(arrays) == ROWS);
		auto scanned = ScanBack(con, stream, std::move(arrays));
		RequireAscendingRows(*scanned, ROWS);
	}
	SECTION("through the batched store") {
		auto handle = SubmitArrow(con, "SELECT i FROM t", 4096);
		DrainWatchdog watchdog(con);
		FormattedResultStream<ArrowFormat> stream(std::move(handle));
		REQUIRE_NOTHROW(stream.GetBufferedData().Cast<BatchedBufferedData>());
		auto arrays = DrainArrays(stream);
		REQUIRE(TotalRows(arrays) == ROWS);
		auto scanned = ScanBack(con, stream, std::move(arrays));
		RequireAscendingRows(*scanned, ROWS);
	}
}

TEST_CASE("An Arrow batch size smaller than a chunk fills every array but the last", "[api][query_result_arrow]") {
	constexpr idx_t ROWS = 20000;
	constexpr idx_t BATCH = 500;
	DuckDB db(nullptr);
	Connection con(db);
	// One producer and no batch boundaries, so the only array the format may cut short is the last
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET preserve_insertion_order=false"));

	SECTION("under the default cap") {
		auto handle = SubmitArrow(con, "SELECT i FROM range(20000) t(i)", BATCH);
		DrainWatchdog watchdog(con);
		FormattedResultStream<ArrowFormat> stream(std::move(handle));
		auto arrays = DrainArrays(stream);
		REQUIRE(TotalRows(arrays) == ROWS);
		REQUIRE(arrays.size() == ROWS / BATCH);
		for (auto &unit : arrays) {
			REQUIRE(unit->row_count == BATCH);
		}
	}
	SECTION("with a cap smaller than one array") {
		// Every unit exceeds the cap, so each one parks its producer and is deposited on the pop
		REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='1b'"));
		auto handle = SubmitArrow(con, "SELECT i FROM range(20000) t(i)", BATCH);
		DrainWatchdog watchdog(con);
		FormattedResultStream<ArrowFormat> stream(std::move(handle));
		auto arrays = DrainArrays(stream);
		REQUIRE(TotalRows(arrays) == ROWS);
		REQUIRE(stream.Poll() == QueryResultState::FINISHED);
	}
}

TEST_CASE("An Arrow batch larger than a row group gives one array per row group", "[api][query_result_arrow]") {
	constexpr idx_t GROUPS = 3;
	const idx_t rows = GROUPS * DEFAULT_ROW_GROUP_SIZE;
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("CREATE TABLE t AS SELECT range i FROM range(%llu)", rows)));

	auto handle = SubmitArrow(con, "SELECT i FROM t", rows);
	DrainWatchdog watchdog(con);
	FormattedResultStream<ArrowFormat> stream(std::move(handle));
	REQUIRE_NOTHROW(stream.GetBufferedData().Cast<BatchedBufferedData>());
	auto arrays = DrainArrays(stream);
	REQUIRE(TotalRows(arrays) == rows);
	// NextBatch finishes the array before the producer moves on, so none spans two row groups
	for (auto &unit : arrays) {
		REQUIRE(unit->row_count <= DEFAULT_ROW_GROUP_SIZE);
	}
	REQUIRE(arrays.size() == GROUPS);
}

TEST_CASE("Arrow extension types round-trip through the format", "[api][query_result_arrow]") {
	constexpr idx_t ROWS = 3000;
	DBConfig config;
	DuckDB db(nullptr, &config);
	Connection con(db);

	SECTION("UUID") {
		auto handle = SubmitArrow(con, "SELECT '4ac7a9e9-607c-4c8a-84f3-843f0191e3fd'::UUID u FROM range(3000)", 1024);
		DrainWatchdog watchdog(con);
		FormattedResultStream<ArrowFormat> stream(std::move(handle));
		auto arrays = DrainArrays(stream);
		REQUIRE(TotalRows(arrays) == ROWS);
		auto scanned = ScanBack(con, stream, std::move(arrays));
		REQUIRE(!scanned->HasError());
		REQUIRE(scanned->Collection().Count() == ROWS);
		REQUIRE(scanned->Collection().GetValue(0, 0).ToString() == "4ac7a9e9-607c-4c8a-84f3-843f0191e3fd");
	}
	SECTION("JSON") {
		if (!db.ExtensionIsLoaded("json")) {
			return;
		}
		auto handle = SubmitArrow(con, "SELECT '{\"a\":1}'::JSON j FROM range(3000)", 1024);
		DrainWatchdog watchdog(con);
		FormattedResultStream<ArrowFormat> stream(std::move(handle));
		auto arrays = DrainArrays(stream);
		REQUIRE(TotalRows(arrays) == ROWS);
		auto scanned = ScanBack(con, stream, std::move(arrays));
		REQUIRE(!scanned->HasError());
		REQUIRE(scanned->Collection().Count() == ROWS);
		REQUIRE(scanned->Collection().GetValue(0, 0).ToString() == "{\"a\":1}");
	}
}

TEST_CASE("An Arrow unit reports the bytes its buffers hold", "[api][query_result_arrow]") {
	constexpr idx_t ROWS = 4096;
	DuckDB db(nullptr);
	Connection con(db);

	auto numbers = SubmitArrow(con, "SELECT i FROM range(4096) t(i)", ROWS);
	DrainWatchdog watchdog(con);
	FormattedResultStream<ArrowFormat> number_stream(std::move(numbers));
	auto number_arrays = DrainArrays(number_stream);
	REQUIRE(number_arrays.size() == 1);
	REQUIRE(number_arrays[0]->row_count == ROWS);
	// A BIGINT column is eight bytes a row, plus whatever the validity mask costs
	REQUIRE(number_arrays[0]->byte_size >= ROWS * sizeof(int64_t));

	auto strings = SubmitArrow(con, "SELECT 'a reasonably long string ' || i AS s FROM range(4096) t(i)", ROWS);
	FormattedResultStream<ArrowFormat> string_stream(std::move(strings));
	auto string_arrays = DrainArrays(string_stream);
	REQUIRE(string_arrays.size() == 1);
	REQUIRE(string_arrays[0]->row_count == ROWS);
	// The same rows as strings: offsets alone match the numbers, and the characters come on top
	REQUIRE(string_arrays[0]->byte_size > number_arrays[0]->byte_size);
}

TEST_CASE("Query with an Arrow format returns the record batches and their schema", "[api][query_result_arrow]") {
	constexpr idx_t ROWS = 20000;
	DuckDB db(nullptr);
	Connection con(db);

	QueryParameters parameters;
	parameters.format = make_shared_ptr<ArrowFormat>(4096);
	auto result = con.context->Query("SELECT i, 'r' || i AS s FROM range(20000) t(i)", parameters);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == ROWS);

	auto &state = result->FormatState<ArrowFormat>();
	auto &schema = state.Schema();
	REQUIRE(schema.n_children == 2);
	REQUIRE(string(schema.children[0]->name) == "i");
	REQUIRE(string(schema.children[1]->name) == "s");
	REQUIRE(string(schema.children[0]->format) == "l");
	REQUIRE(string(schema.children[1]->format) == "u");

	auto &collection = result->Collection<ArrowFormat>();
	REQUIRE(collection.Count() == ROWS);
	REQUIRE(collection.UnitCount() == ROWS / 4096 + 1);
	idx_t rows = 0;
	for (auto &unit : collection.Units()) {
		rows += unit->row_count;
	}
	REQUIRE(rows == ROWS);
}

TEST_CASE("The chunk accessors reject an Arrow result", "[api][query_result_arrow]") {
	DuckDB db(nullptr);
	Connection con(db);

	QueryParameters parameters;
	parameters.format = make_shared_ptr<ArrowFormat>(1024);
	auto result = con.context->Query("SELECT i FROM range(1000) t(i)", parameters);
	REQUIRE_NO_FAIL(*result);

	REQUIRE_THROWS_AS(result->FetchRaw(), InvalidInputException);
	REQUIRE_THROWS_AS(result->Collection(), InvalidInputException);
	REQUIRE_THROWS_AS(result->Fetch(), InvalidInputException);
	// The rows are still there, in the format that was asked for
	REQUIRE(result->Collection<ArrowFormat>().Count() == 1000);
}

#endif
