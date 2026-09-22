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

//! Serves Arrow arrays that are already in hand as an ArrowArrayStream, so arrow_scan can read them
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

//! The arrays of a stream, scanned back into a DuckDB result
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

TEST_CASE("Arrow units over NULL-heavy and nested columns keep their counts", "[api][query_result_arrow]") {
	constexpr idx_t ROWS = 5000;
	constexpr idx_t BATCH = 1024;
	DuckDB db(nullptr);
	Connection con(db);

	auto handle = SubmitArrow(con,
	                          "SELECT CASE WHEN i % 3 = 0 THEN NULL ELSE i END AS n, "
	                          "CASE WHEN i % 5 = 0 THEN NULL ELSE [i, NULL, i + 1] END AS l, "
	                          "{'a': i, 'b': CASE WHEN i % 2 = 0 THEN NULL ELSE 'x' || i END} AS s "
	                          "FROM range(5000) t(i)",
	                          BATCH);
	DrainWatchdog watchdog(con);
	FormattedResultStream<ArrowFormat> stream(std::move(handle));
	auto arrays = DrainArrays(stream);
	REQUIRE(TotalRows(arrays) == ROWS);
	REQUIRE(arrays.size() == ROWS / BATCH + 1);
	for (auto &unit : arrays) {
		auto &array = unit->array.arrow_array;
		REQUIRE(array.n_children == 3);
		REQUIRE(NumericCast<idx_t>(array.children[0]->length) == unit->row_count);
		REQUIRE(NumericCast<idx_t>(array.children[1]->length) == unit->row_count);
		REQUIRE(NumericCast<idx_t>(array.children[2]->length) == unit->row_count);
		REQUIRE(array.children[0]->null_count > 0);
		REQUIRE(array.children[1]->null_count > 0);
		// The struct itself has no NULLs; its second child does
		REQUIRE(array.children[2]->null_count == 0);
		REQUIRE(array.children[2]->children[1]->null_count > 0);
		// Three columns of a few bytes a row, on top of the validity and offset buffers
		REQUIRE(unit->byte_size >= unit->row_count * sizeof(int64_t));
	}

	auto scanned = ScanBack(con, stream, std::move(arrays));
	REQUIRE(!scanned->HasError());
	auto &collection = scanned->Collection();
	REQUIRE(collection.Count() == ROWS);
	auto rows = collection.GetRows();
	REQUIRE(rows.GetValue(0, 0).IsNull());
	REQUIRE(rows.GetValue(0, 1) == Value::BIGINT(1));
	REQUIRE(rows.GetValue(1, 0).IsNull());
	REQUIRE(rows.GetValue(1, 1) ==
	        Value::LIST(LogicalType::BIGINT, {Value::BIGINT(1), Value(LogicalType::BIGINT), Value::BIGINT(2)}));
	REQUIRE(rows.GetValue(2, 2) == Value::STRUCT({{"a", Value::BIGINT(2)}, {"b", Value(LogicalType::VARCHAR)}}));
	REQUIRE(rows.GetValue(2, 3) == Value::STRUCT({{"a", Value::BIGINT(3)}, {"b", Value("x3")}}));
	REQUIRE(rows.GetValue(0, ROWS - 1).IsNull() == ((ROWS - 1) % 3 == 0));
}

TEST_CASE("An empty result in the Arrow format has no units", "[api][query_result_arrow]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(10000)"));

	SECTION("streamed from a source without rows") {
		auto handle = SubmitArrow(con, "SELECT i FROM range(0) t(i)", 1024);
		DrainWatchdog watchdog(con);
		FormattedResultStream<ArrowFormat> stream(std::move(handle));
		auto arrays = DrainArrays(stream);
		REQUIRE(arrays.empty());
		REQUIRE(stream.Poll() == QueryResultState::FINISHED);
		REQUIRE(stream.FormatState().Schema().n_children == 1);
	}
	SECTION("streamed from a table whose rows are all filtered") {
		auto handle = SubmitArrow(con, "SELECT i FROM t WHERE i < 0", 1024);
		DrainWatchdog watchdog(con);
		FormattedResultStream<ArrowFormat> stream(std::move(handle));
		REQUIRE_NOTHROW(stream.GetBufferedData().Cast<BatchedBufferedData>());
		auto arrays = DrainArrays(stream);
		REQUIRE(arrays.empty());
		REQUIRE(stream.Poll() == QueryResultState::FINISHED);
	}
	SECTION("retained") {
		QueryParameters parameters;
		parameters.format = make_shared_ptr<ArrowFormat>(1024);
		auto result = con.context->Query("SELECT i FROM t WHERE i < 0", parameters);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->RowCount() == 0);
		auto &collection = result->Collection<ArrowFormat>();
		REQUIRE(collection.Count() == 0);
		REQUIRE(collection.UnitCount() == 0);
		REQUIRE(collection.Units().empty());
		REQUIRE(result->Fetch<ArrowFormat>() == nullptr);
		REQUIRE(result->FormatState<ArrowFormat>().Schema().n_children == 1);
	}
}

TEST_CASE("Query with an Arrow format returns its arrays and their schema", "[api][query_result_arrow]") {
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

TEST_CASE("An Arrow unit copies into a view that outlives the original", "[api][query_result_arrow]") {
	constexpr idx_t ROWS = 3000;
	DuckDB db(nullptr);
	Connection con(db);

	QueryParameters parameters;
	parameters.format = make_shared_ptr<ArrowFormat>(1024);
	auto result = con.context->Query("SELECT i FROM range(3000) t(i)", parameters);
	REQUIRE_NO_FAIL(*result);

	SECTION("fetching copies, so the result can be read twice") {
		vector<unique_ptr<ArrowUnit>> first_pass;
		while (auto unit = result->Fetch<ArrowFormat>()) {
			first_pass.push_back(std::move(unit));
		}
		REQUIRE(first_pass.size() == ROWS / 1024 + 1);
		REQUIRE(TotalRows(first_pass) == ROWS);
		auto &collection = result->Collection<ArrowFormat>();
		REQUIRE(collection.UnitCount() == first_pass.size());
		REQUIRE(collection.Count() == ROWS);
		for (idx_t i = 0; i < first_pass.size(); i++) {
			auto &stored = collection.Units()[i]->Cast<ArrowUnit>().array.arrow_array;
			auto &fetched = first_pass[i]->array.arrow_array;
			REQUIRE(stored.release != nullptr);
			REQUIRE(fetched.release != nullptr);
			REQUIRE(fetched.length == stored.length);
			// A copy shares the buffers rather than duplicating them
			REQUIRE(fetched.children[0]->buffers[1] == stored.children[0]->buffers[1]);
			REQUIRE(first_pass[i]->byte_size == collection.Units()[i]->byte_size);
		}
	}

	SECTION("a copy stays readable after the original and the collection are gone") {
		auto original = result->Fetch<ArrowFormat>();
		REQUIRE(original);
		auto copy = original->Copy();
		REQUIRE(copy->row_count == original->row_count);
		original.reset();
		result.reset();
		auto &array = copy->Cast<ArrowUnit>().array.arrow_array;
		REQUIRE(array.length == 1024);
		auto values = reinterpret_cast<const int64_t *>(array.children[0]->buffers[1]);
		for (idx_t i = 0; i < 1024; i++) {
			REQUIRE(values[array.offset + array.children[0]->offset + i] == NumericCast<int64_t>(i));
		}
	}

	SECTION("a consumer holding a view keeps the buffers alive on its own") {
		ArrowArray exported;
		result->Fetch<ArrowFormat>()->array.MoveTo(exported);
		result.reset();
		REQUIRE(exported.release != nullptr);
		auto values = reinterpret_cast<const int64_t *>(exported.children[0]->buffers[1]);
		REQUIRE(values[exported.offset + exported.children[0]->offset + 1023] == 1023);
		exported.release(&exported);
		REQUIRE(exported.release == nullptr);
	}
}

namespace {

const int64_t *StructFieldA(const ArrowArray &array) {
	auto &field = *array.children[0]->children[0];
	return reinterpret_cast<const int64_t *>(field.buffers[1]) + field.offset;
}

} // namespace

TEST_CASE("Views of one Arrow unit are independent struct trees over shared buffers", "[api][query_result_arrow]") {
	DuckDB db(nullptr);
	Connection con(db);

	QueryParameters parameters;
	parameters.format = make_shared_ptr<ArrowFormat>(1024);
	auto result =
	    con.context->Query("SELECT {'a': i, 'b': 'x' || i} AS s, [i, i + 1] AS l FROM range(2000) t(i)", parameters);
	REQUIRE_NO_FAIL(*result);
	auto first = result->Fetch<ArrowFormat>();
	REQUIRE(first);
	auto second = first->Copy();
	auto &stored = result->Collection<ArrowFormat>().Units()[0]->Cast<ArrowUnit>().array.arrow_array;
	auto &first_array = first->array.arrow_array;
	auto &second_array = second->Cast<ArrowUnit>().array.arrow_array;
	// Distinct child structs, the same buffers
	REQUIRE(first_array.children != stored.children);
	REQUIRE(first_array.children != second_array.children);
	REQUIRE(first_array.children[0] != stored.children[0]);
	REQUIRE(first_array.children[0]->children[0]->buffers[1] == stored.children[0]->children[0]->buffers[1]);
	REQUIRE(first_array.children[1]->children[0]->buffers[1] == stored.children[1]->children[0]->buffers[1]);

	SECTION("a child moved out of one view leaves the other views whole") {
		// The C interface lets a consumer take a child by copying its struct and clearing its release
		ArrowArray moved = *first_array.children[0];
		first_array.children[0]->release = nullptr;
		REQUIRE(stored.children[0]->release != nullptr);
		REQUIRE(second_array.children[0]->release != nullptr);
		REQUIRE(StructFieldA(second_array)[7] == 7);

		moved.release(&moved);
		REQUIRE(moved.release == nullptr);
		first.reset();
		result.reset();
		REQUIRE(StructFieldA(second_array)[1023] == 1023);
		REQUIRE(second_array.children[1]->children[0]->length == 2048);
	}

	SECTION("a released sibling does not touch the copies still alive") {
		auto third = second->Copy();
		second.reset();
		REQUIRE(StructFieldA(first_array)[5] == 5);
		result.reset();
		first.reset();
		auto &third_array = third->Cast<ArrowUnit>().array.arrow_array;
		REQUIRE(third_array.children[0]->release != nullptr);
		REQUIRE(StructFieldA(third_array)[1000] == 1000);
		ArrowArray exported;
		third->Cast<ArrowUnit>().array.MoveTo(exported);
		third.reset();
		REQUIRE(StructFieldA(exported)[1000] == 1000);
		exported.release(&exported);
	}
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
