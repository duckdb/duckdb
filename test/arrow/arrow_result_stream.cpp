#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

using namespace duckdb;

namespace {

ResultArrowArrayStreamWrapper *Wrap(unique_ptr<QueryResult> result, idx_t batch_size) {
	// Owned by the Arrow stream's private_data, freed by its release callback
	return new ResultArrowArrayStreamWrapper(std::move(result), batch_size);
}

int Next(ResultArrowArrayStreamWrapper &wrapper, ArrowArray &out) {
	out.release = nullptr;
	return wrapper.stream.get_next(&wrapper.stream, &out);
}

string LastError(ResultArrowArrayStreamWrapper &wrapper) {
	return wrapper.stream.get_last_error(&wrapper.stream);
}

vector<int64_t> ReadBigintColumn(const ArrowArray &array, idx_t column) {
	REQUIRE(column < idx_t(array.n_children));
	auto &child = *array.children[column];
	REQUIRE(child.n_buffers == 2);
	auto data = reinterpret_cast<const int64_t *>(child.buffers[1]) + child.offset;
	return vector<int64_t>(data, data + child.length);
}

//! Pulls every batch, returning the first column as BIGINT values and the batch sizes seen
vector<int64_t> Drain(ResultArrowArrayStreamWrapper &wrapper, vector<idx_t> &batch_sizes) {
	vector<int64_t> values;
	while (true) {
		ArrowArray array;
		REQUIRE(Next(wrapper, array) == 0);
		if (!array.release) {
			break;
		}
		batch_sizes.push_back(idx_t(array.length));
		auto batch = ReadBigintColumn(array, 0);
		values.insert(values.end(), batch.begin(), batch.end());
		array.release(&array);
	}
	return values;
}

vector<int64_t> Range(int64_t count) {
	vector<int64_t> values;
	for (int64_t i = 0; i < count; i++) {
		values.push_back(i);
	}
	return values;
}

} // namespace

TEST_CASE("A SELECT is streamed: the first batch returns while the query is still open",
          "[arrow][result_arrow_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='100KB'"));

	auto wrapper = Wrap(con.Submit("SELECT i FROM range(200000) t(i)"), 1000);
	REQUIRE(wrapper->stream_result);
	REQUIRE(!wrapper->result);

	ArrowSchema schema;
	REQUIRE(wrapper->stream.get_schema(&wrapper->stream, &schema) == 0);
	REQUIRE(schema.n_children == 1);
	REQUIRE(string(schema.children[0]->name) == "i");
	schema.release(&schema);

	ArrowArray first;
	REQUIRE(Next(*wrapper, first) == 0);
	REQUIRE(first.length == 1000);
	REQUIRE(ReadBigintColumn(first, 0) == Range(1000));
	first.release(&first);
	REQUIRE(wrapper->stream_result->IsOpen());

	vector<idx_t> batch_sizes;
	auto rest = Drain(*wrapper, batch_sizes);
	REQUIRE(rest.size() == 199000);
	REQUIRE(rest.front() == 1000);
	REQUIRE(rest.back() == 199999);
	for (auto size : batch_sizes) {
		REQUIRE(size == 1000);
	}
	REQUIRE(!wrapper->stream_result->IsOpen());
	// The guarantee is the cap plus one chunk
	REQUIRE(wrapper->stream_result->GetBufferedData().PeakBufferedBytes() <= 100000 + 100000);

	// The end of the stream keeps repeating
	ArrowArray after_end;
	REQUIRE(Next(*wrapper, after_end) == 0);
	REQUIRE(!after_end.release);
	wrapper->stream.release(&wrapper->stream);

	// The connection is free again
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("A statement that completes on return is read from its retained result", "[arrow][result_arrow_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (i BIGINT)"));

	SECTION("INSERT ... RETURNING") {
		auto wrapper = Wrap(con.Submit("INSERT INTO t SELECT i FROM range(10) t(i) RETURNING i"), 4);
		REQUIRE(wrapper->result);
		REQUIRE(!wrapper->stream_result);
		vector<idx_t> batch_sizes;
		REQUIRE(Drain(*wrapper, batch_sizes) == Range(10));
		REQUIRE(batch_sizes == vector<idx_t> {4, 4, 2});
		wrapper->stream.release(&wrapper->stream);
		auto count = con.Query("SELECT count(*) FROM t");
		REQUIRE(CHECK_COLUMN(count, 0, {10}));
	}
	SECTION("CALL") {
		auto wrapper = Wrap(con.Submit("CALL range(3)"), 1000);
		REQUIRE(wrapper->result);
		vector<idx_t> batch_sizes;
		REQUIRE(Drain(*wrapper, batch_sizes) == Range(3));
		wrapper->stream.release(&wrapper->stream);
	}
	SECTION("a handle whose retention is already retained") {
		auto handle = con.Submit("SELECT i FROM range(10) t(i)");
		handle->Materialize();
		auto wrapper = Wrap(std::move(handle), 1000);
		REQUIRE(wrapper->result);
		vector<idx_t> batch_sizes;
		REQUIRE(Drain(*wrapper, batch_sizes) == Range(10));
		wrapper->stream.release(&wrapper->stream);
	}
	SECTION("a completed result") {
		auto wrapper = Wrap(con.Query("SELECT i FROM range(10) t(i)"), 1000);
		REQUIRE(wrapper->result);
		vector<idx_t> batch_sizes;
		REQUIRE(Drain(*wrapper, batch_sizes) == Range(10));
		wrapper->stream.release(&wrapper->stream);
	}
	SECTION("a detached result over a collection") {
		auto source = con.Query("SELECT i FROM range(10) t(i)");
		auto detached =
		    make_uniq<QueryResult>(StatementType::SELECT_STATEMENT, source->GetStatementProperties(),
		                           source->GetNames(), source->TakeCollection(), con.context->GetClientProperties());
		REQUIRE(!detached->HasBufferedData());
		auto wrapper = Wrap(std::move(detached), 4);
		REQUIRE(wrapper->result);
		REQUIRE(!wrapper->stream_result);
		vector<idx_t> batch_sizes;
		REQUIRE(Drain(*wrapper, batch_sizes) == Range(10));
		REQUIRE(batch_sizes == vector<idx_t> {4, 4, 2});
		wrapper->stream.release(&wrapper->stream);
	}
}

TEST_CASE("Extension types are resolved in both modes", "[arrow][result_arrow_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	const string query = "SELECT i, ('POINT(' || i || ' ' || i || ')')::GEOMETRY AS g FROM range(5000) t(i)";

	SECTION("streamed") {
		auto wrapper = Wrap(con.Submit(query), 1000);
		REQUIRE(wrapper->stream_result);
		REQUIRE(wrapper->extension_types.count(1) == 1);
		vector<idx_t> batch_sizes;
		REQUIRE(Drain(*wrapper, batch_sizes).size() == 5000);
		wrapper->stream.release(&wrapper->stream);
	}
	SECTION("retained") {
		auto wrapper = Wrap(con.Query(query), 1000);
		REQUIRE(wrapper->result);
		REQUIRE(wrapper->extension_types.count(1) == 1);
		vector<idx_t> batch_sizes;
		REQUIRE(Drain(*wrapper, batch_sizes).size() == 5000);
		wrapper->stream.release(&wrapper->stream);
	}
}

TEST_CASE("Errors are reported through the Arrow stream", "[arrow][result_arrow_stream]") {
	DuckDB db(nullptr);
	Connection con(db);

	SECTION("a handle that carries an error") {
		auto wrapper = Wrap(con.Submit("SELECT * FROM no_such_table"), 1000);
		REQUIRE(wrapper->result);
		ArrowSchema schema;
		REQUIRE(wrapper->stream.get_schema(&wrapper->stream, &schema) == -1);
		REQUIRE(!schema.release);
		REQUIRE(StringUtil::Contains(LastError(*wrapper), "no_such_table"));
		ArrowArray array;
		REQUIRE(Next(*wrapper, array) == -1);
		REQUIRE(!array.release);
		wrapper->stream.release(&wrapper->stream);
	}
	SECTION("an execution error while streaming") {
		auto wrapper = Wrap(con.Submit("SELECT CASE WHEN i = 150000 THEN error('boom')::BIGINT ELSE i END AS i "
		                               "FROM range(200000) t(i)"),
		                    1000);
		REQUIRE(wrapper->stream_result);
		int rc = 0;
		idx_t rows = 0;
		while (rc == 0) {
			ArrowArray array;
			rc = Next(*wrapper, array);
			if (rc == 0) {
				REQUIRE(array.release);
				rows += idx_t(array.length);
				array.release(&array);
			}
		}
		REQUIRE(rc == -1);
		REQUIRE(rows < 200000);
		REQUIRE(StringUtil::Contains(LastError(*wrapper), "boom"));
		// The error is sticky
		ArrowArray again;
		REQUIRE(Next(*wrapper, again) == -1);
		REQUIRE(!again.release);
		wrapper->stream.release(&wrapper->stream);
	}
	SECTION("a batch size of zero") {
		REQUIRE_THROWS(Wrap(con.Submit("SELECT 42"), 0));
	}
	SECTION("a missing result") {
		REQUIRE_THROWS_AS(Wrap(nullptr, 1000), InvalidInputException);
	}
}

TEST_CASE("Releasing a streamed result before the end closes the query", "[arrow][result_arrow_stream]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto wrapper = Wrap(con.Submit("SELECT i FROM range(200000) t(i)"), 1000);
	ArrowArray first;
	REQUIRE(Next(*wrapper, first) == 0);
	REQUIRE(first.length == 1000);
	first.release(&first);
	wrapper->stream.release(&wrapper->stream);

	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("A streamed result outlives the connection that submitted it", "[arrow][result_arrow_stream]") {
	DuckDB db(nullptr);
	auto con = make_uniq<Connection>(db);

	auto wrapper = Wrap(con->Submit("SELECT i FROM range(3000) t(i)"), 1024);
	ArrowArray first;
	REQUIRE(Next(*wrapper, first) == 0);
	REQUIRE(first.length == 1024);
	first.release(&first);

	// The stream keeps the query, and with it the context, alive
	con.reset();

	vector<idx_t> batch_sizes;
	auto rest = Drain(*wrapper, batch_sizes);
	REQUIRE(rest.size() == 3000 - 1024);
	REQUIRE(rest.front() == 1024);
	REQUIRE(rest.back() == 2999);
	wrapper->stream.release(&wrapper->stream);
}

TEST_CASE("A statement on the connection ends the streamed result, which the stream reports",
          "[arrow][result_arrow_stream]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto wrapper = Wrap(con.Submit("SELECT i FROM range(200000) t(i)"), 1000);
	ArrowArray first;
	REQUIRE(Next(*wrapper, first) == 0);
	REQUIRE(first.length == 1000);
	first.release(&first);

	REQUIRE_NO_FAIL(con.Query("SELECT 42"));

	ArrowArray next;
	REQUIRE(Next(*wrapper, next) != 0);
	REQUIRE(StringUtil::Contains(LastError(*wrapper), "cancelled"));
	wrapper->stream.release(&wrapper->stream);
}
