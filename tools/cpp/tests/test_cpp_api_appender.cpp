#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"

#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Stable C++ API tests: Appender. A ColumnDataCollection plus a statement that
// reads it, wired together with a connection-scoped replacement scan.
// ---------------------------------------------------------------------------

namespace {

using namespace duckdb::cxx;

// Collect a single BIGINT column, asserting every row valid.
std::vector<int64_t> CollectAppended(QueryResult result) {
	std::vector<int64_t> out;
	while (auto chunk = result.FetchChunk()) {
		auto view = chunk.GetVector(0).GetView();
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			REQUIRE(view.IsValid(i));
			out.push_back(view.Data<int64_t>()[view.SelAt(i)]);
		}
	}
	return out;
}

// Buffers one chunk of BIGINTs through the appender.
void AppendValues(Appender &appender, const std::vector<int64_t> &values) {
	DataChunk chunk(appender.ColumnTypes());
	auto vec = chunk.GetVector(0);
	auto *data = vec.GetDataMutable<int64_t>();
	for (size_t i = 0; i < values.size(); i++) {
		data[i] = values[i];
	}
	vec.SetSize(values.size());
	appender.AppendChunk(chunk);
}

} // namespace

TEST_CASE("Stable C++API: appender buffers and flushes into a table", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t (v BIGINT)").Drain();

	Appender appender(conn, "t");
	REQUIRE(appender.ColumnTypes().size() == 1);

	// Buffering touches nothing until the flush.
	AppendValues(appender, {1, 2});
	REQUIRE(CollectAppended(conn.Execute("SELECT count(*)::BIGINT FROM t")) == std::vector<int64_t> {0});

	appender.Flush();
	REQUIRE(CollectAppended(conn.Execute("SELECT v FROM t ORDER BY v")) == std::vector<int64_t> {1, 2});

	// The buffer is empty and reusable afterwards, and a flush of nothing is a no-op.
	appender.Flush();
	REQUIRE(CollectAppended(conn.Execute("SELECT count(*)::BIGINT FROM t")) == std::vector<int64_t> {2});

	// Several batches in a row.
	for (int64_t round = 0; round < 3; round++) {
		AppendValues(appender, {10 + round, 20 + round});
		appender.Flush();
	}
	REQUIRE(CollectAppended(conn.Execute("SELECT count(*)::BIGINT FROM t")) == std::vector<int64_t> {8});
	REQUIRE(CollectAppended(conn.Execute("SELECT sum(v)::BIGINT FROM t")) == std::vector<int64_t> {3 + 33 + 63});
}

TEST_CASE("Stable C++API: appender buffers across several chunks before flushing", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t (v BIGINT)").Drain();

	Appender appender(conn, "t");
	for (int64_t i = 0; i < 5; i++) {
		AppendValues(appender, {i, i + 100});
	}
	appender.Flush();
	REQUIRE(CollectAppended(conn.Execute("SELECT count(*)::BIGINT FROM t")) == std::vector<int64_t> {10});
	REQUIRE(CollectAppended(conn.Execute("SELECT v FROM t ORDER BY v LIMIT 2")) == std::vector<int64_t> {0, 1});
}

TEST_CASE("Stable C++API: appender Clear drops the buffer without writing", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t (v BIGINT)").Drain();

	Appender appender(conn, "t");
	AppendValues(appender, {1, 2});
	appender.Clear();
	appender.Flush();
	REQUIRE(CollectAppended(conn.Execute("SELECT count(*)::BIGINT FROM t")) == std::vector<int64_t> {0});

	// Usable again after a clear.
	AppendValues(appender, {7, 8});
	appender.Flush();
	REQUIRE(CollectAppended(conn.Execute("SELECT v FROM t ORDER BY v")) == std::vector<int64_t> {7, 8});
}

TEST_CASE("Stable C++API: appender destruction drops unflushed rows", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t (v BIGINT)").Drain();

	// Named explicitly, so the test can ask for the buffer by name after the appender is gone.
	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("BIGINT"));
	{
		Appender appender(conn, "INSERT INTO t SELECT * FROM gone_rows", std::move(types), "gone_rows");
		AppendValues(appender, {1, 2});
		// While it lives, the buffer is visible and holds the rows.
		REQUIRE(CollectAppended(conn.Execute("SELECT count(*)::BIGINT FROM gone_rows")) == std::vector<int64_t> {2});
	}
	REQUIRE(CollectAppended(conn.Execute("SELECT count(*)::BIGINT FROM t")) == std::vector<int64_t> {0});

	// The scan the appender left behind outlives it, but declines now that the buffer is gone -- rather than
	// reading through a dangling pointer.
	REQUIRE_THROWS_AS(conn.Execute("SELECT * FROM gone_rows").Drain(), Exception);
}

TEST_CASE("Stable C++API: appender with an explicit query", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t (v BIGINT, tag VARCHAR DEFAULT 'seen')").Drain();

	// A subset of columns, which is what the table constructor cannot express.
	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("BIGINT"));
	Appender appender(conn, "INSERT INTO t (v) SELECT amount FROM my_rows", std::move(types), "my_rows", {"amount"});

	AppendValues(appender, {5, 6});
	appender.Flush();
	REQUIRE(CollectAppended(conn.Execute("SELECT v FROM t ORDER BY v")) == std::vector<int64_t> {5, 6});
	REQUIRE(CollectAppended(conn.Execute("SELECT count(*)::BIGINT FROM t WHERE tag = 'seen'")) ==
	        std::vector<int64_t> {2});

	// The buffer is a table like any other, so it can drive a read too.
	std::vector<LogicalType> read_types;
	read_types.push_back(conn.ParseType("BIGINT"));
	Appender reader(conn, "SELECT amount FROM other_rows", std::move(read_types), "other_rows", {"amount"});
	AppendValues(reader, {9, 9});
	// Flushing a SELECT just drains it; the rows are consumed either way.
	reader.Flush();
}

TEST_CASE("Stable C++API: appender is scoped to its connection", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto other = db.Connect();
	conn.Execute("CREATE TABLE t (v BIGINT)").Drain();

	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("BIGINT"));
	Appender appender(conn, "INSERT INTO t SELECT * FROM scoped_rows", std::move(types), "scoped_rows");

	// The buffer's name resolves on the appender's connection only.
	REQUIRE(CollectAppended(conn.Execute("SELECT count(*)::BIGINT FROM scoped_rows")) == std::vector<int64_t> {0});
	REQUIRE_THROWS_AS(other.Execute("SELECT * FROM scoped_rows").Drain(), Exception);
}

TEST_CASE("Stable C++API: appender refuses a mismatching chunk", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t (v BIGINT)").Drain();

	Appender appender(conn, "t");

	std::vector<LogicalType> wrong;
	wrong.push_back(conn.ParseType("VARCHAR"));
	DataChunk chunk(wrong);
	chunk.GetVector(0).SetSize(0);
	REQUIRE_THROWS_MATCHES(appender.AppendChunk(chunk), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// A refused chunk leaves the appender usable.
	AppendValues(appender, {3});
	appender.Flush();
	REQUIRE(CollectAppended(conn.Execute("SELECT v FROM t")) == std::vector<int64_t> {3});
}

TEST_CASE("Stable C++API: appender construction refusals", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// No columns.
	REQUIRE_THROWS_MATCHES(Appender(conn, "SELECT 1", {}, "buf"), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	// More than one statement.
	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("BIGINT"));
	REQUIRE_THROWS_MATCHES(Appender(conn, "SELECT 1; SELECT 2", std::move(types), "buf"), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	// A table that does not exist.
	REQUIRE_THROWS_AS(Appender(conn, "no_such_table"), Exception);
}
