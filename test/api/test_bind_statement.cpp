#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;

static StatementSignature BindOne(Connection &con, const string &sql) {
	Parser parser(con.context->GetParserOptions());
	parser.ParseQuery(sql);
	REQUIRE(parser.statements.size() == 1);
	return con.context->BindStatement(std::move(parser.statements[0]));
}

TEST_CASE("BindStatement reports the result and parameter schema", "[api][bind_statement]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(a INTEGER, b VARCHAR)"));

	// Result columns, plus $1's type inferred from the comparison against a.
	auto sig = BindOne(con, "SELECT a, b FROM t WHERE a = $1");
	REQUIRE(sig.names.size() == 2);
	REQUIRE(sig.names[0] == "a");
	REQUIRE(sig.types[0] == LogicalType::INTEGER);
	REQUIRE(sig.names[1] == "b");
	REQUIRE(sig.types[1] == LogicalType::VARCHAR);
	REQUIRE(sig.parameters.size() == 1);
	REQUIRE(sig.parameters[0].identifier == "1");
	REQUIRE(sig.parameters[0].type == LogicalType::INTEGER);

	// A non-SELECT still reports a column: the changed-rows count.
	auto insert_sig = BindOne(con, "INSERT INTO t VALUES (1, 'x')");
	REQUIRE(insert_sig.names.size() == 1);
	REQUIRE(insert_sig.types[0] == LogicalType::BIGINT);
	REQUIRE(insert_sig.parameters.empty());

	// A bind error throws.
	REQUIRE_THROWS(BindOne(con, "SELECT * FROM no_such_table"));
}

TEST_CASE("BindStatement does not disturb a live streaming result", "[api][bind_statement]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Open a stream and consume one chunk, leaving it live mid-flight.
	auto stream = con.SendQuery("SELECT i FROM range(5000) t(i)");
	REQUIRE(stream->type == QueryResultType::STREAM_RESULT);
	auto first = stream->Fetch();
	REQUIRE(first);
	idx_t seen = first->size();

	// Binding another statement must not cancel the stream (Prepare would, via InitialCleanup).
	auto sig = BindOne(con, "SELECT 1");
	REQUIRE(sig.types.size() == 1);

	// Every remaining row still arrives.
	while (auto chunk = stream->Fetch()) {
		if (chunk->size() == 0) {
			break;
		}
		seen += chunk->size();
	}
	REQUIRE(seen == 5000);
}
