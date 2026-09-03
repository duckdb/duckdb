#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"

#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Stable C++ API tests: Arrow. The chunk converters need a Context, which only a
// callback has, so what is reachable from a test is the stream export plus the
// schema exporter driven from inside a scalar function.
// ---------------------------------------------------------------------------

namespace {

using namespace duckdb::cxx;

struct ArrowStreamStats {
	int64_t rows = 0;
	idx_t arrays = 0;
	int64_t first_array_rows = 0;
};

// Drives the stream to exhaustion, releasing each array.
ArrowStreamStats DrainCppArrowStream(const ArrowStream &stream) {
	ArrowStreamStats stats;
	while (true) {
		ArrowArray array {};
		if (!stream.Next(array)) {
			break;
		}
		if (stats.arrays == 0) {
			stats.first_array_rows = array.length;
		}
		stats.rows += array.length;
		stats.arrays++;
		array.release(&array);
	}
	return stats;
}

} // namespace

TEST_CASE("Stable C++API: ArrowStream export", "[cpp_api][arrow]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	SECTION("every row and a stable schema") {
		auto stream = conn.Execute("SELECT i, 'r' || i AS s FROM range(1000) t(i)").ToArrowStream();
		REQUIRE(static_cast<bool>(stream));

		ArrowSchema schema {};
		stream.GetSchema(schema);
		REQUIRE(schema.n_children == 2);
		REQUIRE(std::string(schema.children[0]->name) == "i");
		REQUIRE(std::string(schema.children[1]->name) == "s");
		schema.release(&schema);

		auto stats = DrainCppArrowStream(stream);
		REQUIRE(stats.rows == 1000);
		REQUIRE(stats.arrays >= 1);

		// The schema stays readable once the rows are gone: it was cached under the
		// producing transaction rather than built on demand.
		ArrowSchema after {};
		stream.GetSchema(after);
		REQUIRE(after.n_children == 2);
		after.release(&after);
	}

	SECTION("batch_size is honored") {
		auto stream = conn.Execute("SELECT i FROM range(7) t(i)").ToArrowStream(1);
		auto stats = DrainCppArrowStream(stream);
		REQUIRE(stats.rows == 7);
		REQUIRE(stats.arrays == 7);
		REQUIRE(stats.first_array_rows == 1);
	}

	SECTION("the stream holds the connection until destroyed") {
		{
			auto stream = conn.Execute("SELECT i FROM range(100000) t(i)").ToArrowStream();
			REQUIRE_THROWS_MATCHES(conn.Execute("SELECT 1"), Exception, HasErrorCode(DUCKDB_V2_ERROR_RESOURCE_IN_USE));
		}
		// Destroying it releases the stream, which frees the connection even undrained.
		REQUIRE(conn.Execute("SELECT 1").Drain() == 0);
	}

	SECTION("Detach hands the stream over") {
		auto stream = conn.Execute("SELECT i FROM range(10) t(i)").ToArrowStream();
		auto *raw = stream.Detach();
		REQUIRE(raw != nullptr);
		REQUIRE_FALSE(static_cast<bool>(stream));
		// The wrapper is empty, so it releases nothing and the caller owns the stream.
		ArrowSchema schema {};
		REQUIRE(raw->get_schema(raw, &schema) == 0);
		schema.release(&schema);
		raw->release(raw);
		delete raw;
	}

	SECTION("an empty stream refuses reads") {
		auto stream = conn.Execute("SELECT 1").ToArrowStream();
		auto *raw = stream.Detach();
		ArrowSchema schema {};
		REQUIRE_THROWS_MATCHES(stream.GetSchema(schema), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
		ArrowArray array {};
		REQUIRE_THROWS_MATCHES(stream.Next(array), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
		raw->release(raw);
		delete raw;
	}

	SECTION("an execution error surfaces from Next") {
		auto stream = conn.Execute("SELECT CASE WHEN i = 500 THEN error('boom') ELSE i::VARCHAR END "
		                           "FROM range(1000) t(i)")
		                  .ToArrowStream();
		REQUIRE_THROWS_AS(DrainCppArrowStream(stream), Exception);
	}
}

TEST_CASE("Stable C++API: ArrowConverter round-trips a schema", "[cpp_api][arrow]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Both `Schema::ToArrowSchema` and `ArrowConverter` need a `Context`, which only a callback
	// has, so the schema is bound outside and carried in as user data. A callback must not use
	// Catch assertions, so what it observes is latched and checked after the query.
	static idx_t observed_count = 0;
	static std::string observed_names;
	static std::string observed_first_type;
	observed_count = 0;
	observed_names.clear();
	observed_first_type.clear();

	struct SchemaHolder {
		Schema schema;
	};

	auto statements = conn.ParseSQL("SELECT 1::BIGINT AS a, 'x' AS b");
	auto statement = statements.Next();

	auto function = ScalarFunction::Create(conn);
	function.SetName("cpp_arrow_probe");
	function.SetUserData<SchemaHolder>(SchemaHolder {conn.Bind(statement).output});
	function.SetExecCallback([](ScalarFunction::ExecInput &input) {
		auto context = input.GetContext();
		auto &holder = input.GetUserData<SchemaHolder>();

		// Export the DuckDB schema to Arrow, then resolve it straight back: the shape a
		// converter reports should be the types we started from.
		ArrowSchema schema {};
		holder.schema.ToArrowSchema(context, schema);
		ArrowConverter converter(context, schema);
		schema.release(&schema);

		auto resolved = converter.GetSchema();
		observed_count = resolved.GetFieldCount();
		for (idx_t i = 0; i < observed_count; i++) {
			observed_names += std::string(resolved.GetFieldName(i));
		}
		if (observed_count > 0) {
			observed_first_type = resolved.GetFieldType(0).ToText();
		}

		auto result = input.GetResult();
		auto *out = result.GetDataMutable<int32_t>();
		for (idx_t i = 0; i < input.GetRowCount(); i++) {
			out[i] = 1;
		}
	});
	const auto integer = conn.ParseType("INTEGER");
	function.GetSignature().AddParameter("x", integer).SetReturnType(integer);
	function.Register();

	conn.Execute("SELECT cpp_arrow_probe(1)").Drain();
	REQUIRE(observed_count == 2);
	REQUIRE(observed_names == "ab");
	REQUIRE(observed_first_type == "BIGINT");
}
