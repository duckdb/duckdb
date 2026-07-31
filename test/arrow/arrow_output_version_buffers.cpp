#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_type_extension.hpp"
#include "duckdb/common/unordered_map.hpp"

using namespace duckdb;

namespace {

// At arrow_output_version >= 1.4 some string/blob-backed types are appended with
// the 4-buffer view layout while their schema still declares the non-view 3-buffer
// format, so an Arrow C Data importer (e.g. pyarrow) rejects them. These tests
// export schema+array and assert the two stay consistent.

// Fixed buffer count implied by a leaf format string, or -1 if not fixed.
int64_t ExpectedFixedBufferCount(const string &format) {
	if (format == "z" || format == "Z" || format == "u" || format == "U") {
		return 3;
	}
	return -1;
}

// Recursively assert each field's buffer count matches its declared format.
void ValidateField(const ArrowSchema &schema, const ArrowArray &array, const string &path) {
	REQUIRE(schema.format != nullptr);
	const string format = schema.format;

	const int64_t expected = ExpectedFixedBufferCount(format);
	if (expected >= 0) {
		INFO("Field '" << path << "': Expected " << expected << " buffers for imported type '" << format
		               << "', ArrowArray struct has " << array.n_buffers);
		REQUIRE(array.n_buffers == expected);
	} else if (format == "vz" || format == "vu") {
		INFO("Field '" << path << "': view type '" << format << "' must use the variadic-buffer layout");
		REQUIRE(array.n_buffers >= 3);
	}

	if (schema.dictionary) {
		REQUIRE(array.dictionary != nullptr);
		ValidateField(*schema.dictionary, *array.dictionary, path + ".dict");
	}
	REQUIRE(schema.n_children == array.n_children);
	for (int64_t i = 0; i < schema.n_children; i++) {
		ValidateField(*schema.children[i], *array.children[i], path + ".child[" + to_string(i) + "]");
	}
}

// Export `query` to a schema + one combined array using the connection's current
// client properties, then validate every column.
void ExportAndValidateBuffers(Connection &con, const string &query) {
	auto props = con.context->GetClientProperties();

	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	auto types = result->types;
	auto names = result->names;

	ArrowSchema schema;
	ArrowConverter::ToArrowSchema(&schema, types, names, props);

	unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> extension_type_cast;
	ArrowAppender appender(types, STANDARD_VECTOR_SIZE, props, extension_type_cast);
	idx_t count = 0;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		count += chunk->size();
		appender.Append(*chunk, 0, chunk->size(), chunk->size());
	}
	REQUIRE(count > 0);
	ArrowArray array = appender.Finalize();

	REQUIRE(schema.n_children == array.n_children);
	for (int64_t i = 0; i < schema.n_children; i++) {
		ValidateField(*schema.children[i], *array.children[i], "col[" + to_string(i) + "]");
	}

	if (array.release) {
		array.release(&array);
	}
	if (schema.release) {
		schema.release(&schema);
	}
}

// Export `query` (under `setup`) to Arrow, re-import it via arrow_scan, and assert
// the data survives. Buffer-count consistency alone does not catch a schema that
// declares a view/large layout over data written in a different layout: such an
// array passes the count check but a real consumer reads garbage. The roundtrip
// catches that.
void ExpectArrowRoundtrip(const vector<string> &setup, const string &query) {
	DuckDB db;
	Connection con(db);
	for (auto &s : setup) {
		REQUIRE_NO_FAIL(con.Query(s));
	}
	INFO("roundtrip: " << query);
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, query, false));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, query, true));
}

} // namespace

//===--------------------------------------------------------------------===//
// Export schema/buffer-count consistency
//===--------------------------------------------------------------------===//

// VARINT/BIGNUM: arrow.opaque over binary, views at aov >= 1.4.
TEST_CASE("VARINT arrow_output_version buffer consistency", "[arrow]") {
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET arrow_output_version='1.4'"));
	ExportAndValidateBuffers(con, "SELECT (2**100)::VARINT AS v");
}

// ENUM dictionary values are a VARCHAR child: must match the layout they are written in.
TEST_CASE("ENUM string_view arrow_output_version buffer consistency", "[arrow]") {
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET produce_arrow_string_view=true"));
	REQUIRE_NO_FAIL(con.Query("SET arrow_output_version='1.5'"));
	ExportAndValidateBuffers(con, "SELECT 'happy'::ENUM('happy', 'sad') AS v");
}

// GEOMETRY (built-in, WKB/binary storage): views at aov >= 1.4.
TEST_CASE("GEOMETRY arrow_output_version buffer consistency", "[arrow]") {
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET arrow_output_version='1.5'"));
	ExportAndValidateBuffers(con, "SELECT 'POINT(1 2)'::GEOMETRY AS g");
}

// BIT (lossless) shares the extension-schema path with GEOMETRY/VARINT.
TEST_CASE("BIT lossless arrow_output_version buffer consistency", "[arrow]") {
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET arrow_lossless_conversion=true"));
	REQUIRE_NO_FAIL(con.Query("SET arrow_output_version='1.4'"));
	ExportAndValidateBuffers(con, "SELECT '101'::BIT AS v");
}

//===--------------------------------------------------------------------===//
// Data roundtrip: export at arrow_output_version >= 1.4, re-import, compare.
// Buffer-count consistency is necessary but not sufficient — these assert the
// declared layout actually matches the bytes written.
//===--------------------------------------------------------------------===//

TEST_CASE("VARINT arrow_output_version roundtrip", "[arrow]") {
	ExpectArrowRoundtrip({"SET arrow_output_version='1.4'"}, "SELECT (2**100 + i)::VARINT AS v FROM range(5) t(i)");
}

TEST_CASE("BIT lossless arrow_output_version roundtrip", "[arrow]") {
	ExpectArrowRoundtrip({"SET arrow_lossless_conversion=true", "SET arrow_output_version='1.4'"},
	                     "SELECT x::BIT AS v FROM (VALUES ('1010'), ('111'), ('0'), ('110011')) t(x)");
}

TEST_CASE("GEOMETRY arrow_output_version roundtrip", "[arrow]") {
	ExpectArrowRoundtrip({"SET arrow_output_version='1.5'"},
	                     "SELECT ('POINT(' || i || ' ' || (i + 1) || ')')::GEOMETRY AS g FROM range(5) t(i)");
}

TEST_CASE("ENUM string_view arrow_output_version roundtrip", "[arrow]") {
	ExpectArrowRoundtrip({"SET produce_arrow_string_view=true", "SET arrow_output_version='1.5'"},
	                     "SELECT x::ENUM('happy', 'sad', 'angry') AS v "
	                     "FROM (VALUES ('happy'), ('sad'), ('angry'), ('happy')) t(x)");
}

// Same enum, but exercising the large-offset path instead of string view.
TEST_CASE("ENUM large_buffer arrow_output_version roundtrip", "[arrow]") {
	ExpectArrowRoundtrip({"SET arrow_large_buffer_size=true"},
	                     "SELECT x::ENUM('happy', 'sad', 'angry') AS v "
	                     "FROM (VALUES ('happy'), ('sad'), ('angry'), ('happy')) t(x)");
}

// UUID has no string-view appender (it is cast to a regular string), so even with
// produce_arrow_string_view the schema must declare a regular string "u" — never a
// view "vu" whose 4-buffer layout the appender never produces. (A non-lossless UUID
// exports as a string, so this is checked at the schema/buffer level rather than via
// a type-preserving roundtrip.)
TEST_CASE("UUID string_view arrow_output_version export is regular string", "[arrow]") {
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET produce_arrow_string_view=true"));
	REQUIRE_NO_FAIL(con.Query("SET arrow_output_version='1.4'"));
	const string query = "SELECT '550e8400-e29b-41d4-a716-446655440000'::UUID AS v";

	auto props = con.context->GetClientProperties();
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	ArrowSchema schema;
	ArrowConverter::ToArrowSchema(&schema, result->types, result->names, props);
	REQUIRE(string(schema.children[0]->format) == "u");
	if (schema.release) {
		schema.release(&schema);
	}

	// The appended array must agree with that declaration (3 buffers).
	ExportAndValidateBuffers(con, query);
}

// Control: plain BLOB already exports/imports the view layout correctly.
TEST_CASE("BLOB arrow_output_version roundtrip", "[arrow]") {
	ExpectArrowRoundtrip({"SET arrow_output_version='1.4'"}, "SELECT ('blob_' || i)::BLOB AS v FROM range(5) t(i)");
}
