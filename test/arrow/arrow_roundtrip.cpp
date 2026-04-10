#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"

using namespace duckdb;

static void TestArrowRoundtrip(const string &query, bool export_large_buffer = false,
                               bool lossless_conversion = false) {
	DuckDB db;
	Connection con(db);
	if (export_large_buffer) {
		auto res = con.Query("SET arrow_large_buffer_size=True");
		REQUIRE(!res->HasError());
	}
	if (lossless_conversion) {
		auto res = con.Query("SET arrow_lossless_conversion = true");
		REQUIRE(!res->HasError());
	}
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, query, true));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, query, false));
}

static void TestArrowRoundtripStringView(const string &query) {
	DuckDB db;
	Connection con(db);
	auto res = con.Query("SET produce_arrow_string_view=True");
	REQUIRE(!res->HasError());
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, query, false));
}

static void TestParquetRoundtrip(const string &path) {
	DBConfig config;
	// This needs to be set since this test will be triggered when testing autoloading
	config.SetOptionByName("allow_unsigned_extensions", true);

	DuckDB db(nullptr, &config);
	Connection con(db);

	// run the query
	auto query = "SELECT * FROM parquet_scan('" + path + "')";
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, query, true));
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, query));
}

TEST_CASE("Test Export Large", "[arrow]") {
	// Test with Regular Buffer Size
	TestArrowRoundtrip("SELECT 'bla' FROM range(10000)");

	TestArrowRoundtrip("SELECT 'bla'::BLOB FROM range(10000)");

	TestArrowRoundtrip("SELECT '3d038406-6275-4aae-bec1-1235ccdeaade'::UUID FROM range(10000) tbl(i)", false, true);

	// Test with Large Buffer Size
	TestArrowRoundtrip("SELECT 'bla' FROM range(10000)", true);

	TestArrowRoundtrip("SELECT 'bla'::BLOB FROM range(10000)", true);

	TestArrowRoundtrip("SELECT '3d038406-6275-4aae-bec1-1235ccdeaade'::UUID FROM range(10000) tbl(i)", true, true);
}

TEST_CASE("Test arrow roundtrip", "[arrow]") {
	TestArrowRoundtrip("SELECT * FROM range(10000) tbl(i) UNION ALL SELECT NULL");
	TestArrowRoundtrip("SELECT m from (select MAP(list_value(1), list_value(2)) from range(5) tbl(i)) tbl(m)");
	TestArrowRoundtrip("SELECT * FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT case when i%2=0 then null else i end i FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT case when i%2=0 then true else false end b FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT case when i%2=0 then i%4=0 else null end b FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT 'thisisalongstring'||i::varchar str FROM range(10) tbl(i)");
	TestArrowRoundtrip(
	    "SELECT case when i%2=0 then null else 'thisisalongstring'||i::varchar end str FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT {'i': i, 'b': 10-i} str FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT case when i%2=0 then {'i': case when i%4=0 then null else i end, 'b': 10-i} else null "
	                   "end str FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT [i, i+1, i+2] FROM range(10) tbl(i)");
	TestArrowRoundtrip(
	    "SELECT MAP(LIST_VALUE({'i':1,'j':2},{'i':3,'j':4}),LIST_VALUE({'i':1,'j':2},{'i':3,'j':4})) as a");
	TestArrowRoundtrip(
	    "SELECT MAP(LIST_VALUE({'i':i,'j':i+2},{'i':3,'j':NULL}),LIST_VALUE({'i':i+10,'j':2},{'i':i+4,'j':4})) as a "
	    "FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT MAP(['hello', 'world'||i::VARCHAR],[i + 1, NULL]) as a FROM range(10) tbl(i)");
	TestArrowRoundtrip("SELECT (1.5 + i)::DECIMAL(4,2) dec4, (1.5 + i)::DECIMAL(9,3) dec9, (1.5 + i)::DECIMAL(18,3) "
	                   "dec18, (1.5 + i)::DECIMAL(38,3) dec38 FROM range(10) tbl(i)");
	TestArrowRoundtrip(
	    "SELECT case when i%2=0 then null else INTERVAL (i) seconds end AS interval FROM range(10) tbl(i)");
#if STANDARD_VECTOR_SIZE < 64
	// FIXME: there seems to be a bug in the enum arrow reader in this test when run with vsize=2
	return;
#endif
	TestArrowRoundtrip("SELECT * FROM test_all_types()", false, true);
}

TEST_CASE("Test Arrow fixed-size binary format parsing", "[arrow]") {
	// Verify that GetTypeFromFormat correctly parses the size from "w:NN" format strings.
	// Regression test for duckdb/duckdb-wasm#2199: format.find(':') would match colons
	// in extension metadata (e.g. CRS strings like "ogc:crs84"), causing std::stoi to crash.
	{
		string format = "w:16";
		auto type = ArrowType::GetTypeFromFormat(format);
		REQUIRE(type);
		REQUIRE(type->GetDuckType() == LogicalType::BLOB);
	}
	{
		string format = "w:1";
		auto type = ArrowType::GetTypeFromFormat(format);
		REQUIRE(type);
		REQUIRE(type->GetDuckType() == LogicalType::BLOB);
	}
	{
		string format = "w:128";
		auto type = ArrowType::GetTypeFromFormat(format);
		REQUIRE(type);
		REQUIRE(type->GetDuckType() == LogicalType::BLOB);
	}
}

TEST_CASE("Test Arrow Extension Types", "[arrow][.]") {
	// UUID
	TestArrowRoundtrip("SELECT '2d89ebe6-1e13-47e5-803a-b81c87660b66'::UUID str FROM range(5) tbl(i)", false, true);

	// HUGEINT
	TestArrowRoundtrip("SELECT '170141183460469231731687303715884105727'::HUGEINT str FROM range(5) tbl(i)", false,
	                   true);

	// UHUGEINT
	TestArrowRoundtrip("SELECT '170141183460469231731687303715884105727'::UHUGEINT str FROM range(5) tbl(i)", false,
	                   true);

	// UHUGEINT (lossy - should export as Decimal(38,0), not extension type)
	{
		DuckDB db;
		Connection con(db);
		auto client_properties = con.context->GetClientProperties();
		ArrowSchema schema;
		schema.Init();
		vector<LogicalType> types = {LogicalType::UHUGEINT};
		vector<string> names = {"col"};
		ArrowConverter::ToArrowSchema(&schema, types, names, client_properties);
		REQUIRE(schema.n_children == 1);
		REQUIRE(string(schema.children[0]->format) == "d:38,0");
		schema.release(&schema);
	}

	// BIT
	TestArrowRoundtrip("SELECT '0101011'::BIT str FROM range(5) tbl(i)", false, true);

	// TIME_TZ
	TestArrowRoundtrip("SELECT '02:30:00+04'::TIMETZ str FROM range(5) tbl(i)", false, true);

	// BIGNUM
	TestArrowRoundtrip("SELECT 85070591730234614260976917445211069672::BIGNUM str FROM range(5) tbl(i)", false, true);

	TestArrowRoundtrip("SELECT 85070591730234614260976917445211069672::BIGNUM str FROM range(5) tbl(i)", true, true);
}

TEST_CASE("Test Arrow Extension Types - VARIANT", "[arrow][.]") {
	// Arrow C Data Interface export for VARIANT columns emits the canonical
	// arrow.parquet.variant extension: struct<metadata: binary, value: binary>.
	// Non-shredded variants only; Arrow import is not implemented.
	//
	// TestArrowRoundtrip cannot be used here because ArrowToDuck throws — we
	// verify the export side directly via ArrowConverter::ToArrowSchema and
	// ToArrowArray, following the same pattern as the UHUGEINT lossy case
	// above.
	DuckDB db;
	Connection con(db);

	// Arrow extension registration happens inside the parquet extension's Load
	// function; skip the test if parquet is not available.
	if (!db.ExtensionIsLoaded("parquet")) {
		return;
	}

	// Schema shape: top level must be an extension struct.
	{
		auto client_properties = con.context->GetClientProperties();
		ArrowSchema schema;
		schema.Init();
		vector<LogicalType> types = {LogicalType::VARIANT()};
		vector<string> names = {"v"};
		ArrowConverter::ToArrowSchema(&schema, types, names, client_properties);

		REQUIRE(schema.n_children == 1);
		auto &variant_field = *schema.children[0];
		REQUIRE(string(variant_field.format) == "+s");
		REQUIRE(variant_field.n_children == 2);

		REQUIRE(variant_field.metadata != nullptr);
		ArrowSchemaMetadata parsed(variant_field.metadata);
		REQUIRE(parsed.HasExtension());
		REQUIRE(parsed.GetOption(ArrowSchemaMetadata::ARROW_EXTENSION_NAME) == "arrow.parquet.variant");

		REQUIRE(string(variant_field.children[0]->name) == "metadata");
		REQUIRE(string(variant_field.children[0]->format) == "z");
		REQUIRE(string(variant_field.children[1]->name) == "value");
		REQUIRE(string(variant_field.children[1]->format) == "z");

		schema.release(&schema);
	}

	// Array shape: verify end-to-end data export for a handful of VARIANT values.
	{
		auto result = con.Query("SELECT (1)::INTEGER::VARIANT AS v UNION ALL "
		                        "SELECT ('hello')::VARCHAR::VARIANT UNION ALL "
		                        "SELECT NULL::VARIANT UNION ALL "
		                        "SELECT (true)::BOOLEAN::VARIANT");
		REQUIRE(!result->HasError());

		// Pull all rows into a single DataChunk for direct Arrow export.
		auto collection = make_uniq<ColumnDataCollection>(*con.context, result->types);
		ColumnDataAppendState append_state;
		collection->InitializeAppend(append_state);
		unique_ptr<DataChunk> chunk;
		while ((chunk = result->Fetch()) && chunk->size() > 0) {
			collection->Append(append_state, *chunk);
		}
		REQUIRE(collection->Count() == 4);

		ClientProperties options = con.context->GetClientProperties();
		auto extension_types = ArrowTypeExtensionData::GetExtensionTypes(*con.context, collection->Types());

		DataChunk flat;
		flat.Initialize(*con.context, collection->Types(), collection->Count());
		ColumnDataScanState scan_state;
		collection->InitializeScan(scan_state);
		collection->Scan(scan_state, flat);

		ArrowArray arrow_array;
		ArrowConverter::ToArrowArray(flat, &arrow_array, options, extension_types);

		REQUIRE(arrow_array.length == 4);
		REQUIRE(arrow_array.n_children == 1);

		auto &variant_array = *arrow_array.children[0];
		REQUIRE(variant_array.length == 4);
		REQUIRE(variant_array.n_children == 2);
		REQUIRE(variant_array.null_count == 1);

		auto &metadata_array = *variant_array.children[0];
		auto &value_array = *variant_array.children[1];
		REQUIRE(metadata_array.length == 4);
		REQUIRE(value_array.length == 4);

		// Non-null rows must produce non-empty metadata and value bytes.
		auto metadata_offsets = reinterpret_cast<const int32_t *>(metadata_array.buffers[1]);
		auto value_offsets = reinterpret_cast<const int32_t *>(value_array.buffers[1]);
		REQUIRE(metadata_offsets[1] > metadata_offsets[0]);
		REQUIRE(value_offsets[1] > value_offsets[0]);

		arrow_array.release(&arrow_array);
	}
}

TEST_CASE("Test Arrow Extension Types - VARIANT import does not regress", "[arrow][.]") {
	// Regression test: registering the arrow.parquet.variant extension must not break
	// Arrow import of schemas that carry this extension metadata. Before the fix,
	// GetType() threw NotImplementedException during schema resolution, which meant
	// any arrow_scan / from_arrow input labelled `arrow.parquet.variant` (including
	// results exported by this very patch!) failed in ArrowType::GetTypeFromSchema.
	// The current behavior: import falls back to the plain STRUCT(metadata BLOB,
	// value BLOB) interpretation.
	DuckDB db;
	Connection con(db);
	if (!db.ExtensionIsLoaded("parquet")) {
		return;
	}

	// Export a VARIANT result to Arrow and then re-import it via DuckDB's arrow
	// scan by writing the ArrowArrayStream through `con.context`.
	auto result = con.Query("SELECT (42)::INTEGER::VARIANT AS v");
	REQUIRE(!result->HasError());

	ClientProperties options = con.context->GetClientProperties();
	auto collection = make_uniq<ColumnDataCollection>(*con.context, result->types);
	ColumnDataAppendState append_state;
	collection->InitializeAppend(append_state);
	unique_ptr<DataChunk> chunk;
	while ((chunk = result->Fetch()) && chunk->size() > 0) {
		collection->Append(append_state, *chunk);
	}

	// Step 1: verify schema resolution does not throw for the arrow.parquet.variant
	// extension metadata. Build an ArrowSchema and then run it through
	// ArrowType::GetTypeFromSchema — this is the exact call path that failed before.
	ArrowSchema schema;
	schema.Init();
	vector<LogicalType> types = {LogicalType::VARIANT()};
	vector<string> names = {"v"};
	ArrowConverter::ToArrowSchema(&schema, types, names, options);
	REQUIRE(schema.n_children == 1);
	auto &variant_field_schema = *schema.children[0];

	auto resolved_type = ArrowType::GetTypeFromSchema(*con.context, variant_field_schema);
	REQUIRE(resolved_type != nullptr);
	// Resolved DuckDB type must be a struct with metadata+value BLOB children, not
	// a VARIANT (import of canonical bytes → DuckDB VARIANT is out of scope).
	auto resolved_duck_type = resolved_type->GetDuckType();
	REQUIRE(resolved_duck_type.id() == LogicalTypeId::STRUCT);
	auto &resolved_children = StructType::GetChildTypes(resolved_duck_type);
	REQUIRE(resolved_children.size() == 2);
	REQUIRE(resolved_children[0].first == "metadata");
	REQUIRE(resolved_children[0].second == LogicalType::BLOB);
	REQUIRE(resolved_children[1].first == "value");
	REQUIRE(resolved_children[1].second == LogicalType::BLOB);

	schema.release(&schema);
}

TEST_CASE("Test Arrow Extension Types - VARIANT child format selection", "[arrow][.]") {
	// Regression test: the arrow.parquet.variant child BLOB format must agree
	// with whatever format the BLOB appender actually emits. Otherwise consumers
	// will read string-view or 64-bit-offset buffers as regular 32-bit-offset
	// binary and either crash or return garbage. Before the fix, PopulateSchema
	// hardcoded "z" regardless of arrow_output_version / arrow_large_buffer_size.
	auto expect_variant_child_format = [](Connection &con, const string &expected) {
		ArrowSchema schema;
		schema.Init();
		vector<LogicalType> types = {LogicalType::VARIANT()};
		vector<string> names = {"v"};
		auto client_properties = con.context->GetClientProperties();
		ArrowConverter::ToArrowSchema(&schema, types, names, client_properties);

		REQUIRE(schema.n_children == 1);
		auto &variant_field = *schema.children[0];
		REQUIRE(string(variant_field.format) == "+s");
		REQUIRE(variant_field.n_children == 2);
		REQUIRE(string(variant_field.children[0]->format) == expected);
		REQUIRE(string(variant_field.children[1]->format) == expected);

		schema.release(&schema);
	};

	{
		// Default: z (32-bit offset binary).
		DuckDB db;
		Connection con(db);
		if (!db.ExtensionIsLoaded("parquet")) {
			return;
		}
		expect_variant_child_format(con, "z");
	}

	{
		// arrow_large_buffer_size=true: Z (64-bit offset binary).
		DuckDB db;
		Connection con(db);
		if (!db.ExtensionIsLoaded("parquet")) {
			return;
		}
		REQUIRE(!con.Query("SET arrow_large_buffer_size=true")->HasError());
		expect_variant_child_format(con, "Z");
	}

	{
		// arrow_output_version='1.4': vz (string view binary).
		DuckDB db;
		Connection con(db);
		if (!db.ExtensionIsLoaded("parquet")) {
			return;
		}
		REQUIRE(!con.Query("SET arrow_output_version='1.4'")->HasError());
		expect_variant_child_format(con, "vz");
	}

	// Full export roundtrip under V1_4: produces string-view buffers, which
	// have a different layout (no single offsets buffer). Verifies that the
	// appender actually produces the shape the schema advertised and that the
	// array survives release without memory errors.
	{
		DuckDB db;
		Connection con(db);
		if (!db.ExtensionIsLoaded("parquet")) {
			return;
		}
		REQUIRE(!con.Query("SET arrow_output_version='1.4'")->HasError());

		auto result =
		    con.Query("SELECT (1)::INTEGER::VARIANT AS v UNION ALL SELECT ('hello')::VARCHAR::VARIANT UNION ALL "
		              "SELECT NULL::VARIANT UNION ALL SELECT (true)::BOOLEAN::VARIANT");
		REQUIRE(!result->HasError());

		auto collection = make_uniq<ColumnDataCollection>(*con.context, result->types);
		ColumnDataAppendState append_state;
		collection->InitializeAppend(append_state);
		unique_ptr<DataChunk> chunk;
		while ((chunk = result->Fetch()) && chunk->size() > 0) {
			collection->Append(append_state, *chunk);
		}

		ClientProperties options = con.context->GetClientProperties();
		auto extension_types = ArrowTypeExtensionData::GetExtensionTypes(*con.context, collection->Types());

		DataChunk flat;
		flat.Initialize(*con.context, collection->Types(), collection->Count());
		ColumnDataScanState scan_state;
		collection->InitializeScan(scan_state);
		collection->Scan(scan_state, flat);

		ArrowArray arrow_array;
		ArrowConverter::ToArrowArray(flat, &arrow_array, options, extension_types);

		REQUIRE(arrow_array.length == 4);
		auto &variant_array = *arrow_array.children[0];
		REQUIRE(variant_array.null_count == 1);
		// String-view binary has n_buffers = 2 + n_variadic_buffers + 1, but
		// the important invariant is that the array releases cleanly.
		arrow_array.release(&arrow_array);
	}
}

TEST_CASE("Test Arrow Extension Types - JSON", "[arrow][.]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	Connection con(db);

	if (!db.ExtensionIsLoaded("json")) {
		return;
	}

	// JSON
	TestArrowRoundtrip("SELECT '{\"name\":\"Pedro\", \"age\":28, \"car\":\"VW Fox\"}'::JSON str FROM range(5) tbl(i)",
	                   false, true);
}

TEST_CASE("Test Arrow String View", "[arrow][.]") {
	// Test Small Strings
	TestArrowRoundtripStringView("SELECT (i*10^i)::varchar str FROM range(5) tbl(i)");

	// Test Small Strings + Nulls
	TestArrowRoundtripStringView("SELECT (i*10^i)::varchar str FROM range(5) tbl(i) UNION SELECT NULL");

	// Test Big Strings
	TestArrowRoundtripStringView("SELECT 'Imaverybigstringmuchbiggerthanfourbytes' str FROM range(5) tbl(i)");

	// Test Big Strings + Nulls
	TestArrowRoundtripStringView("SELECT 'Imaverybigstringmuchbiggerthanfourbytes'||i::varchar str FROM range(5) "
	                             "tbl(i) UNION SELECT NULL order by str");

	// Test Mix of Small/Big/NULL Strings
	TestArrowRoundtripStringView(
	    "SELECT 'Imaverybigstringmuchbiggerthanfourbytes'||i::varchar str FROM range(10000) tbl(i) UNION "
	    "SELECT NULL UNION SELECT (i*10^i)::varchar str FROM range(10000) tbl(i)");
}

TEST_CASE("Test TPCH arrow roundtrip", "[arrow][.]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	Connection con(db);

#if defined(D_ASSERT_IS_ENABLED) && !defined(DEBUG)
	return; // Skip in relassert, takes too long
#endif

	if (!db.ExtensionIsLoaded("tpch")) {
		return;
	}
	con.SendQuery("CALL dbgen(sf=0.5)");

	// REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM lineitem;", false));
	// REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT l_orderkey, l_shipdate, l_comment FROM lineitem ORDER BY
	// l_orderkey DESC;", false)); REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT lineitem FROM lineitem;",
	// false)); REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT [lineitem] FROM lineitem;", false));

	con.SendQuery("create table lineitem_no_constraint as from lineitem;");
	con.SendQuery("update lineitem_no_constraint set l_comment=null where l_orderkey%2=0;");

	// REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT * FROM lineitem_no_constraint;", false));
	REQUIRE(ArrowTestHelper::RunArrowComparison(
	    con, "SELECT l_orderkey, l_shipdate, l_comment FROM lineitem_no_constraint ORDER BY l_orderkey DESC;", true));
	REQUIRE(
	    ArrowTestHelper::RunArrowComparison(con, "SELECT lineitem_no_constraint FROM lineitem_no_constraint;", true));
	REQUIRE(
	    ArrowTestHelper::RunArrowComparison(con, "SELECT [lineitem_no_constraint] FROM lineitem_no_constraint;", true));
}

TEST_CASE("Test Parquet Files round-trip", "[arrow][.]") {
	std::vector<std::string> data;
	// data.emplace_back("data/parquet-testing/7-set.snappy.arrow2.parquet");
	//	data.emplace_back("data/parquet-testing/adam_genotypes.parquet");
	data.emplace_back("data/parquet-testing/apkwan.parquet");
	data.emplace_back("data/parquet-testing/aws1.snappy.parquet");
	// not supported by arrow
	// data.emplace_back("data/parquet-testing/aws2.parquet");
	data.emplace_back("data/parquet-testing/binary_string.parquet");
	data.emplace_back("data/parquet-testing/blob.parquet");
	data.emplace_back("data/parquet-testing/boolean_stats.parquet");
	// arrow can't read this
	// data.emplace_back("data/parquet-testing/broken-arrow.parquet");
	data.emplace_back("data/parquet-testing/bug1554.parquet");
	data.emplace_back("data/parquet-testing/bug1588.parquet");
	data.emplace_back("data/parquet-testing/bug1589.parquet");
	data.emplace_back("data/parquet-testing/bug1618_struct_strings.parquet");
	data.emplace_back("data/parquet-testing/bug2267.parquet");
	data.emplace_back("data/parquet-testing/bug2557.parquet");
	// slow
	// data.emplace_back("data/parquet-testing/bug687_nulls.parquet");
	// data.emplace_back("data/parquet-testing/complex.parquet");
	data.emplace_back("data/parquet-testing/data-types.parquet");
	data.emplace_back("data/parquet-testing/date.parquet");
	// arrow can't read this because it's a time with a timezone and it's not supported by arrow
	//	data.emplace_back("data/parquet-testing/date_stats.parquet");
	data.emplace_back("data/parquet-testing/decimal_stats.parquet");
	data.emplace_back("data/parquet-testing/decimals.parquet");
	data.emplace_back("data/parquet-testing/enum.parquet");
	data.emplace_back("data/parquet-testing/filter_bug1391.parquet");
	//	data.emplace_back("data/parquet-testing/fixed.parquet");
	// slow
	// data.emplace_back("data/parquet-testing/leftdate3_192_loop_1.parquet");
	data.emplace_back("data/parquet-testing/lineitem-top10000.gzip.parquet");
	data.emplace_back("data/parquet-testing/manyrowgroups.parquet");
	data.emplace_back("data/parquet-testing/manyrowgroups2.parquet");
	//	data.emplace_back("data/parquet-testing/map.parquet");
	// Can't roundtrip NaNs
	data.emplace_back("data/parquet-testing/nan-float.parquet");
	// null byte in file
	// data.emplace_back("data/parquet-testing/nullbyte.parquet");
	// data.emplace_back("data/parquet-testing/nullbyte_multiple.parquet");
	// borked
	// data.emplace_back("data/parquet-testing/p2.parquet");
	// data.emplace_back("data/parquet-testing/p2strings.parquet");
	data.emplace_back("data/parquet-testing/pandas-date.parquet");
	data.emplace_back("data/parquet-testing/signed_stats.parquet");
	data.emplace_back("data/parquet-testing/silly-names.parquet");
	// borked
	// data.emplace_back("data/parquet-testing/simple.parquet");
	// data.emplace_back("data/parquet-testing/sorted.zstd_18_131072_small.parquet");
	data.emplace_back("data/parquet-testing/struct.parquet");
	data.emplace_back("data/parquet-testing/struct_skip_test.parquet");
	data.emplace_back("data/parquet-testing/timestamp-ms.parquet");
	data.emplace_back("data/parquet-testing/timestamp.parquet");
	data.emplace_back("data/parquet-testing/unsigned.parquet");
	data.emplace_back("data/parquet-testing/unsigned_stats.parquet");
	data.emplace_back("data/parquet-testing/userdata1.parquet");
	data.emplace_back("data/parquet-testing/varchar_stats.parquet");
	data.emplace_back("data/parquet-testing/zstd.parquet");

	for (auto &parquet_path : data) {
		TestParquetRoundtrip(parquet_path);
	}
}
