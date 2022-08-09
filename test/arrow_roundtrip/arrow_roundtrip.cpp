#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/main/extension_helper.hpp"

using namespace duckdb;

struct ArrowRoundtripFactory {
	ArrowRoundtripFactory(vector<LogicalType> types_p, vector<string> names_p, string tz_p,
	                      unique_ptr<QueryResult> result_p, bool big_result)
	    : types(move(types_p)), names(move(names_p)), tz(move(tz_p)), result(move(result_p)), big_result(big_result) {
	}

	vector<LogicalType> types;
	vector<string> names;
	string tz;
	unique_ptr<QueryResult> result;
	bool big_result;

public:
	struct ArrowArrayStreamData {
		ArrowArrayStreamData(ArrowRoundtripFactory &factory) : factory(factory) {
		}

		ArrowRoundtripFactory &factory;
	};

	static int ArrowArrayStreamGetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
		if (!stream->private_data) {
			throw InternalException("No private data!?");
		}
		auto &data = *((ArrowArrayStreamData *)stream->private_data);
		data.factory.ToArrowSchema(out);
		return 0;
	}

	static int ArrowArrayStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
		if (!stream->private_data) {
			throw InternalException("No private data!?");
		}
		auto &data = *((ArrowArrayStreamData *)stream->private_data);
		if (!data.factory.big_result) {
			auto chunk = data.factory.result->Fetch();
			if (!chunk || chunk->size() == 0) {
				return 0;
			}
			ArrowConverter::ToArrowArray(*chunk, out);
		} else {
			ArrowAppender appender(data.factory.result->types, STANDARD_VECTOR_SIZE);
			idx_t count = 0;
			while (true) {
				auto chunk = data.factory.result->Fetch();
				if (!chunk || chunk->size() == 0) {
					break;
				}
				count += chunk->size();
				appender.Append(*chunk);
			}
			if (count > 0) {
				*out = appender.Finalize();
			}
		}
		return 0;
	}

	static const char *ArrowArrayStreamGetLastError(struct ArrowArrayStream *stream) {
		throw InternalException("Error!?!!");
	}

	static void ArrowArrayStreamRelease(struct ArrowArrayStream *stream) {
		if (!stream->private_data) {
			return;
		}
		auto data = (ArrowArrayStreamData *)stream->private_data;
		delete data;
		stream->private_data = nullptr;
	}

	static std::unique_ptr<duckdb::ArrowArrayStreamWrapper> CreateStream(uintptr_t this_ptr,
	                                                                     ArrowStreamParameters &parameters) {
		//! Create a new batch reader
		auto &factory = *reinterpret_cast<ArrowRoundtripFactory *>(this_ptr); //! NOLINT
		if (!factory.result) {
			throw InternalException("Stream already consumed!");
		}

		auto stream_wrapper = make_unique<ArrowArrayStreamWrapper>();
		stream_wrapper->number_of_rows = -1;
		auto private_data = make_unique<ArrowArrayStreamData>(factory);
		stream_wrapper->arrow_array_stream.get_schema = ArrowArrayStreamGetSchema;
		stream_wrapper->arrow_array_stream.get_next = ArrowArrayStreamGetNext;
		stream_wrapper->arrow_array_stream.get_last_error = ArrowArrayStreamGetLastError;
		stream_wrapper->arrow_array_stream.release = ArrowArrayStreamRelease;
		stream_wrapper->arrow_array_stream.private_data = private_data.release();

		return stream_wrapper;
	}

	static void GetSchema(uintptr_t factory_ptr, duckdb::ArrowSchemaWrapper &schema) {
		//! Create a new batch reader
		auto &factory = *reinterpret_cast<ArrowRoundtripFactory *>(factory_ptr); //! NOLINT
		factory.ToArrowSchema(&schema.arrow_schema);
	}

	void ToArrowSchema(struct ArrowSchema *out) {
		ArrowConverter::ToArrowSchema(out, types, names, tz);
	}
};

void RunArrowComparison(Connection &con, const string &query, bool big_result = false) {
	// run the query
	auto initial_result = con.Query(query);
	if (!initial_result->success) {
		initial_result->Print();
		FAIL();
	}
	// create the roundtrip factory
	auto tz = ClientConfig::GetConfig(*con.context).ExtractTimezone();
	auto types = initial_result->types;
	auto names = initial_result->names;
	ArrowRoundtripFactory factory(move(types), move(names), tz, move(initial_result), big_result);

	// construct the arrow scan
	vector<Value> params;
	params.push_back(Value::POINTER((uintptr_t)&factory));
	params.push_back(Value::POINTER((uintptr_t)&ArrowRoundtripFactory::CreateStream));
	params.push_back(Value::POINTER((uintptr_t)&ArrowRoundtripFactory::GetSchema));
	params.push_back(Value::UBIGINT(1000000));

	// run the arrow scan over the result
	auto arrow_result = con.TableFunction("arrow_scan", params)->Execute();
	REQUIRE(arrow_result->type == QueryResultType::MATERIALIZED_RESULT);
	if (!arrow_result->success) {
		printf("-------------------------------------\n");
		printf("Arrow round-trip query error: %s\n", arrow_result->error.c_str());
		printf("-------------------------------------\n");
		printf("Query: %s\n", query.c_str());
		printf("-------------------------------------\n");
		FAIL();
	}
	auto &materialized_arrow = (MaterializedQueryResult &)*arrow_result;

	auto result = con.Query(query);

	// compare the results
	string error;
	if (!ColumnDataCollection::ResultEquals(result->Collection(), materialized_arrow.Collection(), error)) {
		printf("-------------------------------------\n");
		printf("Arrow round-trip failed: %s\n", error.c_str());
		printf("-------------------------------------\n");
		printf("Query: %s\n", query.c_str());
		printf("-----------------DuckDB-------------------\n");
		result->Print();
		printf("-----------------Arrow--------------------\n");
		materialized_arrow.Print();
		printf("-------------------------------------\n");
		FAIL();
	}
	REQUIRE(1);
}

static void TestArrowRoundtrip(const string &query) {
	DuckDB db;
	Connection con(db);

	RunArrowComparison(con, query, true);
	RunArrowComparison(con, query);
}

static void TestParquetRoundtrip(const string &path) {
	DuckDB db;
	Connection con(db);

	if (ExtensionHelper::LoadExtension(db, "parquet") == ExtensionLoadResult::NOT_LOADED) {
		FAIL();
		return;
	}

	// run the query
	auto query = "SELECT * FROM parquet_scan('" + path + "')";
	RunArrowComparison(con, query, true);
	RunArrowComparison(con, query);
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
	TestArrowRoundtrip("SELECT * REPLACE "
	                   "(interval (1) seconds AS interval) FROM test_all_types()");
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
	data.emplace_back("data/parquet-testing/date_stats.parquet");
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
