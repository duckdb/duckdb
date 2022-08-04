#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"

using namespace duckdb;

struct ArrowRoundtripFactory {
	ArrowRoundtripFactory(vector<LogicalType> types_p, vector<string> names_p, string tz_p,
	                      unique_ptr<MaterializedQueryResult> result_p)
	    : types(move(types_p)), names(move(names_p)), tz(move(tz_p)), result(move(result_p)) {
	}

	vector<LogicalType> types;
	vector<string> names;
	string tz;
	unique_ptr<MaterializedQueryResult> result;

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
		auto chunk = data.factory.result->Fetch();
		if (!chunk || chunk->size() == 0) {
			return 0;
		}
		ArrowConverter::ToArrowArray(*chunk, out);
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

	static std::unique_ptr<duckdb::ArrowArrayStreamWrapper> CreateStream(uintptr_t this_ptr) {
		//! Create a new batch reader
		auto &factory = *reinterpret_cast<ArrowRoundtripFactory *>(this_ptr); //! NOLINT
		if (!factory.result) {
			throw InternalException("Stream already consumed!");
		}

		auto stream_wrapper = make_unique<ArrowArrayStreamWrapper>();
		stream_wrapper->number_of_rows = factory.result->RowCount();
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

static void TestArrowRoundtrip(const string &query) {
	DuckDB db;
	Connection con(db);

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
	ArrowRoundtripFactory factory(move(types), move(names), tz, move(initial_result));

	// construct the arrow scan
	vector<Value> params;
	params.push_back(Value::POINTER((uintptr_t)&factory));
	params.push_back(Value::POINTER((uintptr_t)&ArrowRoundtripFactory::CreateStream));
	params.push_back(Value::POINTER((uintptr_t)&ArrowRoundtripFactory::GetSchema));
	params.push_back(Value::UBIGINT(1000000));

	// run the arrow scan over the result
	auto arrow_result = con.TableFunction("arrow_scan", params)->Execute();

	// run the initial query again
	auto result = con.Query(query);
	// compare the results
	if (!result->Equals(*arrow_result)) {
		result->Print();
		arrow_result->Print();
		FAIL();
	}
	REQUIRE(1);
}

TEST_CASE("Test arrow roundtrip", "[arrow]") {
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
	TestArrowRoundtrip("SELECT * EXCLUDE (uuid, small_enum, medium_enum, large_enum, hugeint, time_tz, json) REPLACE "
	                   "(interval (1) seconds AS interval) FROM test_all_types()");
}
