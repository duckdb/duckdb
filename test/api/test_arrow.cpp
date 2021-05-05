#include "catch.hpp"
#include "duckdb/common/arrow.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

struct MyArrowArrayStream {
	MyArrowArrayStream(QueryResult *duckdb_result) : duckdb_result(duckdb_result) {
		InitializeFunctionPointers(&stream);
		stream.private_data = this;
	}

	static void InitializeFunctionPointers(ArrowArrayStream *stream) {
		stream->get_schema = GetSchema;
		stream->get_next = GetNext;
		stream->release = Release;
		stream->get_last_error = GetLastError;
	};

	static int GetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
		D_ASSERT(stream->private_data);
		auto my_stream = (MyArrowArrayStream *)stream->private_data;
		if (!stream->release) {
			my_stream->last_error = "stream was released";
			return -1;
		}
		my_stream->duckdb_result->ToArrowSchema(out);
		return 0;
	}

	static int GetNext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
		D_ASSERT(stream->private_data);
		auto my_stream = (MyArrowArrayStream *)stream->private_data;
		if (!stream->release) {
			my_stream->last_error = "stream was released";
			return -1;
		}
		auto chunk = my_stream->duckdb_result->Fetch();
		if (!chunk || chunk->size() == 0) {
			return 0;
		}
		chunk->ToArrowArray(out);
		return 0;
	}

	static void Release(struct ArrowArrayStream *stream) {
		if (!stream->release) {
			return;
		}
		stream->release = nullptr;
		delete (MyArrowArrayStream *)stream->private_data;
	}

	static const char *GetLastError(struct ArrowArrayStream *stream) {
		if (!stream->release) {
			return "stream was released";
		}
		D_ASSERT(stream->private_data);
		auto my_stream = (MyArrowArrayStream *)stream->private_data;
		return my_stream->last_error.c_str();
	}

	QueryResult *duckdb_result;
	ArrowArrayStream stream;
	string last_error;
};

class MyPythonTableArrowArrayStreamFactory {
public:
	explicit MyPythonTableArrowArrayStreamFactory(unique_ptr<QueryResult> duckdb_result)
	    : arrow_table(move(duckdb_result)) {};
	static ArrowArrayStream *Produce(uintptr_t factory_ptr) {
		MyPythonTableArrowArrayStreamFactory *factory = (MyPythonTableArrowArrayStreamFactory *)factory_ptr;
		auto table_stream = new MyArrowArrayStream(factory->arrow_table.get());
		return &table_stream->stream;
	};
	unique_ptr<QueryResult> arrow_table;
};

static void TestArrowRoundTrip(string q) {
	DuckDB db(nullptr);
	Connection con(db);

	// query that creates a bunch of values across the types
	auto result = con.Query(q);
	REQUIRE(result->success);
	auto stream_factory = make_unique<MyPythonTableArrowArrayStreamFactory>(move(result));
	ArrowArrayStream *(*stream_factory_produce)(uintptr_t factory) = MyPythonTableArrowArrayStreamFactory::Produce;

	auto result2 = con.TableFunction("arrow_scan", {Value::POINTER((uintptr_t)stream_factory.get()),
	                                                Value::POINTER((uintptr_t)stream_factory_produce)})
	                   ->Execute();

	idx_t column_count = result2->ColumnCount();
	vector<vector<Value>> values;
	values.resize(column_count);
	while (true) {
		auto chunk = result2->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t c = 0; c < column_count; c++) {
			for (idx_t r = 0; r < chunk->size(); r++) {
				values[c].push_back(chunk->GetValue(c, r));
			}
		}
	}
	auto original_result = con.Query(q);
	for (idx_t c = 0; c < column_count; c++) {
		REQUIRE(CHECK_COLUMN(*original_result, c, values[c]));
	}
}

TEST_CASE("Test Arrow API round trip", "[arrow]") {
	//! many types
	TestArrowRoundTrip(
	    "select NULL c_null, (c % 4 = 0)::bool c_bool, (c%128)::tinyint c_tinyint, c::smallint*1000 c_smallint, "
	    "c::integer*100000 c_integer, c::bigint*1000000000000 c_bigint, c::hugeint*10000000000000000000000000000000 "
	    "c_hugeint, c::float c_float, c::double c_double, 'c_' || c::string c_string, DATE '1992-01-01'::date c_date, "
	    "'1969-01-01'::date, TIME '13:07:16'::time c_time, timestamp '1992-01-01 12:00:00' c_timestamp "
	    "from (select case when range % 2 == 0 then range else null end as c from range(-10, 10)) sq");
	//! big result set
	TestArrowRoundTrip("select i from range(0, 2000) sq(i)");
}

TEST_CASE("Test Arrow API unsigned", "[arrow]") {
	//! all unsigned types
	TestArrowRoundTrip("select (c%128)::utinyint c_utinyint ,c::usmallint*1000 c_usmallint, "
	                   "c::uinteger*100000 c_uinteger, c::ubigint*1000000000000 c_ubigint from (select case when range "
	                   "% 2 == 0 then range else null end as c from range(0, 100)) sq");
}
// TODO interval decimal
