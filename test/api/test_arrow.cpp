#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/arrow.hpp"

using namespace duckdb;
using namespace std;

struct MyArrowArrayStream {
	MyArrowArrayStream(unique_ptr<QueryResult> duckdb_result) : duckdb_result(move(duckdb_result)) {
		stream.get_schema = MyArrowArrayStream::my_stream_getschema;
		stream.get_next = MyArrowArrayStream::my_stream_getnext;
		stream.release = MyArrowArrayStream::my_stream_release;
		stream.get_last_error = MyArrowArrayStream::my_stream_getlasterror;
		stream.private_data = this;
	}

	static int my_stream_getschema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
		assert(stream->private_data);
		auto my_stream = (MyArrowArrayStream *)stream->private_data;
		if (!stream->release) {
			my_stream->last_error = "stream was released";
			return -1;
		}
		my_stream->duckdb_result->ToArrowSchema(out);
		return 0;
	}

	static int my_stream_getnext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
		assert(stream->private_data);
		auto my_stream = (MyArrowArrayStream *)stream->private_data;
		if (!stream->release) {
			my_stream->last_error = "stream was released";
			return -1;
		}
		auto chunk = my_stream->duckdb_result->Fetch();
		chunk->ToArrowArray(out);
		if (chunk->size() == 0) {
			out->release(out);
		}
		return 0;
	}

	static void my_stream_release(struct ArrowArrayStream *stream) {
		if (!stream->release) {
			return;
		}
		stream->release = nullptr;
		delete (MyArrowArrayStream *)stream->private_data;
	}

	static const char *my_stream_getlasterror(struct ArrowArrayStream *stream) {
		if (!stream->release) {
			return "stream was released";
		}
		assert(stream->private_data);
		auto my_stream = (MyArrowArrayStream *)stream->private_data;
		return my_stream->last_error.c_str();
	}

	unique_ptr<QueryResult> duckdb_result;
	ArrowArrayStream stream;
	string last_error;
};

TEST_CASE("Test Arrow API round trip", "[arrow]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto q =
	    "select NULL c_null, (c % 4 = 0)::bool c_bool, (c%128)::tinyint c_tinyint, c::smallint*1000 c_smallint, "
	    "c::integer*100000 c_integer, c::bigint*1000000000000 c_bigint, c::hugeint*10000000000000000000000000000000 "
	    "c_hugeint, c::float c_float, c::double c_double, 'c_' || c::string c_string from (select case when range % 2 "
	    "== 0 then range else null end as c from range(-10000, 10000)) sq";

	// query that creates a bunch of values across the types
	auto result = con.Query(q);
	auto my_stream = new MyArrowArrayStream(move(result));
	auto result2 = con.TableFunction("arrow_scan", {Value::POINTER((uintptr_t)&my_stream->stream)})->Execute();
	REQUIRE(con.Query(q)->Equals(*result2));
}
// TODO timestamp date time interval decimal
