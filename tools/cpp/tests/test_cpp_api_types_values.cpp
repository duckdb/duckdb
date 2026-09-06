#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"
#include "test_helpers.hpp"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>

// ---------------------------------------------------------------------------
// Stable C++ API tests: types and values.
// ---------------------------------------------------------------------------

TEST_CASE("Stable C++API: Value Null and the BIGNUM codec", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto null_value = Value::CreateNull(conn, conn.ParseType("INTEGER"));
	REQUIRE(null_value.IsNull());
	REQUIRE(null_value.GetLogicalType() == conn.ParseType("INTEGER"));

	// 2^64: a 0x01 byte followed by eight 0x00 bytes.
	const std::vector<uint8_t> magnitude = {0x01, 0, 0, 0, 0, 0, 0, 0, 0};
	bignum_t::Decoded in;
	in.magnitude = magnitude;
	in.is_negative = false;
	auto storage = bignum_t::Encode(in);
	auto positive = Value::Create(
	    conn, bignum_t(reinterpret_cast<const char *>(storage.data()), static_cast<uint32_t>(storage.size())));
	REQUIRE(positive.ToText() == "18446744073709551616");

	auto decoded = positive.Get<bignum_t>().Decode();
	REQUIRE(decoded.magnitude == magnitude);
	REQUIRE_FALSE(decoded.is_negative);

	in.is_negative = true;
	auto neg_storage = bignum_t::Encode(in);
	auto negative = Value::Create(conn, bignum_t(reinterpret_cast<const char *>(neg_storage.data()),
	                                             static_cast<uint32_t>(neg_storage.size())))
	                    .Get<bignum_t>()
	                    .Decode();
	REQUIRE(negative.magnitude == magnitude);
	REQUIRE(negative.is_negative);
}
TEST_CASE("Stable C++API: ToText and ParseType round trip", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	REQUIRE(conn.ParseType("INTEGER").ToText() == "INTEGER");

	// Connection sugar.
	auto dec = conn.ParseType("DECIMAL(12,4)");
	REQUIRE(dec.ToText() == "DECIMAL(12,4)");
	REQUIRE(dec == conn.ParseType(dec.ToText()));

	// TUPLE (the unnamed struct) parses and reports its own id; children
	// come back positionally through the generic GetParam path.
	auto tup = conn.ParseType("TUPLE(INTEGER, VARCHAR)");
	REQUIRE(tup.GetParamCount() == 2);
	REQUIRE(tup.GetParam(0).GetName().empty());

	// Connection form.
	auto type_type = conn.ParseType("TYPE");
	auto list = conn.ParseType("INTEGER[]");

	REQUIRE_THROWS_MATCHES(conn.ParseType("definitely_not_a_type"), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_DATABASE_CATALOG));
}
TEST_CASE("Stable C++API: GetTypeId reports the kind, parameters and alias aside", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	REQUIRE(conn.ParseType("INTEGER").GetTypeId() == LogicalTypeId::INTEGER);
	REQUIRE(conn.ParseType("DECIMAL(12,4)").GetTypeId() == LogicalTypeId::DECIMAL);
	REQUIRE(conn.ParseType("INTEGER[]").GetTypeId() == LogicalTypeId::LIST);
	REQUIRE(conn.ParseType("TUPLE(INTEGER, VARCHAR)").GetTypeId() == LogicalTypeId::TUPLE);

	// An alias changes the name, not the kind.
	auto aliased = conn.ParseType("INTEGER").WithAlias(conn, "MY_INT");
	REQUIRE(aliased.GetName() == "MY_INT");
	REQUIRE(aliased.GetTypeId() == LogicalTypeId::INTEGER);
}
TEST_CASE("Stable C++API: CreateType named + positional params and the GetParam dual", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Positional numeric params.
	std::vector<TypeParam> dec_params;
	dec_params.push_back({"", Value::Create(conn, int64_t(12))});
	dec_params.push_back({"", Value::Create(conn, int64_t(4))});
	auto dec = conn.CreateType("decimal", dec_params);
	REQUIRE(dec == conn.ParseType("DECIMAL(12,4)"));
	REQUIRE(dec.GetParamCount() == 2);
	auto width_param = dec.GetParam(0);
	REQUIRE(width_param.GetName().empty());
	REQUIRE(width_param.GetValue().Get<uint8_t>() == 12);

	// Named TYPE-value params.
	std::vector<TypeParam> fields;
	fields.push_back({"a", Value::Create(conn, conn.ParseType("INTEGER"))});
	fields.push_back({"b", Value::Create(conn, conn.ParseType("VARCHAR"))});
	auto s = conn.CreateType("struct", fields);
	REQUIRE(s.ToText() == "STRUCT(a INTEGER, b VARCHAR)");
	auto field = s.GetParam(1);
	REQUIRE(field.GetName() == "b");
	REQUIRE(field.GetValue().Get<LogicalType>() == conn.ParseType("VARCHAR"));

	// The dual: create(name, params(t)) equals t.
	std::vector<TypeParam> rebuilt_params;
	for (idx_t i = 0; i < s.GetParamCount(); i++) {
		rebuilt_params.push_back(s.GetParam(i));
	}
	REQUIRE(conn.CreateType("struct", rebuilt_params) == s);

	REQUIRE_THROWS_MATCHES(conn.CreateType("list", {}), Exception, HasErrorCode(DUCKDB_V2_ERROR_QUERY_BINDER));
}
TEST_CASE("Stable C++API: per-kind type getters are sugar over GetParam", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto dec = conn.ParseType("DECIMAL(18,3)");
	REQUIRE(dec.GetDecimalWidth() == 18);
	REQUIRE(dec.GetDecimalScale() == 3);

	std::vector<TypeParam> entries;
	entries.push_back({"", Value::Create(conn, varchar_t("sad"))});
	entries.push_back({"", Value::Create(conn, varchar_t("ok"))});
	entries.push_back({"", Value::Create(conn, varchar_t("happy"))});
	auto mood = conn.CreateType("enum", entries);
	REQUIRE(mood.GetEnumSize() == 3);
	REQUIRE(mood.GetEnumValue(2) == "happy");

	auto list = conn.ParseType("INTEGER[]");
	REQUIRE(list.GetListChildType() == conn.ParseType("INTEGER"));

	auto arr = conn.ParseType("VARCHAR[7]");
	REQUIRE(arr.GetArrayChildType() == conn.ParseType("VARCHAR"));
	REQUIRE(arr.GetArraySize() == 7);

	auto map = conn.ParseType("MAP(VARCHAR, INTEGER)");
	REQUIRE(map.GetMapKeyType() == conn.ParseType("VARCHAR"));
	REQUIRE(map.GetMapValueType() == conn.ParseType("INTEGER"));

	auto s = conn.ParseType("STRUCT(id INTEGER, label VARCHAR)");
	REQUIRE(s.GetStructChildCount() == 2);
	REQUIRE(s.GetStructChildName(0) == "id");
	REQUIRE(s.GetStructChildType(1) == conn.ParseType("VARCHAR"));

	auto u = conn.ParseType("UNION(i INTEGER, s VARCHAR)");
	REQUIRE(u.GetUnionMemberCount() == 2);
	REQUIRE(u.GetUnionMemberName(1) == "s");
	REQUIRE(u.GetUnionMemberType(0) == conn.ParseType("INTEGER"));

	// The sugar gates on the type kind.
	REQUIRE_THROWS_MATCHES(conn.ParseType("INTEGER").GetDecimalWidth(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
TEST_CASE("Stable C++API: TYPE values and composite Value::Create", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// TYPE values wrap and unwrap.
	auto wrapped = Value::Create(conn, conn.ParseType("INTEGER"));
	REQUIRE(wrapped.Get<LogicalType>() == conn.ParseType("INTEGER"));
	REQUIRE_THROWS_MATCHES(Value::Create(conn, int64_t(1)).Get<LogicalType>(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// LIST: children cast to the declared child type.
	auto list_type = conn.ParseType("BIGINT[]");
	std::vector<Value> elements;
	elements.push_back(Value::Create(conn, int64_t(1)));
	elements.push_back(Value::Create(conn, int64_t(2)));
	auto list = Value::CreateList(conn, elements);
	REQUIRE(list.GetChildCount() == 2);
	REQUIRE(list.GetChild(1).Get<int64_t>() == 2);

	// MAP: alternating key, value.
	std::vector<std::pair<Value, Value>> entries;
	entries.emplace_back(Value::Create(conn, varchar_t("a")), Value::Create(conn, int64_t(1)));
	auto map = Value::CreateMap(conn, entries);
	REQUIRE(map.GetChildCount() == 2);
	REQUIRE(map.GetChild(0).ToText() == "a");

	// UNION values are built via Cast; there is no composite constructor for them.
}
TEST_CASE("Stable C++API: Value::Cast through Context and Connection", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Connection sugar.
	auto parsed = Value::Create(conn, varchar_t("42")).Cast(conn, conn.ParseType("INTEGER"));
	REQUIRE(parsed.Get<int32_t>() == 42);

	// Connection form.
	auto date = Value::Create(conn, varchar_t("2024-03-15")).Cast(conn, conn.ParseType("DATE"));
	REQUIRE(date.ToText() == "2024-03-15");

	// UNION via cast: [0] = tag as UTINYINT, [1] = the active member.
	auto union_type = conn.ParseType("UNION(i INTEGER, s VARCHAR)");
	auto u = Value::Create(conn, varchar_t("x")).Cast(conn, union_type);
	REQUIRE(u.GetChildCount() == 2);
	REQUIRE(u.GetChild(0).Get<uint8_t>() == 1);
	REQUIRE(u.GetChild(1).ToText() == "x");

	// ENUM via cast.
	std::vector<TypeParam> entries;
	entries.push_back({"", Value::Create(conn, varchar_t("sad"))});
	entries.push_back({"", Value::Create(conn, varchar_t("happy"))});
	auto mood = conn.CreateType("enum", entries);
	auto happy = Value::Create(conn, varchar_t("happy")).Cast(conn, mood);
	REQUIRE(happy.ToText() == "happy");

	// Cast failures carry the engine's code.
	REQUIRE_THROWS_MATCHES(Value::Create(conn, varchar_t("abc")).Cast(conn, conn.ParseType("INTEGER")), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
TEST_CASE("Stable C++API: storage-tier conveniences follow the committed tables", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	struct {
		int width;
		LogicalTypeId expected;
	} decimal_cases[] = {
	    {4, LogicalTypeId::SMALLINT}, {5, LogicalTypeId::INTEGER}, {9, LogicalTypeId::INTEGER},
	    {10, LogicalTypeId::BIGINT},  {18, LogicalTypeId::BIGINT}, {19, LogicalTypeId::HUGEINT},
	    {38, LogicalTypeId::HUGEINT},
	};
	for (auto &c : decimal_cases) {
		auto dec = conn.ParseType("DECIMAL(" + std::to_string(c.width) + ",2)");
		REQUIRE(dec.GetDecimalInternalTypeId() == c.expected);
	}

	struct {
		idx_t entries;
		LogicalTypeId expected;
	} enum_cases[] = {
	    {1, LogicalTypeId::UTINYINT},      {255, LogicalTypeId::UTINYINT},   {256, LogicalTypeId::USMALLINT},
	    {65535, LogicalTypeId::USMALLINT}, {65536, LogicalTypeId::UINTEGER},
	};
	for (auto &c : enum_cases) {
		std::vector<TypeParam> entries;
		entries.reserve(c.entries);
		for (idx_t i = 0; i < c.entries; i++) {
			entries.push_back({"", Value::Create(conn, varchar_t("v" + std::to_string(i)))});
		}
		auto mood = conn.CreateType("enum", entries);
		REQUIRE(mood.GetEnumInternalTypeId() == c.expected);
	}

	// Gated on the type kind like the other sugars.
	REQUIRE_THROWS_MATCHES(conn.ParseType("INTEGER").GetDecimalInternalTypeId(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	// A non-ENUM has no dictionary, so the tier query is not an error either;
	// GetEnumSize is what distinguishes an ENUM from anything else.
	REQUIRE(conn.ParseType("INTEGER").GetEnumSize() == 0);
}
TEST_CASE("Stable C++API: writing a VARIANT vector through the boxed value path", "[cpp_api][types_values][variant]") {
	using namespace duckdb::cxx;
	// Companion completeness pin to the VARIANT read test, NOT a performance
	// exercise: per-row boxed SetValue / GetValue is the accepted-inefficient
	// totality path (VARIANT has no committed view layout, so the single-cell
	// bridge is the only access), and core VARIANT work is in flux. Do not
	// optimize this and do not grow surface for it.
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// A boxed NULL fetched up front: the C++ surface has no NULL-value
	// constructor, and results must not be consumed inside the scope below.
	auto null_result = conn.Execute("SELECT NULL::VARIANT");
	auto null_chunk = null_result.FetchChunk();
	auto boxed_null = null_chunk.GetVector(0).GetValue(0);
	REQUIRE(boxed_null.IsNull());

	auto variant_type = conn.ParseType("VARIANT");
	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("VARIANT"));
	DataChunk chunk(types);
	auto vec = chunk.GetVector(0);
	REQUIRE(vec.GetVectorType() == VectorType::FLAT);
	vec.SetSize(5);

	// PROBED: SetValue casts on write to VARIANT engine-side (the
	// to-VARIANT cast needs no context), so plain values write directly.
	vec.SetValue(0, Value::Create(conn, int64_t(42)));
	const char *heap_string = "a string long enough to spill";
	vec.SetValue(1, Value::Create(conn, varchar_t(heap_string)));
	auto list_type = conn.ParseType("INTEGER[]");
	std::vector<Value> elements;
	elements.push_back(Value::Create(conn, int64_t(1)));
	elements.push_back(Value::Create(conn, int64_t(2)));
	elements.push_back(Value::Create(conn, int64_t(3)));
	vec.SetValue(2, Value::CreateList(conn, elements));
	vec.SetValue(3, boxed_null);
	// The explicit route works too: box first, then write.
	vec.SetValue(4, Value::Create(conn, int64_t(43)).Cast(conn, variant_type));

	// Read back through the boxed path: every non-NULL cell is a
	// VARIANT box; Cast back to the known inner type round-trips.
	for (idx_t row : {idx_t(0), idx_t(1), idx_t(2), idx_t(4)}) {
		auto box = vec.GetValue(row);
		REQUIRE_FALSE(box.IsNull());
	}
	REQUIRE(vec.GetValue(0).ToText() == "42");
	REQUIRE(vec.GetValue(0).Cast(conn, conn.ParseType("BIGINT")).Get<int64_t>() == 42);
	REQUIRE(vec.GetValue(1).Cast(conn, conn.ParseType("VARCHAR")).Get<varchar_t>().view() == heap_string);
	auto unboxed_list = vec.GetValue(2).Cast(conn, list_type);
	REQUIRE(unboxed_list.GetChildCount() == 3);
	REQUIRE(unboxed_list.GetChild(2).Get<int32_t>() == 3);
	REQUIRE(vec.GetValue(3).IsNull());
	REQUIRE(vec.GetValue(4).Cast(conn, conn.ParseType("BIGINT")).Get<int64_t>() == 43);

	// MakeConstant interplay: the type-equality hardening refuses the
	// raw non-VARIANT value; the boxed value works.
	std::vector<LogicalType> constant_types;
	constant_types.push_back(conn.ParseType("VARIANT"));
	DataChunk constant_chunk(constant_types);
	auto cvec = constant_chunk.GetVector(0);
	REQUIRE_THROWS_MATCHES(cvec.MakeConstant(Value::Create(conn, int64_t(7)), 3), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	auto boxed = Value::Create(conn, int64_t(7)).Cast(conn, variant_type);
	cvec.MakeConstant(boxed, 3);
	REQUIRE(cvec.GetValue(2).ToText() == "7");
}
TEST_CASE("Stable C++API: typed Value leaf ctors/getters round trip", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Bool / U64 / F64 round-trip through the typed constructors.
	REQUIRE(Value::Create(conn, bool(true)).Get<bool>());
	REQUIRE_FALSE(Value::Create(conn, bool(false)).Get<bool>());
	REQUIRE(Value::Create(conn, uint64_t(18446744073709551615ULL)).Get<uint64_t>() == 18446744073709551615ULL);
	REQUIRE(Value::Create(conn, double(-2.5)).Get<double>() == -2.5);

	// Blob: arbitrary bytes, including an embedded NUL.
	const uint8_t blob_bytes[] = {0x00, 0xFF, 0x10, 0x00, 0x42};
	auto blob_value = Value::Create(conn, blob_t(reinterpret_cast<const char *>(blob_bytes), sizeof(blob_bytes)));
	auto blob_read = blob_value.Get<blob_t>();
	REQUIRE(blob_read.size() == sizeof(blob_bytes));
	REQUIRE(std::memcmp(blob_read.data(), blob_bytes, blob_read.size()) == 0);
	// Empty blob is legal too.
	auto empty_blob = Value::Create(conn, blob_t(nullptr, 0));
	REQUIRE(empty_blob.Get<blob_t>().size() == 0);

	// Date: the engine is the oracle for the days-since-epoch encoding.
	// DATE - DATE yields a BIGINT day count, not an INTEGER.
	auto date_chunk = conn.Execute("SELECT (DATE '2024-03-15' - DATE '1970-01-01')").FetchChunk();
	auto date_days = static_cast<int32_t>(date_chunk.GetVector(0).GetValue(0).Get<int64_t>());
	auto date_value = Value::Create(conn, date_t {date_days});
	REQUIRE(date_value.ToText() == "2024-03-15");
	REQUIRE(date_value.Get<date_t>().days == date_days);

	// Time: epoch_us() of a 1970-01-01 timestamp gives the time-of-day micros.
	auto time_micros = conn.Execute("SELECT epoch_us(TIMESTAMP '1970-01-01 13:45:30.123456')")
	                       .FetchChunk()
	                       .GetVector(0)
	                       .GetValue(0)
	                       .Get<int64_t>();
	auto time_value = Value::Create(conn, dtime_t {time_micros});
	REQUIRE(time_value.ToText() == "13:45:30.123456");
	REQUIRE(time_value.Get<dtime_t>().micros == time_micros);

	// Timestamp: epoch_us() is the direct oracle.
	auto ts_micros = conn.Execute("SELECT epoch_us(TIMESTAMP '2024-03-15 13:45:30.123456')")
	                     .FetchChunk()
	                     .GetVector(0)
	                     .GetValue(0)
	                     .Get<int64_t>();
	auto ts_value = Value::Create(conn, timestamp_t {ts_micros});
	REQUIRE(ts_value.ToText() == "2024-03-15 13:45:30.123456");
	REQUIRE(ts_value.Get<timestamp_t>().micros == ts_micros);

	// TimestampTz: stored as UTC micros since epoch, same as TIMESTAMP;
	// cross-check against a TIMESTAMPTZ literal cast through the engine.
	auto tz_type = conn.ParseType("TIMESTAMP WITH TIME ZONE");
	auto engine_tz = Value::Create(conn, varchar_t("2024-03-15 13:45:30.123456+00")).Cast(conn, tz_type);
	auto tz_value = Value::Create(conn, timestamp_tz_t {ts_micros});
	REQUIRE(tz_value.Get<timestamp_t>().micros == ts_micros);
	REQUIRE(tz_value.Get<timestamp_t>().micros == engine_tz.Get<timestamp_t>().micros);

	// IntervalLayout: identity round trip plus a canonical-value cross-check.
	auto interval_value = Value::Create(conn, interval_t {14, 3, 14706789000LL});
	auto decoded = interval_value.Get<interval_t>();
	REQUIRE(decoded.months == 14);
	REQUIRE(decoded.days == 3);
	REQUIRE(decoded.micros == 14706789000LL);

	auto three_days = Value::Create(conn, varchar_t("3 days")).Cast(conn, conn.ParseType("INTERVAL"));
	auto three_days_decoded = three_days.Get<interval_t>();
	REQUIRE(three_days_decoded.months == 0);
	REQUIRE(three_days_decoded.days == 3);
	REQUIRE(three_days_decoded.micros == 0);

	// A getter throws INVALID_INPUT on a type mismatch, keyed on the logical
	// type id, not the payload width.
	REQUIRE_THROWS_MATCHES(Value::Create(conn, int64_t(1)).Get<date_t>(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	REQUIRE_THROWS_MATCHES(Value::Create(conn, bool(true)).Get<interval_t>(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
TEST_CASE("Stable C++API: typed Value 128-bit getters round trip", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// HUGEINT halves: value == upper * 2^64 + lower. 2^64 is {0, 1}.
	auto big = Value::Create(conn, int128_t({0, 1}));
	REQUIRE(big.ToText() == "18446744073709551616");
	REQUIRE(big.Get<int128_t>().lower == 0);
	REQUIRE(big.Get<int128_t>().upper == 1);

	// A HUGEINT built by the engine reads back into the same halves.
	auto from_engine = Value::Create(conn, varchar_t("18446744073709551616")).Cast(conn, conn.ParseType("HUGEINT"));
	auto halves = from_engine.Get<int128_t>();
	REQUIRE(halves.lower == 0);
	REQUIRE(halves.upper == 1);
	REQUIRE(Value::Create(conn, int128_t(halves)).ToText() == from_engine.ToText());

	// Negative: -1 == {UINT64_MAX, -1}.
	auto neg = Value::Create(conn, varchar_t("-1")).Cast(conn, conn.ParseType("HUGEINT")).Get<int128_t>();
	REQUIRE(neg.lower == ~static_cast<uint64_t>(0));
	REQUIRE(neg.upper == -1);

	// UHUGEINT: 2^64 is {0, 1}.
	auto ubig = Value::Create(conn, uint128_t({0, 1}));
	REQUIRE(ubig.ToText() == "18446744073709551616");
	REQUIRE(ubig.Get<uint128_t>().lower == 0);
	REQUIRE(ubig.Get<uint128_t>().upper == 1);

	// UUID decodes to its canonical 16 big-endian bytes (the storage's sort-order
	// high-bit flip is undone), matching the source string exactly.
	auto uuid =
	    Value::Create(conn, varchar_t("00112233-4455-6677-8899-aabbccddeeff")).Cast(conn, conn.ParseType("UUID"));
	REQUIRE(uuid.ToText() == "00112233-4455-6677-8899-aabbccddeeff");
	const uint8_t expected[16] = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
	                              0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff};
	REQUIRE(std::memcmp(uuid.Get<uuid_t>().Decode().bytes, expected, 16) == 0);

	// The getters convert rather than refuse, so a BIGINT reads as a HUGEINT
	// and the two 128-bit widths read as each other.
	REQUIRE(Value::Create(conn, int64_t(1)).Get<int128_t>().lower == 1);
	REQUIRE(big.Get<uint128_t>().lower == big.Get<int128_t>().lower);
	REQUIRE_THROWS_MATCHES(big.Get<uuid_t>(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
TEST_CASE("Stable C++API: TIME_TZ decodes to micros + offset", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// The engine builds the value; we decode it. 12:30:00 local, +02:00 east.
	auto tz = Value::Create(conn, varchar_t("12:30:00+02:00")).Cast(conn, conn.ParseType("TIME WITH TIME ZONE"));
	auto d = tz.Get<dtime_tz_t>();
	REQUIRE(d.GetMicros() == 45000000000LL); // (12*3600 + 30*60) s
	REQUIRE(d.GetOffset() == 2 * 60 * 60);

	// A western offset.
	auto west = Value::Create(conn, varchar_t("06:00:00-05:30"))
	                .Cast(conn, conn.ParseType("TIME WITH TIME ZONE"))
	                .Get<dtime_tz_t>();
	REQUIRE(west.GetMicros() == 6LL * 60 * 60 * 1000000);
	REQUIRE(west.GetOffset() == -(5 * 60 * 60 + 30 * 60));

	// Type-mismatch guard.
	REQUIRE_THROWS_MATCHES(Value::Create(conn, int64_t(1)).Get<dtime_tz_t>(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
