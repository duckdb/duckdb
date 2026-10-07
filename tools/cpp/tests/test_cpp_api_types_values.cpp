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
	auto &ctx = conn.GetContext();

	auto null_value = Value::CreateNull(ctx, ctx.ParseType("INTEGER"));
	REQUIRE(null_value.IsNull());
	REQUIRE(null_value.GetLogicalType() == ctx.ParseType("INTEGER"));

	// 2^64: a 0x01 byte followed by eight 0x00 bytes.
	const std::vector<uint8_t> magnitude = {0x01, 0, 0, 0, 0, 0, 0, 0, 0};
	bignum_t::Decoded in;
	in.magnitude = magnitude;
	in.is_negative = false;
	auto storage = bignum_t::Encode(in);
	auto positive = Value::Create(
	    ctx, bignum_t(reinterpret_cast<const char *>(storage.data()), static_cast<uint32_t>(storage.size())));
	REQUIRE(positive.ToText() == "18446744073709551616");

	auto decoded = positive.Get<bignum_t>().Decode();
	REQUIRE(decoded.magnitude == magnitude);
	REQUIRE_FALSE(decoded.is_negative);

	in.is_negative = true;
	auto neg_storage = bignum_t::Encode(in);
	auto negative = Value::Create(ctx, bignum_t(reinterpret_cast<const char *>(neg_storage.data()),
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
	auto &ctx = conn.GetContext();

	REQUIRE(ctx.ParseType("INTEGER").ToText() == "INTEGER");

	// Connection sugar.
	auto dec = ctx.ParseType("DECIMAL(12,4)");
	REQUIRE(dec.ToText() == "DECIMAL(12,4)");
	REQUIRE(dec == ctx.ParseType(dec.ToText()));

	// TUPLE (the unnamed struct) parses and reports its own id; children
	// come back positionally through the generic GetParam path.
	auto tup = ctx.ParseType("TUPLE(INTEGER, VARCHAR)");
	REQUIRE(tup.GetParamCount() == 2);
	REQUIRE(tup.GetParam(0).GetName().empty());

	// Connection form.
	auto type_type = ctx.ParseType("TYPE");
	auto list = ctx.ParseType("INTEGER[]");

	REQUIRE_THROWS_MATCHES(ctx.ParseType("definitely_not_a_type"), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_DATABASE_CATALOG));
}
TEST_CASE("Stable C++API: GetTypeId reports the kind, parameters and alias aside", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto &ctx = conn.GetContext();

	REQUIRE(ctx.ParseType("INTEGER").GetTypeId() == LogicalTypeId::INTEGER);
	REQUIRE(ctx.ParseType("DECIMAL(12,4)").GetTypeId() == LogicalTypeId::DECIMAL);
	REQUIRE(ctx.ParseType("INTEGER[]").GetTypeId() == LogicalTypeId::LIST);
	REQUIRE(ctx.ParseType("TUPLE(INTEGER, VARCHAR)").GetTypeId() == LogicalTypeId::TUPLE);

	// An alias changes the name, not the kind.
	auto aliased = ctx.ParseType("INTEGER").WithAlias(ctx, "MY_INT");
	REQUIRE(aliased.GetName() == "MY_INT");
	REQUIRE(aliased.GetTypeId() == LogicalTypeId::INTEGER);
}
TEST_CASE("Stable C++API: CreateType named + positional params and the GetParam dual", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto &ctx = conn.GetContext();

	// Positional numeric params.
	std::vector<TypeParam> dec_params;
	dec_params.push_back({"", Value::Create(ctx, int64_t(12))});
	dec_params.push_back({"", Value::Create(ctx, int64_t(4))});
	auto dec = ctx.CreateType("decimal", dec_params);
	REQUIRE(dec == ctx.ParseType("DECIMAL(12,4)"));
	REQUIRE(dec.GetParamCount() == 2);
	auto width_param = dec.GetParam(0);
	REQUIRE(width_param.GetName().empty());
	REQUIRE(width_param.GetValue().Get<uint8_t>() == 12);

	// Named TYPE-value params.
	std::vector<TypeParam> fields;
	fields.push_back({"a", Value::Create(ctx, ctx.ParseType("INTEGER"))});
	fields.push_back({"b", Value::Create(ctx, ctx.ParseType("VARCHAR"))});
	auto s = ctx.CreateType("struct", fields);
	REQUIRE(s.ToText() == "STRUCT(a INTEGER, b VARCHAR)");
	auto field = s.GetParam(1);
	REQUIRE(field.GetName() == "b");
	REQUIRE(field.GetValue().Get<LogicalType>() == ctx.ParseType("VARCHAR"));

	// The dual: create(name, params(t)) equals t.
	std::vector<TypeParam> rebuilt_params;
	for (idx_t i = 0; i < s.GetParamCount(); i++) {
		rebuilt_params.push_back(s.GetParam(i));
	}
	REQUIRE(ctx.CreateType("struct", rebuilt_params) == s);

	REQUIRE_THROWS_MATCHES(ctx.CreateType("list", {}), Exception, HasErrorCode(DUCKDB_V2_ERROR_QUERY_BINDER));
}
TEST_CASE("Stable C++API: per-kind type getters are sugar over GetParam", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto &ctx = conn.GetContext();

	auto dec = ctx.ParseType("DECIMAL(18,3)");
	REQUIRE(dec.GetDecimalWidth() == 18);
	REQUIRE(dec.GetDecimalScale() == 3);

	std::vector<TypeParam> entries;
	entries.push_back({"", Value::Create(ctx, varchar_t("sad"))});
	entries.push_back({"", Value::Create(ctx, varchar_t("ok"))});
	entries.push_back({"", Value::Create(ctx, varchar_t("happy"))});
	auto mood = ctx.CreateType("enum", entries);
	REQUIRE(mood.GetEnumSize() == 3);
	REQUIRE(mood.GetEnumValue(2) == "happy");

	auto list = ctx.ParseType("INTEGER[]");
	REQUIRE(list.GetListChildType() == ctx.ParseType("INTEGER"));

	auto arr = ctx.ParseType("VARCHAR[7]");
	REQUIRE(arr.GetArrayChildType() == ctx.ParseType("VARCHAR"));
	REQUIRE(arr.GetArraySize() == 7);

	auto map = ctx.ParseType("MAP(VARCHAR, INTEGER)");
	REQUIRE(map.GetMapKeyType() == ctx.ParseType("VARCHAR"));
	REQUIRE(map.GetMapValueType() == ctx.ParseType("INTEGER"));

	auto s = ctx.ParseType("STRUCT(id INTEGER, label VARCHAR)");
	REQUIRE(s.GetStructChildCount() == 2);
	REQUIRE(s.GetStructChildName(0) == "id");
	REQUIRE(s.GetStructChildType(1) == ctx.ParseType("VARCHAR"));

	auto u = ctx.ParseType("UNION(i INTEGER, s VARCHAR)");
	REQUIRE(u.GetUnionMemberCount() == 2);
	REQUIRE(u.GetUnionMemberName(1) == "s");
	REQUIRE(u.GetUnionMemberType(0) == ctx.ParseType("INTEGER"));

	// The sugar gates on the type kind.
	REQUIRE_THROWS_MATCHES(ctx.ParseType("INTEGER").GetDecimalWidth(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
TEST_CASE("Stable C++API: TYPE values and composite Value::Create", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto &ctx = conn.GetContext();

	// TYPE values wrap and unwrap.
	auto wrapped = Value::Create(ctx, ctx.ParseType("INTEGER"));
	REQUIRE(wrapped.Get<LogicalType>() == ctx.ParseType("INTEGER"));
	REQUIRE_THROWS_MATCHES(Value::Create(ctx, int64_t(1)).Get<LogicalType>(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// LIST: children cast to the declared child type.
	auto list_type = ctx.ParseType("BIGINT[]");
	std::vector<Value> elements;
	elements.push_back(Value::Create(ctx, int64_t(1)));
	elements.push_back(Value::Create(ctx, int64_t(2)));
	auto list = Value::CreateList(ctx, elements);
	REQUIRE(list.GetChildCount() == 2);
	REQUIRE(list.GetChild(1).Get<int64_t>() == 2);

	// MAP: alternating key, value.
	std::vector<std::pair<Value, Value>> entries;
	entries.emplace_back(Value::Create(ctx, varchar_t("a")), Value::Create(ctx, int64_t(1)));
	auto map = Value::CreateMap(ctx, entries);
	REQUIRE(map.GetChildCount() == 2);
	REQUIRE(map.GetChild(0).ToText() == "a");

	// UNION values are built via Cast; there is no composite constructor for them.
}
TEST_CASE("Stable C++API: Value::Cast through Context and Connection", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto &ctx = conn.GetContext();

	// Connection sugar.
	auto parsed = Value::Create(ctx, varchar_t("42")).Cast(ctx, ctx.ParseType("INTEGER"));
	REQUIRE(parsed.Get<int32_t>() == 42);

	// Connection form.
	auto date = Value::Create(ctx, varchar_t("2024-03-15")).Cast(ctx, ctx.ParseType("DATE"));
	REQUIRE(date.ToText() == "2024-03-15");

	// UNION via cast: [0] = tag as UTINYINT, [1] = the active member.
	auto union_type = ctx.ParseType("UNION(i INTEGER, s VARCHAR)");
	auto u = Value::Create(ctx, varchar_t("x")).Cast(ctx, union_type);
	REQUIRE(u.GetChildCount() == 2);
	REQUIRE(u.GetChild(0).Get<uint8_t>() == 1);
	REQUIRE(u.GetChild(1).ToText() == "x");

	// ENUM via cast.
	std::vector<TypeParam> entries;
	entries.push_back({"", Value::Create(ctx, varchar_t("sad"))});
	entries.push_back({"", Value::Create(ctx, varchar_t("happy"))});
	auto mood = ctx.CreateType("enum", entries);
	auto happy = Value::Create(ctx, varchar_t("happy")).Cast(ctx, mood);
	REQUIRE(happy.ToText() == "happy");

	// Cast failures carry the engine's code.
	REQUIRE_THROWS_MATCHES(Value::Create(ctx, varchar_t("abc")).Cast(ctx, ctx.ParseType("INTEGER")), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
TEST_CASE("Stable C++API: storage-tier conveniences follow the committed tables", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto &ctx = conn.GetContext();

	struct {
		int width;
		LogicalTypeId expected;
	} decimal_cases[] = {
	    {4, LogicalTypeId::SMALLINT}, {5, LogicalTypeId::INTEGER}, {9, LogicalTypeId::INTEGER},
	    {10, LogicalTypeId::BIGINT},  {18, LogicalTypeId::BIGINT}, {19, LogicalTypeId::HUGEINT},
	    {38, LogicalTypeId::HUGEINT},
	};
	for (auto &c : decimal_cases) {
		auto dec = ctx.ParseType("DECIMAL(" + std::to_string(c.width) + ",2)");
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
			entries.push_back({"", Value::Create(ctx, varchar_t("v" + std::to_string(i)))});
		}
		auto mood = ctx.CreateType("enum", entries);
		REQUIRE(mood.GetEnumInternalTypeId() == c.expected);
	}

	// Gated on the type kind like the other sugars.
	REQUIRE_THROWS_MATCHES(ctx.ParseType("INTEGER").GetDecimalInternalTypeId(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	// A non-ENUM has no dictionary, so the tier query is not an error either;
	// GetEnumSize is what distinguishes an ENUM from anything else.
	REQUIRE(ctx.ParseType("INTEGER").GetEnumSize() == 0);
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
	auto &ctx = conn.GetContext();

	// A boxed NULL fetched up front: the C++ surface has no NULL-value
	// constructor, and results must not be consumed inside the scope below.
	auto null_result = conn.Execute("SELECT NULL::VARIANT");
	auto null_chunk = null_result.FetchChunk();
	auto boxed_null = null_chunk.GetVector(0).GetValue(0);
	REQUIRE(boxed_null.IsNull());

	auto variant_type = ctx.ParseType("VARIANT");
	std::vector<LogicalType> types;
	types.push_back(ctx.ParseType("VARIANT"));
	DataChunk chunk(ctx, types);
	auto vec = chunk.GetVector(0);
	REQUIRE(vec.GetVectorType() == VectorType::FLAT);
	vec.SetSize(5);

	// PROBED: SetValue casts on write to VARIANT engine-side (the
	// to-VARIANT cast needs no context), so plain values write directly.
	vec.SetValue(0, Value::Create(ctx, int64_t(42)));
	const char *heap_string = "a string long enough to spill";
	vec.SetValue(1, Value::Create(ctx, varchar_t(heap_string)));
	auto list_type = ctx.ParseType("INTEGER[]");
	std::vector<Value> elements;
	elements.push_back(Value::Create(ctx, int64_t(1)));
	elements.push_back(Value::Create(ctx, int64_t(2)));
	elements.push_back(Value::Create(ctx, int64_t(3)));
	vec.SetValue(2, Value::CreateList(ctx, elements));
	vec.SetValue(3, boxed_null);
	// The explicit route works too: box first, then write.
	vec.SetValue(4, Value::Create(ctx, int64_t(43)).Cast(ctx, variant_type));

	// Read back through the boxed path: every non-NULL cell is a
	// VARIANT box; Cast back to the known inner type round-trips.
	for (idx_t row : {idx_t(0), idx_t(1), idx_t(2), idx_t(4)}) {
		auto box = vec.GetValue(row);
		REQUIRE_FALSE(box.IsNull());
	}
	REQUIRE(vec.GetValue(0).ToText() == "42");
	REQUIRE(vec.GetValue(0).Cast(ctx, ctx.ParseType("BIGINT")).Get<int64_t>() == 42);
	REQUIRE(vec.GetValue(1).Cast(ctx, ctx.ParseType("VARCHAR")).Get<varchar_t>().view() == heap_string);
	auto unboxed_list = vec.GetValue(2).Cast(ctx, list_type);
	REQUIRE(unboxed_list.GetChildCount() == 3);
	REQUIRE(unboxed_list.GetChild(2).Get<int32_t>() == 3);
	REQUIRE(vec.GetValue(3).IsNull());
	REQUIRE(vec.GetValue(4).Cast(ctx, ctx.ParseType("BIGINT")).Get<int64_t>() == 43);

	// MakeConstant interplay: the type-equality hardening refuses the
	// raw non-VARIANT value; the boxed value works.
	std::vector<LogicalType> constant_types;
	constant_types.push_back(ctx.ParseType("VARIANT"));
	DataChunk constant_chunk(ctx, constant_types);
	auto cvec = constant_chunk.GetVector(0);
	REQUIRE_THROWS_MATCHES(cvec.MakeConstant(Value::Create(ctx, int64_t(7)), 3), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	auto boxed = Value::Create(ctx, int64_t(7)).Cast(ctx, variant_type);
	cvec.MakeConstant(boxed, 3);
	REQUIRE(cvec.GetValue(2).ToText() == "7");
}
TEST_CASE("Stable C++API: typed Value leaf ctors/getters round trip", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto &ctx = conn.GetContext();

	// Bool / U64 / F64 round-trip through the typed constructors.
	REQUIRE(Value::Create(ctx, bool(true)).Get<bool>());
	REQUIRE_FALSE(Value::Create(ctx, bool(false)).Get<bool>());
	REQUIRE(Value::Create(ctx, uint64_t(18446744073709551615ULL)).Get<uint64_t>() == 18446744073709551615ULL);
	REQUIRE(Value::Create(ctx, double(-2.5)).Get<double>() == -2.5);

	// Blob: arbitrary bytes, including an embedded NUL.
	const uint8_t blob_bytes[] = {0x00, 0xFF, 0x10, 0x00, 0x42};
	auto blob_value = Value::Create(ctx, blob_t(reinterpret_cast<const char *>(blob_bytes), sizeof(blob_bytes)));
	auto blob_read = blob_value.Get<blob_t>();
	REQUIRE(blob_read.size() == sizeof(blob_bytes));
	REQUIRE(std::memcmp(blob_read.data(), blob_bytes, blob_read.size()) == 0);
	// Empty blob is legal too.
	auto empty_blob = Value::Create(ctx, blob_t(nullptr, 0));
	REQUIRE(empty_blob.Get<blob_t>().size() == 0);

	// Date: the engine is the oracle for the days-since-epoch encoding.
	// DATE - DATE yields a BIGINT day count, not an INTEGER.
	auto date_chunk = conn.Execute("SELECT (DATE '2024-03-15' - DATE '1970-01-01')").FetchChunk();
	auto date_days = static_cast<int32_t>(date_chunk.GetVector(0).GetValue(0).Get<int64_t>());
	auto date_value = Value::Create(ctx, date_t {date_days});
	REQUIRE(date_value.ToText() == "2024-03-15");
	REQUIRE(date_value.Get<date_t>().days == date_days);

	// Time: epoch_us() of a 1970-01-01 timestamp gives the time-of-day micros.
	auto time_micros = conn.Execute("SELECT epoch_us(TIMESTAMP '1970-01-01 13:45:30.123456')")
	                       .FetchChunk()
	                       .GetVector(0)
	                       .GetValue(0)
	                       .Get<int64_t>();
	auto time_value = Value::Create(ctx, dtime_t {time_micros});
	REQUIRE(time_value.ToText() == "13:45:30.123456");
	REQUIRE(time_value.Get<dtime_t>().micros == time_micros);

	// Timestamp: epoch_us() is the direct oracle.
	auto ts_micros = conn.Execute("SELECT epoch_us(TIMESTAMP '2024-03-15 13:45:30.123456')")
	                     .FetchChunk()
	                     .GetVector(0)
	                     .GetValue(0)
	                     .Get<int64_t>();
	auto ts_value = Value::Create(ctx, timestamp_t {ts_micros});
	REQUIRE(ts_value.ToText() == "2024-03-15 13:45:30.123456");
	REQUIRE(ts_value.Get<timestamp_t>().micros == ts_micros);

	// TimestampTz: stored as UTC micros since epoch, same as TIMESTAMP;
	// cross-check against a TIMESTAMPTZ literal cast through the engine.
	auto tz_type = ctx.ParseType("TIMESTAMP WITH TIME ZONE");
	auto engine_tz = Value::Create(ctx, varchar_t("2024-03-15 13:45:30.123456+00")).Cast(ctx, tz_type);
	auto tz_value = Value::Create(ctx, timestamp_tz_t {ts_micros});
	REQUIRE(tz_value.Get<timestamp_t>().micros == ts_micros);
	REQUIRE(tz_value.Get<timestamp_t>().micros == engine_tz.Get<timestamp_t>().micros);

	// IntervalLayout: identity round trip plus a canonical-value cross-check.
	auto interval_value = Value::Create(ctx, interval_t {14, 3, 14706789000LL});
	auto decoded = interval_value.Get<interval_t>();
	REQUIRE(decoded.months == 14);
	REQUIRE(decoded.days == 3);
	REQUIRE(decoded.micros == 14706789000LL);

	auto three_days = Value::Create(ctx, varchar_t("3 days")).Cast(ctx, ctx.ParseType("INTERVAL"));
	auto three_days_decoded = three_days.Get<interval_t>();
	REQUIRE(three_days_decoded.months == 0);
	REQUIRE(three_days_decoded.days == 3);
	REQUIRE(three_days_decoded.micros == 0);

	// A getter throws INVALID_INPUT on a type mismatch, keyed on the logical
	// type id, not the payload width.
	REQUIRE_THROWS_MATCHES(Value::Create(ctx, int64_t(1)).Get<date_t>(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	REQUIRE_THROWS_MATCHES(Value::Create(ctx, bool(true)).Get<interval_t>(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
TEST_CASE("Stable C++API: typed Value numeric widths round trip", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto &ctx = conn.GetContext();

	REQUIRE(Value::Create(ctx, uint8_t(255)).Get<uint8_t>() == 255);
	REQUIRE(Value::Create(ctx, uint16_t(65535)).Get<uint16_t>() == 65535);
	REQUIRE(Value::Create(ctx, uint32_t(4000000000U)).Get<uint32_t>() == 4000000000U);
	REQUIRE(Value::Create(ctx, int8_t(-128)).Get<int8_t>() == -128);
	REQUIRE(Value::Create(ctx, int16_t(-32768)).Get<int16_t>() == -32768);
	REQUIRE(Value::Create(ctx, int32_t(-2000000000)).Get<int32_t>() == -2000000000);
	REQUIRE(Value::Create(ctx, float(1.25)).Get<float>() == 1.25f);
}

TEST_CASE("Stable C++API: precise temporal and decimal Values round trip", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto &ctx = conn.GetContext();

	REQUIRE(Value::Create(ctx, dtime_ns_t {123456789}).Get<dtime_ns_t>().nanos == 123456789);
	auto time_tz = dtime_tz_t(12LL * 60 * 60 * 1000000, 90 * 60);
	auto read_time_tz = Value::Create(ctx, time_tz).Get<dtime_tz_t>();
	REQUIRE(read_time_tz.GetMicros() == time_tz.GetMicros());
	REQUIRE(read_time_tz.GetOffset() == time_tz.GetOffset());
	REQUIRE(Value::Create(ctx, timestamp_s_t {-7}).Get<timestamp_s_t>().seconds == -7);
	REQUIRE(Value::Create(ctx, timestamp_ms_t {1234}).Get<timestamp_ms_t>().millis == 1234);
	REQUIRE(Value::Create(ctx, timestamp_ns_t {123456789}).Get<timestamp_ns_t>().nanos == 123456789);
	REQUIRE(Value::Create(ctx, timestamp_tz_t {-42}).Get<timestamp_tz_t>().micros == -42);
	REQUIRE(Value::Create(ctx, timestamp_tz_ns_t {987654321}).Get<timestamp_tz_ns_t>().nanos == 987654321);

	auto small = Value::Create(ctx, decimal_t<4, 2> {-1234});
	REQUIRE(small.Get<4, 2>().value == -1234);
	REQUIRE(small.ToText() == "-12.34");
	auto medium = Value::Create(ctx, decimal_t<9, 3> {123456789});
	REQUIRE(medium.Get<9, 3>().value == 123456789);
	auto large = Value::Create(ctx, decimal_t<18, 4> {-123456789012345678LL});
	REQUIRE(large.Get<18, 4>().value == -123456789012345678LL);
	auto huge = Value::Create(ctx, decimal_t<38, 6> {int128_t {123, 0}});
	REQUIRE(huge.Get<38, 6>().value.lower == 123);
	REQUIRE_THROWS_MATCHES((huge.Get<18, 6>()), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

TEST_CASE("Stable C++API: composite and encoded Values cover empty and non-empty forms", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto &ctx = conn.GetContext();

	auto empty_list = Value::CreateList(ctx, ctx.ParseType("INTEGER"));
	REQUIRE(empty_list.GetLogicalType() == ctx.ParseType("INTEGER[]"));
	REQUIRE(empty_list.GetChildCount() == 0);
	auto empty_map = Value::CreateMap(ctx, ctx.ParseType("VARCHAR"), ctx.ParseType("BIGINT"));
	REQUIRE(empty_map.GetLogicalType() == ctx.ParseType("MAP(VARCHAR, BIGINT)"));
	REQUIRE(empty_map.GetChildCount() == 0);

	std::vector<Value> array_values;
	array_values.push_back(Value::Create(ctx, int32_t(10)));
	array_values.push_back(Value::Create(ctx, int32_t(20)));
	auto array = Value::CreateArray(ctx, array_values);
	REQUIRE(array.GetChildCount() == 2);
	REQUIRE(array[1].Get<int32_t>() == 20);

	std::vector<std::pair<std::string, Value>> fields;
	fields.emplace_back("id", Value::Create(ctx, int32_t(7)));
	fields.emplace_back("name", Value::Create(ctx, varchar_t("duck")));
	auto structure = Value::CreateStruct(ctx, fields);
	REQUIRE(structure.GetChildCount() == 2);
	REQUIRE(structure[1].Get<varchar_t>().view() == "duck");

	auto empty_tuple = Value::CreateTuple(ctx);
	REQUIRE(empty_tuple.GetChildCount() == 0);

	auto bit_value = Value::Create(ctx, varchar_t("101001")).Cast(ctx, ctx.ParseType("BIT"));
	auto bits = bit_value.Get<bit_t>();
	REQUIRE(bits.GetBitCount() == 6);
	REQUIRE(bits.GetBitsSize() == 1);
	REQUIRE(bits.GetPaddingBits() == 2);
	REQUIRE(Value::Create(ctx, bits).ToText() == "101001");

	auto uuid_value =
	    Value::Create(ctx, varchar_t("00112233-4455-6677-8899-aabbccddeeff")).Cast(ctx, ctx.ParseType("UUID"));
	auto uuid = uuid_value.Get<uuid_t>();
	REQUIRE(Value::Create(ctx, uuid).ToText() == uuid_value.ToText());
	REQUIRE(uuid_t::Encode(uuid.Decode()).value.lower == uuid.value.lower);
	REQUIRE(uuid_t::Encode(uuid.Decode()).value.upper == uuid.value.upper);
}
TEST_CASE("Stable C++API: typed Value 128-bit getters round trip", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto &ctx = conn.GetContext();

	// HUGEINT halves: value == upper * 2^64 + lower. 2^64 is {0, 1}.
	auto big = Value::Create(ctx, int128_t({0, 1}));
	REQUIRE(big.ToText() == "18446744073709551616");
	REQUIRE(big.Get<int128_t>().lower == 0);
	REQUIRE(big.Get<int128_t>().upper == 1);

	// A HUGEINT built by the engine reads back into the same halves.
	auto from_engine = Value::Create(ctx, varchar_t("18446744073709551616")).Cast(ctx, ctx.ParseType("HUGEINT"));
	auto halves = from_engine.Get<int128_t>();
	REQUIRE(halves.lower == 0);
	REQUIRE(halves.upper == 1);
	REQUIRE(Value::Create(ctx, int128_t(halves)).ToText() == from_engine.ToText());

	// Negative: -1 == {UINT64_MAX, -1}.
	auto neg = Value::Create(ctx, varchar_t("-1")).Cast(ctx, ctx.ParseType("HUGEINT")).Get<int128_t>();
	REQUIRE(neg.lower == ~static_cast<uint64_t>(0));
	REQUIRE(neg.upper == -1);

	// UHUGEINT: 2^64 is {0, 1}.
	auto ubig = Value::Create(ctx, uint128_t({0, 1}));
	REQUIRE(ubig.ToText() == "18446744073709551616");
	REQUIRE(ubig.Get<uint128_t>().lower == 0);
	REQUIRE(ubig.Get<uint128_t>().upper == 1);

	// UUID decodes to its canonical 16 big-endian bytes (the storage's sort-order
	// high-bit flip is undone), matching the source string exactly.
	auto uuid = Value::Create(ctx, varchar_t("00112233-4455-6677-8899-aabbccddeeff")).Cast(ctx, ctx.ParseType("UUID"));
	REQUIRE(uuid.ToText() == "00112233-4455-6677-8899-aabbccddeeff");
	const uint8_t expected[16] = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
	                              0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff};
	REQUIRE(std::memcmp(uuid.Get<uuid_t>().Decode().bytes, expected, 16) == 0);

	// The getters convert rather than refuse, so a BIGINT reads as a HUGEINT
	// and the two 128-bit widths read as each other.
	REQUIRE(Value::Create(ctx, int64_t(1)).Get<int128_t>().lower == 1);
	REQUIRE(big.Get<uint128_t>().lower == big.Get<int128_t>().lower);
	REQUIRE_THROWS_MATCHES(big.Get<uuid_t>(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
TEST_CASE("Stable C++API: TIME_TZ decodes to micros + offset", "[cpp_api][types_values]") {
	using namespace duckdb::cxx;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto &ctx = conn.GetContext();

	// The engine builds the value; we decode it. 12:30:00 local, +02:00 east.
	auto tz = Value::Create(ctx, varchar_t("12:30:00+02:00")).Cast(ctx, ctx.ParseType("TIME WITH TIME ZONE"));
	auto d = tz.Get<dtime_tz_t>();
	REQUIRE(d.GetMicros() == 45000000000LL); // (12*3600 + 30*60) s
	REQUIRE(d.GetOffset() == 2 * 60 * 60);

	// A western offset.
	auto west = Value::Create(ctx, varchar_t("06:00:00-05:30"))
	                .Cast(ctx, ctx.ParseType("TIME WITH TIME ZONE"))
	                .Get<dtime_tz_t>();
	REQUIRE(west.GetMicros() == 6LL * 60 * 60 * 1000000);
	REQUIRE(west.GetOffset() == -(5 * 60 * 60 + 30 * 60));

	// Type-mismatch guard.
	REQUIRE_THROWS_MATCHES(Value::Create(ctx, int64_t(1)).Get<dtime_tz_t>(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
