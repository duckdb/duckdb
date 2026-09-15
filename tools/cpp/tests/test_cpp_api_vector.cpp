#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"
#include "test_helpers.hpp"

// Internal engine headers: used only to construct a DICTIONARY vector, which
// no public surface builds directly. Tests are the sanctioned mixing point.
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>

// ---------------------------------------------------------------------------
// Stable C++ API tests: the vector read and write surface.
// ---------------------------------------------------------------------------

namespace {

// Read a transparent duckdb_v2_bytes slot's bytes (inlined or heap form).
// Mirrors V2StringView in capi_v2_test_helpers.hpp; kept separate because
// the C++ suite does not include the V1-including C helpers header.
std::string SlotBytes(const duckdb_v2_bytes &s) {
	uint32_t len = s.value.inlined.length;
	const char *ptr = len <= DUCKDB_V2_BYTES_INLINE_LENGTH ? s.value.inlined.inlined : s.value.pointer.ptr;
	return std::string(ptr, len);
}

} // namespace
TEST_CASE("Stable C++API: Vector AssignString", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("VARCHAR"));

	DataChunk chunk(types);
	auto vec = chunk.GetVector(0);
	vec.SetSize(3);

	// AssignString resolves the heap once and reuses it for the rest.
	vec.AssignString(0, "hi"); // inlined
	const std::string long_str(100, 'x');
	vec.AssignString(1, long_str); // copied into the heap
	vec.AssignString(2, "");       // empty

	// The cpp_api has no VARCHAR read path yet; read the transparent
	// duckdb_v2_bytes slots directly to confirm the bytes round-trip.
	auto *slots = vec.GetDataMutable<duckdb_v2_bytes>();
	REQUIRE(SlotBytes(slots[0]) == "hi");
	REQUIRE(SlotBytes(slots[1]) == long_str);
	REQUIRE(SlotBytes(slots[2]).empty());
}
TEST_CASE("Stable C++API: Vector AssignString over a batch of slots", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("VARCHAR"));

	DataChunk chunk(types);
	auto vec = chunk.GetVector(0);
	vec.SetSize(3);

	// A single write, then a bulk write starting at index 1; both share the
	// cached heap. The bulk batch mixes an inlined and a heap-allocated value.
	vec.AssignString(0, "head");
	const std::vector<std::string> owned = {"second", "this tail value is comfortably longer than twelve bytes"};
	const std::vector<std::string_view> tail(owned.begin(), owned.end());
	for (idx_t i = 0; i < tail.size(); i++) {
		vec.AssignString(1 + i, tail[i]);
	}

	auto *slots = vec.GetDataMutable<duckdb_v2_bytes>();
	REQUIRE(SlotBytes(slots[0]) == "head");
	REQUIRE(SlotBytes(slots[1]) == owned[0]);
	REQUIRE(SlotBytes(slots[2]) == owned[1]);
}
TEST_CASE("Stable C++API: StringHeap primitive (dedup + scatter)", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("VARCHAR"));

	DataChunk chunk(types);
	auto vec = chunk.GetVector(0);
	vec.SetSize(4);

	auto heap = vec.GetHeap();

	// Dedup: intern a (non-inlined) value once, reference it from many slots.
	const std::string shared_str = "this is a shared value, longer than twelve bytes";
	auto shared = heap.AddString(shared_str);
	vec.SetString(0, shared);
	vec.SetString(2, shared);

	// Intern each, then scatter the tokens into arbitrary positions.
	const std::vector<std::string> owned = {"x", "another longer-than-inline string value"};
	std::vector<varchar_t> tokens;
	for (const auto &value : owned) {
		tokens.push_back(heap.AddString(value));
	}
	vec.SetString(3, tokens[0]);
	vec.SetString(1, tokens[1]);

	auto *slots = vec.GetDataMutable<duckdb_v2_bytes>();
	REQUIRE(SlotBytes(slots[0]) == shared_str);
	REQUIRE(SlotBytes(slots[1]) == owned[1]);
	REQUIRE(SlotBytes(slots[2]) == shared_str);
	REQUIRE(SlotBytes(slots[3]) == "x");
}
TEST_CASE("Stable C++API: StringHeap::Allocate write-in-place", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("VARCHAR"));

	DataChunk chunk(types);
	auto vec = chunk.GetVector(0);
	vec.SetSize(2);

	auto heap = vec.GetHeap();

	// Write-in-place: generate bytes straight into the heap, then build a token over them.
	const uint32_t len = 64;
	auto *bytes = heap.Allocate(len);
	REQUIRE(bytes != nullptr);
	std::memset(bytes, 'q', len);
	varchar_t token(reinterpret_cast<char *>(bytes), len);
	REQUIRE_FALSE(token.is_inlined());
	REQUIRE(token.size() == len);
	REQUIRE(token.data() == reinterpret_cast<const char *>(bytes));
	vec.SetString(0, token);

	// Inlined token: the bytes live in the value itself.
	auto small = heap.AddString("tiny");
	REQUIRE(small.is_inlined());
	REQUIRE(small.size() == 4);
	REQUIRE(std::string(small.data(), small.size()) == "tiny");
	vec.SetString(1, small);

	auto *slots = vec.GetDataMutable<duckdb_v2_bytes>();
	REQUIRE(SlotBytes(slots[0]) == std::string(len, 'q'));
	REQUIRE(SlotBytes(slots[1]) == "tiny");
}
TEST_CASE("Stable C++API: AssignString rejects misuse", "[cpp_api]") {
	using namespace duckdb::cxx;

	// A non-string vector has no heap: AssignString surfaces INVALID_INPUT.
	{
		Environment env;
		auto db = env.Open(":memory:");
		auto conn = db.Connect();
		std::vector<LogicalType> types;
		types.push_back(conn.ParseType("INTEGER"));
		DataChunk chunk(types);
		auto vec = chunk.GetVector(0);
		vec.SetSize(1);
		REQUIRE_THROWS_MATCHES(vec.AssignString(0, "x"), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}

	// A CONSTANT vector's data array holds one slot: only index 0 is writable.
	{
		Environment env;
		auto db = env.Open(":memory:");
		auto conn = db.Connect();
		std::vector<LogicalType> types;
		types.push_back(conn.ParseType("VARCHAR"));
		DataChunk chunk(types);
		auto vec = chunk.GetVector(0);
		vec.MakeConstant(Value::Create(conn, varchar_t("const")), 2);
		REQUIRE_THROWS_MATCHES(vec.AssignString(1, "x"), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
		REQUIRE_NOTHROW(vec.AssignString(0, "ok"));
	}
}
#if (STANDARD_VECTOR_SIZE > 10)
TEST_CASE("Stable C++API: VectorView NULL-aware read of a queried chunk", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto result = conn.Execute("SELECT CASE WHEN i % 3 = 0 THEN NULL ELSE i END AS v FROM range(10) t(i)");
	auto chunk = result.FetchChunk();
	REQUIRE(chunk);

	auto vec = chunk.GetVector(0);
	REQUIRE(vec.GetVectorType() == VectorType::FLAT);

	auto view = vec.GetView();
	REQUIRE(view.count == 10);
	REQUIRE(view.sel == nullptr); // FLAT: identity
	auto data = view.Data<int64_t>();
	for (idx_t i = 0; i < view.count; i++) {
		if (i % 3 == 0) {
			REQUIRE_FALSE(view.IsValid(i));
			continue;
		}
		REQUIRE(view.IsValid(i));
		REQUIRE(data[view.SelAt(i)] == static_cast<int64_t>(i));
	}
}
#endif

#if (STANDARD_VECTOR_SIZE > 3)
TEST_CASE("Stable C++API: VectorView CONSTANT without flatten", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("BIGINT"));
	DataChunk chunk(types);
	auto vec = chunk.GetVector(0);

	vec.MakeConstant(Value::Create(conn, int64_t(7)), 4);
	REQUIRE(vec.GetVectorType() == VectorType::CONSTANT);

	auto view = vec.GetView();
	REQUIRE(view.count == 4);
	REQUIRE(view.sel != nullptr); // zero singleton, not identity
	auto data = view.Data<int64_t>();
	for (idx_t i = 0; i < view.count; i++) {
		REQUIRE(view.SelAt(i) == 0); // every row resolves to the one slot
		REQUIRE(view.IsValid(i));
		REQUIRE(data[view.SelAt(i)] == 7);
	}
	// The view did not flatten.
	REQUIRE(vec.GetVectorType() == VectorType::CONSTANT);
}
#endif

#if (STANDARD_VECTOR_SIZE > 3)
TEST_CASE("Stable C++API: VectorView DICTIONARY resolves validity through sel", "[cpp_api]") {
	using namespace duckdb::cxx;

	// Built via core (the vector handle is identity = duckdb::Vector *): a
	// FLAT dictionary with physical row 1 NULL, sliced so logical row 3
	// dispatches to it.
	duckdb::Vector flat(duckdb::LogicalType::INTEGER);
	auto *fd = duckdb::FlatVector::GetDataMutable<int32_t>(flat);
	fd[0] = 10;
	fd[1] = 20;
	fd[2] = 30;
	duckdb::FlatVector::SetNull(flat, 1, true);

	duckdb::SelectionVector sel(4);
	sel.set_index(0, 2);
	sel.set_index(1, 0);
	sel.set_index(2, 2);
	sel.set_index(3, 1);
	duckdb::Vector dict(flat, sel, 4);

	auto vec = detail::Factory::Make<Vector>(reinterpret_cast<duckdb_v2_vector_handle>(&dict));
	REQUIRE(vec.GetVectorType() == VectorType::DICTIONARY);

	auto view = vec.GetView();
	REQUIRE(view.count == 4);
	REQUIRE(view.sel != nullptr); // the dictionary's own sel
	auto data = view.Data<int32_t>();

	// Reads dispatch through sel.
	REQUIRE(view.SelAt(0) == 2);
	REQUIRE(data[view.SelAt(0)] == 30);
	REQUIRE(data[view.SelAt(1)] == 10);
	REQUIRE(data[view.SelAt(2)] == 30);

	// Validity follows sel: logical row 3 is NULL (physical 1), while the
	// naive physical read of row 3 would answer valid.
	REQUIRE_FALSE(view.IsValid(3));
	REQUIRE(view.RowIsValid(3));
	for (idx_t i = 0; i < 3; i++) {
		REQUIRE(view.IsValid(i));
	}

	// The view did not flatten the parent.
	REQUIRE(vec.GetVectorType() == VectorType::DICTIONARY);
}
#endif

#if (STANDARD_VECTOR_SIZE > 3)
TEST_CASE("Stable C++API: MakeSequence and MakeConstant round-trip", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("BIGINT"));
	types.push_back(conn.ParseType("BIGINT"));
	DataChunk chunk(types);

	// A SEQUENCE reads as Other; Flatten materialises it to FLAT.
	auto seq = chunk.GetVector(0);
	seq.MakeSequence(100, 2, 4);
	REQUIRE(seq.GetVectorType() == VectorType::OTHER);
	seq.Flatten();
	REQUIRE(seq.GetVectorType() == VectorType::FLAT);
	auto view = seq.GetView();
	REQUIRE(view.count == 4);
	auto data = view.Data<int64_t>();
	for (idx_t i = 0; i < view.count; i++) {
		REQUIRE(data[view.SelAt(i)] == 100 + 2 * static_cast<int64_t>(i));
	}

	// A CONSTANT holds one slot referenced by every logical row.
	auto con = chunk.GetVector(1);
	con.MakeConstant(Value::Create(conn, int64_t(-5)), 3);
	REQUIRE(con.GetVectorType() == VectorType::CONSTANT);
	auto cview = con.GetView();
	REQUIRE(cview.count == 3);
	REQUIRE(cview.Data<int64_t>()[cview.SelAt(2)] == -5);
}
#endif

#if (STANDARD_VECTOR_SIZE > 2)
#ifndef DUCKDB_DEBUG_NO_INLINE
TEST_CASE("Stable C++API: VectorView VARCHAR and BLOB reads via blob_t", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	const std::string long_str = "this string is comfortably longer than twelve bytes";
	auto result = conn.Execute("SELECT v, v::BLOB AS b FROM (VALUES ('tiny'), ('" + long_str + "'), (NULL)) t(v)");
	auto chunk = result.FetchChunk();
	REQUIRE(chunk);

	auto view = chunk.GetVector(0).GetView();
	REQUIRE(view.count == 3);
	auto strings = view.Data<blob_t>();

	const auto &small = strings[view.SelAt(0)];
	REQUIRE(view.IsValid(0));
	REQUIRE(small.is_inlined());
	REQUIRE(small.view() == "tiny");

	const auto &large = strings[view.SelAt(1)];
	REQUIRE(view.IsValid(1));
	REQUIRE_FALSE(large.is_inlined());
	REQUIRE(large.view() == long_str);

	REQUIRE_FALSE(view.IsValid(2));

	// BLOB shares the storage layout; the bytes read the same way.
	auto bview = chunk.GetVector(1).GetView();
	auto blobs = bview.Data<blob_t>();
	REQUIRE(blobs[bview.SelAt(0)].view() == "tiny");
	REQUIRE(blobs[bview.SelAt(1)].view() == long_str);
	REQUIRE_FALSE(bview.IsValid(2));
}
#endif
#endif

#if (STANDARD_VECTOR_SIZE > 2)
TEST_CASE("Stable C++API: validity write round-trip", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("INTEGER"));
	types.push_back(conn.ParseType("BIGINT"));
	DataChunk chunk(types);

	// FLAT: ValidityMask writes are observed by the read view.
	auto vec = chunk.GetVector(0);
	vec.SetSize(3);
	auto data = vec.GetDataMutable<int32_t>();
	data[0] = 1;
	data[1] = 2;
	data[2] = 3;

	auto mask = vec.GetValidityMutable();
	REQUIRE(mask.words != nullptr);
	mask.SetInvalid(1);
	REQUIRE(mask.RowIsValid(0));
	REQUIRE_FALSE(mask.RowIsValid(1));

	auto view = vec.GetView();
	REQUIRE(view.IsValid(0));
	REQUIRE_FALSE(view.IsValid(1));
	REQUIRE(view.IsValid(2));

	mask.SetValid(1);
	REQUIRE(vec.GetView().IsValid(1));

	// CONSTANT: SetConstantValid flips the single bit for every row.
	auto con = chunk.GetVector(1);
	con.MakeConstant(Value::Create(conn, int64_t(9)), 4);
	con.SetConstantValid(false);
	auto cview = con.GetView();
	for (idx_t i = 0; i < 4; i++) {
		REQUIRE_FALSE(cview.IsValid(i));
	}
	con.SetConstantValid(true);
	REQUIRE(con.GetView().IsValid(0));
}
#endif

TEST_CASE("Stable C++API: validity mask word-boundary rows", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("INTEGER"));
	DataChunk chunk(types);
	auto vec = chunk.GetVector(0);
	vec.SetSize(130); // spans three 64-row validity words

	// Clear the bits adjacent to both word boundaries (63|64 and 127|128).
	auto mask = vec.GetValidityMutable();
	for (idx_t row : {idx_t(63), idx_t(64), idx_t(127), idx_t(128)}) {
		mask.SetInvalid(row);
		REQUIRE_FALSE(mask.RowIsValid(row));
	}
	// Neighbours in the adjacent words are untouched: no cross-word bleed.
	for (idx_t row : {idx_t(0), idx_t(62), idx_t(65), idx_t(126), idx_t(129)}) {
		REQUIRE(mask.RowIsValid(row));
	}

	// A fresh view observes the same bits through VectorView's own bit math.
	auto view = vec.GetView();
	REQUIRE(view.count == 130);
	for (idx_t row : {idx_t(63), idx_t(64), idx_t(127), idx_t(128)}) {
		REQUIRE_FALSE(view.IsValid(row));
	}
	for (idx_t row : {idx_t(0), idx_t(62), idx_t(65), idx_t(126), idx_t(129)}) {
		REQUIRE(view.IsValid(row));
	}

	// Flip one bit per word back; its boundary partner stays invalid.
	mask.SetValid(64);
	mask.SetValid(127);
	auto reread = vec.GetView();
	REQUIRE(reread.IsValid(64));
	REQUIRE(reread.IsValid(127));
	REQUIRE_FALSE(reread.IsValid(63));
	REQUIRE_FALSE(reread.IsValid(128));
}
TEST_CASE("Stable C++API: VectorView::AllValid ignores bits past the row count", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("INTEGER"));
	DataChunk chunk(types);
	auto vec = chunk.GetVector(0);
	vec.SetSize(70); // a full word plus a 6-row tail

	// Without a mask, the view reports all valid.
	REQUIRE(vec.GetView().AllValid());

	// Rebuild the mask the born-invalid way: SetAllInvalid also clears the bits past row 69 in the tail word, so an
	// all-valid verdict must not depend on them.
	auto mask = vec.GetValidityMutable();
	mask.SetAllInvalid(70);
	REQUIRE_FALSE(vec.GetView().AllValid());
	for (idx_t row = 0; row < 70; row++) {
		mask.SetValid(row);
	}
	REQUIRE(vec.GetView().AllValid());

	// One NULL row, in the tail word, flips the verdict.
	mask.SetInvalid(69);
	REQUIRE_FALSE(vec.GetView().AllValid());
}
TEST_CASE("Stable C++API: vector read surface rejects misuse", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("BIGINT"));
	types.push_back(conn.ParseType("BIGINT"));
	DataChunk chunk(types);

	// GetView rejects VectorType::OTHER (a SEQUENCE) until flattened.
	auto seq = chunk.GetVector(0);
	seq.MakeSequence(0, 1, 4);
	REQUIRE(seq.GetVectorType() == VectorType::OTHER);
	REQUIRE_THROWS_MATCHES(seq.GetView(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// GetValidityMutable is FLAT-only.
	auto con = chunk.GetVector(1);
	con.MakeConstant(Value::Create(conn, int64_t(1)), 2);
	REQUIRE_THROWS_MATCHES(con.GetValidityMutable(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// SetConstantValid is CONSTANT-only.
	seq.Flatten();
	REQUIRE_THROWS_MATCHES(seq.SetConstantValid(true), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

TEST_CASE("Stable C++API: Vector SetNull recurses into nested children", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("STRUCT(name VARCHAR, score DOUBLE)"));

	DataChunk chunk(types);
	auto vec = chunk.GetVector(0);
	vec.SetSize(3);

	auto name_vec = vec.GetChild(0);
	auto score_vec = vec.GetChild(1);
	auto *scores = score_vec.GetDataMutable<double>();
	for (idx_t i = 0; i < 3; i++) {
		name_vec.AssignString(i, "n");
		scores[i] = static_cast<double>(i);
	}

	vec.SetNull(1);

	// The NULL broadcasts into both field slots; other rows keep values.
	REQUIRE_FALSE(vec.GetView().RowIsValid(1));
	REQUIRE_FALSE(name_vec.GetView().RowIsValid(1));
	REQUIRE_FALSE(score_vec.GetView().RowIsValid(1));
	REQUIRE(vec.GetView().RowIsValid(0));
	REQUIRE(name_vec.GetView().RowIsValid(2));
	REQUIRE(score_vec.GetView().Data<double>()[2] == 2.0);

	// FLAT-only and bounds-checked, matching the C contract.
	REQUIRE_THROWS_MATCHES(vec.SetNull(3), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

TEST_CASE("Stable C++API: ValidityMask SetAllInvalid born-invalid pattern", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("VARCHAR"));

	DataChunk chunk(types);
	auto vec = chunk.GetVector(0);
	// Cross a word boundary so more than one mask word is cleared.
	vec.SetSize(70);

	// Born-invalid: clear everything up front, then earn validity by
	// writing. Rows never written stay NULL without further bookkeeping.
	auto validity = vec.GetValidityMutable();
	validity.SetAllInvalid(70);

	vec.AssignString(3, "three");
	validity.SetValid(3);
	vec.AssignString(64, "sixty-four");
	validity.SetValid(64);

	auto view = vec.GetView();
	for (idx_t i = 0; i < 70; i++) {
		REQUIRE(view.RowIsValid(i) == (i == 3 || i == 64));
	}
}

TEST_CASE("Stable C++API: ValidityMask SetAllValid born-valid and reset", "[cpp_api]") {
	using namespace duckdb::cxx;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("INTEGER"));

	DataChunk chunk(types);
	auto vec = chunk.GetVector(0);
	// Cross a word boundary so more than one mask word is touched.
	vec.SetSize(70);
	auto *data = vec.GetDataMutable<int32_t>();
	auto validity = vec.GetValidityMutable();

	// Born-valid: mark everything valid, then only clear the nulls. Fewer
	// writes than born-invalid when most rows carry a value.
	validity.SetAllValid(70);
	for (idx_t i = 0; i < 70; i++) {
		data[i] = static_cast<int32_t>(i);
	}
	validity.SetInvalid(5);
	validity.SetInvalid(64);

	auto view = vec.GetView();
	for (idx_t i = 0; i < 70; i++) {
		REQUIRE(view.RowIsValid(i) == (i != 5 && i != 64));
	}

	// Reuse: SetAllValid restores every row, clearing the two nulls from the
	// previous fill without touching them one by one.
	validity.SetAllValid(70);
	auto reset_view = vec.GetView();
	for (idx_t i = 0; i < 70; i++) {
		REQUIRE(reset_view.RowIsValid(i));
	}
}
