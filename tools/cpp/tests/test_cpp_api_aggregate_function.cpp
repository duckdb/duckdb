#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"

#include <atomic>
#include <vector>

// ---------------------------------------------------------------------------
// Stable C++ API tests: AggregateFunction. The extension-registration path is
// covered by the static extension demo (test/api/capi/v2/static_extension);
// these cover the connection path.
// ---------------------------------------------------------------------------

namespace {

using namespace duckdb::cxx;

// Collect a single INTEGER column, asserting every row valid.
std::vector<int32_t> CollectInts(QueryResult result) {
	std::vector<int32_t> out;
	while (auto chunk = result.FetchChunk()) {
		auto view = chunk.GetVector(0).GetView();
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			REQUIRE(view.IsValid(i));
			out.push_back(view.Data<int32_t>()[view.SelAt(i)]);
		}
	}
	return out;
}

// Collect a single BIGINT column, asserting every row valid.
std::vector<int64_t> CollectBigInts(QueryResult result) {
	std::vector<int64_t> out;
	while (auto chunk = result.FetchChunk()) {
		auto view = chunk.GetVector(0).GetView();
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			REQUIRE(view.IsValid(i));
			out.push_back(view.Data<int64_t>()[view.SelAt(i)]);
		}
	}
	return out;
}

// ---------------------------------------------------------------------------
// cpp_sum(INTEGER) -> BIGINT: an int64 running sum kept in each state, plus a
// counter proving the destroy callback ran.
// ---------------------------------------------------------------------------

std::atomic<int> destroy_runs {0};

void SumSize(AggregateFunction::SizeInput &input) {
	input.SetStateSize(sizeof(int64_t));
}

void SumInit(AggregateFunction::InitInput &input) {
	auto states = input.GetStates();
	for (idx_t i = 0; i < input.GetStateCount(); i++) {
		*static_cast<int64_t *>(states[i]) = 0;
	}
}

void SumUpdate(AggregateFunction::UpdateInput &input) {
	const auto a = input.GetArg(0).GetView();
	auto states = input.GetStates();
	for (idx_t i = 0; i < input.GetRowCount(); i++) {
		*static_cast<int64_t *>(states[i]) += a.Data<int32_t>()[a.SelAt(i)];
	}
}

void SumCombine(AggregateFunction::CombineInput &input) {
	auto sources = input.GetSources();
	auto targets = input.GetTargets();
	for (idx_t i = 0; i < input.GetStateCount(); i++) {
		*static_cast<int64_t *>(targets[i]) += *static_cast<const int64_t *>(sources[i]);
	}
}

void SumFinalize(AggregateFunction::FinalizeInput &input) {
	auto states = input.GetStates();
	auto result = input.GetResult();
	auto *out = result.GetDataMutable<int64_t>();
	const auto offset = input.GetResultOffset();
	for (idx_t i = 0; i < input.GetStateCount(); i++) {
		out[offset + i] = *static_cast<const int64_t *>(states[i]);
	}
}

void SumDestroy(AggregateFunction::DestroyInput &input) {
	destroy_runs += static_cast<int>(input.GetStateCount());
}

// ---------------------------------------------------------------------------
// The data slots, one struct per slot, plus counters proving each callback
// ran. Reset by the test that uses them. cpp_flow_sum(a) computes
// sum(a * factor) + offset, the factor from user data and the offset from
// bind data.
// ---------------------------------------------------------------------------

struct Factor {
	int value;
};
struct Offset {
	int value;
	bool operator==(const Offset &other) const {
		return value == other.value;
	}
};

std::atomic<int> bind_runs {0};
std::atomic<int> size_runs {0};
std::atomic<int> init_runs {0};
std::atomic<int> update_runs {0};
std::atomic<int> combine_runs {0};
std::atomic<int> finalize_runs {0};

void FlowBind(AggregateFunction::BindInput &input) {
	bind_runs++;
	const auto &factor = input.GetUserData<Factor>();
	input.SetBindData<Offset>(Offset {factor.value + 7});
}

void FlowSize(AggregateFunction::SizeInput &input) {
	size_runs++;
	// User data and bind data are both readable here.
	(void)input.GetUserData<Factor>();
	(void)input.GetBindData<Offset>();
	input.SetStateSize(sizeof(int64_t));
}

void FlowInit(AggregateFunction::InitInput &input) {
	init_runs++;
	(void)input.GetUserData<Factor>();
	(void)input.GetBindData<Offset>();
	auto states = input.GetStates();
	for (idx_t i = 0; i < input.GetStateCount(); i++) {
		*static_cast<int64_t *>(states[i]) = 0;
	}
}

void FlowUpdate(AggregateFunction::UpdateInput &input) {
	update_runs++;
	const auto factor = input.GetUserData<Factor>().value;
	const auto a = input.GetArg(0).GetView();
	auto states = input.GetStates();
	for (idx_t i = 0; i < input.GetRowCount(); i++) {
		*static_cast<int64_t *>(states[i]) += a.Data<int32_t>()[a.SelAt(i)] * factor;
	}
}

void FlowCombine(AggregateFunction::CombineInput &input) {
	combine_runs++;
	(void)input.GetBindData<Offset>();
	auto sources = input.GetSources();
	auto targets = input.GetTargets();
	for (idx_t i = 0; i < input.GetStateCount(); i++) {
		*static_cast<int64_t *>(targets[i]) += *static_cast<const int64_t *>(sources[i]);
	}
}

void FlowFinalize(AggregateFunction::FinalizeInput &input) {
	finalize_runs++;
	const auto offset_value = input.GetBindData<Offset>().value;
	auto states = input.GetStates();
	auto result = input.GetResult();
	auto *out = result.GetDataMutable<int64_t>();
	const auto offset = input.GetResultOffset();
	for (idx_t i = 0; i < input.GetStateCount(); i++) {
		out[offset + i] = *static_cast<const int64_t *>(states[i]) + offset_value;
	}
}

// Resolves an ANY return type to INTEGER through the binding context. The
// aggregate keeps the last int32 it saw and finalizes to double that value.
void AnyReturnBind(AggregateFunction::BindInput &input) {
	input.SetReturnType(input.GetContext().ParseType("INTEGER"));
}

void LastSize(AggregateFunction::SizeInput &input) {
	input.SetStateSize(sizeof(int32_t));
}

void LastInit(AggregateFunction::InitInput &input) {
	auto states = input.GetStates();
	for (idx_t i = 0; i < input.GetStateCount(); i++) {
		*static_cast<int32_t *>(states[i]) = 0;
	}
}

void LastUpdate(AggregateFunction::UpdateInput &input) {
	const auto a = input.GetArg(0).GetView();
	auto states = input.GetStates();
	for (idx_t i = 0; i < input.GetRowCount(); i++) {
		*static_cast<int32_t *>(states[i]) = a.Data<int32_t>()[a.SelAt(i)];
	}
}

void LastCombine(AggregateFunction::CombineInput &input) {
	auto sources = input.GetSources();
	auto targets = input.GetTargets();
	for (idx_t i = 0; i < input.GetStateCount(); i++) {
		const auto source = *static_cast<const int32_t *>(sources[i]);
		if (source != 0) {
			*static_cast<int32_t *>(targets[i]) = source;
		}
	}
}

void DoubleLastFinalize(AggregateFunction::FinalizeInput &input) {
	auto states = input.GetStates();
	auto result = input.GetResult();
	auto *out = result.GetDataMutable<int32_t>();
	const auto offset = input.GetResultOffset();
	for (idx_t i = 0; i < input.GetStateCount(); i++) {
		out[offset + i] = *static_cast<const int32_t *>(states[i]) * 2;
	}
}

// Fails the query by throwing; the exception class survives the C boundary.
void FailingUpdate(AggregateFunction::UpdateInput &) {
	throw InvalidInputException("aggregate update failed on purpose");
}

// Reads a slot nothing planted: the guard throws rather than derefing null.
void NoUserDataUpdate(AggregateFunction::UpdateInput &input) {
	(void)input.GetUserData<int>();
}

} // namespace

TEST_CASE("Stable C++API: aggregate function registers and executes", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	destroy_runs = 0;

	auto function = AggregateFunction::Create(conn);
	function.SetName("cpp_sum")
		.WithSignature([&](FunctionSignature &sig) {
			sig.AddParameter("a", conn.ParseType("INTEGER")).SetReturnType(conn.ParseType("BIGINT"));
		})
	    .SetSizeCallback(SumSize)
	    .SetInitCallback(SumInit)
	    .SetUpdateCallback(SumUpdate)
	    .SetCombineCallback(SumCombine)
	    .SetFinalizeCallback(SumFinalize)
	    .SetDestroyCallback(SumDestroy);

	// Ungrouped, more rows than one chunk, so the callbacks see full vectors.
	REQUIRE(CollectBigInts(conn.Execute("SELECT cpp_sum(r::INTEGER) FROM range(5000) t(r)")) ==
	        std::vector<int64_t> {5000LL * 4999 / 2});
	// A constant argument vector.
	REQUIRE(CollectBigInts(conn.Execute("SELECT cpp_sum(5) FROM range(10)")) == std::vector<int64_t> {50});
	// Grouped: every row lands in its group's state; result offsets are
	// exercised by finalizing many groups.
	REQUIRE(CollectBigInts(conn.Execute("SELECT sum(s)::BIGINT FROM (SELECT cpp_sum(r::INTEGER) AS s "
	                                    "FROM range(5000) t(r) GROUP BY r % 3000)")) ==
	        std::vector<int64_t> {5000LL * 4999 / 2});
	// The destroy callback ran on the finished states.
	REQUIRE(destroy_runs >= 1);
}

TEST_CASE("Stable C++API: aggregate function data flows user->bind->callbacks", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	bind_runs = 0;
	size_runs = 0;
	init_runs = 0;
	update_runs = 0;
	combine_runs = 0;
	finalize_runs = 0;

	auto function = AggregateFunction::Create(conn);
	function.SetName("cpp_flow_sum").SetUserData<Factor>(Factor {3});
	function.GetSignature().AddParameter("a", conn.ParseType("INTEGER")).SetReturnType(conn.ParseType("BIGINT"));
	function.SetBindCallback(FlowBind)
	    .SetSizeCallback(FlowSize)
	    .SetInitCallback(FlowInit)
	    .SetUpdateCallback(FlowUpdate)
	    .SetCombineCallback(FlowCombine)
	    .SetFinalizeCallback(FlowFinalize);
	function.Register();

	// 5 * 3 + (3 + 7) = 25
	REQUIRE(CollectBigInts(conn.Execute("SELECT cpp_flow_sum(5)")) == std::vector<int64_t> {25});
	REQUIRE(bind_runs == 1);
	REQUIRE(size_runs >= 1);
	REQUIRE(init_runs >= 1);
	REQUIRE(update_runs >= 1);
	REQUIRE(finalize_runs >= 1);

	// A second query binds afresh; the user data planted at registration is still there.
	// (0 + 1 + 2) * 3 + 10 = 19
	REQUIRE(CollectBigInts(conn.Execute("SELECT cpp_flow_sum(r::INTEGER) FROM range(3) t(r)")) ==
	        std::vector<int64_t> {19});
	REQUIRE(bind_runs == 2);
}

TEST_CASE("Stable C++API: aggregate function bind resolves an ANY return type", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto function = AggregateFunction::Create(conn);
	function.SetName("cpp_double_last")
	    .SetBindCallback(AnyReturnBind)
	    .SetSizeCallback(LastSize)
	    .SetInitCallback(LastInit)
	    .SetUpdateCallback(LastUpdate)
	    .SetCombineCallback(LastCombine)
	    .SetFinalizeCallback(DoubleLastFinalize);
	function.GetSignature()
	    .AddParameter("a", conn.ParseType("INTEGER"))
	    .SetReturnType(conn.CreateType(LogicalTypeId::ANY));
	function.Register();

	auto result = conn.Execute("SELECT cpp_double_last(21)");
	REQUIRE(result.GetSchema().GetFieldType(0).ToText() == "INTEGER");
	REQUIRE(CollectInts(std::move(result)) == std::vector<int32_t> {42});
}

TEST_CASE("Stable C++API: aggregate function callback errors fail the query", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto integer = conn.ParseType("INTEGER");

	auto failing = AggregateFunction::Create(conn);
	failing.SetName("cpp_agg_fail")
	    .SetSizeCallback(LastSize)
	    .SetInitCallback(LastInit)
	    .SetUpdateCallback(FailingUpdate)
	    .SetCombineCallback(LastCombine)
	    .SetFinalizeCallback(DoubleLastFinalize);
	failing.GetSignature().AddParameter("a", integer).SetReturnType(integer);
	failing.Register();

	REQUIRE_THROWS_MATCHES(conn.Execute("SELECT cpp_agg_fail(1)").Drain(), InvalidInputException,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// GetUserData when nothing was planted reports the misuse rather than derefing null.
	auto no_data = AggregateFunction::Create(conn);
	no_data.SetName("cpp_agg_no_user_data")
	    .SetSizeCallback(LastSize)
	    .SetInitCallback(LastInit)
	    .SetUpdateCallback(NoUserDataUpdate)
	    .SetCombineCallback(LastCombine)
	    .SetFinalizeCallback(DoubleLastFinalize);
	no_data.GetSignature().AddParameter("a", integer).SetReturnType(integer);
	no_data.Register();

	REQUIRE_THROWS_MATCHES(conn.Execute("SELECT cpp_agg_no_user_data(1)").Drain(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

TEST_CASE("Stable C++API: aggregate function registration validation", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto integer = conn.ParseType("INTEGER");

	// No name.
	{
		auto function = AggregateFunction::Create(conn);
		function.SetSizeCallback(SumSize)
		    .SetInitCallback(SumInit)
		    .SetUpdateCallback(SumUpdate)
		    .SetCombineCallback(SumCombine)
		    .SetFinalizeCallback(SumFinalize);
		function.GetSignature().AddParameter("a", integer).SetReturnType(integer);
		REQUIRE_THROWS_MATCHES(function.Register(), InvalidInputException, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}

	// A required callback missing.
	{
		auto function = AggregateFunction::Create(conn);
		function.SetName("cpp_no_update")
		    .SetSizeCallback(SumSize)
		    .SetInitCallback(SumInit)
		    .SetCombineCallback(SumCombine)
		    .SetFinalizeCallback(SumFinalize);
		function.GetSignature().AddParameter("a", integer).SetReturnType(integer);
		REQUIRE_THROWS_MATCHES(function.Register(), InvalidInputException, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}

	// An ANY return type without a bind callback to resolve it.
	{
		auto function = AggregateFunction::Create(conn);
		function.SetName("cpp_agg_unresolved_any")
		    .SetSizeCallback(SumSize)
		    .SetInitCallback(SumInit)
		    .SetUpdateCallback(SumUpdate)
		    .SetCombineCallback(SumCombine)
		    .SetFinalizeCallback(SumFinalize);
		function.GetSignature().AddParameter("a", integer).SetReturnType(conn.CreateType(LogicalTypeId::ANY));
		REQUIRE_THROWS_MATCHES(function.Register(), InvalidInputException, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}
}
