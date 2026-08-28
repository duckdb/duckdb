#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"

#include <atomic>
#include <vector>

// ---------------------------------------------------------------------------
// Stable C++ API tests: ScalarFunction and FunctionSignature. The
// extension-registration path is covered by the static extension demo
// (test/api/capi/v2/static_extension); these cover the connection path.
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

// out[i] = a[i] + 1
void PlusOneExec(ScalarFunction::ExecInput &input) {
	const auto a = input.GetArg(0).GetView();
	auto result = input.GetResult();
	auto *out = result.GetDataMutable<int32_t>();
	for (idx_t i = 0; i < input.GetRowCount(); i++) {
		out[i] = a.Data<int32_t>()[a.SelAt(i)] + 1;
	}
}

// The three data slots, one struct per slot, plus counters proving each
// callback ran. Reset by the test that uses them.
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
std::atomic<int> init_runs {0};
std::atomic<int> exec_runs {0};

void FlowBind(ScalarFunction::BindInput &input) {
	bind_runs++;
	const auto &factor = input.GetUserData<Factor>();
	input.SetBindData<Offset>(Offset {factor.value + 7});
}

void FlowInit(ScalarFunction::InitInput &input) {
	init_runs++;
	// User data and bind data are both readable here.
	(void)input.GetUserData<Factor>();
	input.SetInitData<int>(input.GetBindData<Offset>().value);
}

// out[i] = a[i] * factor + offset
void FlowExec(ScalarFunction::ExecInput &input) {
	exec_runs++;
	const auto factor = input.GetUserData<Factor>().value;
	REQUIRE(input.GetBindData<Offset>().value == input.GetInitData<int>());
	const auto offset = input.GetInitData<int>();
	const auto a = input.GetArg(0).GetView();
	auto result = input.GetResult();
	auto *out = result.GetDataMutable<int32_t>();
	for (idx_t i = 0; i < input.GetRowCount(); i++) {
		out[i] = a.Data<int32_t>()[a.SelAt(i)] * factor + offset;
	}
}

// out[i] = sum over every argument: the fixed parameter plus the variadic tail.
void VsumExec(ScalarFunction::ExecInput &input) {
	auto result = input.GetResult();
	auto *out = result.GetDataMutable<int32_t>();
	const auto count = input.GetRowCount();
	const auto arg_count = input.GetArgCount();
	for (idx_t col = 0; col < arg_count; col++) {
		const auto view = input.GetArg(col).GetView();
		for (idx_t i = 0; i < count; i++) {
			const auto value = view.Data<int32_t>()[view.SelAt(i)];
			out[i] = col == 0 ? value : out[i] + value;
		}
	}
}

// Bind data without operator==: the wrapper falls back to comparing slots by
// identity instead of requiring one.
struct OpaqueOffset {
	int value;
};

void OpaqueBind(ScalarFunction::BindInput &input) {
	input.SetBindData<OpaqueOffset>(OpaqueOffset {100});
}

// out[i] = a[i] + opaque offset
void OpaqueExec(ScalarFunction::ExecInput &input) {
	const auto offset = input.GetBindData<OpaqueOffset>().value;
	const auto a = input.GetArg(0).GetView();
	auto result = input.GetResult();
	auto *out = result.GetDataMutable<int32_t>();
	for (idx_t i = 0; i < input.GetRowCount(); i++) {
		out[i] = a.Data<int32_t>()[a.SelAt(i)] + offset;
	}
}

// Resolves an ANY return type to INTEGER through the binding context.
void AnyReturnBind(ScalarFunction::BindInput &input) {
	input.SetReturnType(input.GetContext().ParseType("INTEGER"));
}

// out[i] = a[i] * 2
void DoubleExec(ScalarFunction::ExecInput &input) {
	const auto a = input.GetArg(0).GetView();
	auto result = input.GetResult();
	auto *out = result.GetDataMutable<int32_t>();
	for (idx_t i = 0; i < input.GetRowCount(); i++) {
		out[i] = a.Data<int32_t>()[a.SelAt(i)] * 2;
	}
}

// Fails the query by throwing; the exception class survives the C boundary.
void FailingExec(ScalarFunction::ExecInput &) {
	throw InvalidInputException("scalar exec failed on purpose");
}

// Reads a slot nothing planted: the guard throws rather than derefing null.
void NoUserDataExec(ScalarFunction::ExecInput &input) {
	(void)input.GetUserData<int>();
}

} // namespace

TEST_CASE("Stable C++API: scalar function registers and executes", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto integer = conn.ParseType("INTEGER");

	auto function = ScalarFunction::Create(conn);
	function.SetName("plus_one").SetExecCallback(PlusOneExec);
	function.GetSignature().AddParameter("a", integer).SetReturnType(integer);
	function.Register();

	REQUIRE(CollectInts(conn.Execute("SELECT plus_one(41)")) == std::vector<int32_t> {42});
	REQUIRE(CollectInts(conn.Execute("SELECT plus_one(r::INTEGER) FROM range(4) t(r)")) ==
	        std::vector<int32_t> {1, 2, 3, 4});
}

TEST_CASE("Stable C++API: scalar function data flows user->bind->init->exec", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto integer = conn.ParseType("INTEGER");

	bind_runs = 0;
	init_runs = 0;
	exec_runs = 0;

	auto function = ScalarFunction::Create(conn);
	function.SetName("cpp_flow").SetUserData<Factor>(Factor {3});
	function.GetSignature().AddParameter("a", integer).SetReturnType(integer);
	function.SetBindCallback(FlowBind).SetInitCallback(FlowInit).SetExecCallback(FlowExec);
	function.Register();

	// 5 * 3 + (3 + 7) = 25
	REQUIRE(CollectInts(conn.Execute("SELECT cpp_flow(5)")) == std::vector<int32_t> {25});
	REQUIRE(bind_runs == 1);
	REQUIRE(init_runs >= 1);
	REQUIRE(exec_runs >= 1);

	// A second query binds afresh; the user data planted at registration is still there.
	auto rows = CollectInts(conn.Execute("SELECT cpp_flow(r::INTEGER) FROM range(3) t(r)"));
	REQUIRE(rows == std::vector<int32_t> {10, 13, 16});
	REQUIRE(bind_runs == 2);
}

TEST_CASE("Stable C++API: scalar function bind data without operator== compares by identity", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto integer = conn.ParseType("INTEGER");

	auto function = ScalarFunction::Create(conn);
	function.SetName("cpp_opaque").SetBindCallback(OpaqueBind).SetExecCallback(OpaqueExec);
	function.GetSignature().AddParameter("a", integer).SetReturnType(integer);
	function.Register();

	REQUIRE(CollectInts(conn.Execute("SELECT cpp_opaque(1)")) == std::vector<int32_t> {101});
	// Two calls in one query mean two bind-data instances that the engine may compare.
	REQUIRE(CollectInts(conn.Execute("SELECT cpp_opaque(r::INTEGER) + cpp_opaque(0) FROM range(3) t(r)")) ==
	        std::vector<int32_t> {200, 201, 202});
}

TEST_CASE("Stable C++API: scalar function variadic tail via GetArgCount", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto integer = conn.ParseType("INTEGER");

	auto function = ScalarFunction::Create(conn);
	function.SetName("cpp_vsum").SetExecCallback(VsumExec);
	function.GetSignature().AddParameter("a", integer).SetVarArgs(integer).SetReturnType(integer);
	function.Register();

	REQUIRE(CollectInts(conn.Execute("SELECT cpp_vsum(1)")) == std::vector<int32_t> {1});
	REQUIRE(CollectInts(conn.Execute("SELECT cpp_vsum(1, 2, 3, 4)")) == std::vector<int32_t> {10});
	REQUIRE(CollectInts(conn.Execute("SELECT cpp_vsum(r::INTEGER, 10, 100) FROM range(2) t(r)")) ==
	        std::vector<int32_t> {110, 111});
}

TEST_CASE("Stable C++API: scalar function bind resolves an ANY return type", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto function = ScalarFunction::Create(conn);
	function.SetName("cpp_double").SetBindCallback(AnyReturnBind).SetExecCallback(DoubleExec);
	function.GetSignature()
	    .AddParameter("a", conn.ParseType("INTEGER"))
	    .SetReturnType(conn.CreateType(LogicalTypeId::ANY));
	function.Register();

	auto result = conn.Execute("SELECT cpp_double(21)");
	REQUIRE(result.GetSchema().GetFieldType(0).ToText() == "INTEGER");
	REQUIRE(CollectInts(std::move(result)) == std::vector<int32_t> {42});
}

TEST_CASE("Stable C++API: scalar function callback errors fail the query", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto integer = conn.ParseType("INTEGER");

	auto failing = ScalarFunction::Create(conn);
	failing.SetName("cpp_fail").SetExecCallback(FailingExec);
	failing.GetSignature().AddParameter("a", integer).SetReturnType(integer);
	failing.Register();

	REQUIRE_THROWS_MATCHES(conn.Execute("SELECT cpp_fail(1)").Drain(), InvalidInputException,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// GetUserData when nothing was planted reports the misuse rather than derefing null.
	auto no_data = ScalarFunction::Create(conn);
	no_data.SetName("cpp_no_user_data").SetExecCallback(NoUserDataExec);
	no_data.GetSignature().AddParameter("a", integer).SetReturnType(integer);
	no_data.Register();

	REQUIRE_THROWS_MATCHES(conn.Execute("SELECT cpp_no_user_data(1)").Drain(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

TEST_CASE("Stable C++API: scalar function registration validation", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto integer = conn.ParseType("INTEGER");

	// No name.
	{
		auto function = ScalarFunction::Create(conn);
		function.SetExecCallback(PlusOneExec);
		function.GetSignature().AddParameter("a", integer).SetReturnType(integer);
		REQUIRE_THROWS_MATCHES(function.Register(), InvalidInputException,
		                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}

	// No exec callback.
	{
		auto function = ScalarFunction::Create(conn);
		function.SetName("cpp_no_exec");
		function.GetSignature().AddParameter("a", integer).SetReturnType(integer);
		REQUIRE_THROWS_MATCHES(function.Register(), InvalidInputException,
		                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}

	// An ANY return type without a bind callback to resolve it.
	{
		auto function = ScalarFunction::Create(conn);
		function.SetName("cpp_unresolved_any").SetExecCallback(PlusOneExec);
		function.GetSignature().AddParameter("a", integer).SetReturnType(conn.CreateType(LogicalTypeId::ANY));
		REQUIRE_THROWS_MATCHES(function.Register(), InvalidInputException,
		                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}
}
