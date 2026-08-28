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
		REQUIRE_THROWS_MATCHES(function.Register(), InvalidInputException, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}

	// No exec callback.
	{
		auto function = ScalarFunction::Create(conn);
		function.SetName("cpp_no_exec");
		function.GetSignature().AddParameter("a", integer).SetReturnType(integer);
		REQUIRE_THROWS_MATCHES(function.Register(), InvalidInputException, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}

	// An ANY return type without a bind callback to resolve it.
	{
		auto function = ScalarFunction::Create(conn);
		function.SetName("cpp_unresolved_any").SetExecCallback(PlusOneExec);
		function.GetSignature().AddParameter("a", integer).SetReturnType(conn.CreateType(LogicalTypeId::ANY));
		REQUIRE_THROWS_MATCHES(function.Register(), InvalidInputException, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}
}

// ---------------------------------------------------------------------------
// ScalarExecutor: the row-at-a-time helper over primitive types.
// ---------------------------------------------------------------------------

namespace {

// Collect a single INTEGER column, keeping NULLs as (false, 0).
std::vector<std::pair<bool, int32_t>> CollectNullableInts(QueryResult result) {
	std::vector<std::pair<bool, int32_t>> out;
	while (auto chunk = result.FetchChunk()) {
		auto view = chunk.GetVector(0).GetView();
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			const auto valid = view.IsValid(i);
			out.emplace_back(valid, valid ? view.Data<int32_t>()[view.SelAt(i)] : 0);
		}
	}
	return out;
}

void ExecutorAddExec(ScalarFunction::ExecInput &input) {
	ScalarExecutor::Execute<int32_t, int32_t, int32_t>(input, [](int32_t a, int32_t b) { return a + b; });
}

void ExecutorSevenExec(ScalarFunction::ExecInput &input) {
	ScalarExecutor::Execute<int32_t>(input, []() { return 7; });
}

// Heterogeneous argument types: INTEGER + BIGINT -> BIGINT.
void ExecutorMixedExec(ScalarFunction::ExecInput &input) {
	ScalarExecutor::Execute<int64_t, int32_t, int64_t>(input, [](int32_t a, int64_t b) { return a * b; });
}

// The executor's type list disagrees with the registered signature's arity.
void ExecutorWrongArityExec(ScalarFunction::ExecInput &input) {
	ScalarExecutor::Execute<int32_t, int32_t, int32_t>(input, [](int32_t a, int32_t b) { return a + b; });
}

} // namespace

TEST_CASE("Stable C++API: ScalarExecutor computes rows and propagates NULLs", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto integer = conn.ParseType("INTEGER");

	auto function = ScalarFunction::Create(conn);
	function.SetName("exec_add").SetExecCallback(ExecutorAddExec);
	function.GetSignature().AddParameter("a", integer).AddParameter("b", integer).SetReturnType(integer);
	function.Register();

	// Constant, flat, and filtered (selection vector) inputs.
	REQUIRE(CollectInts(conn.Execute("SELECT exec_add(40, 2)")) == std::vector<int32_t> {42});
	REQUIRE(CollectInts(conn.Execute("SELECT exec_add(r::INTEGER, 10) FROM range(4) t(r)")) ==
	        std::vector<int32_t> {10, 11, 12, 13});
	REQUIRE(CollectInts(conn.Execute("SELECT exec_add(r::INTEGER, 0) FROM range(8) t(r) WHERE r % 2 = 0")) ==
	        std::vector<int32_t> {0, 2, 4, 6});

	// A NULL in either argument yields NULL for that row alone.
	auto rows = CollectNullableInts(conn.Execute("SELECT exec_add(x, 1) FROM (VALUES (1), (NULL), (3)) t(x)"));
	auto expected = std::vector<std::pair<bool, int32_t>> {{true, 2}, {false, 0}, {true, 4}};
	REQUIRE(rows == expected);
}

TEST_CASE("Stable C++API: ScalarExecutor handles nullary and mixed-type functions", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto integer = conn.ParseType("INTEGER");

	auto seven = ScalarFunction::Create(conn);
	seven.SetName("exec_seven").SetExecCallback(ExecutorSevenExec);
	seven.GetSignature().SetReturnType(integer);
	seven.Register();

	REQUIRE(CollectInts(conn.Execute("SELECT exec_seven()")) == std::vector<int32_t> {7});
	REQUIRE(CollectInts(conn.Execute("SELECT exec_seven() FROM range(3)")) == std::vector<int32_t> {7, 7, 7});

	auto mixed = ScalarFunction::Create(conn);
	mixed.SetName("exec_mixed").SetExecCallback(ExecutorMixedExec);
	mixed.GetSignature()
	    .AddParameter("a", integer)
	    .AddParameter("b", conn.ParseType("BIGINT"))
	    .SetReturnType(conn.ParseType("BIGINT"));
	mixed.Register();

	REQUIRE(CollectInts(conn.Execute("SELECT exec_mixed(6, 7)::INTEGER")) == std::vector<int32_t> {42});
}

TEST_CASE("Stable C++API: ScalarExecutor refuses an arity mismatch", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto integer = conn.ParseType("INTEGER");

	auto function = ScalarFunction::Create(conn);
	function.SetName("exec_wrong_arity").SetExecCallback(ExecutorWrongArityExec);
	function.GetSignature().AddParameter("a", integer).SetReturnType(integer);
	function.Register();

	REQUIRE_THROWS_MATCHES(conn.Execute("SELECT exec_wrong_arity(1)").Drain(), InvalidInputException,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

TEST_CASE("Stable C++API: ScalarExecutor drives vectors directly", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto integer = conn.ParseType("INTEGER");

	// Two argument columns and a result column, no scalar function involved.
	std::vector<LogicalType> arg_types;
	arg_types.push_back(conn.ParseType("INTEGER"));
	arg_types.push_back(conn.ParseType("INTEGER"));
	DataChunk args(conn, arg_types);

	constexpr idx_t kCount = 4;
	auto a = args.GetVector(0);
	a.SetSize(kCount);
	auto b = args.GetVector(1);
	b.SetSize(kCount);
	auto *a_data = a.GetDataMutable<int32_t>();
	auto *b_data = b.GetDataMutable<int32_t>();
	for (idx_t i = 0; i < kCount; i++) {
		a_data[i] = static_cast<int32_t>(i);
		b_data[i] = 10;
	}
	a.SetNull(2);

	std::vector<LogicalType> out_types;
	out_types.push_back(conn.ParseType("INTEGER"));
	DataChunk out(conn, out_types);
	auto result = out.GetVector(0);
	result.SetSize(kCount);

	ScalarExecutor::Execute<int32_t, int32_t, int32_t>(a, b, result, kCount,
	                                                   [](int32_t x, int32_t y) -> int32_t { return x + y; });

	auto view = result.GetView();
	REQUIRE(view.IsValid(0));
	REQUIRE(view.Data<int32_t>()[view.SelAt(0)] == 10);
	REQUIRE(view.Data<int32_t>()[view.SelAt(1)] == 11);
	REQUIRE(!view.IsValid(2)); // the NULL argument row propagated
	REQUIRE(view.IsValid(3));
	REQUIRE(view.Data<int32_t>()[view.SelAt(3)] == 13);
}

// ---------------------------------------------------------------------------
// ScalarExecutor composed type forms: std::optional, std::tuple, references.
// ---------------------------------------------------------------------------

namespace {

// optional result: NULL is produced by the callable itself.
void SafeDivExec(ScalarFunction::ExecInput &input) {
	ScalarExecutor::Execute<std::optional<int32_t>, int32_t, int32_t>(
	    input, [](int32_t a, int32_t b) -> std::optional<int32_t> {
		    if (b == 0) {
			    return std::nullopt;
		    }
		    return a / b;
	    });
}

// optional argument: the callable sees NULL rows as nullopt instead of the
// row being skipped. Counts them to prove it was invoked.
std::atomic<int> nullopt_seen {0};

void OptBumpExec(ScalarFunction::ExecInput &input) {
	ScalarExecutor::Execute<std::optional<int32_t>, std::optional<int32_t>>(
	    input, [](std::optional<int32_t> a) -> std::optional<int32_t> {
		    if (!a) {
			    nullopt_seen++;
			    return std::nullopt;
		    }
		    return *a + 1;
	    });
}

// tuple argument: STRUCT(x INTEGER, y INTEGER) -> x + y.
void PointSumExec(ScalarFunction::ExecInput &input) {
	ScalarExecutor::Execute<int32_t, std::tuple<int32_t, int32_t>>(
	    input, [](const std::tuple<int32_t, int32_t> &p) { return std::get<0>(p) + std::get<1>(p); });
}

// tuple argument with an optional field: a NULL y no longer nulls the row.
void PointSumOptYExec(ScalarFunction::ExecInput &input) {
	ScalarExecutor::Execute<int32_t, std::tuple<int32_t, std::optional<int32_t>>>(
	    input, [](const std::tuple<int32_t, std::optional<int32_t>> &p) {
		    return std::get<0>(p) + std::get<1>(p).value_or(0);
	    });
}

// tuple result: INTEGER -> STRUCT(x INTEGER, y INTEGER).
void MakePointExec(ScalarFunction::ExecInput &input) {
	ScalarExecutor::Execute<std::tuple<int32_t, int32_t>, int32_t>(input,
	                                                               [](int32_t a) { return std::make_tuple(a, a * 2); });
}

// reference argument: reads in place instead of copying.
void RefAddExec(ScalarFunction::ExecInput &input) {
	ScalarExecutor::Execute<int32_t, std::reference_wrapper<const int32_t>, std::reference_wrapper<const int32_t>>(
	    input, [](std::reference_wrapper<const int32_t> a, std::reference_wrapper<const int32_t> b) {
		    return a.get() + b.get();
	    });
}

} // namespace

TEST_CASE("Stable C++API: ScalarExecutor optional results and arguments", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto integer = conn.ParseType("INTEGER");

	auto safe_div = ScalarFunction::Create(conn);
	safe_div.SetName("exec_safe_div").SetExecCallback(SafeDivExec);
	safe_div.GetSignature().AddParameter("a", integer).AddParameter("b", integer).SetReturnType(integer);
	safe_div.Register();

	auto rows =
	    CollectNullableInts(conn.Execute("SELECT exec_safe_div(a, b) FROM (VALUES (10, 2), (1, 0), (9, 3)) t(a, b)"));
	auto expected = std::vector<std::pair<bool, int32_t>> {{true, 5}, {false, 0}, {true, 3}};
	REQUIRE(rows == expected);

	nullopt_seen = 0;
	auto opt_bump = ScalarFunction::Create(conn);
	opt_bump.SetName("exec_opt_bump").SetExecCallback(OptBumpExec);
	opt_bump.GetSignature().AddParameter("a", integer).SetReturnType(integer);
	opt_bump.Register();

	rows = CollectNullableInts(conn.Execute("SELECT exec_opt_bump(x) FROM (VALUES (1), (NULL), (3)) t(x)"));
	expected = std::vector<std::pair<bool, int32_t>> {{true, 2}, {false, 0}, {true, 4}};
	REQUIRE(rows == expected);
	// The callable ran for the NULL row rather than the row being skipped.
	REQUIRE(nullopt_seen == 1);
}

TEST_CASE("Stable C++API: ScalarExecutor tuple arguments read STRUCT fields", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto integer = conn.ParseType("INTEGER");
	const auto point = conn.ParseType("STRUCT(x INTEGER, y INTEGER)");

	auto psum = ScalarFunction::Create(conn);
	psum.SetName("exec_psum").SetExecCallback(PointSumExec);
	psum.GetSignature().AddParameter("p", point).SetReturnType(integer);
	psum.Register();

	// A NULL struct row and a NULL non-optional field both null the row.
	auto rows = CollectNullableInts(
	    conn.Execute("SELECT exec_psum(s::STRUCT(x INTEGER, y INTEGER)) FROM (VALUES ({'x': 1, 'y': 2}), (NULL), "
	                 "({'x': 4, 'y': NULL})) t(s)"));
	auto expected = std::vector<std::pair<bool, int32_t>> {{true, 3}, {false, 0}, {false, 0}};
	REQUIRE(rows == expected);

	// With the field declared optional, only the whole-struct NULL nulls the row.
	auto psum_opt = ScalarFunction::Create(conn);
	psum_opt.SetName("exec_psum_opt").SetExecCallback(PointSumOptYExec);
	psum_opt.GetSignature().AddParameter("p", point).SetReturnType(integer);
	psum_opt.Register();

	rows = CollectNullableInts(
	    conn.Execute("SELECT exec_psum_opt(s::STRUCT(x INTEGER, y INTEGER)) FROM (VALUES ({'x': 1, 'y': 2}), (NULL), "
	                 "({'x': 4, 'y': NULL})) t(s)"));
	expected = std::vector<std::pair<bool, int32_t>> {{true, 3}, {false, 0}, {true, 4}};
	REQUIRE(rows == expected);
}

TEST_CASE("Stable C++API: ScalarExecutor tuple result writes STRUCT fields", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto make_point = ScalarFunction::Create(conn);
	make_point.SetName("exec_make_point").SetExecCallback(MakePointExec);
	make_point.GetSignature()
	    .AddParameter("a", conn.ParseType("INTEGER"))
	    .SetReturnType(conn.ParseType("STRUCT(x INTEGER, y INTEGER)"));
	make_point.Register();

	auto rows = CollectNullableInts(conn.Execute("SELECT (exec_make_point(r::INTEGER)).x + (exec_make_point("
	                                             "r::INTEGER)).y FROM range(3) t(r)"));
	auto expected = std::vector<std::pair<bool, int32_t>> {{true, 0}, {true, 3}, {true, 6}};
	REQUIRE(rows == expected);

	// A NULL argument nulls the whole struct row, fields included.
	rows = CollectNullableInts(conn.Execute("SELECT (exec_make_point(x)).y FROM (VALUES (2), (NULL)) t(x)"));
	expected = std::vector<std::pair<bool, int32_t>> {{true, 4}, {false, 0}};
	REQUIRE(rows == expected);
}

TEST_CASE("Stable C++API: ScalarExecutor reference arguments", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto integer = conn.ParseType("INTEGER");

	auto ref_add = ScalarFunction::Create(conn);
	ref_add.SetName("exec_ref_add").SetExecCallback(RefAddExec);
	ref_add.GetSignature().AddParameter("a", integer).AddParameter("b", integer).SetReturnType(integer);
	ref_add.Register();

	REQUIRE(CollectInts(conn.Execute("SELECT exec_ref_add(r::INTEGER, 100) FROM range(3) t(r)")) ==
	        std::vector<int32_t> {100, 101, 102});
}
