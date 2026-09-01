#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"

#include <atomic>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Stable C++ API tests: TableFunction. The extension-registration path is
// covered by the static extension demo (test/api/capi/v2/static_extension);
// these cover the connection path.
// ---------------------------------------------------------------------------

namespace {

using namespace duckdb::cxx;

// Collect a single BIGINT column, asserting every row valid.
std::vector<int64_t> CollectBigints(QueryResult result) {
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

// Collect a single VARCHAR column, asserting every row valid.
std::vector<std::string> CollectStrings(QueryResult result) {
	std::vector<std::string> out;
	while (auto chunk = result.FetchChunk()) {
		auto view = chunk.GetVector(0).GetView();
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			REQUIRE(view.IsValid(i));
			out.emplace_back(view.Data<varchar_t>()[view.SelAt(i)].view());
		}
	}
	return out;
}

// ---------------------------------------------------------------------------
// cpp_range(n): one BIGINT column "i" carrying 0..n-1, produced in batches.
// ---------------------------------------------------------------------------

struct RangeBind {
	int64_t count = 0;
};
struct RangeGlobal {
	int64_t position = 0;
};

void RangeBind_(TableFunction::BindInput &input) {
	const auto count = input.GetArgument(0).Get<int64_t>();
	input.AddResultColumn("i", input.GetContext().ParseType("BIGINT"));
	input.SetCardinality(static_cast<idx_t>(count), true);
	input.SetBindData<RangeBind>(RangeBind {count});
}

void RangeInitGlobal(TableFunction::InitGlobalInput &input) {
	input.SetGlobalState<RangeGlobal>(RangeGlobal {0});
}

// How many rows a batch produces. The API exposes no output-chunk capacity, so the tests stay within the smallest
// vector size the build can be configured with.
constexpr int64_t BATCH_ROWS = 2;

void RangeExec(TableFunction::ExecInput &input) {
	const auto &bind = input.GetBindData<RangeBind>();
	auto &global = input.GetGlobalState<RangeGlobal>();

	auto chunk = input.GetOutputChunk();
	auto vec = chunk.GetVector(0);
	auto *out = vec.GetDataMutable<int64_t>();

	auto produced = bind.count - global.position;
	if (produced > BATCH_ROWS) {
		produced = BATCH_ROWS;
	}
	if (produced < 0) {
		produced = 0;
	}
	for (int64_t i = 0; i < produced; i++) {
		out[i] = global.position + i;
	}
	global.position += produced;

	// The first vector's size is the batch's row count; 0 ends the scan.
	vec.SetSize(static_cast<idx_t>(produced));
}

// Registers cpp_range on the connection.
void RegisterRange(Connection &conn, const std::string &name) {
	auto function = TableFunction::Create(conn);
	function.SetName(name);
	function.GetSignature().AddParameter("n", conn.ParseType("BIGINT"));
	function.SetBindCallback(RangeBind_).SetInitGlobalCallback(RangeInitGlobal).SetExecCallback(RangeExec);
	function.Register();
}

// ---------------------------------------------------------------------------
// cpp_pairs(n): two columns, to pin that the row count set on the first vector
// reaches the others.
// ---------------------------------------------------------------------------

void PairsBind_(TableFunction::BindInput &input) {
	const auto count = input.GetArgument(0).Get<int64_t>();
	auto ctx = input.GetContext();
	input.AddResultColumn("a", ctx.ParseType("INTEGER"));
	input.AddResultColumn("b", ctx.ParseType("VARCHAR"));
	input.SetBindData<RangeBind>(RangeBind {count});
}

void PairsExec(TableFunction::ExecInput &input) {
	const auto &bind = input.GetBindData<RangeBind>();
	auto &global = input.GetGlobalState<RangeGlobal>();

	auto chunk = input.GetOutputChunk();
	auto produced = bind.count - global.position;
	if (produced > BATCH_ROWS) {
		produced = BATCH_ROWS;
	}
	if (produced < 0) {
		produced = 0;
	}

	auto a = chunk.GetVector(0);
	auto b = chunk.GetVector(1);
	auto *values = a.GetDataMutable<int32_t>();
	for (int64_t i = 0; i < produced; i++) {
		values[i] = static_cast<int32_t>(global.position + i);
		// Longer than blob_t::INLINE_LENGTH, so the bytes land in the vector's arena: an assertion build compiles the
		// engine with DUCKDB_DEBUG_NO_INLINE, whose string_t reads the pointer even for values the API would inline.
		b.AssignString(static_cast<idx_t>(i), "row-payload-" + std::to_string(global.position + i));
	}
	global.position += produced;

	// Only the first vector is sized; the engine propagates the count to "b".
	a.SetSize(static_cast<idx_t>(produced));
}

// ---------------------------------------------------------------------------
// cpp_args(a, b := 7, ...): reports the argument slots the bind callback sees.
// ---------------------------------------------------------------------------

struct ArgsBind {
	std::vector<int64_t> values;
};

void ArgsBind_(TableFunction::BindInput &input) {
	ArgsBind bind;
	for (idx_t i = 0; i < input.GetArgCount(); i++) {
		REQUIRE(input.GetArgType(i).GetTypeId() == LogicalTypeId::BIGINT);
		bind.values.push_back(input.GetArgument(i).Get<int64_t>());
	}
	input.AddResultColumn("v", input.GetContext().ParseType("BIGINT"));
	input.SetBindData<ArgsBind>(std::move(bind));
}

// Emits one row per argument slot, in slot order, a batch at a time.
void ArgsExec(TableFunction::ExecInput &input) {
	const auto &bind = input.GetBindData<ArgsBind>();
	auto &global = input.GetGlobalState<RangeGlobal>();

	auto chunk = input.GetOutputChunk();
	auto vec = chunk.GetVector(0);
	auto *out = vec.GetDataMutable<int64_t>();

	idx_t produced = 0;
	while (produced < BATCH_ROWS && global.position < static_cast<int64_t>(bind.values.size())) {
		out[produced++] = bind.values[static_cast<size_t>(global.position++)];
	}
	vec.SetSize(produced);
}

// ---------------------------------------------------------------------------
// State plumbing and the optional hooks.
// ---------------------------------------------------------------------------

struct Seed {
	int64_t tag = 0;
};
struct StateGlobal {
	int64_t remaining = 0;
};
struct StateLocal {
	int64_t tag = 0;
};

std::atomic<int> init_global_runs {0};
std::atomic<int> init_local_runs {0};
std::atomic<int> cardinality_runs {0};

void StateBind(TableFunction::BindInput &input) {
	input.AddResultColumn("v", input.GetContext().ParseType("BIGINT"));
	input.SetBindData<RangeBind>(RangeBind {3});
}

void StateInitGlobal(TableFunction::InitGlobalInput &input) {
	init_global_runs++;
	const auto &bind = input.GetBindData<RangeBind>();
	input.SetGlobalState<StateGlobal>(StateGlobal {bind.count});
	input.SetMaxThreads(1);
}

void StateInitLocal(TableFunction::InitLocalInput &input) {
	init_local_runs++;
	// The local state derives its tag from the user data, reached through this phase's input.
	const auto &seed = input.GetUserData<Seed>();
	// Touching the global state here is what a real scan does to claim work.
	(void)input.GetGlobalState<StateGlobal>();
	input.SetLocalState<StateLocal>(StateLocal {seed.tag});
}

void StateExec(TableFunction::ExecInput &input) {
	auto &global = input.GetGlobalState<StateGlobal>();
	const auto &local = input.GetLocalState<StateLocal>();

	auto chunk = input.GetOutputChunk();
	auto vec = chunk.GetVector(0);
	auto *out = vec.GetDataMutable<int64_t>();

	idx_t produced = 0;
	if (global.remaining > 0) {
		global.remaining--;
		out[0] = local.tag;
		produced = 1;
	}
	vec.SetSize(produced);
}

void StateCardinality(TableFunction::CardinalityInput &input) {
	cardinality_runs++;
	input.SetCardinality(static_cast<idx_t>(input.GetBindData<RangeBind>().count), true);
}

// A bind callback that throws: the exception must surface as the query's error.
void ThrowingBind(TableFunction::BindInput &) {
	throw InvalidInputException("bind refused");
}

// An exec callback that throws once the scan is under way.
void ThrowingExec(TableFunction::ExecInput &) {
	throw InvalidInputException("exec refused");
}

} // namespace

TEST_CASE("Stable C++API: table function registers and scans", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	RegisterRange(conn, "cpp_range");

	REQUIRE(CollectBigints(conn.Execute("SELECT * FROM cpp_range(4)")) == std::vector<int64_t> {0, 1, 2, 3});
	REQUIRE(CollectBigints(conn.Execute("SELECT sum(i) FROM cpp_range(10)")) == std::vector<int64_t> {45});
	// A scan producing nothing at all, and one spanning several batches.
	REQUIRE(CollectBigints(conn.Execute("SELECT * FROM cpp_range(0)")).empty());
	REQUIRE(CollectBigints(conn.Execute("SELECT count(*) FROM cpp_range(5000)")) == std::vector<int64_t> {5000});
}

TEST_CASE("Stable C++API: table function with several result columns", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto function = TableFunction::Create(conn);
	function.SetName("cpp_pairs");
	function.GetSignature().AddParameter("n", conn.ParseType("BIGINT"));
	function.SetBindCallback(PairsBind_).SetInitGlobalCallback(RangeInitGlobal).SetExecCallback(PairsExec);
	function.Register();

	// Both columns carry all rows: the count set on "a" reached "b".
	REQUIRE(CollectBigints(conn.Execute("SELECT count(b) FROM cpp_pairs(5)")) == std::vector<int64_t> {5});
	REQUIRE(CollectBigints(conn.Execute("SELECT sum(a) FROM cpp_pairs(5)")) == std::vector<int64_t> {10});
	REQUIRE(CollectStrings(conn.Execute("SELECT string_agg(b, ',' ORDER BY a) FROM cpp_pairs(3)")) ==
	        std::vector<std::string> {"row-payload-0,row-payload-1,row-payload-2"});
}

TEST_CASE("Stable C++API: table function parameter defaults, named arguments and varargs", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto bigint = conn.ParseType("BIGINT");

	auto function = TableFunction::Create(conn);
	function.SetName("cpp_args");
	function.WithSignature([&](FunctionSignature &sig) {
		sig.AddParameter("a", bigint);
		sig.AddParameter("b", bigint, Value::Create(conn, int64_t {7}));
		sig.SetVarArgs(bigint);
	});
	function.SetBindCallback(ArgsBind_).SetInitGlobalCallback(RangeInitGlobal).SetExecCallback(ArgsExec);
	function.Register();

	// Only the required parameter: the defaulted one is still present, carrying its default.
	REQUIRE(CollectBigints(conn.Execute("SELECT * FROM cpp_args(1)")) == std::vector<int64_t> {1, 7});
	// A defaulted parameter passed by name.
	REQUIRE(CollectBigints(conn.Execute("SELECT * FROM cpp_args(1, b => 5)")) == std::vector<int64_t> {1, 5});
	// The variadic tail follows the fixed slots, in call order.
	REQUIRE(CollectBigints(conn.Execute("SELECT * FROM cpp_args(1, 30, 40)")) == std::vector<int64_t> {1, 7, 30, 40});
}

TEST_CASE("Stable C++API: table function user data, global and local state", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	init_global_runs = 0;
	init_local_runs = 0;
	cardinality_runs = 0;

	auto function = TableFunction::Create(conn);
	function.SetName("cpp_state").SetUserData<Seed>(Seed {42});
	function.SetBindCallback(StateBind)
	    .SetInitGlobalCallback(StateInitGlobal)
	    .SetInitLocalCallback(StateInitLocal)
	    .SetExecCallback(StateExec)
	    .SetCardinalityCallback(StateCardinality);
	function.Register();

	// The bind data seeded three rows, each carrying the local state's tag, which came from the user data.
	REQUIRE(CollectBigints(conn.Execute("SELECT * FROM cpp_state()")) == std::vector<int64_t> {42, 42, 42});
	REQUIRE(init_global_runs >= 1);
	REQUIRE(init_local_runs >= 1);
	REQUIRE(cardinality_runs >= 1);
}

TEST_CASE("Stable C++API: table function callbacks report failure by throwing", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto bad_bind = TableFunction::Create(conn);
	bad_bind.SetName("cpp_bad_bind").SetBindCallback(ThrowingBind).SetExecCallback(StateExec);
	bad_bind.Register();

	auto bad_exec = TableFunction::Create(conn);
	bad_exec.SetName("cpp_bad_exec")
	    .SetBindCallback(StateBind)
	    .SetInitGlobalCallback(StateInitGlobal)
	    .SetInitLocalCallback(StateInitLocal)
	    .SetExecCallback(ThrowingExec);
	bad_exec.Register();

	REQUIRE_THROWS_MATCHES(conn.Execute("SELECT * FROM cpp_bad_bind()").Drain(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	REQUIRE_THROWS_MATCHES(conn.Execute("SELECT * FROM cpp_bad_exec()").Drain(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

TEST_CASE("Stable C++API: table function registration refusals", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto bigint = conn.ParseType("BIGINT");

	// No name.
	{
		auto function = TableFunction::Create(conn);
		function.SetBindCallback(StateBind).SetExecCallback(StateExec);
		REQUIRE_THROWS_MATCHES(function.Register(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}
	// No bind callback: a table function has no other way to declare its columns.
	{
		auto function = TableFunction::Create(conn);
		function.SetName("cpp_no_bind").SetExecCallback(StateExec);
		REQUIRE_THROWS_MATCHES(function.Register(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}
	// No exec callback.
	{
		auto function = TableFunction::Create(conn);
		function.SetName("cpp_no_exec").SetBindCallback(StateBind);
		REQUIRE_THROWS_MATCHES(function.Register(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}
	// A return type on the signature: the columns come from bind instead.
	{
		auto function = TableFunction::Create(conn);
		function.SetName("cpp_return_type").SetBindCallback(StateBind).SetExecCallback(StateExec);
		function.GetSignature().SetReturnType(bigint);
		REQUIRE_THROWS_MATCHES(function.Register(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}
}
