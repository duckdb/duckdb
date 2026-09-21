#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"

#include <algorithm>
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

void StateBind(TableFunction::BindInput &input) {
	input.AddResultColumn("v", input.GetContext().ParseType("BIGINT"));
	input.SetCardinality(3, true);
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

// A bind callback that throws: the exception must surface as the query's error.
void ThrowingBind(TableFunction::BindInput &) {
	throw InvalidInputException("bind refused");
}

// An exec callback that throws once the scan is under way.
void ThrowingExec(TableFunction::ExecInput &) {
	throw InvalidInputException("exec refused");
}

// ---------------------------------------------------------------------------
// cpp_proj(): three BIGINT columns x, y, z, three rows, cell = declared column * 100 + row. The exec callback
// fills whatever the scan asks for through the column mapping.
// ---------------------------------------------------------------------------

struct ProjGlobal {
	idx_t position = 0;
};

constexpr idx_t PROJ_ROWS = 3;

std::vector<idx_t> proj_columns;

void ProjBind(TableFunction::BindInput &input) {
	auto bigint = input.GetContext().ParseType("BIGINT");
	input.AddResultColumn("x", bigint);
	input.AddResultColumn("y", bigint);
	input.AddResultColumn("z", bigint);
}

void ProjInitGlobal(TableFunction::InitGlobalInput &input) {
	proj_columns.clear();
	for (idx_t i = 0; i < input.GetColumnCount(); i++) {
		proj_columns.push_back(input.GetColumnIndex(i));
	}
	input.SetGlobalState<ProjGlobal>();
}

void ProjExec(TableFunction::ExecInput &input) {
	auto &global = input.GetGlobalState<ProjGlobal>();
	idx_t rows = PROJ_ROWS - global.position;
	if (rows > static_cast<idx_t>(BATCH_ROWS)) {
		rows = static_cast<idx_t>(BATCH_ROWS);
	}

	auto chunk = input.GetOutputChunk();
	for (idx_t i = 0; i < input.GetColumnCount(); i++) {
		auto column = input.GetColumnIndex(i);
		auto vec = chunk.GetVector(i);
		auto *out = vec.GetDataMutable<int64_t>();
		for (idx_t row = 0; row < rows; row++) {
			out[row] = static_cast<int64_t>(column * 100 + global.position + row);
		}
		if (i == 0) {
			vec.SetSize(rows);
		}
	}
	global.position += rows;
}

// ---------------------------------------------------------------------------
// cpp_claim(n): column i BIGINT with 0..n-1. Claims every "i < constant" predicate, then deliberately produces two
// rows past the bound, which only reach the result if the engine really stopped applying the predicate.
// ---------------------------------------------------------------------------

struct ClaimBind {
	int64_t count = 0;
	int64_t bound = -1;
};

// Every predicate the pushdown callback saw, rendered.
std::vector<std::string> claim_seen;

std::string RenderExpression(const TableFunction::FilterPushdownInput &input, const Expression &expr) {
	switch (expr.GetType()) {
	case ExpressionType::VALUE_CONSTANT:
		return expr.GetConstantValue().ToText();
	case ExpressionType::BOUND_COLUMN_REF:
		return "col" + std::to_string(input.GetColumnIndex(expr.GetColumnIndex()));
	default:
		break;
	}
	std::string children;
	for (idx_t i = 0; i < expr.GetChildCount(); i++) {
		if (i > 0) {
			children += ", ";
		}
		children += RenderExpression(input, expr.GetChild(i));
	}
	switch (expr.GetType()) {
	case ExpressionType::OPERATOR_CAST:
		return std::string(expr.GetCastMode() == CastMode::TRY ? "try_cast(" : "cast(") + children +
		       ")::" + expr.GetReturnType().ToText();
	case ExpressionType::CONJUNCTION_AND:
		return "and(" + children + ")";
	case ExpressionType::CONJUNCTION_OR:
		return "or(" + children + ")";
	case ExpressionType::OPERATOR_IS_NULL:
		return "is_null(" + children + ")";
	case ExpressionType::INVALID:
		return "invalid(" + children + ")";
	case ExpressionType::BOUND_FUNCTION:
		return expr.GetFunctionQualifiedName().Render() + "(" + children + ")";
	default:
		return expr.GetFunctionName() + "(" + children + ")";
	}
}

void ClaimBind_(TableFunction::BindInput &input) {
	const auto count = input.GetArgument(0).Get<int64_t>();
	input.AddResultColumn("i", input.GetContext().ParseType("BIGINT"));
	input.SetBindData<ClaimBind>(ClaimBind {count, -1});
}

void ClaimPushdown(TableFunction::FilterPushdownInput &input) {
	auto &bind = input.GetBindData<ClaimBind>();
	for (idx_t i = 0; i < input.GetFilterCount(); i++) {
		auto filter = input.GetFilter(i);
		claim_seen.push_back(RenderExpression(input, filter));
		if (filter.GetType() != ExpressionType::COMPARE_LESSTHAN) {
			continue;
		}
		auto left = filter.GetChild(0);
		auto right = filter.GetChild(1);
		if (left.GetType() != ExpressionType::BOUND_COLUMN_REF || right.GetType() != ExpressionType::VALUE_CONSTANT ||
		    input.GetColumnIndex(left.GetColumnIndex()) != 0) {
			continue;
		}
		bind.bound = right.GetConstantValue().Get<int64_t>();
		input.Accept(i);
	}
}

void ClaimExec(TableFunction::ExecInput &input) {
	const auto &bind = input.GetBindData<ClaimBind>();
	auto &global = input.GetGlobalState<RangeGlobal>();

	auto chunk = input.GetOutputChunk();
	auto vec = chunk.GetVector(0);
	auto *out = vec.GetDataMutable<int64_t>();

	auto limit = bind.bound < 0 ? bind.count : bind.bound + 2;
	if (limit > bind.count) {
		limit = bind.count;
	}
	auto produced = limit - global.position;
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
	vec.SetSize(static_cast<idx_t>(produced));
}

// Whether an accessor refused a node it does not apply to, checked inside the callback where the node lives.
bool misuse_refused = false;

void MisusePushdown(TableFunction::FilterPushdownInput &input) {
	auto filter = input.GetFilter(0);
	misuse_refused = false;
	try {
		filter.GetConstantValue();
	} catch (const InvalidInputException &) {
		misuse_refused = true;
	}
}

void ThrowingPushdown(TableFunction::FilterPushdownInput &) {
	throw InvalidInputException("pushdown refused");
}

void RegisterClaim(Connection &conn, const std::string &name, TableFunction::FilterPushdownCallback pushdown) {
	auto function = TableFunction::Create(conn);
	function.SetName(name);
	function.GetSignature().AddParameter("n", conn.ParseType("BIGINT"));
	function.SetBindCallback(ClaimBind_)
	    .SetInitGlobalCallback(RangeInitGlobal)
	    .SetExecCallback(ClaimExec)
	    .SetFilterPushdownCallback(pushdown);
	function.Register();
}

} // namespace

TEST_CASE("Stable C++API: table function projection pushdown", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto function = TableFunction::Create(conn);
	function.SetName("cpp_proj")
	    .SetBindCallback(ProjBind)
	    .SetInitGlobalCallback(ProjInitGlobal)
	    .SetExecCallback(ProjExec)
	    .SetProjectionPushdown(true);
	function.Register();

	REQUIRE(CollectBigints(conn.Execute("SELECT z FROM cpp_proj()")) == std::vector<int64_t> {200, 201, 202});
	REQUIRE(proj_columns == std::vector<idx_t> {2});
	REQUIRE(CollectBigints(conn.Execute("SELECT x + z FROM cpp_proj()")) == std::vector<int64_t> {200, 202, 204});
	REQUIRE(proj_columns.size() == 2);
}

TEST_CASE("Stable C++API: table function filter pushdown", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	RegisterClaim(conn, "cpp_claim", ClaimPushdown);

	claim_seen.clear();
	REQUIRE(CollectBigints(conn.Execute("SELECT count(*) FROM cpp_claim(100) WHERE i < 10")) ==
	        std::vector<int64_t> {12});
	REQUIRE(claim_seen == std::vector<std::string> {"<(col0, 10)"});

	// The optimizer may offer the unclaimed predicates more than once, so only membership is pinned here.
	claim_seen.clear();
	REQUIRE(CollectBigints(conn.Execute("SELECT count(*) FROM cpp_claim(100) WHERE i < 10 AND i % 2 = 0")) ==
	        std::vector<int64_t> {6});
	REQUIRE(std::count(claim_seen.begin(), claim_seen.end(), "<(col0, 10)") == 1);
	REQUIRE(std::count(claim_seen.begin(), claim_seen.end(), "=(\"system\".main.\"%\"(col0, 2), 0)") >= 1);

	claim_seen.clear();
	REQUIRE(CollectBigints(conn.Execute(
	            "SELECT count(*) FROM cpp_claim(100) WHERE (i = 1 OR i IS NULL) AND TRY_CAST(i AS VARCHAR) = '1'")) ==
	        std::vector<int64_t> {1});
	std::sort(claim_seen.begin(), claim_seen.end());
	claim_seen.erase(std::unique(claim_seen.begin(), claim_seen.end()), claim_seen.end());
	REQUIRE(claim_seen == std::vector<std::string> {"=(try_cast(col0)::VARCHAR, 1)", "or(=(col0, 1), is_null(col0))"});
}

TEST_CASE("Stable C++API: expression accessors refuse other node types and errors propagate", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	RegisterClaim(conn, "cpp_misuse", MisusePushdown);
	RegisterClaim(conn, "cpp_throwing", ThrowingPushdown);

	REQUIRE(CollectBigints(conn.Execute("SELECT count(*) FROM cpp_misuse(5) WHERE i < 3")) == std::vector<int64_t> {3});
	REQUIRE(misuse_refused);
	REQUIRE_THROWS_MATCHES(conn.Execute("SELECT count(*) FROM cpp_throwing(5) WHERE i < 3").Drain(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

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

	auto function = TableFunction::Create(conn);
	function.SetName("cpp_state").SetUserData<Seed>(Seed {42});
	function.SetBindCallback(StateBind)
	    .SetInitGlobalCallback(StateInitGlobal)
	    .SetInitLocalCallback(StateInitLocal)
	    .SetExecCallback(StateExec);
	function.Register();

	// The bind data seeded three rows, each carrying the local state's tag, which came from the user data.
	REQUIRE(CollectBigints(conn.Execute("SELECT * FROM cpp_state()")) == std::vector<int64_t> {42, 42, 42});
	REQUIRE(init_global_runs >= 1);
	REQUIRE(init_local_runs >= 1);
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
