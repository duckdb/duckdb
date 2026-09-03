#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <fstream>
#include <sstream>
#include <string>

// ---------------------------------------------------------------------------
// Stable C++ API tests: CopyFunction. The extension-registration path is
// covered by the static extension demo (test/api/capi/v2/static_extension);
// these cover the connection path.
//
// Callbacks latch what they observe into file-scope statics rather than
// asserting in place: a failed REQUIRE inside a callback would surface as a
// query error rather than as the assertion itself.
// ---------------------------------------------------------------------------

namespace {

using namespace duckdb::cxx;

// The COPY statement driving a format, writing `source` to `path`.
std::string CopyStatement(const std::string &source, const std::string &path, const std::string &format) {
	return "COPY (" + source + ") TO '" + path + "' (FORMAT " + format + ", USE_TMP_FILE FALSE)";
}

std::string ReadFile(const std::string &path) {
	std::ifstream in(path, std::ios::binary);
	std::stringstream out;
	out << in.rdbuf();
	return out.str();
}

// ---------------------------------------------------------------------------
// cpp_copy: the bind data records the column count, the init data owns the
// output path and accumulates the rows flushed to it, and each batch is kept
// as its own batch data until the flush counts its rows. Finalize writes
// "<columns> <rows>" to the target through the engine's file system.
// ---------------------------------------------------------------------------

struct FileState {
	std::string path;
	idx_t columns = 0;
	idx_t total_rows = 0;
};

struct {
	idx_t column_count = 0;
	std::string column_name;
	LogicalTypeId column_type = LogicalTypeId::INVALID;
	std::atomic<idx_t> batches {0};

	void Reset() {
		column_count = 0;
		column_name.clear();
		column_type = LogicalTypeId::INVALID;
		batches = 0;
	}
} summary_seen;

void SummaryBind(CopyFunction::BindInput &input) {
	summary_seen.column_count = input.GetColumnCount();
	summary_seen.column_name = input.GetColumnName(0);
	summary_seen.column_type = input.GetColumnType(0).GetTypeId();
	input.SetBindData<idx_t>(input.GetColumnCount());
}

// Asks for batches larger than the input: the whole copy arrives as one batch per thread.
void SummaryBatchSize(CopyFunction::BatchSizeInput &input) {
	// The batch size callback runs after bind and sees its data.
	if (input.GetBindData<idx_t>() != 1) {
		throw InvalidInputException("batch size callback did not see the bind data");
	}
	input.SetTarget(100000);
}

void SummaryInit(CopyFunction::InitInput &input) {
	input.SetInitData<FileState>(FileState {input.GetFilePath(), input.GetBindData<idx_t>(), 0});
}

void SummaryBatch(CopyFunction::BatchInput &input) {
	summary_seen.batches++;
	// The batch is ours: hand it over to the flush as is.
	input.SetBatchData<ColumnDataCollection>(input.TakeBatch());
}

void SummaryFlush(CopyFunction::FlushInput &input) {
	input.GetInitData<FileState>().total_rows += input.GetBatchData<ColumnDataCollection>().GetRowCount();
}

void SummaryFinalize(CopyFunction::FinalizeInput &input) {
	const auto &state = input.GetInitData<FileState>();
	const auto contents = std::to_string(state.columns) + " " + std::to_string(state.total_rows);

	auto fs = input.GetContext().GetFileSystem();
	auto file = fs.OpenFile(state.path, {FileFlags::WRITE, FileFlags::FILE_CREATE});
	file.Write(contents.data(), contents.size());
}

// Data-flow probes: each callback latches the user data it was handed.
struct {
	std::atomic<const std::string *> in_bind {nullptr};
	std::atomic<const std::string *> in_init {nullptr};
	std::atomic<const std::string *> in_batch {nullptr};
	std::atomic<const std::string *> in_flush {nullptr};
	std::atomic<const std::string *> in_finalize {nullptr};

	void Reset() {
		in_bind = nullptr;
		in_init = nullptr;
		in_batch = nullptr;
		in_flush = nullptr;
		in_finalize = nullptr;
	}
} user_data_seen;

void UserDataBind(CopyFunction::BindInput &input) {
	user_data_seen.in_bind = &input.GetUserData<std::string>();
}
void UserDataInit(CopyFunction::InitInput &input) {
	user_data_seen.in_init = &input.GetUserData<std::string>();
}
void UserDataBatch(CopyFunction::BatchInput &input) {
	user_data_seen.in_batch = &input.GetUserData<std::string>();
}
void UserDataFlush(CopyFunction::FlushInput &input) {
	user_data_seen.in_flush = &input.GetUserData<std::string>();
}
void UserDataFinalize(CopyFunction::FinalizeInput &input) {
	user_data_seen.in_finalize = &input.GetUserData<std::string>();
}

// Reads a slot that was never set: the wrapper refuses instead of handing out null.
void NoUserDataBind(CopyFunction::BindInput &input) {
	input.GetUserData<int>();
}
void NoBindDataInit(CopyFunction::InitInput &input) {
	input.GetBindData<int>();
}
void NoInitDataBatch(CopyFunction::BatchInput &input) {
	input.GetInitData<int>();
}
void NoBatchDataFlush(CopyFunction::FlushInput &input) {
	input.GetBatchData<int>();
}

void NoopBind(CopyFunction::BindInput &) {
}
void NoopInit(CopyFunction::InitInput &) {
}
void NoopBatch(CopyFunction::BatchInput &) {
}
void NoopFlush(CopyFunction::FlushInput &) {
}

void FailingBatch(CopyFunction::BatchInput &) {
	throw InvalidInputException("copy batch failed on purpose");
}

// A batch size callback that sets no target fails the statement.
void EmptyBatchSize(CopyFunction::BatchSizeInput &) {
}

// The batch can only be taken once.
void TakeTwiceBatch(CopyFunction::BatchInput &input) {
	auto first = input.TakeBatch();
	input.TakeBatch();
}

} // namespace

// ---------------------------------------------------------------------------
// Register on a connection and drive through COPY ... TO.
// ---------------------------------------------------------------------------

TEST_CASE("Stable C++API: copy function registers and executes", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// One sink thread and a batch size beyond the input: the whole copy arrives as a single batch.
	conn.Execute("SET threads = 1").Drain();
	auto function = CopyFunction::Create(conn);
	function.SetName("cpp_copy")
	    .SetBindCallback(SummaryBind)
	    .SetBatchSizeCallback(SummaryBatchSize)
	    .SetInitCallback(SummaryInit)
	    .SetBatchCallback(SummaryBatch)
	    .SetFlushCallback(SummaryFlush)
	    .SetFinalizeCallback(SummaryFinalize);
	function.Register();

	summary_seen.Reset();
	const auto path = duckdb::TestCreatePath("cpp_copy_summary.txt");
	// COPY reports the number of rows written.
	REQUIRE(conn.Execute(CopyStatement("SELECT r AS i FROM range(5000) t(r)", path, "cpp_copy")).Drain() == 5000);

	// Bind saw the SELECT's single BIGINT column.
	REQUIRE(summary_seen.column_count == 1);
	REQUIRE(summary_seen.column_name == "i");
	REQUIRE(summary_seen.column_type == LogicalTypeId::BIGINT);
	REQUIRE(summary_seen.batches.load() == 1);

	// The bind -> init -> batch -> flush -> finalize chain observed 1 column and 5000 rows.
	REQUIRE(ReadFile(path) == "1 5000");
}

// ---------------------------------------------------------------------------
// Data flow: user data reaches every phase; a slot that was never set is refused.
// ---------------------------------------------------------------------------

TEST_CASE("Stable C++API: copy function data flows through the phases", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto path = duckdb::TestCreatePath("cpp_copy_user_data.txt");

	auto function = CopyFunction::Create(conn);
	function.SetName("cpp_copy_user_data")
	    .SetUserData<std::string>("copy user data")
	    .SetBindCallback(UserDataBind)
	    .SetInitCallback(UserDataInit)
	    .SetBatchCallback(UserDataBatch)
	    .SetFlushCallback(UserDataFlush)
	    .SetFinalizeCallback(UserDataFinalize);
	function.Register();

	user_data_seen.Reset();
	conn.Execute(CopyStatement("SELECT r FROM range(3) t(r)", path, "cpp_copy_user_data")).Drain();

	// One object, visible in all five phases.
	const std::string *seen = user_data_seen.in_bind;
	REQUIRE(seen != nullptr);
	REQUIRE(*seen == "copy user data");
	REQUIRE(user_data_seen.in_init.load() == seen);
	REQUIRE(user_data_seen.in_batch.load() == seen);
	REQUIRE(user_data_seen.in_flush.load() == seen);
	REQUIRE(user_data_seen.in_finalize.load() == seen);

	// A slot that was never set is a clear error, not a null deref, from whichever phase reads it.
	auto no_user_data = CopyFunction::Create(conn);
	no_user_data.SetName("cpp_copy_no_user_data")
	    .SetBindCallback(NoUserDataBind)
	    .SetBatchCallback(NoopBatch)
	    .SetFlushCallback(NoopFlush);
	no_user_data.Register();
	REQUIRE_THROWS_MATCHES(conn.Execute(CopyStatement("SELECT r FROM range(3) t(r)", path, "cpp_copy_no_user_data")),
	                       Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	auto no_bind_data = CopyFunction::Create(conn);
	no_bind_data.SetName("cpp_copy_no_bind_data")
	    .SetBindCallback(NoopBind)
	    .SetInitCallback(NoBindDataInit)
	    .SetBatchCallback(NoopBatch)
	    .SetFlushCallback(NoopFlush);
	no_bind_data.Register();
	REQUIRE_THROWS_MATCHES(
	    conn.Execute(CopyStatement("SELECT r FROM range(3) t(r)", path, "cpp_copy_no_bind_data")).Drain(), Exception,
	    HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	auto no_init_data = CopyFunction::Create(conn);
	no_init_data.SetName("cpp_copy_no_init_data")
	    .SetInitCallback(NoopInit)
	    .SetBatchCallback(NoInitDataBatch)
	    .SetFlushCallback(NoopFlush);
	no_init_data.Register();
	REQUIRE_THROWS_MATCHES(
	    conn.Execute(CopyStatement("SELECT r FROM range(3) t(r)", path, "cpp_copy_no_init_data")).Drain(), Exception,
	    HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	auto no_batch_data = CopyFunction::Create(conn);
	no_batch_data.SetName("cpp_copy_no_batch_data").SetBatchCallback(NoopBatch).SetFlushCallback(NoBatchDataFlush);
	no_batch_data.Register();
	REQUIRE_THROWS_MATCHES(
	    conn.Execute(CopyStatement("SELECT r FROM range(3) t(r)", path, "cpp_copy_no_batch_data")).Drain(), Exception,
	    HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

TEST_CASE("Stable C++API: copy SetUserData is consumed by Register", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto path = duckdb::TestCreatePath("cpp_copy_consumed.txt");

	auto function = CopyFunction::Create(conn);
	function.SetName("cpp_copy_consumed")
	    .SetUserData<std::string>("first")
	    .SetBindCallback(UserDataBind)
	    .SetBatchCallback(NoopBatch)
	    .SetFlushCallback(NoopFlush);
	function.Register();

	user_data_seen.Reset();
	conn.Execute(CopyStatement("SELECT r FROM range(3) t(r)", path, "cpp_copy_consumed")).Drain();
	REQUIRE(user_data_seen.in_bind.load() != nullptr);
	REQUIRE(*user_data_seen.in_bind.load() == "first");

	// Register consumed the user data: the second registration has none.
	function.SetName("cpp_copy_consumed2").SetBindCallback(NoUserDataBind);
	function.Register();
	REQUIRE_THROWS_MATCHES(conn.Execute(CopyStatement("SELECT r FROM range(3) t(r)", path, "cpp_copy_consumed2")),
	                       Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

TEST_CASE("Stable C++API: copy function callback errors fail the query", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	const auto path = duckdb::TestCreatePath("cpp_copy_fails.txt");

	auto function = CopyFunction::Create(conn);
	function.SetName("cpp_copy_fails").SetBatchCallback(FailingBatch).SetFlushCallback(NoopFlush);
	function.Register();

	REQUIRE_THROWS_MATCHES(conn.Execute(CopyStatement("SELECT r FROM range(3) t(r)", path, "cpp_copy_fails")).Drain(),
	                       InvalidInputException, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	auto empty_batch_size = CopyFunction::Create(conn);
	empty_batch_size.SetName("cpp_copy_empty_batch_size")
	    .SetBatchSizeCallback(EmptyBatchSize)
	    .SetBatchCallback(NoopBatch)
	    .SetFlushCallback(NoopFlush);
	empty_batch_size.Register();
	REQUIRE_THROWS_MATCHES(
	    conn.Execute(CopyStatement("SELECT r FROM range(3) t(r)", path, "cpp_copy_empty_batch_size")),
	    InvalidInputException, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	auto take_twice = CopyFunction::Create(conn);
	take_twice.SetName("cpp_copy_take_twice").SetBatchCallback(TakeTwiceBatch).SetFlushCallback(NoopFlush);
	take_twice.Register();
	REQUIRE_THROWS_MATCHES(
	    conn.Execute(CopyStatement("SELECT r FROM range(3) t(r)", path, "cpp_copy_take_twice")).Drain(),
	    InvalidInputException, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

TEST_CASE("Stable C++API: copy function registration validation", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// No name.
	{
		auto function = CopyFunction::Create(conn);
		function.SetBatchCallback(NoopBatch).SetFlushCallback(NoopFlush);
		REQUIRE_THROWS_MATCHES(function.Register(), InvalidInputException, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}

	// The batch callback missing.
	{
		auto function = CopyFunction::Create(conn);
		function.SetName("cpp_copy_no_batch").SetFlushCallback(NoopFlush);
		REQUIRE_THROWS_MATCHES(function.Register(), InvalidInputException, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}

	// The flush callback missing.
	{
		auto function = CopyFunction::Create(conn);
		function.SetName("cpp_copy_no_flush").SetBatchCallback(NoopBatch);
		REQUIRE_THROWS_MATCHES(function.Register(), InvalidInputException, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}
}
