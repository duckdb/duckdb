#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/transaction/shared_transaction_lock.hpp"
#include "test_helpers.hpp"

#include <thread>

using namespace duckdb;

// Every case here needs something a .test file cannot express: a snapshot token travelling between
// connections, concurrent statements, a connection being destroyed, or the C++ transaction API.
// Single-connection validation lives in test/sql/transactions/transaction_snapshot_validation.test.

static string ExportTransactionSnapshot(Connection &connection) {
	auto result = connection.Query("SELECT duckdb_export_transaction_snapshot()");
	REQUIRE_NO_FAIL(*result);
	return result->GetValue(0, 0).GetValue<string>();
}

static void SetTransactionSnapshot(Connection &connection, const string &transaction_id) {
	REQUIRE_NO_FAIL(connection.Query("BEGIN"));
	REQUIRE_NO_FAIL(connection.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'"));
}

//! Publishes a token from inside a running statement and holds that statement open until released.
//! Deliberately built from atomics: a mutex plus condition variable inside a scalar function makes
//! ThreadSanitizer report the executor's re-entry into the function as a double lock.
struct CaptureTransactionState {
	mutex token_lock;
	string token;
	atomic<bool> captured {false};
	atomic<bool> release {false};
};

static bool WaitFor(const std::function<bool()> &condition, idx_t timeout_seconds = 5) {
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_seconds);
	while (!condition()) {
		if (std::chrono::steady_clock::now() > deadline) {
			return false;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return true;
}

static void RegisterCaptureTransactionFunction(Connection &connection,
                                               const shared_ptr<CaptureTransactionState> &capture) {
	ScalarFunction function("capture_shared_transaction", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                        [capture](DataChunk &input, ExpressionState &, Vector &result) {
		                        auto token = input.GetValue(0, 0).GetValue<string>();
		                        {
			                        lock_guard<mutex> guard(capture->token_lock);
			                        capture->token = token;
		                        }
		                        capture->captured = true;
		                        WaitFor([&]() { return capture->release.load(); });
		                        result.Reference(Value(token), count_t(input.size()));
	                        });
	CreateScalarFunctionInfo info(function);
	connection.context->RunFunctionInTransaction(
	    [&]() { Catalog::GetSystemCatalog(*connection.context).CreateFunction(*connection.context, info); });
}

static void ReleaseCapture(const shared_ptr<CaptureTransactionState> &capture) {
	capture->release = true;
}

static bool WaitForCapture(const shared_ptr<CaptureTransactionState> &capture) {
	return WaitFor([&]() { return capture->captured.load(); });
}

static string CapturedToken(const shared_ptr<CaptureTransactionState> &capture) {
	lock_guard<mutex> guard(capture->token_lock);
	return capture->token;
}

//! A barrier that only releases once `target` statements are inside the shared gate at the same time.
//! Atomics only, for the same reason as CaptureTransactionState.
struct ConcurrencyProbe {
	atomic<idx_t> active {0};
	atomic<idx_t> peak {0};
	idx_t target = 0;
	atomic<bool> released {false};
	atomic<bool> timed_out {false};
};

static void RegisterConcurrencyProbe(Connection &connection, const shared_ptr<ConcurrencyProbe> &probe) {
	ScalarFunction function("concurrency_probe", {LogicalType::BIGINT}, LogicalType::BIGINT,
	                        [probe](DataChunk &input, ExpressionState &, Vector &result) {
		                        auto arrived = ++probe->active;
		                        auto seen = probe->peak.load();
		                        while (arrived > seen && !probe->peak.compare_exchange_weak(seen, arrived)) {
		                        }
		                        if (arrived >= probe->target) {
			                        // The last arrival releases the round for everyone, permanently.
			                        probe->released = true;
		                        } else if (!WaitFor([&]() { return probe->released.load(); })) {
			                        // The gate serialized us: the round never filled.
			                        probe->timed_out = true;
			                        probe->released = true;
		                        }
		                        --probe->active;
		                        result.Reference(input.data[0]);
	                        });
	function.SetVolatile();
	CreateScalarFunctionInfo info(function);
	connection.context->RunFunctionInTransaction(
	    [&]() { Catalog::GetSystemCatalog(*connection.context).CreateFunction(*connection.context, info); });
}

TEST_CASE("Transactions can be shared between connections", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	Connection observer(database);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	auto transaction_id = ExportTransactionSnapshot(owner);
	SetTransactionSnapshot(joiner, transaction_id);

	// The joiner sees the owner's uncommitted rows; other connections do not.
	auto result = joiner.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = observer.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (2)"));
	result = joiner.Query("SELECT value FROM shared_values ORDER BY value");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

	// Participants only read.
	auto write = joiner.Query("INSERT INTO shared_values VALUES (3)");
	REQUIRE_FAIL(write);
	REQUIRE(write->GetError().find("only the owning connection can modify") != string::npos);

	// Only the owner commits.
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	result = observer.Query("SELECT value FROM shared_values ORDER BY value");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

	// The joiner is told the transaction has ended and detaches with ROLLBACK.
	auto ended = joiner.Query("SELECT count(*) FROM shared_values");
	REQUIRE_FAIL(ended);
	REQUIRE(ended->GetError().find("Shared transaction has ended") != string::npos);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	result = joiner.Query("SELECT value FROM shared_values ORDER BY value");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
}

TEST_CASE("A participant's COMMIT only detaches", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	Connection observer(database);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	SetTransactionSnapshot(joiner, ExportTransactionSnapshot(owner));
	// The participant's own temporary changes commit; the shared transaction stays with the owner.
	REQUIRE_NO_FAIL(joiner.Query("CREATE TEMP TABLE staged AS SELECT value FROM shared_values"));
	REQUIRE_NO_FAIL(joiner.Query("COMMIT"));
	auto result = joiner.Query("SELECT value FROM staged");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = observer.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = joiner.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	result = owner.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	result = observer.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Exporter rollback ends the joiner's view", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (42)"));
	SetTransactionSnapshot(joiner, ExportTransactionSnapshot(owner));
	auto result = joiner.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));

	REQUIRE_FAIL(joiner.Query("SELECT value FROM shared_values"));
	// COMMIT and ROLLBACK both detach once the owner has ended the transaction.
	REQUIRE_NO_FAIL(joiner.Query("COMMIT"));
	result = setup.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("Closing the owner rolls back a shared transaction", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));

	Connection joiner(database);
	{
		Connection owner(database);
		REQUIRE_NO_FAIL(owner.Query("BEGIN"));
		REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (42)"));
		SetTransactionSnapshot(joiner, ExportTransactionSnapshot(owner));
		auto result = joiner.Query("SELECT value FROM shared_values");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	REQUIRE_FAIL(joiner.Query("SELECT 42"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));

	auto result = setup.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("Closing a joiner leaves the shared transaction intact", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	auto transaction_id = ExportTransactionSnapshot(owner);
	{
		Connection joiner(database);
		SetTransactionSnapshot(joiner, transaction_id);
		auto result = joiner.Query("SELECT value FROM shared_values");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (2)"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	auto result = setup.Query("SELECT value FROM shared_values ORDER BY value");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
}

TEST_CASE("Closing the owner hands off an in-flight joiner statement", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection joiner(database);
	auto capture = make_shared_ptr<CaptureTransactionState>();
	RegisterCaptureTransactionFunction(setup, capture);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO shared_values VALUES (1)"));

	auto owner = make_uniq<Connection>(database);
	REQUIRE_NO_FAIL(owner->Query("BEGIN"));
	SetTransactionSnapshot(joiner, ExportTransactionSnapshot(*owner));
	unique_ptr<QueryResult> joiner_result;
	std::thread joiner_thread([&]() {
		joiner_result = joiner.Query("SELECT capture_shared_transaction(CAST(value AS VARCHAR)) FROM shared_values");
	});
	REQUIRE(WaitForCapture(capture));

	// Closing the owner cannot wait for readers, so it hands the transaction to the last participant instead.
	atomic<bool> owner_closed {false};
	std::thread close_thread([&]() {
		owner.reset();
		owner_closed = true;
	});
	close_thread.join();
	REQUIRE(owner_closed.load());

	// The statement that was already running still completes against the handed-off transaction.
	ReleaseCapture(capture);
	joiner_thread.join();
	REQUIRE_NO_FAIL(*joiner_result);

	// New work is refused, and detaching runs the rollback the owner left behind.
	REQUIRE_FAIL(joiner.Query("SELECT 42"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	auto result = setup.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Participant reads run concurrently and exclude owner writes", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection reader_a(database);
	Connection reader_b(database);
	auto capture = make_shared_ptr<CaptureTransactionState>();
	RegisterCaptureTransactionFunction(setup, capture);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values SELECT * FROM range(1000)"));
	auto transaction_id = ExportTransactionSnapshot(owner);
	SetTransactionSnapshot(reader_a, transaction_id);
	SetTransactionSnapshot(reader_b, transaction_id);

	// reader_a holds the statement lock shared for as long as its statement is blocked.
	unique_ptr<QueryResult> blocked_result;
	std::thread blocked_thread([&]() {
		blocked_result = reader_a.Query("SELECT capture_shared_transaction('token') FROM shared_values LIMIT 1");
	});
	REQUIRE(WaitForCapture(capture));

	// Another participant reads concurrently.
	auto result = reader_b.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1000}));

	// The owner's write waits for the reader to finish.
	atomic<bool> write_finished {false};
	unique_ptr<QueryResult> write_result;
	std::thread write_thread([&]() {
		write_result = owner.Query("INSERT INTO shared_values VALUES (1000)");
		write_finished = true;
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	REQUIRE(!write_finished.load());
	ReleaseCapture(capture);
	blocked_thread.join();
	write_thread.join();
	REQUIRE_NO_FAIL(*blocked_result);
	REQUIRE_NO_FAIL(*write_result);

	result = reader_b.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1001}));
	REQUIRE_NO_FAIL(reader_a.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(reader_b.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	result = setup.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1001}));
}

TEST_CASE("Appender on a joiner is rejected", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	SetTransactionSnapshot(joiner, ExportTransactionSnapshot(owner));

	bool rejected = false;
	try {
		Appender appender(joiner, "shared_values");
		appender.AppendRow(int32_t(1));
		appender.Close();
	} catch (std::exception &ex) {
		rejected = string(ex.what()).find("only the owning connection can modify") != string::npos;
	}
	REQUIRE(rejected);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	auto result = owner.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("Shared transaction ids are stable and preserve catalog names", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);

	REQUIRE_NO_FAIL(owner.Query("ATTACH ':memory:' AS \"catalog/with/slash\""));
	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE \"catalog/with/slash\".main.values_table (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO \"catalog/with/slash\".main.values_table VALUES (7)"));
	auto result = owner.Query("SELECT duckdb_export_transaction_snapshot('catalog/with/slash')");
	REQUIRE_NO_FAIL(*result);
	auto transaction_id = result->GetValue(0, 0).GetValue<string>();
	REQUIRE(ExportTransactionSnapshot(owner) == transaction_id);
	SetTransactionSnapshot(joiner, transaction_id);
	REQUIRE(ExportTransactionSnapshot(joiner) == transaction_id);

	result = joiner.Query("SELECT value FROM \"catalog/with/slash\".main.values_table");
	REQUIRE(CHECK_COLUMN(result, 0, {7}));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("Shared transactions use an explicit database boundary", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS database_a"));
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS database_b"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE database_a.main.values_table (value INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE database_b.main.values_table (value INTEGER)"));

	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO database_a.main.values_table VALUES (1)"));
	REQUIRE_NO_FAIL(owner.Query("SELECT * FROM database_b.main.values_table"));
	auto result = owner.Query("SELECT duckdb_export_transaction_snapshot('database_a')");
	REQUIRE_NO_FAIL(*result);
	auto transaction_id = result->GetValue(0, 0).GetValue<string>();
	SetTransactionSnapshot(joiner, transaction_id);
	result = joiner.Query("SELECT value FROM database_a.main.values_table");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// The joiner's other databases follow the usual rules; the shared database is read-only for it.
	REQUIRE_NO_FAIL(joiner.Query("INSERT INTO database_b.main.values_table VALUES (84)"));
	REQUIRE_FAIL(joiner.Query("INSERT INTO database_a.main.values_table VALUES (42)"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
	result = setup.Query("SELECT count(*) FROM database_b.main.values_table");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("Sharing occurs when the function executes", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);

	// Binding and explaining the function must not require or export a transaction.
	auto prepared = owner.Prepare("SELECT duckdb_export_transaction_snapshot() FROM range(4097)");
	REQUIRE(!prepared->HasError());
	REQUIRE_NO_FAIL(owner.Query("EXPLAIN SELECT duckdb_export_transaction_snapshot()"));

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	auto result = prepared->Execute();
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetResultType() == QueryResultType::MATERIALIZED_RESULT);
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	auto first_id = chunk->GetValue(0, 0).GetValue<string>();
	result.reset();
	SetTransactionSnapshot(joiner, first_id);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));

	// A new execution must export the current transaction instead of returning a cached capability.
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	result = prepared->Execute();
	REQUIRE_NO_FAIL(*result);
	chunk = result->Fetch();
	REQUIRE(chunk);
	auto second_id = chunk->GetValue(0, 0).GetValue<string>();
	result.reset();
	REQUIRE(second_id != first_id);
	SetTransactionSnapshot(joiner, second_id);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("Shared transaction ids are exact capabilities and expire", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	auto transaction_id = ExportTransactionSnapshot(owner);
	auto tampered_id = transaction_id;
	tampered_id[0] = tampered_id[0] == '0' ? '1' : '0';
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_FAIL(joiner.Query("SET TRANSACTION SNAPSHOT '" + tampered_id + "'"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));

	// Starting another transaction against the same database must not revive the old capability.
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("SELECT count(*) FROM shared_values"));
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_FAIL(joiner.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
}

TEST_CASE("SET TRANSACTION SNAPSHOT must precede any use of the database", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);

	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (42)"));
	auto transaction_id = ExportTransactionSnapshot(owner);

	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_NO_FAIL(joiner.Query("SELECT count(*) FROM shared_values"));
	auto late = joiner.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'");
	REQUIRE_FAIL(late);
	REQUIRE(late->GetError().find("must be executed before any statement") != string::npos);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));

	// Statements that only touch other databases do not count.
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_NO_FAIL(joiner.Query("CREATE TEMP TABLE staged (value INTEGER)"));
	REQUIRE_NO_FAIL(joiner.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'"));
	auto result = joiner.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("SET TRANSACTION SNAPSHOT validates local state before taking part", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	auto transaction_id = ExportTransactionSnapshot(owner);

	// Local changes block the join.
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_NO_FAIL(joiner.Query("INSERT INTO shared_values VALUES (2)"));
	REQUIRE_FAIL(joiner.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));

	// An invalidated local transaction blocks the join.
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	REQUIRE_FAIL(joiner.Query("SELECT CAST('not an integer' AS INTEGER)"));
	REQUIRE_FAIL(joiner.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));

	// Joining twice is rejected.
	SetTransactionSnapshot(joiner, transaction_id);
	REQUIRE_FAIL(joiner.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));

	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	auto result = setup.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("A meta transaction can only take part in one shared database", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS database_a"));
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS database_b"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE database_a.main.values_table (value INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE database_b.main.values_table (value INTEGER)"));

	Connection owner_a(database);
	REQUIRE_NO_FAIL(owner_a.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner_a.Query("INSERT INTO database_a.main.values_table VALUES (1)"));
	auto transaction_a = ExportTransactionSnapshot(owner_a);

	Connection owner_b(database);
	REQUIRE_NO_FAIL(owner_b.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner_b.Query("INSERT INTO database_b.main.values_table VALUES (2)"));
	auto transaction_b = ExportTransactionSnapshot(owner_b);

	Connection joiner_a(database);
	SetTransactionSnapshot(joiner_a, transaction_a);
	REQUIRE_FAIL(joiner_a.Query("SET TRANSACTION SNAPSHOT '" + transaction_b + "'"));
	REQUIRE_NO_FAIL(joiner_a.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner_a.Query("COMMIT"));
	REQUIRE_NO_FAIL(owner_b.Query("COMMIT"));
}

TEST_CASE("Participants cannot write regardless of the owner's mode", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN TRANSACTION READ ONLY"));
	SetTransactionSnapshot(joiner, ExportTransactionSnapshot(owner));
	auto insert_result = joiner.Query("INSERT INTO shared_values VALUES (1)");
	REQUIRE_FAIL(insert_result);
	REQUIRE(insert_result->GetError().find("only the owning connection can modify") != string::npos);
	REQUIRE(insert_result->GetError().find("\"\"memory\"\"") == string::npos);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	auto result = setup.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("Shared transaction capabilities survive database aliases", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS original_name"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE original_name.main.shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO original_name.main.shared_values VALUES (1)"));
	auto result = owner.Query("SELECT duckdb_export_transaction_snapshot('original_name')");
	REQUIRE_NO_FAIL(*result);
	auto transaction_id = result->GetValue(0, 0).GetValue<string>();
	REQUIRE_NO_FAIL(setup.Query("ALTER DATABASE original_name SET ALIAS TO renamed_database"));
	SetTransactionSnapshot(joiner, transaction_id);
	result = joiner.Query("SELECT value FROM renamed_database.main.shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	result = setup.Query("SELECT value FROM renamed_database.main.shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Shared transaction capabilities stay bound across detach and reattach", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	Connection late_joiner(database);
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS shared_database"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_database.main.shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_database.main.shared_values VALUES (1)"));
	auto result = owner.Query("SELECT duckdb_export_transaction_snapshot('shared_database')");
	REQUIRE_NO_FAIL(*result);
	auto transaction_id = result->GetValue(0, 0).GetValue<string>();
	REQUIRE_NO_FAIL(setup.Query("DETACH shared_database"));
	REQUIRE_NO_FAIL(setup.Query("ATTACH ':memory:' AS shared_database"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_database.main.shared_values (value INTEGER)"));
	SetTransactionSnapshot(joiner, transaction_id);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));

	// A connection that already resolved the new database under that name cannot adopt the old one.
	REQUIRE_NO_FAIL(late_joiner.Query("BEGIN"));
	REQUIRE_NO_FAIL(late_joiner.Query("SELECT count(*) FROM shared_database.main.shared_values"));
	auto join_result = late_joiner.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'");
	REQUIRE_FAIL(join_result);
	REQUIRE(join_result->GetError().find("different attached database") != string::npos);
	REQUIRE_NO_FAIL(late_joiner.Query("ROLLBACK"));

	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	result = setup.Query("SELECT count(*) FROM shared_database.main.shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE_NO_FAIL(setup.Query("SELECT 42"));
}

TEST_CASE("The sharing statement owns the shared statement lock before publishing", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	auto capture = make_shared_ptr<CaptureTransactionState>();
	RegisterCaptureTransactionFunction(owner, capture);
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(joiner.Query("BEGIN"));
	unique_ptr<QueryResult> owner_result;
	std::thread owner_thread([&]() {
		owner_result = owner.Query("SELECT capture_shared_transaction(duckdb_export_transaction_snapshot())");
	});
	REQUIRE(WaitForCapture(capture));
	atomic<bool> join_finished {false};
	unique_ptr<QueryResult> join_result;
	std::thread join_thread([&]() {
		join_result = joiner.Query("SET TRANSACTION SNAPSHOT '" + CapturedToken(capture) + "'");
		join_finished = true;
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	REQUIRE(!join_finished.load());
	ReleaseCapture(capture);
	owner_thread.join();
	join_thread.join();
	REQUIRE_NO_FAIL(*owner_result);
	REQUIRE_NO_FAIL(*join_result);
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("Waiting for a shared statement lock is interruptible", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("CREATE SEQUENCE shared_sequence"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	SetTransactionSnapshot(joiner, ExportTransactionSnapshot(owner));
	auto stream = joiner.SendQuery("SELECT i FROM range(10000000) t(i)");
	REQUIRE(stream->GetResultType() == QueryResultType::STREAM_RESULT);
	atomic<bool> query_finished {false};
	unique_ptr<QueryResult> blocked_result;
	std::thread blocked_thread([&]() {
		blocked_result = owner.Query("SELECT nextval('shared_sequence')");
		query_finished = true;
	});
	for (idx_t i = 0; i < 100 && !query_finished.load(); i++) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	auto was_waiting = !query_finished.load();
	owner.Interrupt();
	blocked_thread.join();
	REQUIRE(was_waiting);
	REQUIRE_FAIL(blocked_result);
	// The abandoned acquisition must not leave the connection counted as a gate holder: a stale count would make
	// its own teardown skip both the gate and the handoff.
	REQUIRE(!owner.context->HasSharedTransactionGuard());
	stream->Cast<StreamQueryResult>().Close();
	stream.reset();
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE(!owner.context->HasSharedTransactionGuard());
}

TEST_CASE("Waiting for a shared statement lock honours max_execution_time", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	Connection observer(database);
	REQUIRE_NO_FAIL(owner.Query("CREATE SEQUENCE shared_sequence"));
	REQUIRE_NO_FAIL(owner.Query("SELECT nextval('shared_sequence')"));
	REQUIRE_NO_FAIL(owner.Query("PREPARE read_value AS SELECT $1"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	SetTransactionSnapshot(joiner, ExportTransactionSnapshot(owner));
	REQUIRE_NO_FAIL(owner.Query("SET max_execution_time = 200"));
	string query;
	SECTION("Sequence increment") {
		query = "SELECT nextval('shared_sequence')";
	}
	SECTION("Nested sequence in an EXECUTE argument") {
		query = "EXECUTE read_value(abs(nextval('shared_sequence')))";
	}
	SECTION("Nested sequence in a table function argument") {
		query = "SELECT * FROM range(abs(nextval('shared_sequence')))";
	}
	SECTION("Sequence in a lambda body") {
		query = "SELECT * FROM range(list_transform([1], lambda x: abs(nextval('shared_sequence')))[1])";
	}
	auto stream = joiner.SendQuery("SELECT i FROM range(10000000) t(i)");
	REQUIRE(stream->GetResultType() == QueryResultType::STREAM_RESULT);
	auto start = std::chrono::steady_clock::now();
	auto blocked_result = owner.Query(query);
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
	REQUIRE_FAIL(blocked_result);
	REQUIRE(blocked_result->GetError().find("maximum execution time") != string::npos);
	// The deadline is checked on every poll of the lock, not on the throttled interrupt path.
	REQUIRE(elapsed.count() < 1500);
	auto sequence_result = observer.Query("SELECT currval('shared_sequence')");
	REQUIRE(CHECK_COLUMN(sequence_result, 0, {1}));
	stream->Cast<StreamQueryResult>().Close();
	stream.reset();
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("A waiting owner takes precedence over new readers", "[api][transaction_snapshot]") {
	SharedTransactionLock statement_lock;
	statement_lock.LockShared();
	atomic<bool> writer_acquired {false};
	std::thread writer([&]() {
		statement_lock.LockExclusive();
		writer_acquired = true;
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	REQUIRE(!writer_acquired.load());
	// A new reader must wait behind the writer even though a reader currently holds the lock.
	REQUIRE(!statement_lock.TryLockSharedFor(std::chrono::milliseconds(50)));
	statement_lock.UnlockShared();
	writer.join();
	REQUIRE(writer_acquired.load());
	statement_lock.UnlockExclusive();
	REQUIRE(statement_lock.TryLockSharedFor(std::chrono::milliseconds(50)));
	statement_lock.UnlockShared();
}

TEST_CASE("Shared transaction locks can be released by another thread", "[api][transaction_snapshot]") {
	auto statement_lock = make_shared_ptr<SharedTransactionLock>();
	atomic<bool> acquired {false};
	std::thread worker([&]() { acquired = statement_lock->TryLockExclusiveFor(std::chrono::seconds(1)); });
	worker.join();
	REQUIRE(acquired.load());
	statement_lock->UnlockExclusive();
	REQUIRE(statement_lock->TryLockSharedFor(std::chrono::seconds(1)));
	REQUIRE(statement_lock->TryLockSharedFor(std::chrono::seconds(1)));
	REQUIRE(!statement_lock->TryLockExclusiveFor(std::chrono::milliseconds(10)));
	statement_lock->UnlockShared();
	statement_lock->UnlockShared();
	statement_lock->LockExclusive();
	statement_lock->UnlockExclusive();
}

TEST_CASE("Participant work fails once the owner has ended", "[api][transaction_snapshot]") {
	// A participant reaches its borrowed transaction only from paths that hold the statement lock, so the checks
	// live there rather than in a bare Transaction::TryGet. Drive them through a query and through the C++ API.
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (1)"));
	SetTransactionSnapshot(joiner, ExportTransactionSnapshot(owner));

	// While the owner holds the transaction, both reach it happily.
	auto result = joiner.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(joiner.TableInfo("shared_values"));

	REQUIRE_NO_FAIL(owner.Query("COMMIT"));

	// Both refuse afterwards: the statement path with an error, the API path by throwing.
	auto ended = joiner.Query("SELECT value FROM shared_values");
	REQUIRE_FAIL(ended);
	REQUIRE(ended->GetError().find("Shared transaction has ended") != string::npos);
	REQUIRE_THROWS(joiner.TableInfo("shared_values"));

	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	result = joiner.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Destroying the owner during unwinding hands off the transaction", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection joiner(database);
	auto capture = make_shared_ptr<CaptureTransactionState>();
	RegisterCaptureTransactionFunction(setup, capture);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO shared_values VALUES (1)"));

	unique_ptr<QueryResult> joiner_result;
	std::thread joiner_thread;
	try {
		Connection owner(database);
		REQUIRE_NO_FAIL(owner.Query("BEGIN"));
		REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (2)"));
		SetTransactionSnapshot(joiner, ExportTransactionSnapshot(owner));
		joiner_thread = std::thread([&]() {
			joiner_result =
			    joiner.Query("SELECT capture_shared_transaction(CAST(value AS VARCHAR)) FROM shared_values");
		});
		REQUIRE(WaitForCapture(capture));
		// Unwinding destroys the owner while the joiner's statement is still running. This path skips
		// ClientContext::Destroy, so ~TransactionContext must hand the transaction over rather than block.
		throw std::runtime_error("unwind");
	} catch (std::runtime_error &) {
	}
	ReleaseCapture(capture);
	joiner_thread.join();
	REQUIRE_NO_FAIL(*joiner_result);

	REQUIRE_FAIL(joiner.Query("SELECT 42"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	auto result = setup.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Joiner temporary changes roll back on detach", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (42)"));
	SetTransactionSnapshot(joiner, ExportTransactionSnapshot(owner));
	REQUIRE_NO_FAIL(joiner.Query("CREATE TEMP TABLE staged AS SELECT value FROM shared_values"));
	auto result = joiner.Query("SELECT value FROM staged");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_FAIL(joiner.Query("SELECT * FROM staged"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	result = owner.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}

TEST_CASE("The owner and participants read the snapshot concurrently", "[api][transaction_snapshot]") {
	constexpr idx_t PARTICIPANT_COUNT = 4;
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	auto probe = make_shared_ptr<ConcurrencyProbe>();
	probe->target = PARTICIPANT_COUNT + 1;
	RegisterConcurrencyProbe(setup, probe);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value BIGINT)"));

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	// Uncommitted: only the owner and its participants can see these rows.
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values SELECT * FROM range(1000)"));
	auto transaction_id = ExportTransactionSnapshot(owner);

	vector<unique_ptr<Connection>> participants;
	for (idx_t i = 0; i < PARTICIPANT_COUNT; i++) {
		participants.push_back(make_uniq<Connection>(database));
		SetTransactionSnapshot(*participants.back(), transaction_id);
	}

	const string query = "SELECT count(concurrency_probe(value)) FROM shared_values";
	unique_ptr<PreparedStatement> prepared;
	SECTION("Direct owner query") {
	}
	SECTION("Prepared owner query") {
		prepared = owner.Prepare(query);
		REQUIRE(!prepared->HasError());
	}
	// The owner and every participant must execute simultaneously, otherwise the probe times out.
	vector<unique_ptr<QueryResult>> results(PARTICIPANT_COUNT + 1);
	vector<std::thread> threads;
	for (idx_t i = 0; i < PARTICIPANT_COUNT; i++) {
		threads.emplace_back([&, i]() { results[i] = participants[i]->Query(query); });
	}
	threads.emplace_back([&]() { results[PARTICIPANT_COUNT] = prepared ? prepared->Execute() : owner.Query(query); });
	for (auto &thread : threads) {
		thread.join();
	}
	for (idx_t i = 0; i <= PARTICIPANT_COUNT; i++) {
		REQUIRE_NO_FAIL(*results[i]);
		REQUIRE(CHECK_COLUMN(results[i], 0, {1000}));
	}
	REQUIRE(!probe->timed_out.load());
	REQUIRE(probe->peak.load() == PARTICIPANT_COUNT + 1);

	for (auto &participant : participants) {
		REQUIRE_NO_FAIL(participant->Query("ROLLBACK"));
	}
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("Owner writes and finalization wait for every participant stream", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection observer(database);
	Connection reader_a(database);
	Connection reader_b(database);
	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value BIGINT)"));
	REQUIRE_NO_FAIL(owner.Query("CREATE SEQUENCE shared_sequence"));
	REQUIRE_NO_FAIL(owner.Query("SELECT nextval('shared_sequence')"));
	REQUIRE_NO_FAIL(owner.Query("PREPARE read_value AS SELECT $1"));
	REQUIRE_NO_FAIL(owner.Query("PREPARE insert_value AS INSERT INTO shared_values VALUES ($1)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values SELECT * FROM range(100000)"));
	auto transaction_id = ExportTransactionSnapshot(owner);
	SetTransactionSnapshot(reader_a, transaction_id);
	SetTransactionSnapshot(reader_b, transaction_id);

	string query;
	bool finalizes = false;
	SECTION("Insert") {
		query = "INSERT INTO shared_values VALUES (-1)";
	}
	SECTION("Prepared insert") {
		query = "EXECUTE insert_value(-1)";
	}
	SECTION("Update") {
		query = "UPDATE shared_values SET value = value + 1";
	}
	SECTION("Delete") {
		query = "DELETE FROM shared_values";
	}
	SECTION("Schema change") {
		query = "ALTER TABLE shared_values ADD COLUMN extra VARCHAR";
	}
	SECTION("Sequence increment") {
		query = "SELECT nextval('shared_sequence')";
	}
	SECTION("Sequence assignment") {
		query = "SELECT setval('shared_sequence', 42)";
	}
	SECTION("Sequence in an EXECUTE argument") {
		query = "EXECUTE read_value(nextval('shared_sequence'))";
	}
	SECTION("Sequence in a table function argument") {
		query = "SELECT * FROM range(nextval('shared_sequence'))";
	}
	SECTION("Nested sequence in an EXECUTE argument") {
		query = "EXECUTE read_value(abs(nextval('shared_sequence')))";
	}
	SECTION("Nested sequence in a table function argument") {
		query = "SELECT * FROM range(abs(nextval('shared_sequence')))";
	}
	SECTION("Nested sequence assignment") {
		query = "SELECT * FROM range(abs(setval('shared_sequence', 42)))";
	}
	SECTION("Sequence in a lambda body") {
		query = "SELECT * FROM range(list_transform([1], lambda x: abs(nextval('shared_sequence')))[1])";
	}
	SECTION("Commit") {
		query = "COMMIT";
		finalizes = true;
	}
	SECTION("Rollback") {
		query = "ROLLBACK";
		finalizes = true;
	}

	auto stream_a = reader_a.SendQuery("SELECT value FROM shared_values");
	auto stream_b = reader_b.SendQuery("SELECT value FROM shared_values");
	REQUIRE(stream_a->GetResultType() == QueryResultType::STREAM_RESULT);
	REQUIRE(stream_b->GetResultType() == QueryResultType::STREAM_RESULT);
	REQUIRE(reader_a.context->HasSharedTransactionGuard());
	REQUIRE(reader_b.context->HasSharedTransactionGuard());

	atomic<bool> started {false};
	atomic<bool> finished {false};
	unique_ptr<QueryResult> result;
	std::thread writer([&]() {
		started = true;
		result = owner.Query(query);
		finished = true;
	});
	auto did_start = WaitFor([&]() { return started.load(); });
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	auto waited_for_both = !finished.load();
	auto sequence_result = observer.Query("SELECT currval('shared_sequence')");
	stream_a->Cast<StreamQueryResult>().Close();
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	auto waited_for_last = !finished.load();
	stream_b->Cast<StreamQueryResult>().Close();
	writer.join();
	REQUIRE(did_start);
	REQUIRE(waited_for_both);
	REQUIRE(waited_for_last);
	REQUIRE(CHECK_COLUMN(sequence_result, 0, {1}));
	REQUIRE_NO_FAIL(*result);
	REQUIRE_NO_FAIL(reader_a.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(reader_b.Query("ROLLBACK"));
	if (!finalizes) {
		REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	}
}

TEST_CASE("An owner stream allows participant reads between inserts", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE shared_values (value BIGINT)"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values SELECT * FROM range(100000)"));
	SetTransactionSnapshot(joiner, ExportTransactionSnapshot(owner));
	REQUIRE_NO_FAIL(joiner.Query("SET max_execution_time = 2000"));
	for (idx_t round = 0; round < 3; round++) {
		auto stream = owner.SendQuery("SELECT value FROM shared_values");
		REQUIRE(stream->GetResultType() == QueryResultType::STREAM_RESULT);
		auto result = joiner.Query("SELECT count(*) FROM shared_values");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(100000 + round)}));
		stream->Cast<StreamQueryResult>().Close();
		REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (-1)"));
	}
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	auto result = joiner.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {100003}));
}

TEST_CASE("Participant sequence modifications are rejected before evaluation", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(owner.Query("CREATE SEQUENCE shared_sequence"));
	REQUIRE_NO_FAIL(owner.Query("SELECT nextval('shared_sequence')"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	auto transaction_id = ExportTransactionSnapshot(owner);
	REQUIRE_NO_FAIL(joiner.Query("PREPARE read_value AS SELECT $1"));
	REQUIRE_NO_FAIL(joiner.Query("PREPARE advance_sequence AS SELECT nextval($1)"));
	const vector<string> queries {
	    "SELECT nextval('shared_sequence')",
	    "SELECT setval('shared_sequence', 42)",
	    "EXECUTE advance_sequence('shared_sequence')",
	    "EXECUTE read_value(nextval('shared_sequence'))",
	    "SELECT * FROM range(nextval('shared_sequence'))",
	    "SET threads = nextval('shared_sequence')",
	    "EXECUTE read_value(abs(nextval('shared_sequence')))",
	    "SELECT * FROM range(abs(nextval('shared_sequence')))",
	    "SELECT * FROM range(abs(setval('shared_sequence', 42)))",
	    "SELECT * FROM range(list_transform([1], lambda x: abs(nextval('shared_sequence')))[1])",
	    "SET threads = abs(nextval('shared_sequence'))",
	};
	for (auto &query : queries) {
		INFO(query);
		SetTransactionSnapshot(joiner, transaction_id);
		auto result = joiner.Query(query);
		REQUIRE_FAIL(result);
		INFO(result->GetError());
		REQUIRE(result->GetError().find("only the owning connection can modify") != string::npos);
		result = owner.Query("SELECT currval('shared_sequence')");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	}
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("A participant statement still uses intra-query parallelism", "[api][transaction_snapshot]") {
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value BIGINT)"));

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values SELECT * FROM range(500000)"));
	SetTransactionSnapshot(joiner, ExportTransactionSnapshot(owner));

	REQUIRE_NO_FAIL(joiner.Query("SET threads = 4"));
	auto result = joiner.Query("SELECT count(*), sum(value) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {500000}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(124999750000LL)}));
	// The gate is held once per statement, so the scan is free to fan out across the thread pool.
	result = joiner.Query("SELECT current_setting('threads')");
	REQUIRE(CHECK_COLUMN(result, 0, {Value("4")}));

	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
}

TEST_CASE("A rollback outside a statement hands off to participants", "[api][transaction_snapshot]") {
	// Owner rollbacks do not all happen inside a statement: automatic rollback of a failed statement runs after
	// EndQueryInternal released that query's guard, and teardown runs with no active query at all. Such a rollback
	// must not tear the transaction down underneath a participant, and must not block waiting for one either.
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	auto capture = make_shared_ptr<CaptureTransactionState>();
	RegisterCaptureTransactionFunction(setup, capture);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO shared_values VALUES (1)"));

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (2)"));
	SetTransactionSnapshot(joiner, ExportTransactionSnapshot(owner));

	unique_ptr<QueryResult> joiner_result;
	std::thread joiner_thread([&]() {
		joiner_result = joiner.Query("SELECT capture_shared_transaction(CAST(value AS VARCHAR)) FROM shared_values");
	});
	REQUIRE(WaitForCapture(capture));

	// An explicit rollback outside a statement takes the gate, so it waits for the in-flight participant.
	atomic<bool> rollback_finished {false};
	std::thread rollback_thread([&]() {
		owner.context->transaction.Rollback(nullptr);
		rollback_finished = true;
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(150));
	REQUIRE(!rollback_finished.load());
	ReleaseCapture(capture);
	joiner_thread.join();
	rollback_thread.join();
	REQUIRE(rollback_finished.load());
	REQUIRE_NO_FAIL(*joiner_result);

	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	auto result = setup.Query("SELECT value FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("A participant streams while the owner is destroyed", "[api][transaction_snapshot]") {
	// A participant's streaming result holds the statement lock until it is drained. The owner cannot wait for
	// that in its destructor, so destruction hands the transaction to the last participant instead of tearing it
	// down. Without that handoff a single thread holding an unconsumed participant result deadlocks itself here.
	DuckDB database(nullptr);
	Connection setup(database);
	Connection joiner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value BIGINT)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO shared_values SELECT * FROM range(100000)"));

	auto owner = make_uniq<Connection>(database);
	REQUIRE_NO_FAIL(owner->Query("BEGIN"));
	REQUIRE_NO_FAIL(owner->Query("INSERT INTO shared_values VALUES (-1)"));
	SetTransactionSnapshot(joiner, ExportTransactionSnapshot(*owner));

	// Participants still stream.
	auto stream = joiner.SendQuery("SELECT value FROM shared_values");
	REQUIRE(stream->GetResultType() == QueryResultType::STREAM_RESULT);
	auto chunk = stream->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() > 0);

	// Destroying the owner on this very thread must not block, even though the stream is still open.
	owner.reset();

	// The stream keeps returning the snapshot it started on, including the owner's uncommitted row.
	idx_t rows = chunk->size();
	while (auto next = stream->Fetch()) {
		if (next->size() == 0) {
			break;
		}
		rows += next->size();
	}
	REQUIRE(rows == 100001);
	stream.reset();

	REQUIRE_FAIL(joiner.Query("SELECT 42"));
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	// The last participant out rolled the owner's transaction back.
	auto result = setup.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {100000}));
}

TEST_CASE("Abandoning a participant stream is safe", "[api][transaction_snapshot]") {
	// StreamQueryResult's destructor does not close the result, so an abandoned stream leaves its scan tasks
	// running. Teardown must drain them before the participant detaches, because detaching can be what runs the
	// rollback the owner handed over, freeing the very transaction those tasks are reading.
	DuckDB database(nullptr);
	Connection setup(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value BIGINT)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO shared_values SELECT * FROM range(200000)"));

	{
		Connection joiner(database);
		auto owner = make_uniq<Connection>(database);
		REQUIRE_NO_FAIL(owner->Query("BEGIN"));
		REQUIRE_NO_FAIL(owner->Query("INSERT INTO shared_values VALUES (-1)"));
		SetTransactionSnapshot(joiner, ExportTransactionSnapshot(*owner));

		auto stream = joiner.SendQuery("SELECT value FROM shared_values");
		REQUIRE(stream->GetResultType() == QueryResultType::STREAM_RESULT);
		REQUIRE(stream->Fetch());
		// The owner goes first, handing its transaction to the participant.
		owner.reset();
		// Both the stream and the participant are then abandoned without being drained or rolled back.
	}

	auto result = setup.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {200000}));
}

TEST_CASE("An invalidated owner stops participants reading", "[api][transaction_snapshot]") {
	// A failed statement invalidates its transaction, and DuckDB's guarantee is that nobody observes what that
	// statement left behind. Sharing the transaction hands that state to other connections, so the owner has to
	// close them off: an invalidated transaction is certain to roll back and must no longer be read or joined.
	DuckDB database(nullptr);
	Connection setup(database);
	Connection owner(database);
	Connection joiner(database);
	Connection active_reader(database);
	Connection late_joiner(database);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE shared_values (value INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO shared_values VALUES (1)"));

	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("INSERT INTO shared_values VALUES (2)"));
	auto transaction_id = ExportTransactionSnapshot(owner);
	SetTransactionSnapshot(joiner, transaction_id);
	SetTransactionSnapshot(active_reader, transaction_id);
	auto stream = active_reader.SendQuery("SELECT value FROM shared_values, range(100000)");
	REQUIRE(stream->GetResultType() == QueryResultType::STREAM_RESULT);
	REQUIRE_NO_FAIL(owner.Query("SET max_execution_time = 2000"));

	// While the transaction is sound the participant reads the owner's uncommitted row.
	auto result = joiner.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	// A failed statement on the owner invalidates the transaction.
	REQUIRE_FAIL(owner.Query("SELECT CAST('not an integer' AS INTEGER)"));

	// The participant that already joined may no longer read it.
	auto rejected = joiner.Query("SELECT count(*) FROM shared_values");
	REQUIRE_FAIL(rejected);
	REQUIRE(rejected->GetError().find("no longer readable") != string::npos);
	// Neither may the C++ API, which reaches the transaction without a query.
	REQUIRE_THROWS(joiner.TableInfo("shared_values"));

	// A connection that had not joined yet is refused outright.
	REQUIRE_NO_FAIL(late_joiner.Query("BEGIN"));
	auto refused = late_joiner.Query("SET TRANSACTION SNAPSHOT '" + transaction_id + "'");
	REQUIRE_FAIL(refused);
	REQUIRE(refused->GetError().find("no longer available") != string::npos);
	REQUIRE_NO_FAIL(late_joiner.Query("ROLLBACK"));

	// An already running read can finish against the unchanged data, even though new statements are refused.
	idx_t rows = 0;
	while (auto chunk = stream->Fetch()) {
		rows += chunk->size();
	}
	REQUIRE_NO_FAIL(*stream);
	REQUIRE(rows == 200000);
	REQUIRE_NO_FAIL(active_reader.Query("ROLLBACK"));

	// Detaching still works, and the owner cannot keep what it wrote: an invalidated transaction turns COMMIT
	// into ROLLBACK, which is exactly why participants must stop reading it.
	REQUIRE_NO_FAIL(joiner.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(owner.Query("COMMIT"));
	result = setup.Query("SELECT count(*) FROM shared_values");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}
