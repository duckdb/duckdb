#include "catch.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace Catch::Matchers;

struct TestClientContextState : ClientContextState {
	vector<string> query_errors;
	vector<string> transaction_errors;

	TestClientContextState() = default;
	TestClientContextState(const TestClientContextState &) = delete;

	void QueryEnd(ClientContext &, optional_ptr<ErrorData> error) override {
		if (error && error->HasError()) {
			query_errors.push_back(error->Message());
		}
	}

	void TransactionRollback(MetaTransaction &transaction, ClientContext &context,
	                         optional_ptr<ErrorData> error) override {
		if (error && error->HasError()) {
			transaction_errors.push_back(error->Message());
		}
	}
};

shared_ptr<TestClientContextState> WithLifecycleState(const Connection &conn) {
	auto &register_state = conn.context->registered_state;
	auto state = make_shared_ptr<TestClientContextState>();
	register_state->Insert("test_state", state);
	return state;
}

TEST_CASE("Test ClientContextState", "[api]") {
	DuckDB db(nullptr);
	Connection conn(db);
	conn.Query("CREATE TABLE my_table(i INT)");
	auto state = WithLifecycleState(conn);

	const TableFunction table_fun(
	    "raise_exception_tf", {},
	    [](ClientContext &, TableFunctionInput &, DataChunk &) {
		    throw std::runtime_error("This is a test exception.");
	    },
	    [](ClientContext &, TableFunctionBindInput &, vector<LogicalType> &return_types,
	       vector<string> &names) -> unique_ptr<FunctionData> {
		    return_types.push_back(LogicalType::VARCHAR);
		    names.push_back("message");
		    return nullptr;
	    });

	ExtensionUtil::RegisterFunction(*db.instance, table_fun);

	SECTION("No error, No explicit transaction") {
		REQUIRE_NO_FAIL(conn.Query("SELECT * FROM my_table"));
		REQUIRE(state->query_errors.empty());
		REQUIRE(state->transaction_errors.empty());
	}

	SECTION("Error, No explicit transaction") {
		REQUIRE_FAIL(conn.Query("SELECT * FROM this_table_does_not_exist"));
		REQUIRE((state->query_errors.size() == 1));
		REQUIRE_THAT(state->query_errors.at(0), Contains("Table with name this_table_does_not_exist does not exist!"));
		REQUIRE((state->transaction_errors.size() == 1));
		REQUIRE_THAT(state->transaction_errors.at(0),
		             Contains("Table with name this_table_does_not_exist does not exist!"));
	}

	SECTION("No error, Explicit commit") {
		conn.BeginTransaction();
		REQUIRE_NO_FAIL(conn.Query("SELECT * FROM my_table"));
		conn.Commit();
		REQUIRE(state->query_errors.empty());
		REQUIRE(state->transaction_errors.empty());
	}

	SECTION("No error, Explicit rollback") {
		conn.BeginTransaction();
		REQUIRE_NO_FAIL(conn.Query("SELECT * FROM my_table"));
		conn.Rollback();
		REQUIRE(state->query_errors.empty());
		REQUIRE(state->transaction_errors.empty());
	}

	SECTION("Binding error, Explicit rollback") {
		// These errors do not invalidate the transaction...
		conn.BeginTransaction();
		REQUIRE_FAIL(conn.Query("SELECT * FROM this_table_does_not_exist_1"));
		REQUIRE_FAIL(conn.Query("SELECT * FROM this_table_does_not_exist_2"));
		REQUIRE_NO_FAIL(conn.Query("SELECT * FROM my_table"));
		conn.Rollback();
		REQUIRE((state->query_errors.size() == 2));
		REQUIRE_THAT(state->query_errors.at(0),
		             Contains("Table with name this_table_does_not_exist_1 does not exist!"));
		REQUIRE_THAT(state->query_errors.at(1),
		             Contains("Table with name this_table_does_not_exist_2 does not exist!"));
		REQUIRE((state->transaction_errors.empty()));
	}

	SECTION("Binding error, Explicit commit") {
		// These errors do not invalidate the transaction...
		conn.BeginTransaction();
		REQUIRE_FAIL(conn.Query("SELECT * FROM this_table_does_not_exist_1"));
		REQUIRE_FAIL(conn.Query("SELECT * FROM this_table_does_not_exist_2"));
		REQUIRE_NO_FAIL(conn.Query("SELECT * FROM my_table"));
		conn.Commit();
		REQUIRE((state->query_errors.size() == 2));
		REQUIRE_THAT(state->query_errors.at(0),
		             Contains("Table with name this_table_does_not_exist_1 does not exist!"));
		REQUIRE_THAT(state->query_errors.at(1),
		             Contains("Table with name this_table_does_not_exist_2 does not exist!"));
		REQUIRE((state->transaction_errors.empty()));
	}

	SECTION("Runtime error, Explicit commit") {
		conn.BeginTransaction();
		REQUIRE_NO_FAIL(conn.Query("INSERT INTO my_table VALUES (1)"));
		REQUIRE_FAIL(conn.Query("SELECT * FROM raise_exception_tf()"));
		REQUIRE_FAIL(conn.Query("CREATE TABLE my_table2(i INT)"));
		conn.Commit();
		REQUIRE((state->query_errors.size() == 2));
		REQUIRE_THAT(state->query_errors.at(0), Contains("This is a test exception."));
		REQUIRE_THAT(state->query_errors.at(1), Contains("Current transaction is aborted"));
		REQUIRE((state->transaction_errors.size() == 1));
		REQUIRE_THAT(state->transaction_errors.at(0), Contains("Failed to commit"));
		REQUIRE_FAIL(conn.Query("SELECT * FROM my_table2"));
	}

	SECTION("Runtime error, No explicit transaction") {
		REQUIRE_FAIL(conn.Query("SELECT * FROM raise_exception_tf()"));
		REQUIRE((state->query_errors.size() == 1));
		REQUIRE_THAT(state->query_errors.at(0), Contains("This is a test exception."));
		REQUIRE((state->transaction_errors.size() == 1));
		REQUIRE_THAT(state->transaction_errors.at(0), Contains("This is a test exception."));
	}

	SECTION("Manually invalidated transaction, Explicit commit") {
		conn.BeginTransaction();
		REQUIRE_NO_FAIL(conn.Query("CREATE TABLE my_table2(i INT)"));
		auto &transaction = conn.context->ActiveTransaction();
		ValidChecker::Invalidate(transaction, "42");
		try {
			conn.Commit();
		} catch (...) {
			// Ignore
		}
		REQUIRE((state->transaction_errors.size() == 1));
		REQUIRE_FAIL(conn.Query("SELECT * FROM my_table2"));
	}

	SECTION("Manually invalidated transaction, Explicit rollback") {
		conn.BeginTransaction();
		REQUIRE_NO_FAIL(conn.Query("CREATE TABLE my_table2(i INT)"));
		auto &transaction = conn.context->ActiveTransaction();
		ValidChecker::Invalidate(transaction, "42");
		conn.Rollback();
		REQUIRE((state->transaction_errors.size() == 1));
		REQUIRE_FAIL(conn.Query("SELECT * FROM my_table2"));
	}
}
// ClientContextState
