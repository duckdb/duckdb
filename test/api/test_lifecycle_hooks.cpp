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
	void QueryEnd(optional_ptr<ErrorData> error) override {
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

TEST_CASE("Test ClientContextState", "[api]") {
	DuckDB db(nullptr);
	Connection conn(db);
	conn.Query("CREATE TABLE my_table(i INT)");
	auto &register_state = conn.context->registered_state;
	register_state["test_state"] = make_uniq<TestClientContextState>();
	auto &state = static_cast<TestClientContextState &>(*register_state["test_state"]);

	table_function_bind_t bind = [](ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &return_types,
	                                vector<string> &names) -> unique_ptr<FunctionData> {
		return_types.push_back(LogicalType::VARCHAR);
		names.push_back("message");
		return nullptr;
	};

	const TableFunction table_fun(
	    "raise_exception_tf", {},
	    [](ClientContext &, TableFunctionInput &, DataChunk &) {
		    throw std::runtime_error("This is a test exception.");
	    },
	    bind);

	ExtensionUtil::RegisterFunction(*db.instance, table_fun);

	SECTION("No error and no explicit transaction") {
		REQUIRE_NO_FAIL(conn.Query("SELECT * FROM my_table"));
		REQUIRE(state.query_errors.empty());
		REQUIRE(state.transaction_errors.empty());
	}

	SECTION("Error and no explicit transaction") {
		REQUIRE_FAIL(conn.Query("SELECT * FROM this_table_does_not_exist"));
		REQUIRE((state.query_errors.size() == 1));
		REQUIRE_THAT(state.query_errors.at(0), Contains("Table with name this_table_does_not_exist does not exist!"));
		REQUIRE((state.transaction_errors.size() == 1));
		REQUIRE_THAT(state.transaction_errors.at(0),
		             Contains("Table with name this_table_does_not_exist does not exist!"));
	}

	SECTION("No error and explicit transaction") {
		conn.BeginTransaction();
		REQUIRE_NO_FAIL(conn.Query("SELECT * FROM my_table"));
		conn.Commit();
		REQUIRE(state.query_errors.empty());
		REQUIRE(state.transaction_errors.empty());
	}

	SECTION("No error and explicit rollback") {
		conn.BeginTransaction();
		REQUIRE_NO_FAIL(conn.Query("SELECT * FROM my_table"));
		conn.Rollback();
		REQUIRE(state.query_errors.empty());
		REQUIRE(state.transaction_errors.empty());
	}

	SECTION("No error and explicit error rollback") {
		// Multi statement transactions cannot have errors associated with them, as they have to be deliberate
		// -> no error here
		conn.BeginTransaction();
		REQUIRE_FAIL(conn.Query("SELECT * FROM this_table_does_not_exist_1"));
		REQUIRE_FAIL(conn.Query("SELECT * FROM this_table_does_not_exist_2"));
		conn.Rollback();
		REQUIRE((state.query_errors.size() == 2));
		REQUIRE_THAT(state.query_errors.at(0), Contains("Table with name this_table_does_not_exist_1 does not exist!"));
		REQUIRE_THAT(state.query_errors.at(1), Contains("Table with name this_table_does_not_exist_2 does not exist!"));
		REQUIRE((state.transaction_errors.empty()));
	}

	SECTION("Error during runtime") {
		REQUIRE_FAIL(conn.Query("SELECT * FROM raise_exception_tf()"));
		REQUIRE((state.query_errors.size() == 1));
		REQUIRE_THAT(state.query_errors.at(0), Contains("This is a test exception."));
		REQUIRE((state.transaction_errors.size() == 1));
		REQUIRE_THAT(state.transaction_errors.at(0), Contains("This is a test exception."));
	}
}
// ClientContextState
