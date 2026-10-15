#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

using namespace duckdb;

namespace {

//! The SQL of every statement routed to the connected database
vector<string> routed_queries;

//! A catalog that can be CONNECT-ed to, and answers every routed statement with its SQL
class RoutingCatalog : public DuckCatalog {
public:
	explicit RoutingCatalog(AttachedDatabase &db) : DuckCatalog(db) {
	}

public:
	using DuckCatalog::RemoteExecute;

	bool Supports(RemoteCapability capability) const override {
		return capability == RemoteCapability::CONNECT;
	}

	unique_ptr<TableRef> RemoteExecute(ClientContext &context, const string &sql) override {
		routed_queries.push_back(sql);
		auto node = make_uniq<SelectNode>();
		node->select_list.push_back(ConstantExpression::String(sql));
		node->from_table = make_uniq<EmptyTableRef>();
		auto select = make_uniq<SelectStatement>();
		select->node = std::move(node);
		return make_uniq<SubqueryRef>(std::move(select));
	}
};

struct RoutingStorageExtension : StorageExtension {
	RoutingStorageExtension() {
		attach = [](optional_ptr<StorageExtensionInfo>, ClientContext &, AttachedDatabase &db, const string &,
		            AttachInfo &info, AttachOptions &) -> unique_ptr<Catalog> {
			info.path = IN_MEMORY_PATH;
			return make_uniq_base<Catalog, RoutingCatalog>(db);
		};
		create_transaction_manager = [](optional_ptr<StorageExtensionInfo>, AttachedDatabase &db,
		                                Catalog &) -> unique_ptr<TransactionManager> {
			return make_uniq<DuckTransactionManager>(db);
		};
	}
};

} // namespace

TEST_CASE("Prepare statements while CONNECT-ed", "[api][connect]") {
	DBConfig config;
	StorageExtension::Register(config, "connect_routing_test", make_shared_ptr<RoutingStorageExtension>());
	DuckDB db(nullptr, &config);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("ATTACH 'routed' AS remote (TYPE connect_routing_test)"));
	// parsed before CONNECT, so its parameter is known
	auto parameterized = con.ExtractStatements("SELECT $1::INTEGER");
	REQUIRE_NO_FAIL(con.Query("CONNECT remote"));
	routed_queries.clear();

	// preparing routes nothing
	auto select = con.Prepare("SELECT 42");
	REQUIRE(!select->HasError());
	auto insert = con.Prepare("INSERT INTO t VALUES (1)");
	REQUIRE(!insert->HasError());
	REQUIRE(routed_queries.empty());

	// each execution routes the statement as written
	auto result = select->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {"SELECT 42"}));
	result = insert->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {"INSERT INTO t VALUES (1)"}));
	result = insert->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {"INSERT INTO t VALUES (1)"}));
	REQUIRE(routed_queries == vector<string> {"SELECT 42", "INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (1)"});

	// parameter values cannot be routed
	REQUIRE(insert->GetParameterCount() == 0);
	vector<Value> values {Value::INTEGER(1)};
	result = insert->Execute(values);
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->GetError(), "cannot be executed while CONNECT-ed"));
	auto prepared = con.Prepare(std::move(parameterized[0]));
	REQUIRE(prepared->HasError());
	REQUIRE(StringUtil::Contains(prepared->GetError(), "cannot be executed while CONNECT-ed"));
	REQUIRE(routed_queries.size() == 3);

	// DISCONNECT prepared while CONNECT-ed runs locally
	auto disconnect = con.Prepare("DISCONNECT");
	REQUIRE(!disconnect->HasError());
	REQUIRE_NO_FAIL(disconnect->Execute());
	REQUIRE(!con.context->IsConnected());
	REQUIRE(routed_queries.size() == 3);

	// statements prepared for the connected database do not run locally
	REQUIRE_FAIL(select->Execute());
	REQUIRE_FAIL(insert->Execute());
	result = con.Query("SELECT count(*) FROM t");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(routed_queries.size() == 3);
}
