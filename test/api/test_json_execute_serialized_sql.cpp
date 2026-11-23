#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/external_dependencies.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

#include <atomic>

using namespace duckdb;

TEST_CASE("json_execute_serialized_sql refuses a result that changed since the outer query was bound", "[api][json]") {
	DuckDB db(nullptr);
	if (!db.ExtensionIsLoaded("json")) {
		return;
	}
	Connection con(db);
	Connection other(db);
	// with one thread nothing of the submitted query runs before Complete
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1), (2)"));
	const string query = "SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT * FROM t'))";

	SECTION("unchanged") {
		auto result = con.Submit(query);
		result->Complete();
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	}
	SECTION("a column changes type") {
		auto result = con.Submit(query);
		REQUIRE_NO_FAIL(other.Query("ALTER TABLE t ALTER i TYPE VARCHAR"));
		result->Complete();
		REQUIRE_FAIL(result);
		REQUIRE(StringUtil::Contains(result->GetError(), "Expected the following columns: [i INTEGER]"));
		REQUIRE(StringUtil::Contains(result->GetError(), "But result contained the following: [i VARCHAR]"));
		REQUIRE(!StringUtil::Contains(result->GetError(), "an alias keeps its name stable"));
	}
	SECTION("a column is renamed") {
		auto result = con.Submit(query);
		REQUIRE_NO_FAIL(other.Query("ALTER TABLE t RENAME i TO j"));
		result->Complete();
		REQUIRE_FAIL(result);
		REQUIRE(StringUtil::Contains(result->GetError(), "But result contained the following: [j INTEGER]"));
	}
	SECTION("a column is added") {
		auto result = con.Submit(query);
		REQUIRE_NO_FAIL(other.Query("ALTER TABLE t ADD COLUMN k INTEGER"));
		result->Complete();
		REQUIRE_FAIL(result);
		REQUIRE(StringUtil::Contains(result->GetError(), "But result contained the following: [i INTEGER, k INTEGER]"));
	}
}

static std::atomic<idx_t> replacement_calls {0};
static std::atomic<int32_t> outside_answer {42};

static unique_ptr<TableRef> ReplaceWithOutsideObject(ClientContext &context, ReplacementScanInput &input,
                                                     optional_ptr<ReplacementScanData> data) {
	if (input.table_name != "outside_object") {
		return nullptr;
	}
	replacement_calls++;
	Parser parser(context);
	parser.ParseQuery(StringUtil::Format("SELECT %d AS answer", outside_answer.load()));
	auto select = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
	auto ref = make_uniq<SubqueryRef>(std::move(select), "outside_object");
	ref->external_dependency = make_shared_ptr<ExternalDependency>();
	return std::move(ref);
}

TEST_CASE("json_execute_serialized_sql looks up an object outside the catalog at bind and at every scan start",
          "[api][json]") {
	DuckDB db(nullptr);
	if (!db.ExtensionIsLoaded("json")) {
		return;
	}
	DBConfig::GetConfig(*db.instance).replacement_scans.emplace_back(ReplaceWithOutsideObject);
	Connection con(db);
	const string query =
	    "SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT * FROM outside_object'))";

	replacement_calls = 0;
	auto result = con.Query(query);
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(replacement_calls == 1 + 1);

	// every iteration of the recursive query starts the scan again
	replacement_calls = 0;
	result = con.Query("WITH RECURSIVE c AS (SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT * "
	                   "FROM outside_object'))), walk(depth) AS (SELECT 0 UNION ALL SELECT depth + 1 FROM walk WHERE "
	                   "depth < (SELECT count(*) + 2 FROM c)) SELECT max(depth) FROM walk");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(replacement_calls == 1 + 4);

	// with one thread nothing of the submitted query runs before Complete, so its scan starts after the object changed
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	auto pending = con.Submit(query);
	outside_answer = 43;
	pending->Complete();
	outside_answer = 42;
	REQUIRE(CHECK_COLUMN(pending, 0, {43}));
}

static std::atomic<idx_t> second_outside_calls {0};
static std::atomic<idx_t> dependency_free_calls {0};

static unique_ptr<TableRef> ReplaceWithOtherObjects(ClientContext &context, ReplacementScanInput &input,
                                                    optional_ptr<ReplacementScanData> data) {
	string query;
	bool external;
	if (input.table_name == "second_outside_object") {
		second_outside_calls++;
		query = "SELECT 43 AS second_answer";
		external = true;
	} else if (input.table_name == "dependency_free_object") {
		dependency_free_calls++;
		query = "SELECT 7 AS seven";
		external = false;
	} else {
		return nullptr;
	}
	Parser parser(context);
	parser.ParseQuery(query);
	auto select = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
	auto ref = make_uniq<SubqueryRef>(std::move(select), Identifier(input.table_name));
	if (external) {
		ref->external_dependency = make_shared_ptr<ExternalDependency>();
	}
	return std::move(ref);
}

TEST_CASE("json_execute_serialized_sql looks up objects with and without an external dependency alike", "[api][json]") {
	DuckDB db(nullptr);
	if (!db.ExtensionIsLoaded("json")) {
		return;
	}
	DBConfig::GetConfig(*db.instance).replacement_scans.emplace_back(ReplaceWithOutsideObject);
	DBConfig::GetConfig(*db.instance).replacement_scans.emplace_back(ReplaceWithOtherObjects);
	Connection con(db);

	replacement_calls = 0;
	dependency_free_calls = 0;
	auto result = con.Query("SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT * FROM "
	                        "outside_object, dependency_free_object'))");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {7}));
	REQUIRE(replacement_calls == 1 + 1);
	REQUIRE(dependency_free_calls == 1 + 1);

	replacement_calls = 0;
	dependency_free_calls = 0;
	result =
	    con.Query("WITH RECURSIVE c AS (SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT * "
	              "FROM outside_object, dependency_free_object'))), walk(depth) AS (SELECT 0 UNION ALL SELECT depth + "
	              "1 FROM walk WHERE depth < (SELECT count(*) + 2 FROM c)) SELECT max(depth) FROM walk");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(replacement_calls == 1 + 4);
	REQUIRE(dependency_free_calls == 1 + 4);
}

TEST_CASE(
    "json_execute_serialized_sql looks up each of two objects outside the catalog at bind and at every scan start",
    "[api][json]") {
	DuckDB db(nullptr);
	if (!db.ExtensionIsLoaded("json")) {
		return;
	}
	DBConfig::GetConfig(*db.instance).replacement_scans.emplace_back(ReplaceWithOutsideObject);
	DBConfig::GetConfig(*db.instance).replacement_scans.emplace_back(ReplaceWithOtherObjects);
	Connection con(db);

	replacement_calls = 0;
	second_outside_calls = 0;
	auto result = con.Query("SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT * FROM "
	                        "outside_object JOIN second_outside_object ON answer + 1 = second_answer'))");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {43}));
	REQUIRE(replacement_calls == 1 + 1);
	REQUIRE(second_outside_calls == 1 + 1);

	replacement_calls = 0;
	second_outside_calls = 0;
	result = con.Query("WITH RECURSIVE c AS (SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT * "
	                   "FROM outside_object JOIN second_outside_object ON answer + 1 = second_answer'))), walk(depth) "
	                   "AS (SELECT 0 UNION ALL SELECT depth + 1 FROM walk WHERE depth < (SELECT count(*) + 2 FROM c)) "
	                   "SELECT max(depth) FROM walk");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(replacement_calls == 1 + 4);
	REQUIRE(second_outside_calls == 1 + 4);
}

TEST_CASE("json_execute_serialized_sql logs the text of its statement once for every scan start", "[api][json]") {
	DuckDB db(nullptr);
	if (!db.ExtensionIsLoaded("json")) {
		return;
	}
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CALL enable_logging('QueryLog', storage = 'memory')"));
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT 7 AS seven'))"));
	// the scan is part of the recursive step, which runs for depth 0, 1 and 2 until a run yields no row
	auto result = con.Query("WITH RECURSIVE c AS (SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT "
	                        "1 UNION ALL SELECT 2'))), walk(depth) AS (SELECT 0 UNION ALL SELECT depth + 1 FROM walk "
	                        "WHERE depth < (SELECT count(*) FROM c)) SELECT max(depth) FROM walk");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE_NO_FAIL(con.Query("CALL disable_logging()"));

	// the prepare at each of the two binds writes one entry without text
	result =
	    con.Query("SELECT message, count(*) FROM duckdb_logs_parsed('QueryLog') WHERE message IN ('', 'SELECT 7 AS "
	              "seven', '(SELECT 1) UNION ALL (SELECT 2)') GROUP BY ALL ORDER BY ALL");
	REQUIRE(CHECK_COLUMN(result, 0, {"", "(SELECT 1) UNION ALL (SELECT 2)", "SELECT 7 AS seven"}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 3, 1}));
}

namespace {

vector<string> json_pushed_nodes;

//! A local catalog that claims to be remote and answers every pushed query node with a single "pushed" row
class JsonPushdownCatalog : public DuckCatalog {
public:
	explicit JsonPushdownCatalog(AttachedDatabase &db) : DuckCatalog(db) {
	}

public:
	using DuckCatalog::RemoteExecute;

	bool Supports(RemoteCapability capability) const override {
		return capability == RemoteCapability::IS_REMOTE || capability == RemoteCapability::EXECUTE_QUERY_NODE;
	}

	unique_ptr<TableRef> RemoteExecute(ClientContext &context, unique_ptr<QueryNode> node) override {
		json_pushed_nodes.push_back(node->ToString());
		auto result = make_uniq<SelectNode>();
		result->select_list.push_back(ConstantExpression::String("pushed"));
		result->from_table = make_uniq<EmptyTableRef>();
		auto select = make_uniq<SelectStatement>();
		select->node = std::move(result);
		return make_uniq<SubqueryRef>(std::move(select));
	}
};

struct JsonPushdownStorageExtension : StorageExtension {
	JsonPushdownStorageExtension() {
		attach = [](optional_ptr<StorageExtensionInfo>, ClientContext &, AttachedDatabase &db, const string &,
		            AttachInfo &info, AttachOptions &) -> unique_ptr<Catalog> {
			info.path = IN_MEMORY_PATH;
			return make_uniq_base<Catalog, JsonPushdownCatalog>(db);
		};
		create_transaction_manager = [](optional_ptr<StorageExtensionInfo>, AttachedDatabase &db,
		                                Catalog &) -> unique_ptr<TransactionManager> {
			return make_uniq<DuckTransactionManager>(db);
		};
	}
};

} // namespace

TEST_CASE("json_execute_serialized_sql runs its statement as any SELECT when a remote catalog is attached",
          "[api][json]") {
	DBConfig config;
	StorageExtension::Register(config, "json_test_pushdown", make_shared_ptr<JsonPushdownStorageExtension>());
	DuckDB db(nullptr, &config);
	if (!db.ExtensionIsLoaded("json")) {
		return;
	}
	DBConfig::GetConfig(*db.instance).replacement_scans.emplace_back(ReplaceWithOutsideObject);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1)"));
	REQUIRE_NO_FAIL(con.Query("ATTACH 'remote' AS s (TYPE json_test_pushdown)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE s.tbl AS SELECT range AS i FROM range(5)"));

	SECTION("a statement that reads only the remote catalog is sent to it at bind and at each scan start") {
		const string query =
		    "SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT ''pushed'' FROM s.tbl'))";
		json_pushed_nodes.clear();
		REQUIRE(!con.Prepare(query)->HasError());
		REQUIRE(json_pushed_nodes.size() == 1);
		REQUIRE(json_pushed_nodes[0] == "SELECT 'pushed' FROM tbl");

		json_pushed_nodes.clear();
		auto result = con.Query(query);
		REQUIRE(result->RowCount() == 1);
		REQUIRE(CHECK_COLUMN(result, 0, {"pushed"}));
		REQUIRE(json_pushed_nodes.size() == 2);
	}
	SECTION("the columns are those of the remote answer") {
		auto direct = con.Query("SELECT i FROM s.tbl");
		auto result = con.Query("SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT i FROM s.tbl'))");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->RowCount() == 1);
		REQUIRE(CHECK_COLUMN(result, 0, {"pushed"}));
		REQUIRE(result->GetNames() == direct->GetNames());
		REQUIRE(result->GetTypes() == direct->GetTypes());
	}
	SECTION("a statement that reads no remote catalog runs locally under the names a direct SELECT gets") {
		json_pushed_nodes.clear();
		auto direct = con.Query("SELECT i + (1 + 2) FROM t");
		auto result =
		    con.Query("SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT i + (1 + 2) FROM t'))");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->RowCount() == 1);
		REQUIRE(CHECK_COLUMN(result, 0, {4}));
		REQUIRE(result->GetNames() == direct->GetNames());
		REQUIRE(json_pushed_nodes.empty());
	}
	SECTION("an object from a replacement scan joined with a remote table runs locally, as in a direct SELECT") {
		json_pushed_nodes.clear();
		auto direct = con.Query("SELECT answer, i FROM outside_object, s.tbl ORDER BY i");
		auto result = con.Query("SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT answer, i FROM "
		                        "outside_object, s.tbl')) ORDER BY i");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(CHECK_COLUMN(result, 0, {42, 42, 42, 42, 42}));
		REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 2, 3, 4}));
		REQUIRE(result->GetNames() == direct->GetNames());
		REQUIRE(json_pushed_nodes.empty());
	}
	SECTION("a statement with a parameter that reads only the remote catalog is handed to it whole, unrefused") {
		json_pushed_nodes.clear();
		auto result =
		    con.Query("SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT $1 FROM s.tbl'))");
		REQUIRE(CHECK_COLUMN(result, 0, {"pushed"}));
		REQUIRE(json_pushed_nodes == vector<string> {"SELECT $1 FROM tbl", "SELECT $1 FROM tbl"});
	}
	SECTION("an unaliased column over now() fails the column check, an aliased one does not") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE ev(id INTEGER, created TIMESTAMPTZ)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO ev VALUES (1, TIMESTAMPTZ '2026-01-01 00:00:00+00')"));
		// the remote rewrite names an unaliased column after now()'s value, which differs between bind and scan start
		auto result = con.Query(
		    "SELECT id FROM json_execute_serialized_sql(json_serialize_sql('SELECT id, now() - created FROM ev'))");
		REQUIRE_FAIL(result);
		REQUIRE(StringUtil::Contains(result->GetError(), "Result mismatch in query!"));
		REQUIRE(StringUtil::Contains(result->GetError(), "an alias keeps its name stable"));
		result = con.Query("SELECT id FROM json_execute_serialized_sql(json_serialize_sql('SELECT id, now() - created "
		                   "AS age FROM ev'))");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}
}
