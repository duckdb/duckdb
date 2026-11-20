#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

using namespace duckdb;

namespace {

//! Simulate an extension that silently discards parent schema names.
class FlatCatalog : public DuckCatalog {
public:
	explicit FlatCatalog(AttachedDatabase &db) : DuckCatalog(db) {
	}

	bool SupportsNestedSchemas() const override {
		return false;
	}

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &lookup,
	                                              OnEntryNotFound if_not_found) override {
		return DuckCatalog::LookupSchema(
		    transaction, EntryLookupInfo(lookup, QualifiedName(lookup.GetEntryIdentifier())), if_not_found);
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override {
		auto copy = info.Copy();
		auto &flat_info = copy->Cast<CreateSchemaInfo>();
		flat_info.SetQualifiedName(QualifiedName(GetName(), info.SchemaName(), Identifier()));
		return DuckCatalog::CreateSchema(transaction, flat_info);
	}
};

class FlatEntryCatalog : public FlatCatalog {
public:
	FlatEntryCatalog(AttachedDatabase &db, bool is_remote) : FlatCatalog(db), is_remote(is_remote) {
	}

	//! Only counted as a remote catalog (which enables the remote pushdown optimizer) when set at ATTACH
	const bool is_remote;
	//! Whether statements may be pushed down, which is off during the test's setup
	bool remote = false;

	bool Supports(RemoteCapability capability) const override {
		if (capability == RemoteCapability::IS_REMOTE) {
			return is_remote;
		}
		return remote && (capability == RemoteCapability::EXECUTE_QUERY_NODE ||
		                  capability == RemoteCapability::EXECUTE_STATEMENT);
	}

private:
	CatalogEntryLookup TryLookupEntryInternal(CatalogTransaction transaction, const EntryLookupInfo &lookup) override {
		EntryLookupInfo schema_lookup(CatalogType::SCHEMA_ENTRY, QualifiedName(lookup.GetSchema()));
		auto schema = LookupSchema(transaction, schema_lookup, OnEntryNotFound::RETURN_NULL);
		if (!schema) {
			return {nullptr, nullptr, ErrorData()};
		}
		EntryLookupInfo flat_lookup(lookup, QualifiedName(GetName(), lookup.GetSchema(), lookup.GetEntryIdentifier()));
		return {schema, schema->LookupEntry(transaction, flat_lookup), ErrorData()};
	}
};

struct FlatStorageExtension : StorageExtension {
	FlatStorageExtension() {
		attach = [](optional_ptr<StorageExtensionInfo>, ClientContext &, AttachedDatabase &db, const string &,
		            AttachInfo &info, AttachOptions &) -> unique_ptr<Catalog> {
			bool is_remote = info.path == "remote";
			bool override_entry_lookup = is_remote || info.path == "override";
			info.path = IN_MEMORY_PATH;
			if (override_entry_lookup) {
				return make_uniq_base<Catalog, FlatEntryCatalog>(db, is_remote);
			}
			return make_uniq_base<Catalog, FlatCatalog>(db);
		};
		create_transaction_manager = [](optional_ptr<StorageExtensionInfo>, AttachedDatabase &db,
		                                Catalog &) -> unique_ptr<TransactionManager> {
			return make_uniq<DuckTransactionManager>(db);
		};
	}
};

} // namespace

TEST_CASE("Unsupported catalogs reject nested schemas", "[api][nested_schema]") {
	string mode = "standard";
	bool remote = false;
	SECTION("Default entry lookup") {
	}
	SECTION("Overridden entry lookup") {
		mode = "override";
	}
	SECTION("Remote pushdown") {
		mode = "remote";
		remote = true;
	}
	DBConfig config;
	StorageExtension::Register(config, "flat_schema_test", make_shared_ptr<FlatStorageExtension>());
	DuckDB db(nullptr, &config);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + mode + "' AS flat (TYPE flat_schema_test)"));
	REQUIRE_NO_FAIL(con.Query("USE flat"));
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA a"));
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA b"));
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA \"a.b\""));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE b.t AS SELECT 42 AS i"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE \"a.b\".t AS SELECT 99 AS i"));
	REQUIRE_NO_FAIL(con.Query("CREATE MACRO b.f(x) AS x + 1"));

	// a dotted call on a qualified column, an alias or a lambda parameter is not a nested schema path
	auto check_value = [&con](const string &query, const Value &expected) {
		INFO(query);
		auto result = con.Query(query);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(CHECK_COLUMN(result, result->ColumnCount() - 1, {expected}));
	};
	check_value("SELECT t.i.abs() FROM b.t t", Value::INTEGER(42));
	check_value("SELECT 'AbC' AS s, alias.s.lower()", Value("abc"));
	check_value("SELECT list_transform([{'f': -2}], lambda y: y.f.abs())", Value::LIST({Value::INTEGER(2)}));

	if (remote) {
		con.BeginTransaction();
		Catalog::GetCatalog(*con.context, "flat").Cast<FlatEntryCatalog>().remote = true;
		con.Commit();
	}

	auto check_error = [&con](const string &query, const string &schema_path) {
		INFO(query);
		auto result = con.Query(query);
		REQUIRE_FAIL(result);
		INFO(result->GetError());
		auto message = StringUtil::Split(result->GetErrorObject().RawMessage(), '\n')[0];
		REQUIRE(message == "Catalog \"flat\" does not support nested schemas: " + schema_path);
	};

	// a nested path can't name anything in the catalog, so these find nothing instead of the flattened b.t
	auto check_not_found = [&con](const string &query) {
		INFO(query);
		auto result = con.Query(query);
		REQUIRE_FAIL(result);
		INFO(result->GetError());
		REQUIRE(result->GetErrorType() == ExceptionType::CATALOG);
		auto &message = result->GetErrorObject().RawMessage();
		REQUIRE((StringUtil::Contains(message, "does not exist") ||
		         StringUtil::Contains(message, "No catalog + schema named")));
	};

	const vector<string> lookups {
	    "SELECT * FROM flat.a.b.t",
	    "SELECT * FROM a.b.t",
	    "SELECT flat.a.b.f(1)",
	    "INSERT INTO flat.a.b.t VALUES (7)",
	    "UPDATE flat.a.b.t SET i = 7",
	    "DELETE FROM flat.a.b.t",
	    "CREATE TABLE flat.a.b.new_table(i INTEGER)",
	    "DROP TABLE flat.a.b.t",
	    "USE flat.a.b",
	    "SET schema = 'flat.a.b'",
	    "SET search_path = 'flat.a.b'",
	    "SHOW TABLES FROM flat.a.b",
	    "SELECT * FROM lakee.sales.t",
	    "SELECT * FROM flat.a.b.c.t",
	    "SELECT * FROM flat.\"a.b\".\"c\"\"d\".t",
	    "DROP SCHEMA flat.a.b CASCADE",
	    "DROP SCHEMA flat.\"a.b\".\"c\"\"d\"",
	};
	for (auto &query : lookups) {
		check_not_found(query);
	}
	REQUIRE_NO_FAIL(con.Query("DROP SCHEMA IF EXISTS flat.a.b CASCADE"));

	const vector<string> create_schema_statements {
	    "CREATE SCHEMA flat.a.b",
	    "CREATE SCHEMA IF NOT EXISTS flat.a.b",
	    "CREATE OR REPLACE SCHEMA flat.a.b",
	};
	for (auto &query : create_schema_statements) {
		check_error(query, "a.b");
	}
	check_error("CREATE SCHEMA flat.\"a.b\".\"c\"\"d\"", "\"a.b\".\"c\"\"d\"");

	if (remote) {
		con.BeginTransaction();
		Catalog::GetCatalog(*con.context, "flat").Cast<FlatEntryCatalog>().remote = false;
		con.Commit();
	}

	con.BeginTransaction();
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName::Parse("flat.a.b.t"));
	REQUIRE(!Catalog::GetEntry(*con.context, lookup, OnEntryNotFound::RETURN_NULL));
	vector<Identifier> schema_path {"a", "b"};
	REQUIRE(!Catalog::GetSchema(*con.context, "flat", schema_path, OnEntryNotFound::RETURN_NULL));
	// the catalog's own LookupSchema would flatten a.b to b
	EntryLookupInfo schema_lookup(CatalogType::SCHEMA_ENTRY, QualifiedName::FromPath({"flat", "a", "b"}));
	REQUIRE(!Catalog::GetCatalog(*con.context, "flat")
	             .GetSchema(*con.context, schema_lookup, OnEntryNotFound::RETURN_NULL));
	con.Rollback();

	auto result = con.Query("SELECT * FROM b.t");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE_NO_FAIL(con.Query("USE flat.\"a.b\""));
	result = con.Query("SELECT * FROM t");
	REQUIRE(CHECK_COLUMN(result, 0, {99}));
	REQUIRE_NO_FAIL(con.Query("USE flat.main"));
	REQUIRE_NO_FAIL(con.Query("DROP SCHEMA flat.\"a.b\" CASCADE"));
	result = con.Query("SELECT * FROM b.t");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}
