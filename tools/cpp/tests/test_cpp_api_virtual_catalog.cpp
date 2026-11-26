#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"

#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

// ---------------------------------------------------------------------------
// Stable C++ API tests: VirtualCatalog. A database type implemented by deriving
// from VirtualDatabase, VirtualCatalogTransaction and the entry classes:
// attach, transactions, lookups and listings, scans, views, DDL, and what a
// database that overrides nothing optional answers.
// ---------------------------------------------------------------------------

namespace {

using namespace duckdb::cxx;

// The objects of a virtual catalog keep their address: DuckDB refers to them by it.
static_assert(!std::is_copy_constructible<VirtualTableEntry>::value, "entries are not copyable");
static_assert(!std::is_move_constructible<VirtualTableEntry>::value, "entries are not movable");
static_assert(!std::is_move_constructible<VirtualViewEntry>::value, "entries are not movable");
static_assert(!std::is_copy_assignable<VirtualCatalogTransaction>::value, "transactions are not copyable");

// What the virtual catalog saw, latched for the test to assert on afterwards. A function DuckDB calls must not use
// REQUIRE: it would throw through the callback boundary into the engine.
struct MemObserved {
	std::string attached_name;
	std::string attached_path;
	bool attached_read_only = false;
	int64_t option_rows = -1;
	std::string user_data_tag;

	int live_databases = 0;
	int live_transactions = 0;
	int begins = 0;
	int commits = 0;
	int rollbacks = 0;
	int read_only_begins = 0;
	int lookups_in_transaction = 0;

	std::vector<std::string> created_columns;
	std::vector<std::string> built_columns;
	std::vector<std::string> created_constraints;
	std::string created_view_query;
	std::vector<std::string> created_view_aliases;
	int cascade_drops = 0;
};
MemObserved observed;

struct MemUserData {
	std::string tag;
};

int64_t QueryBigint(Connection &conn, const std::string &sql) {
	auto result = conn.Execute(sql);
	auto chunk = result.FetchChunk();
	REQUIRE(static_cast<bool>(chunk));
	REQUIRE(chunk.GetRowCount() == 1);
	auto view = chunk.GetVector(0).GetView();
	REQUIRE(view.IsValid(0));
	return view.Data<int64_t>()[view.SelAt(0)];
}

// --- The scan function: every column is BIGINT, and column c of row r carries base + r + c. ---

struct MemScanData {
	int64_t base = 0;
	int64_t count = 0;

	bool operator==(const MemScanData &other) const {
		return base == other.base && count == other.count;
	}
};
struct MemScanGlobal {
	int64_t position = 0;
};

constexpr int64_t MEM_BATCH_ROWS = 2;

void MemScanInitGlobal(TableFunction::InitGlobalInput &input) {
	input.SetGlobalState<MemScanGlobal>();
}

void MemScanExec(TableFunction::ExecInput &input) {
	const auto &bind = input.GetBindData<MemScanData>();
	auto &global = input.GetGlobalState<MemScanGlobal>();

	auto chunk = input.GetOutputChunk();
	auto produced = bind.count - global.position;
	if (produced > MEM_BATCH_ROWS) {
		produced = MEM_BATCH_ROWS;
	}
	if (produced > 0) {
		for (idx_t c = 0; c < chunk.GetVectorCount(); c++) {
			auto *out = chunk.GetVector(c).GetDataMutable<int64_t>();
			for (int64_t i = 0; i < produced; i++) {
				out[i] = bind.base + global.position + i + static_cast<int64_t>(c);
			}
		}
	}
	global.position += produced;
	chunk.GetVector(0).SetSize(static_cast<idx_t>(produced));
}

// --- The implementation ---

class MemDatabase;

class MemTransaction final : public VirtualCatalogTransaction {
public:
	MemTransaction() {
		observed.live_transactions++;
	}
	~MemTransaction() override {
		observed.live_transactions--;
	}

	void Commit() override {
		observed.commits++;
	}
	void Rollback() override {
		observed.rollbacks++;
	}

	int lookups = 0;
};

class MemTable final : public VirtualTableEntry {
public:
	MemTable(VirtualSchemaEntry &schema, const VirtualTableInfo &info, int64_t base, int64_t rows)
	    : VirtualTableEntry(schema, info), base(base), rows(rows) {
	}

	void BindScan(ScanBindInput &input) override {
		// The transaction arrives as the class of the implementation.
		input.GetTransaction().Cast<MemTransaction>().lookups++;
		input.SetBindData<MemScanData>(MemScanData {base, rows});
		input.SetCardinality(static_cast<idx_t>(rows), true);
	}

	int64_t base;
	int64_t rows;
};

class MemSchema final : public VirtualSchemaEntry {
public:
	MemSchema(MemDatabase &database, std::string_view name);

	VirtualCatalogEntry *FindTable(TableFindInput &input) override {
		observed.lookups_in_transaction = ++input.GetTransaction().Cast<MemTransaction>().lookups;
		auto name = std::string(input.GetName());
		if (name == "broken") {
			throw InvalidInputException("the mem schema refused to look");
		}
		auto table = tables.find(name);
		if (table != tables.end()) {
			return table->second.get();
		}
		auto view = views.find(name);
		if (view != views.end()) {
			return view->second.get();
		}
		return nullptr;
	}

	void ListTables(TableListInput &input) override {
		for (auto &table : tables) {
			input.Add(*table.second);
		}
		for (auto &view : views) {
			input.Add(*view.second);
		}
	}

	VirtualTableEntry &CreateTable(TableCreateInput &input) override;
	VirtualViewEntry &CreateView(ViewCreateInput &input) override;
	void DropTable(VirtualTableEntry &table, TableDropInput &input) override;
	void DropView(VirtualViewEntry &view, ViewDropInput &input) override;

	MemDatabase &mem;
	std::map<std::string, std::unique_ptr<MemTable>> tables;
	std::map<std::string, std::unique_ptr<VirtualViewEntry>> views;
};

class MemDatabase final : public VirtualDatabase {
public:
	explicit MemDatabase(VirtualCatalog::AttachInput &input)
	    : VirtualDatabase(input), scan(MakeScan(input.GetContext().GetFactory())) {
		observed.attached_name = std::string(input.GetName());
		observed.attached_path = std::string(input.GetPath());
		observed.attached_read_only = input.IsReadOnly();
		observed.user_data_tag = input.GetUserData<MemUserData>().tag;

		auto &factory = input.GetContext().GetFactory();
		int64_t rows = 3;
		auto option = input.GetOption("rows");
		if (option) {
			rows = option->Cast(factory, factory.ParseType("BIGINT")).Get<int64_t>();
			observed.option_rows = rows;
		}
		if (rows > 1000) {
			throw InvalidInputException("the mem database holds at most 1000 rows");
		}
		auto default_schema = input.GetOption("default_schema");
		if (default_schema) {
			input.SetDefaultSchema(default_schema->ToText());
		}

		auto main = std::make_unique<MemSchema>(*this, "main");

		VirtualTableInfo numbers("numbers");
		numbers.AddColumn("value", factory.ParseType("BIGINT")).SetScanFunction(scan);
		main->tables["numbers"] = std::make_unique<MemTable>(*main, numbers, 10, rows);

		VirtualTableInfo pairs("pairs");
		pairs.AddColumn("a", factory.ParseType("BIGINT")).AddColumn("b", factory.ParseType("BIGINT"));
		pairs.SetScanFunction(scan);
		main->tables["pairs"] = std::make_unique<MemTable>(*main, pairs, 100, rows);

		// A table without a scan function, as a plain VirtualTableEntry would be.
		VirtualTableInfo opaque("opaque");
		opaque.AddColumn("value", factory.ParseType("BIGINT"));
		main->tables["opaque"] = std::make_unique<MemTable>(*main, opaque, 0, 0);

		VirtualViewInfo doubled("doubled", "SELECT value * 2 AS twice FROM numbers");
		main->views["doubled"] = std::make_unique<VirtualViewEntry>(*main, doubled);

		auto renamed = VirtualViewInfo::FromSQL("CREATE VIEW renamed (first) AS SELECT a FROM pairs");
		main->views["renamed"] = std::make_unique<VirtualViewEntry>(*main, renamed);

		schemas["main"] = std::move(main);
		// Counted once constructed: a constructor that throws has no destructor to count it down again.
		observed.live_databases++;
	}

	~MemDatabase() override {
		observed.live_databases--;
	}

	static VirtualScanFunction MakeScan(Factory &factory) {
		auto function = TableFunction::Create(factory);
		function.SetInitGlobalCallback(MemScanInitGlobal).SetExecCallback(MemScanExec);
		return VirtualScanFunction(std::move(function));
	}

	std::unique_ptr<VirtualCatalogTransaction> BeginTransaction(BeginTransactionInput &input) override {
		observed.begins++;
		if (input.IsReadOnly()) {
			observed.read_only_begins++;
		}
		return std::make_unique<MemTransaction>();
	}

	VirtualSchemaEntry *FindSchema(SchemaFindInput &input) override {
		auto schema = schemas.find(std::string(input.GetName()));
		return schema == schemas.end() ? nullptr : schema->second.get();
	}

	void ListSchemas(SchemaListInput &input) override {
		for (auto &schema : schemas) {
			input.Add(*schema.second);
		}
	}

	VirtualSchemaEntry &CreateSchema(SchemaCreateInput &input) override {
		auto name = std::string(input.GetName());
		auto &slot = schemas[name];
		slot = std::make_unique<MemSchema>(*this, name);
		return *slot;
	}

	void DropSchema(VirtualSchemaEntry &schema, SchemaDropInput &input) override {
		if (input.IsCascade()) {
			observed.cascade_drops++;
		}
		auto entry = schemas.find(std::string(schema.GetName()));
		dropped_schemas.push_back(std::move(entry->second));
		schemas.erase(entry);
	}

	VirtualScanFunction scan;
	std::map<std::string, std::unique_ptr<MemSchema>> schemas;
	// Dropped entries outlive the transactions that may still use them: kept until the database is detached.
	std::vector<std::unique_ptr<MemSchema>> dropped_schemas;
	std::vector<std::unique_ptr<VirtualCatalogEntry>> dropped_entries;
};

MemSchema::MemSchema(MemDatabase &database, std::string_view name) : VirtualSchemaEntry(database, name), mem(database) {
}

VirtualTableEntry &MemSchema::CreateTable(TableCreateInput &input) {
	auto name = std::string(input.GetName());
	VirtualTableInfo info(name);
	observed.created_columns.clear();
	observed.created_constraints.clear();
	for (idx_t i = 0; i < input.GetColumnCount(); i++) {
		auto column = std::string(input.GetColumnName(i)) + " " + input.GetColumnType(i).ToText();
		if (input.IsColumnNotNull(i)) {
			column += " NOT NULL";
		}
		auto column_default = input.GetColumnDefault(i);
		if (column_default) {
			column += " DEFAULT " + std::string(*column_default);
		}
		observed.created_columns.push_back(column);
		info.AddColumn(std::string(input.GetColumnName(i)), input.GetColumnType(i));
		if (input.IsColumnNotNull(i)) {
			info.SetColumnNotNull(i);
		}
	}
	for (idx_t i = 0; i < input.GetConstraintCount(); i++) {
		observed.created_constraints.emplace_back(input.GetConstraint(i));
	}
	info.SetScanFunction(mem.scan);
	// Created tables are empty: the scan function only writes BIGINT.
	auto &slot = tables[name];
	slot = std::make_unique<MemTable>(*this, info, 0, 0);
	// What the table that was built reports about itself.
	observed.built_columns.clear();
	for (idx_t i = 0; i < slot->GetColumnCount(); i++) {
		auto column = std::string(slot->GetColumnName(i)) + " " + slot->GetColumnType(i).ToText();
		if (slot->IsColumnNotNull(i)) {
			column += " NOT NULL";
		}
		observed.built_columns.push_back(column);
	}
	return *slot;
}

VirtualViewEntry &MemSchema::CreateView(ViewCreateInput &input) {
	auto name = std::string(input.GetName());
	observed.created_view_query = std::string(input.GetQuery());
	observed.created_view_aliases.clear();
	VirtualViewInfo info(name, std::string(input.GetQuery()));
	for (idx_t i = 0; i < input.GetColumnAliasCount(); i++) {
		observed.created_view_aliases.emplace_back(input.GetColumnAlias(i));
		info.AddColumnAlias(std::string(input.GetColumnAlias(i)));
	}
	auto &slot = views[name];
	slot = std::make_unique<VirtualViewEntry>(*this, info);
	return *slot;
}

void MemSchema::DropTable(VirtualTableEntry &table, TableDropInput &input) {
	if (input.IsCascade()) {
		observed.cascade_drops++;
	}
	auto entry = tables.find(std::string(table.GetName()));
	mem.dropped_entries.push_back(std::move(entry->second));
	tables.erase(entry);
}

void MemSchema::DropView(VirtualViewEntry &view, ViewDropInput &) {
	auto entry = views.find(std::string(view.GetName()));
	mem.dropped_entries.push_back(std::move(entry->second));
	views.erase(entry);
}

void RegisterMem(Connection &conn) {
	observed = MemObserved();
	auto catalog = VirtualCatalog::Create<MemDatabase>(conn.GetFactory(), "vmem");
	catalog.SetUserData<MemUserData>(MemUserData {"shared"});
	conn.Register(catalog);
}

// --- A database that implements only what it must ---

class BareSchema final : public VirtualSchemaEntry {
public:
	BareSchema(VirtualDatabase &database, Factory &factory) : VirtualSchemaEntry(database, "main") {
		VirtualTableInfo info("plain");
		info.AddColumn("value", factory.ParseType("BIGINT"));
		plain = std::make_unique<VirtualTableEntry>(*this, info);
	}

	VirtualCatalogEntry *FindTable(TableFindInput &input) override {
		return input.GetName() == "plain" ? plain.get() : nullptr;
	}
	void ListTables(TableListInput &input) override {
		input.Add(*plain);
	}

	std::unique_ptr<VirtualTableEntry> plain;
};

class BareTransaction final : public VirtualCatalogTransaction {
public:
	void Commit() override {
	}
	void Rollback() override {
	}
};

class BareDatabase final : public VirtualDatabase {
public:
	explicit BareDatabase(VirtualCatalog::AttachInput &input)
	    : VirtualDatabase(input), main(*this, input.GetContext().GetFactory()) {
	}

	std::unique_ptr<VirtualCatalogTransaction> BeginTransaction(BeginTransactionInput &) override {
		return std::make_unique<BareTransaction>();
	}
	VirtualSchemaEntry *FindSchema(SchemaFindInput &input) override {
		return input.GetName() == "main" ? &main : nullptr;
	}
	void ListSchemas(SchemaListInput &input) override {
		input.Add(main);
		// A schema added twice is listed once.
		input.Add(main);
	}

	BareSchema main;
};

} // namespace

TEST_CASE("Stable C++API: virtual catalog attaches and scans", "[cpp_api][cpp_api_vcatalog]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();
	RegisterMem(conn);

	conn.Execute("ATTACH 'somewhere' AS mem (TYPE vmem, rows 5)").Drain();
	REQUIRE(observed.live_databases == 1);
	REQUIRE(observed.attached_name == "mem");
	REQUIRE(observed.attached_path == "somewhere");
	REQUIRE_FALSE(observed.attached_read_only);
	REQUIRE(observed.option_rows == 5);
	REQUIRE(observed.user_data_tag == "shared");

	SECTION("tables are read through the scan function") {
		// 10 + 11 + 12 + 13 + 14
		REQUIRE(QueryBigint(conn, "SELECT sum(value)::BIGINT FROM mem.main.numbers") == 60);
		REQUIRE(QueryBigint(conn, "SELECT count(*) FROM mem.numbers") == 5);
		// column c of row r carries base + r + c
		REQUIRE(QueryBigint(conn, "SELECT sum(b - a)::BIGINT FROM mem.main.pairs") == 5);
		REQUIRE(QueryBigint(conn, "SELECT min(a) FROM mem.main.pairs") == 100);
	}

	SECTION("views bind against the tables of their schema") {
		REQUIRE(QueryBigint(conn, "SELECT sum(twice)::BIGINT FROM mem.main.doubled") == 120);
		REQUIRE(QueryBigint(conn, "SELECT sum(first)::BIGINT FROM mem.main.renamed") == 510);
	}

	SECTION("schemas, tables and views are listed") {
		REQUIRE(QueryBigint(conn, "SELECT count(*) FROM duckdb_schemas() WHERE database_name = 'mem'") == 1);
		REQUIRE(QueryBigint(conn, "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'mem'") == 3);
		REQUIRE(QueryBigint(conn, "SELECT count(*) FROM duckdb_views() WHERE database_name = 'mem'") == 2);
	}

	SECTION("a table without a scan function cannot be read") {
		REQUIRE_THROWS_WITH(conn.Execute("SELECT * FROM mem.main.opaque").Drain(),
		                    Catch::Matchers::Contains("has no scan function"));
	}

	SECTION("what a function throws is the error of the statement") {
		REQUIRE_THROWS_WITH(conn.Execute("SELECT * FROM mem.main.broken").Drain(),
		                    Catch::Matchers::Contains("the mem schema refused to look"));
		REQUIRE_THROWS_WITH(conn.Execute("SELECT * FROM mem.main.missing").Drain(),
		                    Catch::Matchers::Contains("does not exist"));
	}

	SECTION("detaching destroys the database") {
		conn.Execute("DETACH mem").Drain();
		REQUIRE(observed.live_databases == 0);
		REQUIRE(observed.live_transactions == 0);
	}
}

TEST_CASE("Stable C++API: virtual catalog attach failures", "[cpp_api][cpp_api_vcatalog]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();
	RegisterMem(conn);

	// The constructor throws: nothing is attached, and the half-built database is gone.
	REQUIRE_THROWS_WITH(conn.Execute("ATTACH 'x' AS bad (TYPE vmem, rows 5000)").Drain(),
	                    Catch::Matchers::Contains("holds at most 1000 rows"));
	REQUIRE(observed.live_databases == 0);

	// An option the constructor never asked for.
	REQUIRE_THROWS_WITH(conn.Execute("ATTACH 'x' AS bad (TYPE vmem, colour 'red')").Drain(),
	                    Catch::Matchers::Contains("colour"));
	REQUIRE(observed.live_databases == 0);

	// An option that was not given has no value.
	observed.option_rows = -1;
	conn.Execute("ATTACH 'x' AS ro (TYPE vmem, READ_ONLY)").Drain();
	REQUIRE(observed.attached_read_only);
	REQUIRE(observed.option_rows == -1);
	REQUIRE(QueryBigint(conn, "SELECT count(*) FROM ro.numbers") == 3);

	// A database names its default schema: the one a name without a schema refers to.
	conn.Execute("ATTACH 'x' AS other (TYPE vmem, default_schema 'extra')").Drain();
	conn.Execute("CREATE SCHEMA other.extra").Drain();
	conn.Execute("CREATE TABLE other.extra.only_here (i BIGINT)").Drain();
	REQUIRE(QueryBigint(conn, "SELECT count(*) FROM other.only_here") == 0);
	REQUIRE_THROWS_WITH(conn.Execute("SELECT * FROM other.numbers").Drain(),
	                    Catch::Matchers::Contains("does not exist"));
	REQUIRE(QueryBigint(conn, "SELECT count(*) FROM other.main.numbers") == 3);

	// The name is taken once registered.
	auto again = VirtualCatalog::Create<MemDatabase>(conn.GetFactory(), "vmem");
	REQUIRE_THROWS_AS(conn.Register(again), Exception);
}

TEST_CASE("Stable C++API: virtual catalog transactions", "[cpp_api][cpp_api_vcatalog]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();
	RegisterMem(conn);
	conn.Execute("ATTACH 'x' AS mem (TYPE vmem)").Drain();

	auto begins = observed.begins;
	auto commits = observed.commits;
	auto rollbacks = observed.rollbacks;

	conn.Execute("BEGIN").Drain();
	REQUIRE(QueryBigint(conn, "SELECT count(*) FROM mem.numbers") == 3);
	REQUIRE(QueryBigint(conn, "SELECT count(*) FROM mem.pairs") == 3);
	// Both statements ran in the one transaction: its object counted their lookups and binds.
	REQUIRE(observed.begins == begins + 1);
	REQUIRE(observed.lookups_in_transaction >= 3);
	REQUIRE(observed.live_transactions == 1);
	conn.Execute("COMMIT").Drain();
	REQUIRE(observed.commits == commits + 1);
	REQUIRE(observed.live_transactions == 0);

	conn.Execute("BEGIN").Drain();
	REQUIRE(QueryBigint(conn, "SELECT count(*) FROM mem.numbers") == 3);
	conn.Execute("ROLLBACK").Drain();
	REQUIRE(observed.rollbacks == rollbacks + 1);
	REQUIRE(observed.live_transactions == 0);

	auto read_only = observed.read_only_begins;
	conn.Execute("BEGIN TRANSACTION READ ONLY").Drain();
	REQUIRE(QueryBigint(conn, "SELECT count(*) FROM mem.numbers") == 3);
	conn.Execute("COMMIT").Drain();
	REQUIRE(observed.read_only_begins == read_only + 1);
}

TEST_CASE("Stable C++API: virtual catalog DDL", "[cpp_api][cpp_api_vcatalog]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();
	RegisterMem(conn);
	conn.Execute("ATTACH 'x' AS mem (TYPE vmem)").Drain();

	SECTION("schemas") {
		conn.Execute("CREATE SCHEMA mem.extra").Drain();
		REQUIRE(QueryBigint(conn, "SELECT count(*) FROM duckdb_schemas() WHERE database_name = 'mem'") == 2);
		conn.Execute("CREATE SCHEMA IF NOT EXISTS mem.extra").Drain();
		REQUIRE_THROWS_AS(conn.Execute("CREATE SCHEMA mem.extra").Drain(), Exception);

		conn.Execute("DROP SCHEMA mem.extra CASCADE").Drain();
		REQUIRE(observed.cascade_drops == 1);
		REQUIRE(QueryBigint(conn, "SELECT count(*) FROM duckdb_schemas() WHERE database_name = 'mem'") == 1);
		conn.Execute("DROP SCHEMA IF EXISTS mem.extra").Drain();
	}

	SECTION("tables") {
		conn.Execute("CREATE TABLE mem.main.made (id BIGINT NOT NULL DEFAULT 7, name VARCHAR, PRIMARY KEY (id))")
		    .Drain();
		REQUIRE(observed.created_columns.size() == 2);
		REQUIRE(observed.created_columns[0] == "id BIGINT NOT NULL DEFAULT 7");
		REQUIRE(observed.created_columns[1] == "name VARCHAR");
		REQUIRE(observed.created_constraints.size() == 1);
		REQUIRE(observed.created_constraints[0] == "PRIMARY KEY(id)");
		REQUIRE(QueryBigint(conn, "SELECT count(*) FROM mem.main.made") == 0);
		// The table that was built reports its columns, and carries the NOT NULL it was given.
		REQUIRE(observed.built_columns.size() == 2);
		REQUIRE(observed.built_columns[0] == "id BIGINT NOT NULL");
		REQUIRE(observed.built_columns[1] == "name VARCHAR");
		REQUIRE(QueryBigint(conn, "SELECT count(*) FROM pragma_table_info('mem.main.made') WHERE \"notnull\"") == 1);

		conn.Execute("CREATE TABLE IF NOT EXISTS mem.main.made (other INTEGER)").Drain();
		REQUIRE(observed.created_columns.size() == 2);
		conn.Execute("CREATE OR REPLACE TABLE mem.main.made (other INTEGER)").Drain();
		REQUIRE(observed.created_columns.size() == 1);
		REQUIRE(observed.created_columns[0] == "other INTEGER");

		conn.Execute("DROP TABLE mem.main.made").Drain();
		REQUIRE_THROWS_WITH(conn.Execute("SELECT * FROM mem.main.made").Drain(),
		                    Catch::Matchers::Contains("does not exist"));
		// A table is not a view.
		REQUIRE_THROWS_AS(conn.Execute("DROP VIEW mem.main.numbers").Drain(), Exception);
	}

	SECTION("views") {
		conn.Execute("CREATE VIEW mem.main.tripled (thrice) AS SELECT value * 3 FROM numbers").Drain();
		REQUIRE(observed.created_view_aliases.size() == 1);
		REQUIRE(observed.created_view_aliases[0] == "thrice");
		// The query arrives as DuckDB renders it.
		REQUIRE(observed.created_view_query == "SELECT (\"value\" * 3) FROM numbers");
		// 30 + 33 + 36
		REQUIRE(QueryBigint(conn, "SELECT sum(thrice)::BIGINT FROM mem.main.tripled") == 99);

		conn.Execute("DROP VIEW mem.main.tripled").Drain();
		REQUIRE_THROWS_WITH(conn.Execute("SELECT * FROM mem.main.tripled").Drain(),
		                    Catch::Matchers::Contains("does not exist"));
	}
}

TEST_CASE("Stable C++API: virtual catalog defaults", "[cpp_api][cpp_api_vcatalog]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();
	auto catalog = VirtualCatalog::Create<BareDatabase>(conn.GetFactory(), "vbare");
	conn.Register(catalog);
	conn.Execute("ATTACH 'x' AS bare (TYPE vbare)").Drain();

	REQUIRE(QueryBigint(conn, "SELECT count(*) FROM duckdb_schemas() WHERE database_name = 'bare'") == 1);
	REQUIRE(QueryBigint(conn, "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'bare'") == 1);

	// What is not overridden is not supported.
	REQUIRE_THROWS_WITH(conn.Execute("CREATE SCHEMA bare.extra").Drain(),
	                    Catch::Matchers::Contains("Creating schemas is not supported"));
	REQUIRE_THROWS_WITH(conn.Execute("DROP SCHEMA bare.main").Drain(),
	                    Catch::Matchers::Contains("Dropping schemas is not supported"));
	REQUIRE_THROWS_WITH(conn.Execute("CREATE TABLE bare.main.t (i INTEGER)").Drain(),
	                    Catch::Matchers::Contains("Creating tables is not supported"));
	REQUIRE_THROWS_WITH(conn.Execute("CREATE VIEW bare.main.v AS SELECT 1").Drain(),
	                    Catch::Matchers::Contains("Creating views is not supported"));
	REQUIRE_THROWS_WITH(conn.Execute("DROP TABLE bare.main.plain").Drain(),
	                    Catch::Matchers::Contains("Dropping tables is not supported"));
	REQUIRE_THROWS_WITH(conn.Execute("SELECT * FROM bare.main.plain").Drain(),
	                    Catch::Matchers::Contains("has no scan function"));
}

TEST_CASE("Stable C++API: virtual catalog objects cast to the class of the implementation",
          "[cpp_api][cpp_api_vcatalog]") {
	BareTransaction transaction;
	VirtualCatalogTransaction &base = transaction;
	REQUIRE(&base.Cast<BareTransaction>() == &transaction);
	const VirtualCatalogTransaction &const_base = transaction;
	REQUIRE(&const_base.Cast<BareTransaction>() == &transaction);
}
