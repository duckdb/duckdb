#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/extension_callback.hpp"

using namespace duckdb;

namespace {

struct CheckpointChangeRecorder : public ExtensionCallback {
	vector<CheckpointRowIDChangeInfo> changes;

	void OnCheckpointRowIDsChanged(DatabaseInstance &, const CheckpointRowIDChangeInfo &info) override {
		changes.push_back(info);
	}
};

DuckTableEntry &GetTableEntry(ClientContext &context, const string &table_name) {
	return Catalog::GetEntry<DuckTableEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, table_name);
}

} // namespace

TEST_CASE("Checkpoint row id callbacks report the first changed old row group", "[api]") {
	DBConfig config;
	auto recorder = make_shared_ptr<CheckpointChangeRecorder>();
	ExtensionCallback::Register(config, recorder);

	auto path = TestCreatePath("checkpoint_row_id_callback.db");
	DeleteDatabase(path);
	DuckDB db(path, &config);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT i AS id, i % 100 AS val FROM range(500000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM t WHERE id < 200000"));

	con.BeginTransaction();
	auto table_oid = GetTableEntry(*con.context, "t").oid;
	con.Commit();

	REQUIRE(recorder->changes.empty());

	REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));

	REQUIRE(recorder->changes.size() == 1);
	REQUIRE(recorder->changes[0].table_oid == table_oid);
	REQUIRE(recorder->changes[0].first_changed_old_row_group == 0);
	REQUIRE(recorder->changes[0].row_ids_changed);
}
