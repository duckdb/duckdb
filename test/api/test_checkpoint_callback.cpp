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
	idx_t start_calls = 0;
	vector<CheckpointEventInfo> end_calls;

	void OnCheckpointStart(DatabaseInstance &, const CheckpointOptions &) override {
		start_calls++;
	}

	void OnCheckpointEnd(DatabaseInstance &, const CheckpointEventInfo &info) override {
		end_calls.push_back(info);
	}
};

idx_t GetTableOID(ClientContext &context, const string &table_name) {
	idx_t table_oid;
	context.RunFunctionInTransaction([&]() {
		table_oid = Catalog::GetEntry<DuckTableEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, table_name).oid;
	});
	return table_oid;
}

} // namespace

TEST_CASE("Checkpoint callbacks report table events", "[api]") {
	DBConfig config;
	auto recorder = make_shared_ptr<CheckpointChangeRecorder>();
	ExtensionCallback::Register(config, recorder);

	auto path = TestCreatePath("checkpoint_row_id_callback.db");
	DeleteDatabase(path);
	DuckDB db(path, &config);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT i AS id, i % 100 AS val FROM range(500000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM t WHERE id < 200000"));

	auto table_oid = GetTableOID(*con.context, "t");

	REQUIRE(recorder->start_calls == 0);
	REQUIRE(recorder->end_calls.empty());

	REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));

	REQUIRE(recorder->start_calls == 1);
	REQUIRE(recorder->end_calls.size() == 1);
	REQUIRE(recorder->end_calls[0].tables.size() == 1);

	auto &table_event = recorder->end_calls[0].tables[0];
	REQUIRE(table_event.table_oid == table_oid);
	REQUIRE(table_event.first_affected_old_row_group.IsValid());
	REQUIRE(table_event.first_affected_old_row_group.GetIndex() == 0);
	REQUIRE(table_event.row_ids_remapped);
	REQUIRE(!table_event.indexes_rebuilt);
	REQUIRE(!table_event.row_group_lineage.empty());

	bool found_dropped_prefix = false;
	bool found_remapped_target = false;
	for (auto &lineage_entry : table_event.row_group_lineage) {
		if (lineage_entry.kind == CheckpointRowGroupLineageKind::DROP && lineage_entry.old_row_group_index == 0) {
			found_dropped_prefix = true;
		}
		if (lineage_entry.new_row_group_index.IsValid()) {
			found_remapped_target = true;
		}
	}
	REQUIRE(found_dropped_prefix);
	REQUIRE(found_remapped_target);
}
