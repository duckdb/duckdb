#include "catch.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/index_type_set.hpp"
#include "duckdb/main/config.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

//! Models an index type that cannot apply the commits made while it is built
class NoDeltaART : public ART {
public:
	using ART::ART;

	static unique_ptr<BoundIndex> Create(CreateIndexInput &input) {
		return make_uniq<NoDeltaART>(input.name, input.constraint_type, input.column_ids, input.table_io_manager,
		                             input.unbound_expressions, input.db, nullptr, input.storage_info);
	}

	bool SupportsDeltaIndexes() const override {
		return false;
	}
};

void RegisterNoDeltaIndexType(DuckDB &db) {
	auto index_type = ART::GetARTIndexType();
	index_type.name = "NO_DELTA_ART";
	index_type.create_instance = NoDeltaART::Create;
	DBConfig::GetConfig(*db.instance).GetIndexTypes().RegisterIndexType(index_type);
}

//! Starts CREATE INDEX on con, commits change on other while it runs, then finishes the CREATE INDEX
unique_ptr<QueryResult> CreateIndexDuringChange(Connection &con, Connection &other, const string &change) {
	auto result = con.Submit("CREATE INDEX idx ON tbl USING NO_DELTA_ART (i)");
	REQUIRE(result->ExecuteTask() == QueryResultState::NOT_READY);
	REQUIRE_NO_FAIL(other.Query(change));
	result->Complete();
	return result;
}

} // namespace

TEST_CASE("CREATE INDEX without delta support fails on concurrent changes", "[index]") {
	auto config = GetTestConfig();
	config->options.checkpoint_wal_size = idx_t(-1);
	DuckDB db(nullptr, config.get());
	RegisterNoDeltaIndexType(db);
	Connection con(db);
	Connection other(db);
	// only the explicit ExecuteTask calls run the CREATE INDEX
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl AS SELECT i FROM range(1000000) r(i)"));

	for (auto &change :
	     {"INSERT INTO tbl VALUES (-1)", "DELETE FROM tbl WHERE i = 7", "UPDATE tbl SET i = -2 WHERE i = 8"}) {
		auto result = CreateIndexDuringChange(con, other, change);
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "Retry the CREATE INDEX"));
	}

	// a commit before the CREATE INDEX statement in the same transaction is no conflict
	REQUIRE_NO_FAIL(con.Query("BEGIN"));
	REQUIRE_NO_FAIL(other.Query("INSERT INTO tbl VALUES (-3)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX idx ON tbl USING NO_DELTA_ART (i)"));
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	auto result = con.Query("SELECT count(*) FILTER (i = -1), count(*) FILTER (i = 7), count(*) FILTER (i = -2), "
	                        "count(*) FILTER (i = 8), count(*) FILTER (i = -3) FROM tbl");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {1}));
	REQUIRE(CHECK_COLUMN(result, 3, {0}));
	REQUIRE(CHECK_COLUMN(result, 4, {1}));
	result = con.Query("SELECT count(*) FROM duckdb_indexes() WHERE index_name = 'idx'");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}
