#include "catch.hpp"
#include "test_helpers.hpp"
#include "tpch-extension.hpp"
#include "cbuffer_manager_helpers.hpp"

#include <chrono>
#include <iostream>
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<DBConfig> ConfigWithCBufferManager(MyBufferManager *manager) {
	unique_ptr<DBConfig> config = make_unique<DBConfig>();
	CBufferManagerConfig cbuffer_manager_config = DefaultCBufferManagerConfig(manager);
	config->SetVirtualBufferManager(make_unique<CBufferManager>(cbuffer_manager_config));
	return config;
}

TEST_CASE("Test CBufferManager with TPCH", "[api]") {
	unique_ptr<QueryResult> result;
	MyBufferManager manager;
	auto config = ConfigWithCBufferManager(&manager);
	DuckDB db(nullptr, config.get());
	Connection con(db);
	double sf = 0.01;
	if (!db.ExtensionIsLoaded("tpch")) {
		return;
	}

	REQUIRE_NO_FAIL(con.Query("CALL dbgen(sf=" + to_string(sf) + ")"));

	for (idx_t i = 1; i < 22; i++) {
		auto query = TPCHExtension::GetQuery(i);
		auto result = con.Query(query);
		COMPARE_CSV(result, TPCHExtension::GetAnswer(sf, i), true);
		REQUIRE(true);
	}
}
