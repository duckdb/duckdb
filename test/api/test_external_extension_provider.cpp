#include "catch.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/external_extension_provider.hpp"
#include "duckdb/main/extension/linked_extension_registry.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

TEST_CASE("A linked loadable_extensions library is the default external extension provider", "[api]") {
	DuckDB db(nullptr);
	auto &config = DBConfig::GetConfig(*db.instance);
	auto &provider = config.GetExternalExtensionProvider();
	bool linked = false;
	for (auto &entry : config.linked_extensions) {
		linked = linked || entry.name == "loadable_extensions";
	}
	CHECK(provider.GetName() == (linked ? "dynamic" : "none"));
	if (!linked) {
		return;
	}
	CHECK(provider.SupportsExternalExtensions());

	Connection con(db);
	auto result = con.Query("SELECT count(*) FROM duckdb_extensions() WHERE extension_name = 'loadable_extensions'");
	REQUIRE_NO_FAIL(*result);
	CHECK(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("The none external extension provider refuses installing and loading", "[api]") {
	DBConfig config;
	config.SetOptionByName("autoload_known_extensions", true);
	DuckDB db(nullptr, &config);
	db.instance->config.SetExternalExtensionProvider(make_shared_ptr<ExternalExtensionProvider>());
	Connection con(db);

	auto load = con.Query("LOAD 'does_not_exist.duckdb_extension'");
	REQUIRE(load->HasError());
	CHECK(StringUtil::Contains(load->GetError(), "does not link the loadable_extensions library"));

	auto install = con.Query("INSTALL does_not_exist FROM 'http://127.0.0.1:9'");
	REQUIRE(install->HasError());
	CHECK(StringUtil::Contains(install->GetError(), "does not link the loadable_extensions library"));

	CHECK(!ExtensionHelper::CanAutoloadExtension(*db.instance, "json"));
	REQUIRE_THROWS(db.instance->config.SetExternalExtensionProvider(nullptr));
}
