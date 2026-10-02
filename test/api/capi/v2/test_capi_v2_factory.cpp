#include "test_capi_v2.hpp"

namespace test_capi_v2 {

TEST_CASE("V2: factory getters reject null arguments", "[capi_v2][factory]") {
	EnvFixture fx;
	duckdb_v2_factory_handle factory = nullptr;
	REQUIRE(duckdb_v2_connection_get_factory(nullptr, &factory, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(factory == nullptr);
	REQUIRE(duckdb_v2_connection_get_factory(fx.conn, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_context_get_factory(nullptr, &factory, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(factory == nullptr);
}

TEST_CASE("V2: a connection's factory is borrowed and stable", "[capi_v2][factory]") {
	EnvFixture fx;
	auto factory = Factory(fx.conn);
	REQUIRE(factory != nullptr);
	REQUIRE(Factory(fx.conn) == factory);

	duckdb_v2_connection_handle other = nullptr;
	REQUIRE(duckdb_v2_connection_create(fx.instance, &other, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Factory(other) != factory);

	// What a factory creates is owned by the caller and outlives the connection it came from.
	duckdb_v2_value_handle value = nullptr;
	REQUIRE(duckdb_v2_value_create_int(Factory(other), 42, &value, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_handle type = nullptr;
	auto text = Convert("INTEGER[]");
	REQUIRE(duckdb_v2_logical_type_create_from_text(Factory(other), &text, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_connection_destroy(&other) == DUCKDB_V2_ERROR_NONE);

	int32_t out = 0;
	REQUIRE(duckdb_v2_value_get_int(value, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out == 42);
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_id(type, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_LIST);
	duckdb_v2_value_destroy(&value);
	duckdb_v2_logical_type_destroy(&type);
}

TEST_CASE("V2: a connection's factory looks up names in the connection's open transaction", "[capi_v2][factory]") {
	EnvFixture fx;
	duckdb_v2_connection_handle other = nullptr;
	REQUIRE(duckdb_v2_connection_create(fx.instance, &other, nullptr) == DUCKDB_V2_ERROR_NONE);

	ExecSQL(fx.conn, "BEGIN");
	ExecSQL(fx.conn, "CREATE TYPE mood AS ENUM('sad', 'ok', 'happy')");

	// The uncommitted type is visible through the factory of the connection that created it...
	auto text = Convert("mood");
	duckdb_v2_logical_type_handle type = nullptr;
	REQUIRE(duckdb_v2_logical_type_create_from_text(Factory(fx.conn), &text, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_id(type, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_ENUM);
	duckdb_v2_logical_type_destroy(&type);

	// ...but not through another connection's.
	REQUIRE(duckdb_v2_logical_type_create_from_text(Factory(other), &text, &type, nullptr) ==
	        DUCKDB_V2_ERROR_DATABASE_CATALOG);
	REQUIRE(type == nullptr);

	ExecSQL(fx.conn, "ROLLBACK");
	duckdb_v2_connection_destroy(&other);
}

} // namespace test_capi_v2
