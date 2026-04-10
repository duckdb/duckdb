#include "catch.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

using duckdb::ClientProperties;
using duckdb::Connection;
using duckdb::ConnectionException;
using duckdb::DuckDB;

TEST_CASE("ClientProperties: default-constructed has no context", "[api][client_properties]") {
	ClientProperties props;

	REQUIRE(props.TryGetClientContext() == nullptr);
	REQUIRE_THROWS_AS(props.GetClientContextOrThrow(), ConnectionException);
}

TEST_CASE("ClientProperties: live context is returned", "[api][client_properties]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto props = con.context->GetClientProperties();

	auto ctx = props.TryGetClientContext();
	REQUIRE(ctx != nullptr);
	REQUIRE(ctx.get() == con.context.get());

	auto ctx2 = props.GetClientContextOrThrow();
	REQUIRE(ctx2.get() == con.context.get());
}

TEST_CASE("ClientProperties: expired context throws ConnectionException", "[api][client_properties]") {
	// Simulates a consumer (e.g. an Arrow C stream capsule) being consumed
	// after the originating connection has been closed: the weak_ptr inside
	// ClientProperties must detect this and throw instead of returning a
	// dangling pointer.
	ClientProperties props;
	{
		DuckDB db(nullptr);
		Connection con(db);
		props = con.context->GetClientProperties();
		REQUIRE(props.TryGetClientContext() != nullptr);
	}
	// Connection (and thus its ClientContext) has been destroyed here.

	REQUIRE(props.TryGetClientContext() == nullptr);
	REQUIRE_THROWS_AS(props.GetClientContextOrThrow(), ConnectionException);

	try {
		props.GetClientContextOrThrow();
		FAIL("expected ConnectionException");
	} catch (const ConnectionException &ex) {
		std::string msg = ex.what();
		REQUIRE(msg.find("connection has been closed") != std::string::npos);
	}
}
