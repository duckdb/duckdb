#include "duckdb/common/exception.hpp"
#include "duckdb/common/http_util.hpp"
#include "catch.hpp"

using namespace duckdb;

TEST_CASE("Parse HTTP proxy host", "[http]") {
	string hostname;
	idx_t port;

	string proxy = "http://proxy.example.com:3128/";
	HTTPUtil::ParseHTTPProxyHost(proxy, hostname, port);
	REQUIRE(hostname == "proxy.example.com");
	REQUIRE(port == 3128);

	proxy = "http://proxy.example.com/";
	HTTPUtil::ParseHTTPProxyHost(proxy, hostname, port);
	REQUIRE(hostname == "proxy.example.com");
	REQUIRE(port == 80);

	proxy = "http://proxy.example.com:3128/foo";
	REQUIRE_THROWS_AS(HTTPUtil::ParseHTTPProxyHost(proxy, hostname, port), InvalidInputException);

	proxy = "http://proxy.example.com:3128//";
	REQUIRE_THROWS_AS(HTTPUtil::ParseHTTPProxyHost(proxy, hostname, port), InvalidInputException);
}
