#include "catch.hpp"
#include "httplib.hpp"

#include <thread>

using namespace duckdb_httplib;

TEST_CASE("A redirect without Location reaches the response handler and content receiver", "[httplib]") {
	// S3 answers a request to the wrong region with a 301 that has no Location: the region is in the headers and
	// the body, which the client must hand over rather than drop while trying to follow
	const std::string body =
	    "<Error><Code>PermanentRedirect</Code><Endpoint>bucket.s3.eu-west-1.amazonaws.com</Endpoint></Error>";
	Server server;
	server.Get("/", [&](const Request &, Response &response) {
		response.status = 301;
		response.set_header("x-amz-bucket-region", "eu-west-1");
		response.set_content(body, "application/xml");
	});
	auto port = server.bind_to_any_port("127.0.0.1");
	REQUIRE(port > 0);
	std::thread listener([&]() { server.listen_after_bind(); });
	detail::scope_exit cleanup([&]() {
		server.stop();
		listener.join();
	});
	server.wait_until_ready();

	Client client("127.0.0.1", port);
	client.set_follow_location(true);
	int handled_status = 0;
	std::string received;
	auto result = client.Get(
	    "/",
	    [&](const Response &response) {
		    handled_status = response.status;
		    return true;
	    },
	    [&](const char *data, size_t length) {
		    received.append(data, length);
		    return true;
	    });
	REQUIRE(result);
	REQUIRE(result->status == 301);
	REQUIRE(result->get_header_value("x-amz-bucket-region") == "eu-west-1");
	REQUIRE(handled_status == 301);
	REQUIRE(received == body);
}
