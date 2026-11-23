#include "catch.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/http/http_util.hpp"
#include "httplib.hpp"

#include <thread>

namespace duckdb {

TEST_CASE("Built-in HTTP client downloads responses larger than 100 MiB", "[httplib]") {
	DBConfig config;
	config.options.load_extensions = false;
	DuckDB db(nullptr, &config);
	bool linked = false;
	for (auto &entry : db.instance->config.linked_extensions) {
		linked = linked || entry.name == "httplib";
	}
	if (!linked) {
		return;
	}
	auto &http_util = HTTPUtil::Get(*db.instance);
	REQUIRE(http_util.GetName() == "Built-In");

	idx_t response_length = 100 * 1024 * 1024 + 1;
	bool chunked = false;
	bool truncated = false;
	SECTION("Content-Length") {
	}
	SECTION("Chunked") {
		chunked = true;
	}
	SECTION("Truncated Content-Length") {
		response_length = 128;
		truncated = true;
	}

	const string block(64 * 1024, 'x');
	duckdb_httplib::Server server;
	server.Get("/", [&](const duckdb_httplib::Request &, duckdb_httplib::Response &response) {
		if (chunked) {
			response.set_chunked_content_provider(
			    "application/octet-stream", [&](idx_t offset, duckdb_httplib::DataSink &sink) {
				    auto length = MinValue<idx_t>(block.size(), response_length - offset);
				    if (!sink.write(block.data(), length)) {
					    return false;
				    }
				    if (offset + length == response_length) {
					    sink.done();
				    }
				    return true;
			    });
		} else {
			response.set_content_provider(
			    response_length, "application/octet-stream", [&](idx_t offset, idx_t, duckdb_httplib::DataSink &sink) {
				    if (truncated && offset > 0) {
					    return false;
				    }
				    auto length = truncated ? idx_t(16) : MinValue<idx_t>(block.size(), response_length - offset);
				    return sink.write(block.data(), length);
			    });
		}
	});
	auto port = server.bind_to_any_port("127.0.0.1");
	REQUIRE(port > 0);
	std::thread listener([&]() { server.listen_after_bind(); });
	duckdb_httplib::detail::scope_exit cleanup([&]() {
		server.stop();
		listener.join();
	});
	server.wait_until_ready();

	HTTPParams params(http_util);
	params.retries = 0;
	params.keep_alive = false;
	params.follow_location = false;
	const auto url = StringUtil::Format("http://127.0.0.1:%d/", port);
	GetRequestInfo request(url, HTTPHeaders(), params, nullptr, nullptr);
	auto client = http_util.InitializeClient(params, request.proto_host_port);
	REQUIRE(client);
	auto response = client->Get(request);
	REQUIRE(response);
	if (truncated) {
		CHECK(response->HasRequestError());
		CHECK(response->GetRequestError() == "Failed to read connection");
	} else {
		REQUIRE(!response->HasRequestError());
		CHECK(response->status == HTTPStatusCode::OK_200);
		CHECK(response->body.size() == response_length);
		CHECK(response->body.find_first_not_of('x') == string::npos);
	}
}

} // namespace duckdb
