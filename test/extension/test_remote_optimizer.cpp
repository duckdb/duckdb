#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"

// whatever
#include <signal.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdio.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>

using namespace duckdb;
using namespace std;

TEST_CASE("Test using a remote optimizer pass in case thats important to someone", "[extension]") {

	pid_t pid = fork();

	int port = 4242;

	if (pid == 0) { // child process
		// sockets, man, how do they work?!
		struct sockaddr_in servaddr, cli;

		auto sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		REQUIRE(sockfd != -1);
		bzero(&servaddr, sizeof(servaddr));

		servaddr.sin_family = AF_INET;
		servaddr.sin_addr.s_addr = inet_addr("127.0.0.1");
		servaddr.sin_port = htons(port);
		REQUIRE(::bind(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) == 0);
		REQUIRE((listen(sockfd, 5)) == 0);

		socklen_t len = sizeof(cli);
		auto connfd = accept(sockfd, (struct sockaddr *)&cli, &len);
		REQUIRE(connfd >= 0);

		DuckDB db2; // patent pending
		Connection con2(db2);

		while (true) {
			ssize_t bytes;
			REQUIRE(read(connfd, &bytes, sizeof(idx_t)) == sizeof(idx_t));

			if (bytes == 0) {
				break;
			}

			auto buffer = malloc(bytes);
			REQUIRE(buffer);
			REQUIRE(read(connfd, buffer, bytes) == bytes);

			BufferedDeserializer deserializer((data_ptr_t)buffer, bytes);
			PlanDeserializationState state(*con2.context);
			auto plan = LogicalOperator::Deserialize(deserializer, state);
			plan->ResolveOperatorTypes();

			auto statement = make_unique<LogicalPlanStatement>(move(plan));
			auto result = con2.Query(move(statement));
			auto &collection = result->Collection();
			idx_t num_chunks = collection.ChunkCount();
			REQUIRE(write(connfd, &num_chunks, sizeof(idx_t)) == sizeof(idx_t));
			for (auto &chunk : collection.Chunks()) {
				BufferedSerializer serializer;
				chunk.Serialize(serializer);
				auto data = serializer.GetData();
				ssize_t len = data.size;
				REQUIRE(write(connfd, &len, sizeof(idx_t)) == sizeof(idx_t));
				REQUIRE(write(connfd, data.data.get(), len) == len);
			}
		}
		exit(0);

	} else if (pid > 0) { // parent process

		DBConfig config;
		config.options.allow_unsigned_extensions = true;
		DuckDB db1(nullptr, &config);
		Connection con1(db1);
		auto load_parquet = con1.Query("LOAD 'parquet'");
		if (load_parquet->HasError()) {
			// Do not execute the test.
			if (kill(pid, SIGKILL) != 0) {
				FAIL();
			}
			return;
		}

		REQUIRE_NO_FAIL(con1.Query("LOAD '" DUCKDB_BUILD_DIRECTORY
		                           "/test/extension/loadable_extension_optimizer_demo.duckdb_extension'"));
		REQUIRE_NO_FAIL(con1.Query("SET waggle_location_host='127.0.0.1'"));
		REQUIRE_NO_FAIL(con1.Query("SET waggle_location_port=4242"));
		usleep(100000); // need to wait a bit till socket is up

		REQUIRE_NO_FAIL(con1.Query(
		    "SELECT first_name FROM PARQUET_SCAN('data/parquet-testing/userdata1.parquet') GROUP BY first_name"));

		if (kill(pid, SIGKILL) != 0) {
			FAIL();
		}

	} else {
		FAIL();
	}
}
