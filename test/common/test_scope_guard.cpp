#include "catch.hpp"
#include "duckdb/common/scope_guard.hpp"

#include <stdexcept>
#include <vector>

using namespace duckdb;

TEST_CASE("ScopeGuard empty guard", "[scope_guard]") {
	ScopeGuard guard;
}

TEST_CASE("ScopeGuard with SCOPE_EXIT macro", "[scope_guard]") {
	int counter = 0;
	{
		SCOPE_EXIT {
			counter++;
		};
		REQUIRE(counter == 0);
	}
	REQUIRE(counter == 1);
}

TEST_CASE("ScopeGuard multiple SCOPE_EXIT blocks", "[scope_guard]") {
	std::vector<int> execution_order;
	{
		SCOPE_EXIT {
			execution_order.push_back(1);
		};
		SCOPE_EXIT {
			execution_order.push_back(2);
		};
		SCOPE_EXIT {
			execution_order.push_back(3);
		};
		REQUIRE(execution_order.empty());
	}

	REQUIRE(execution_order.size() == 3);
	REQUIRE(execution_order[0] == 3);
	REQUIRE(execution_order[1] == 2);
	REQUIRE(execution_order[2] == 1);
}

TEST_CASE("ScopeGuard nested scopes", "[scope_guard]") {
	std::vector<int> execution_order;

	{
		SCOPE_EXIT {
			execution_order.push_back(1);
		};
		{
			SCOPE_EXIT {
				execution_order.push_back(2);
			};
			{
				SCOPE_EXIT {
					execution_order.push_back(3);
				};
				REQUIRE(execution_order.empty());
			}
			REQUIRE(execution_order.size() == 1);
			REQUIRE(execution_order[0] == 3);
		}
		REQUIRE(execution_order.size() == 2);
		REQUIRE(execution_order[1] == 2);
	}
	REQUIRE(execution_order.size() == 3);
	REQUIRE(execution_order[2] == 1);
}
