#include "test_helpers.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "catch.hpp"

using namespace duckdb;

namespace {

constexpr idx_t THREAD_COUNT = 4;
constexpr idx_t PER_THREAD = 2000;

bool IsWellFormedV4(hugeint_t uuid) {
	// version nibble lives in byte 6 (the lower half of `upper`), variant bits in byte 8 (top of `lower`)
	auto version = (static_cast<uint64_t>(uuid.upper) >> 8) & 0xF0;
	auto variant = (uuid.lower >> 56) & 0xC0;
	return version == 0x40 && variant == 0x80;
}

} // namespace

TEST_CASE("Test UUID API", "[api]") {
	REQUIRE(UUID::ToString(UUID::FromUHugeint(uhugeint_t(0))) == "00000000-0000-0000-0000-000000000000");
	REQUIRE(UUID::ToString(UUID::FromUHugeint(uhugeint_t(1))) == "00000000-0000-0000-0000-000000000001");
	REQUIRE(UUID::ToString(UUID::FromUHugeint(NumericLimits<uhugeint_t>::Maximum())) ==
	        "ffffffff-ffff-ffff-ffff-ffffffffffff");
	REQUIRE(UUID::ToString(UUID::FromUHugeint(NumericLimits<uhugeint_t>::Maximum() - 1)) ==
	        "ffffffff-ffff-ffff-ffff-fffffffffffe");
	REQUIRE(UUID::ToString(UUID::FromUHugeint(NumericLimits<uhugeint_t>::Maximum() / 2)) ==
	        "7fffffff-ffff-ffff-ffff-ffffffffffff");
	REQUIRE(UUID::ToString(UUID::FromUHugeint((NumericLimits<uhugeint_t>::Maximum() / 2) + 1)) ==
	        "80000000-0000-0000-0000-000000000000");

	REQUIRE_THAT(UUID::ToUHugeint(UUID::FromString("00000000-0000-0000-0000-000000000000")),
	             Catch::Predicate<uhugeint_t>([&](const uhugeint_t &input) {
		             return input.upper == 0x0000000000000000 && input.lower == 0x0000000000000000;
	             }));
	REQUIRE_THAT(UUID::ToUHugeint(UUID::FromString("00000000-0000-0000-0000-000000000001")),
	             Catch::Predicate<uhugeint_t>([&](const uhugeint_t &input) {
		             return input.upper == 0x0000000000000000 && input.lower == 0x0000000000000001;
	             }));
	REQUIRE_THAT(UUID::ToUHugeint(UUID::FromString("ffffffff-ffff-ffff-ffff-ffffffffffff")),
	             Catch::Predicate<uhugeint_t>([&](const uhugeint_t &input) {
		             return input.upper == 0xffffffffffffffff && input.lower == 0xffffffffffffffff;
	             }));
	REQUIRE_THAT(UUID::ToUHugeint(UUID::FromString("ffffffff-ffff-ffff-ffff-fffffffffffe")),
	             Catch::Predicate<uhugeint_t>([&](const uhugeint_t &input) {
		             return input.upper == 0xffffffffffffffff && input.lower == 0xfffffffffffffffe;
	             }));
	REQUIRE_THAT(UUID::ToUHugeint(UUID::FromString("7fffffff-ffff-ffff-ffff-ffffffffffff")),
	             Catch::Predicate<uhugeint_t>([&](const uhugeint_t &input) {
		             return input.upper == 0x7fffffffffffffff && input.lower == 0xffffffffffffffff;
	             }));
	REQUIRE_THAT(UUID::ToUHugeint(UUID::FromString("80000000-0000-0000-0000-000000000000")),
	             Catch::Predicate<uhugeint_t>([&](const uhugeint_t &input) {
		             return input.upper == 0x8000000000000000 && input.lower == 0x0000000000000000;
	             }));
}

TEST_CASE("Test UUID::GenerateUniqueUUID", "[api]") {
	SECTION("no two calls return the same value") {
		// distinctness here is a guarantee, not a probability - callers name resources with it
		set<hugeint_t> uuids;
		for (idx_t i = 0; i < 10000; i++) {
			uuids.insert(UUID::GenerateUniqueUUID());
		}
		REQUIRE(uuids.size() == 10000);
	}
	SECTION("the result stays a well-formed v4 UUID") {
		for (idx_t i = 0; i < 10000; i++) {
			REQUIRE(IsWellFormedV4(UUID::GenerateUniqueUUID()));
		}
	}
	SECTION("concurrent callers do not collide") {
		vector<vector<hugeint_t>> per_thread(THREAD_COUNT);
		vector<thread> threads;
		for (idx_t t = 0; t < THREAD_COUNT; t++) {
			threads.emplace_back([&per_thread, t]() {
				for (idx_t i = 0; i < PER_THREAD; i++) {
					per_thread[t].push_back(UUID::GenerateUniqueUUID());
				}
			});
		}
		for (auto &t : threads) {
			t.join();
		}
		set<hugeint_t> uuids;
		for (auto &results : per_thread) {
			uuids.insert(results.begin(), results.end());
		}
		REQUIRE(uuids.size() == THREAD_COUNT * PER_THREAD);
	}
}
