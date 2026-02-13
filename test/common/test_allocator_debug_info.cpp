#include "catch.hpp"
#include "duckdb/common/allocator.hpp"

using namespace duckdb;

#ifdef DEBUG
TEST_CASE("Allocator handles nullptr debug_info gracefully", "[allocator]") {
	// This is a regression test that only runs with debug builds. It:
	// * constructs an Allocator using the default allocate/free/reallocate functions.
	// * resets debug_info to nullptr (just like it is in a release build).
	// * tests that AllocateData, FreeData and ReallocateData don't throw
	// Also see https://github.com/duckdblabs/duckdb-internal/issues/7378
	auto private_data = make_uniq<PrivateAllocatorData>();
	Allocator alloc(Allocator::DefaultAllocate, Allocator::DefaultFree, Allocator::DefaultReallocate,
	                std::move(private_data));
	// release() instead of reset() because AllocatorDebugInfo is an incomplete type here.
	(void)alloc.GetPrivateData()->debug_info.release();

	SECTION("AllocateData with nullptr debug_info") {
		data_ptr_t ptr = nullptr;
		REQUIRE_NOTHROW(ptr = alloc.AllocateData(64));
		REQUIRE(ptr != nullptr);
		alloc.FreeData(ptr, 64);
	}

	SECTION("ReallocateData with nullptr debug_info") {
		data_ptr_t ptr = Allocator::DefaultAllocate(nullptr, 64);
		REQUIRE(ptr != nullptr);
		data_ptr_t new_ptr = nullptr;
		REQUIRE_NOTHROW(new_ptr = alloc.ReallocateData(ptr, 64, 128));
		REQUIRE(new_ptr != nullptr);
		alloc.FreeData(new_ptr, 128);
	}
}
#endif
