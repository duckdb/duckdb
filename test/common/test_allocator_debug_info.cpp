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
	// To make sure that debug_info is set to a nullptr we have to jump through a
	// bit of a hoop. AllocatorDebugInfo is an incomplete type here, so we can't
	// call reset(). So instead we create a tmp PrivateAllocatorData, swap
	// debug_info into it, and let the destructor run. This way there's no leak.
	{
		auto tmp = make_uniq<PrivateAllocatorData>();
		tmp->debug_info.swap(alloc.GetPrivateData()->debug_info);
	}

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
