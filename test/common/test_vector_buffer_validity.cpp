#include "catch.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("StandardVectorBuffer validity capacity covers all entries", "[vector_buffer]") {
	const idx_t count = 2 * STANDARD_VECTOR_SIZE + 7;
	const idx_t type_size = sizeof(int64_t);

	SECTION("external data pointer") {
		auto storage = Allocator::DefaultAllocator().Allocate(count * type_size);
		StandardVectorBuffer buffer(storage.get(), count_t(count), type_size);
		REQUIRE(buffer.GetValidityMask().Capacity() >= count);
		// the covering capacity is metadata only: an all-valid mask must stay unallocated
		REQUIRE(buffer.GetValidityMask().CannotHaveNull());
		REQUIRE(buffer.GetValidityMask().GetAllocationSize() == 0);
		// on a fixed build this read passes the DEBUG capacity check in RowIsValid
		REQUIRE(buffer.GetValidityMask().RowIsValid(count - 1));
	}
	SECTION("owned allocation") {
		auto data = Allocator::DefaultAllocator().Allocate(count * type_size);
		StandardVectorBuffer buffer(std::move(data), count_t(count), type_size);
		REQUIRE(buffer.GetValidityMask().Capacity() >= buffer.Capacity());
		REQUIRE(buffer.GetValidityMask().CannotHaveNull());
		REQUIRE(buffer.GetValidityMask().GetAllocationSize() == 0);
		REQUIRE(buffer.GetValidityMask().RowIsValid(count - 1));
	}
	SECTION("owned allocation larger than the count") {
		// the mask must cover the allocation-derived capacity, not just the count
		const idx_t small_count = 42;
		auto data = Allocator::DefaultAllocator().Allocate(count * type_size);
		StandardVectorBuffer buffer(std::move(data), count_t(small_count), type_size);
		REQUIRE(buffer.Capacity() == count);
		REQUIRE(buffer.GetValidityMask().Capacity() >= count);
		REQUIRE(buffer.GetValidityMask().CannotHaveNull());
		REQUIRE(buffer.GetValidityMask().GetAllocationSize() == 0);
		REQUIRE(buffer.GetValidityMask().RowIsValid(count - 1));
	}
}
