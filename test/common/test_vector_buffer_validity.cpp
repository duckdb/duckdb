#include "catch.hpp"
#include "vector_buffer_lifetime.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
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
		const idx_t small_count = STANDARD_VECTOR_SIZE / 2;
		auto data = Allocator::DefaultAllocator().Allocate(count * type_size);
		StandardVectorBuffer buffer(std::move(data), count_t(small_count), type_size);
		REQUIRE(buffer.Capacity() == count);
		REQUIRE(buffer.GetValidityMask().Capacity() >= count);
		REQUIRE(buffer.GetValidityMask().CannotHaveNull());
		REQUIRE(buffer.GetValidityMask().GetAllocationSize() == 0);
		REQUIRE(buffer.GetValidityMask().RowIsValid(count - 1));
	}
}

TEST_CASE("Constant slices keep a single physical validity entry", "[vector_buffer]") {
	const idx_t count = 2 * STANDARD_VECTOR_SIZE + 7;
	// the mask of a constant covers one physical entry: at most the default capacity, never the logical count
	const idx_t max_capacity = STANDARD_VECTOR_SIZE;
	const idx_t max_allocation = ValidityMask::EntryCount(max_capacity) * sizeof(validity_t);

	SECTION("NULL constants allocate no logical-count bitmap") {
		const duckdb::vector<LogicalType> types {LogicalType::BIGINT, LogicalType::VARCHAR,
		                                         LogicalType::LIST(LogicalType::BIGINT)};
		for (auto &type : types) {
			Vector source(Value(type), count_t(1ULL));
			Vector sliced(source, 0, count);
			REQUIRE(sliced.GetVectorType() == VectorType::CONSTANT_VECTOR);
			REQUIRE(sliced.size() == count);
			auto &mask = sliced.Buffer().GetValidityMask();
			REQUIRE(mask.Capacity() <= max_capacity);
			REQUIRE(mask.GetAllocationSize() <= max_allocation);
			REQUIRE(ConstantVector::IsNull(sliced));
			REQUIRE(sliced.GetValue(count - 1).IsNull());
		}
	}
	SECTION("non-NULL constants select physical row zero at any logical row") {
		const duckdb::vector<Value> values {Value::BIGINT(42), Value("constant slice"),
		                                    Value::LIST(LogicalType::BIGINT, {Value::BIGINT(1), Value::BIGINT(2)})};
		for (auto &value : values) {
			Vector source(value, count_t(1ULL));
			Vector sliced(source, 0, count);
			REQUIRE(sliced.GetVectorType() == VectorType::CONSTANT_VECTOR);
			REQUIRE(sliced.size() == count);
			auto &mask = sliced.Buffer().GetValidityMask();
			REQUIRE(mask.Capacity() <= max_capacity);
			// a non-NULL constant keeps its mask unallocated
			REQUIRE(mask.GetAllocationSize() == 0);
			REQUIRE(sliced.GetValue(count - 1) == value);
		}
	}
}

TEST_CASE("Buffer slices retain their payload after the source is destroyed", "[vector_buffer][buffer_ptr]") {
	const duckdb::vector<Value> values {
	    Value::BIGINT(42), Value("a sliced string longer than the inline representation"),
	    Value::LIST(LogicalType::VARCHAR,
	                {Value("a nested string longer than the inline representation"), Value(LogicalType::VARCHAR)})};
	for (auto &value : values) {
		for (idx_t mode = 0; mode < 4; mode++) {
			INFO(value.type().ToString());
			INFO(mode);
			Vector result(value.type());
			duckdb::shared_ptr<bool> destroyed;
			{
				Vector source(value.type(), 3);
				source.Append(Value(value.type()));
				source.Append(value);
				source.Append(value);
				if (mode == 0) {
					source.Reference(value, count_t(1));
				}
				destroyed = ObserveVectorBufferDestruction(source);
				if (mode == 0) {
					result.Slice(source, 0, 7);
				} else if (mode == 1) {
					result.Slice(source, 1, 3);
				} else {
					SelectionVector selection(3);
					selection.set_index(0, 2);
					selection.set_index(1, 0);
					selection.set_index(2, 1);
					Vector dictionary(source, selection, 3);
					if (mode == 2) {
						result.Reference(dictionary);
					} else {
						result.Slice(dictionary, 1, 3);
					}
				}
			}
			REQUIRE_FALSE(*destroyed);
			REQUIRE(result.size() == (mode == 0 ? 7 : mode == 2 ? 3 : 2));
			for (idx_t row = 0; row < result.size(); row++) {
				const bool is_null = (mode == 2 && row == 1) || (mode == 3 && row == 0);
				REQUIRE(Value::NotDistinctFrom(result.GetValue(row), is_null ? Value(value.type()) : value));
			}
			Vector empty(value.type());
			result.Reference(empty);
			REQUIRE(*destroyed);
		}
	}
}
