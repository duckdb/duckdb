#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

TEST_CASE("Tuple data allocation accounting follows retained blocks", "[tuple_data_allocation]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	vector<LogicalType> types {LogicalType::BIGINT, LogicalType::VARCHAR};
	auto layout = make_shared_ptr<TupleDataLayout>();
	layout->Initialize(types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	TupleDataCollection first(buffer_manager, layout, MemoryTag::HASH_TABLE);
	TupleDataCollection second(buffer_manager, layout, MemoryTag::HASH_TABLE);
	REQUIRE(first.GetBlockAllocationSize() == 0);

	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	{
		auto keys = FlatVector::Writer<int64_t>(chunk.data[0], STANDARD_VECTOR_SIZE);
		auto strings = FlatVector::Writer<string_t>(chunk.data[1], STANDARD_VECTOR_SIZE);
		const string payload(4096, 'x');
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			keys.WriteValue(NumericCast<int64_t>(i));
			strings.WriteValue(string_t(payload));
		}
	}
	chunk.CheckCardinality(STANDARD_VECTOR_SIZE);
	first.Append(chunk);
	const auto first_size = first.GetBlockAllocationSize();
	REQUIRE(first_size >= STANDARD_VECTOR_SIZE * (4096 + layout->GetRowWidth()));
	first.Unpin();
	REQUIRE(first.GetBlockAllocationSize() == first_size);

	second.Append(chunk);
	const auto second_size = second.GetBlockAllocationSize();
	first.Combine(second);
	REQUIRE(first.GetBlockAllocationSize() == first_size + second_size);
	REQUIRE(second.GetBlockAllocationSize() == 0);

	// Reusing the source must not release allocations transferred to the destination.
	second.Append(chunk);
	second.Reset();
	REQUIRE(second.GetBlockAllocationSize() == 0);
	REQUIRE(first.GetBlockAllocationSize() == first_size + second_size);

	{
		TupleDataChunkIterator iterator(first, TupleDataPinProperties::DESTROY_AFTER_DONE, true);
		while (!iterator.Done()) {
			iterator.Next();
		}
	}
	REQUIRE(first.GetBlockAllocationSize() == 0);
	first.Reset();
	first.Append(chunk);
	REQUIRE(first.GetBlockAllocationSize() == first_size);
	first.Reset();
	REQUIRE(first.GetBlockAllocationSize() == 0);
}

} // namespace duckdb
