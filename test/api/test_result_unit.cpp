#include "catch.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/main/result_unit.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

class OtherUnit : public ResultUnit {
public:
	static constexpr const char *TAG = "other";

public:
	OtherUnit() : ResultUnit(0, 0) {
	}

public:
	const char *TypeTag() const override {
		return TAG;
	}
};

//! A chunk unit as another image would present it: the same tag text at a different address
class ForeignImageChunkUnit : public ChunkUnit {
public:
	explicit ForeignImageChunkUnit(unique_ptr<DataChunk> chunk) : ChunkUnit(std::move(chunk)) {
	}

public:
	const char *TypeTag() const override {
		return tag.c_str();
	}

private:
	string tag = ChunkUnit::TAG;
};

template <class F>
ErrorData CastError(F &&cast) {
	try {
		cast();
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
	return ErrorData();
}

unique_ptr<DataChunk> MakeChunk(idx_t rows) {
	auto chunk = make_uniq<DataChunk>();
	chunk->Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT, LogicalType::VARCHAR});
	for (idx_t i = 0; i < rows; i++) {
		chunk->data[0].SetValue(i, Value::BIGINT(NumericCast<int64_t>(i)));
		chunk->data[1].SetValue(i, Value(string(i, 'x')));
	}
	chunk->SetChildCardinality(rows);
	return chunk;
}

} // namespace

TEST_CASE("A chunk unit takes its rows and bytes from the chunk it holds", "[api][result_unit]") {
	auto chunk = MakeChunk(100);
	const auto bytes = chunk->GetDataSize();
	unique_ptr<ResultUnit> unit = make_uniq<ChunkUnit>(std::move(chunk));
	REQUIRE(unit->row_count == 100);
	REQUIRE(unit->byte_size == bytes);
	REQUIRE(string(unit->TypeTag()) == ChunkUnit::TAG);
	REQUIRE(unit->Is<ChunkUnit>());
	REQUIRE(!unit->Is<OtherUnit>());
	REQUIRE(unit->Cast<ChunkUnit>().chunk->size() == 100);

	unique_ptr<ResultUnit> empty = make_uniq<ChunkUnit>(MakeChunk(0));
	REQUIRE(empty->row_count == 0);
	REQUIRE(empty->Cast<ChunkUnit>().chunk->size() == 0);
}

TEST_CASE("A cast to a unit type with another tag throws", "[api][result_unit]") {
	unique_ptr<ResultUnit> chunk_unit = make_uniq<ChunkUnit>(MakeChunk(1));
	unique_ptr<ResultUnit> other_unit = make_uniq<OtherUnit>();
	auto to_chunk = CastError([&]() { other_unit->Cast<ChunkUnit>(); });
	REQUIRE(to_chunk.Type() == ExceptionType::INTERNAL);
	REQUIRE(to_chunk.RawMessage() == "Failed to cast result unit of type \"other\" to \"chunk\"");
	auto to_other = CastError([&]() { chunk_unit->Cast<OtherUnit>(); });
	REQUIRE(to_other.Type() == ExceptionType::INTERNAL);
	REQUIRE(to_other.RawMessage() == "Failed to cast result unit of type \"chunk\" to \"other\"");
	const ResultUnit &const_other = *other_unit;
	REQUIRE_THROWS_AS(const_other.Cast<ChunkUnit>(), InternalException);
	REQUIRE(other_unit->Is<OtherUnit>());
	REQUIRE(string(other_unit->Cast<OtherUnit>().TypeTag()) == OtherUnit::TAG);
}

TEST_CASE("A tag is matched by content, so a unit from another image casts", "[api][result_unit]") {
	unique_ptr<ResultUnit> unit = make_uniq<ForeignImageChunkUnit>(MakeChunk(5));
	REQUIRE(unit->TypeTag() != ChunkUnit::TAG);
	REQUIRE(string(unit->TypeTag()) == ChunkUnit::TAG);
	REQUIRE(unit->Is<ChunkUnit>());
	REQUIRE(unit->Cast<ChunkUnit>().chunk->size() == 5);
	const ResultUnit &const_unit = *unit;
	REQUIRE(const_unit.Cast<ChunkUnit>().chunk->size() == 5);
	REQUIRE(!unit->Is<OtherUnit>());
}
