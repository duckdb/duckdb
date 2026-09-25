#include "catch.hpp"
#include "duckdb/common/types/chunk_layout.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"

using namespace duckdb;

TEST_CASE("Chunk layouts preserve group identity and project overlapping columns", "[chunk_layout]") {
	ChunkLayoutBuilder builder;
	auto empty = builder.AddColumns({});
	auto values = builder.AddColumns({LogicalType::INTEGER, LogicalType::VARCHAR});
	auto flags = builder.AddColumns({LogicalType::BOOLEAN});
	auto original = builder.Build();
	auto layout = std::move(original);
	auto copy = layout;
	REQUIRE_THROWS(builder.AddColumn(LogicalType::INTEGER));
	REQUIRE_THROWS(builder.Build());

	DataChunk source;
	source.Initialize(Allocator::DefaultAllocator(), layout.GetTypes());
	{
		auto writer = FlatVector::Writer<int32_t>(layout.Columns(source, values).Column(0), 3);
		writer.WriteValue(11);
		writer.WriteNull();
		writer.WriteValue(33);
	}
	{
		auto writer = FlatVector::Writer<string_t>(layout.Columns(source, values).Column(1), 3);
		writer.WriteValue(string_t("first"));
		writer.WriteValue(string_t("second"));
		writer.WriteNull();
	}
	{
		auto writer = FlatVector::Writer<bool>(copy.Columns(source, flags).Column(0), 3);
		writer.WriteValue(true);
		writer.WriteValue(false);
		writer.WriteNull();
	}
	source.CheckCardinality(3);
	REQUIRE(layout.Columns(source, empty).ColumnCount() == 0);
	REQUIRE(layout.Columns(source, empty).RowCount() == 3);
	REQUIRE(!layout.Columns(source, empty).ContiguousVectors());

	ChunkLayoutBuilder output_builder;
	output_builder.AddColumns({LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::INTEGER});
	auto output_layout = output_builder.Build();
	ChunkProjection projection(copy, output_layout, {values.Column(1), values.Column(0), values.Column(0)});
	DataChunk output;
	output.InitializeEmpty(output_layout.GetTypes());
	projection.Reference(source, output);
	REQUIRE(output.size() == 3);
	REQUIRE(output.GetValue(0, 0).ToString() == "first");
	REQUIRE(output.GetValue(0, 2).IsNull());
	REQUIRE(output.GetValue(1, 1).IsNull());
	REQUIRE(output.GetValue(2, 2).GetValue<int32_t>() == 33);

	SelectionVector selection(2);
	selection.set_index(0, 2);
	selection.set_index(1, 0);
	source.Slice(selection, 2);
	projection.Reference(source, output);
	REQUIRE(output.size() == 2);
	REQUIRE(output.GetValue(1, 0).GetValue<int32_t>() == 33);
	REQUIRE(output.GetValue(0, 1).ToString() == "first");

	ChunkLayoutBuilder empty_builder;
	auto empty_layout = empty_builder.Build();
	ChunkProjection empty_projection(layout, empty_layout, {});
	DataChunk empty_output;
	empty_output.InitializeEmpty({});
	empty_projection.Reference(source, empty_output);
	REQUIRE(empty_output.size() == 2);

	DataChunk group;
	group.InitializeEmpty({LogicalType::INTEGER, LogicalType::VARCHAR});
	layout.Columns(source, values).ReferenceInto(group);
	REQUIRE(group.size() == 2);
	REQUIRE(group.GetValue(0, 1).GetValue<int32_t>() == 11);

	DataChunk assembled;
	assembled.InitializeEmpty(layout.GetTypes());
	layout.Columns(assembled, flags).ReferenceFrom(layout.Columns(source, flags));
	layout.Columns(assembled, values).ReferenceFrom(layout.Columns(source, values));
	assembled.CheckCardinality(2);
	REQUIRE(assembled.GetValue(2, 0).IsNull());
	REQUIRE(assembled.GetValue(0, 1).GetValue<int32_t>() == 11);
}

TEST_CASE("Chunk projections reject incomplete and incompatible mappings", "[chunk_layout]") {
	ChunkLayoutBuilder source_builder;
	auto integer = source_builder.AddColumn(LogicalType::INTEGER);
	auto text = source_builder.AddColumn(LogicalType::VARCHAR);
	auto source = source_builder.Build();
	ChunkLayoutBuilder target_builder;
	auto foreign = target_builder.AddColumn(LogicalType::INTEGER);
	auto target = target_builder.Build();
	REQUIRE_THROWS(ChunkProjection(source, target, {}));
	REQUIRE_THROWS(ChunkProjection(source, target, {integer, integer}));
	REQUIRE_THROWS(ChunkProjection(source, target, {text}));
	REQUIRE_THROWS(ChunkProjection(source, target, {foreign}));
}

TEST_CASE("Empty chunk groups retain logical row counts", "[chunk_layout]") {
	ChunkLayoutBuilder builder;
	auto empty = builder.AddColumns({});
	auto layout = builder.Build();
	DataChunk source;
	source.InitializeEmpty({});
	source.SetCardinalityUnsafe(7);
	DataChunk target;
	target.InitializeEmpty({});
	ChunkProjection projection(layout, layout, {});
	projection.Reference(source, target);
	REQUIRE(target.size() == 7);
	layout.Columns(source, empty).ReferenceInto(target);
	REQUIRE(target.size() == 7);
	source.SetCardinalityUnsafe(0);
	projection.Reference(source, target);
	REQUIRE(target.size() == 0);
}
