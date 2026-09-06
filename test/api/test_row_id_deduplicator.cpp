#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/execution/row_id_deduplicator.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

static void AppendRow(DataChunk &chunk, int32_t payload, const string &filename, int64_t file_row_number) {
	chunk.data[0].Append(Value::INTEGER(payload));
	chunk.data[1].Append(Value(filename));
	chunk.data[2].Append(Value::BIGINT(file_row_number));
}

TEST_CASE("Deduplicate composite row IDs", "[row_id_deduplicator]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	RowIdDeduplicator deduplicator(context, {LogicalType::VARCHAR, LogicalType::BIGINT});

	DataChunk first;
	first.Initialize(context, {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::BIGINT});
	AppendRow(first, 10, "file-a.parquet", 0);
	AppendRow(first, 20, "file-a.parquet", 0);
	first.SetChildCardinality(2);

	SelectionVector first_seen(first.size());
	REQUIRE(deduplicator.Register(first, 1, first_seen) == 1);
	REQUIRE(first_seen.get_index(0) == 0);

	DataChunk second;
	second.Initialize(context, {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::BIGINT});
	AppendRow(second, 30, "file-a.parquet", 1);
	AppendRow(second, 40, "file-b.parquet", 0);
	second.SetChildCardinality(2);

	SelectionVector second_seen(second.size());
	REQUIRE(deduplicator.Register(second, 1, second_seen) == 2);
	REQUIRE(second_seen.get_index(0) == 0);
	REQUIRE(second_seen.get_index(1) == 1);

	DataChunk third;
	third.Initialize(context, {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::BIGINT});
	AppendRow(third, 50, "file-b.parquet", 0);
	AppendRow(third, 60, "file-a.parquet", 2);
	third.SetChildCardinality(2);

	SelectionVector third_seen(third.size());
	REQUIRE(deduplicator.Register(third, 1, third_seen) == 1);
	REQUIRE(third_seen.get_index(0) == 1);

	DataChunk fourth;
	fourth.Initialize(context, {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::BIGINT});
	AppendRow(fourth, 70, "file-c.parquet", 0);
	AppendRow(fourth, 80, "file-a.parquet", 2);
	fourth.SetChildCardinality(2);

	SelectionVector fourth_seen(fourth.size());
	REQUIRE(deduplicator.Register(fourth, 1, fourth_seen) == 1);
	REQUIRE(fourth_seen.get_index(0) == 0);
}

TEST_CASE("Deduplicate sliced composite row IDs", "[row_id_deduplicator]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;

	DataChunk source;
	source.Initialize(context, {LogicalType::VARCHAR, LogicalType::BIGINT});
	source.data[0].Append(Value("file-a.parquet"));
	source.data[1].Append(Value::BIGINT(0));
	source.data[0].Append(Value("file-b.parquet"));
	source.data[1].Append(Value::BIGINT(0));
	source.SetChildCardinality(2);

	SelectionVector reverse(2);
	reverse.set_index(0, 1);
	reverse.set_index(1, 0);
	DataChunk sliced;
	sliced.Initialize(context, source.GetTypes());
	sliced.Slice(source, reverse, 2);

	RowIdDeduplicator deduplicator(context, source.GetTypes());
	DataChunk existing;
	existing.Initialize(context, source.GetTypes());
	existing.data[0].Append(Value("file-a.parquet"));
	existing.data[1].Append(Value::BIGINT(0));
	existing.SetChildCardinality(1);
	REQUIRE(deduplicator.Register(existing, 0) == 1);

	SelectionVector first_seen(sliced.size());
	REQUIRE(deduplicator.Register(sliced, 0, first_seen) == 1);
	REQUIRE(first_seen.get_index(0) == 0);
}

TEST_CASE("Deduplicate a prefix of a row ID vector", "[row_id_deduplicator]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	RowIdDeduplicator deduplicator(context, {LogicalType::ROW_TYPE});

	Vector row_ids(LogicalType::ROW_TYPE);
	row_ids.Append(Value::BIGINT(10));
	row_ids.Append(Value::BIGINT(11));

	SelectionVector first_seen(1);
	REQUIRE(deduplicator.Register(row_ids, 1, first_seen) == 1);
	REQUIRE(first_seen.get_index(0) == 0);

	Vector last_row_id(LogicalType::ROW_TYPE);
	last_row_id.Append(Value::BIGINT(11));
	REQUIRE(deduplicator.Register(last_row_id, 1) == 1);
}

TEST_CASE("Preserve input order when row ID hashes collide", "[row_id_deduplicator]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	RowIdDeduplicator deduplicator(context, {LogicalType::ROW_TYPE});

	Vector row_ids(LogicalType::ROW_TYPE);
	// These values have the same initial hash-table bucket and salt.
	row_ids.Append(Value::BIGINT(2589199));
	row_ids.Append(Value::BIGINT(9044771));

	SelectionVector first_seen(2);
	REQUIRE(deduplicator.Register(row_ids, 2, first_seen) == 2);
	REQUIRE(first_seen.get_index(0) == 0);
	REQUIRE(first_seen.get_index(1) == 1);
}
