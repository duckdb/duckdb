#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/optimistic_data_writer.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/transaction/transaction_data.hpp"

using namespace duckdb;

// Regression test for exaforce#42 (heap-use-after-free in RowGroupAppendState).
//
// RowGroupAppendState::row_group is a raw RowGroup* (append_state.hpp). During an
// append, OptimisticDataWriter::FlushToDisk can replace the shared_ptr<RowGroup> in
// the segment tree (SetRowGroup -> SegmentNode::SetNode), freeing the old RowGroup.
// If that RowGroup is the *current append target*, the raw pointer dangles and the
// next Append() dereferences freed memory (row_group_collection.cpp GetAllocationSize).
//
// This test drives the OptimisticDataWriter API directly into the precise pre-condition
// (complete_row_groups == row_group_count, append mode APPEND_TO_EXISTING, last row group
// full) so that the *next* append creates a new row group whose index equals
// complete_row_groups -- which WriteNewRowGroup then adds to the flush set and flushes,
// replacing the active append target. On the buggy code this is a use-after-free that
// AddressSanitizer reports; with a correct fix (do not flush the active row group, or
// re-initialize the append state after a flush) it must complete cleanly.
//
// Must run on a persistent (on-disk) database: OptimisticDataWriter::PrepareWrite() is a
// no-op for in-memory / temporary / read-only tables, so no flush (and no bug) occurs.

static void FillChunk(DataChunk &chunk, int64_t base) {
	chunk.SetCardinality(STANDARD_VECTOR_SIZE);
	auto data = FlatVector::GetData<int64_t>(chunk.data[0]);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		data[i] = base + NumericCast<int64_t>(i);
	}
}

TEST_CASE("exaforce#42 - flushing the active append row group must not dangle the append state", "[storage][.]") {
	auto db_path = TestCreatePath("rowgroup_append_uaf.db");
	DeleteDatabase(db_path);

	DuckDB db(db_path);
	Connection con(db);
	// flush as soon as a single row group is buffered, so the flush hits the active row group
	REQUIRE_NO_FAIL(con.Query("SET write_buffer_row_group_count=1;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (a BIGINT);"));

	auto &context = *con.context;
	con.BeginTransaction();

	auto &table_entry =
	    Catalog::GetEntry<TableCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, "t").Cast<DuckTableEntry>();
	auto &data_table = table_entry.GetStorage();
	auto types = table_entry.GetTypes();

	auto writer = make_uniq<OptimisticDataWriter>(context, data_table);
	auto coll = writer->CreateCollection(data_table, types);
	auto &collection = *coll->collection;
	collection.InitializeEmpty();

	const idx_t row_group_size = collection.GetRowGroupSize();
	REQUIRE(row_group_size % STANDARD_VECTOR_SIZE == 0);
	const idx_t chunks_per_row_group = row_group_size / STANDARD_VECTOR_SIZE;

	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);

	TransactionData tdata(0, 0);

	// Phase 1: append exactly one full row group, then flush everything to disk.
	// After WriteUnflushedRowGroups, complete_row_groups == row_group_count (== 1) and
	// row group 0 is full + persistent, while the append mode stays APPEND_TO_EXISTING.
	{
		TableAppendState state;
		collection.InitializeAppend(tdata, state);
		for (idx_t c = 0; c < chunks_per_row_group; c++) {
			FillChunk(chunk, NumericCast<int64_t>(c * STANDARD_VECTOR_SIZE));
			collection.Append(chunk, state);
		}
		collection.FinalizeAppend(tdata, state);
	}
	writer->WriteUnflushedRowGroups(*coll);
	writer->FinalFlush();

	// Phase 2: a fresh append (mimicking a subsequent PhysicalInsert::Combine re-append).
	// The first chunk overflows the full last row group and creates a new row group whose
	// index equals complete_row_groups; WriteNewRowGroup inserts that index and flushes it,
	// replacing the very row group the append state still points at. The second chunk then
	// dereferences that (freed-on-buggy-code) pointer.
	{
		TableAppendState state;
		collection.InitializeAppend(tdata, state);

		FillChunk(chunk, 0);
		auto new_row_group_idx = collection.Append(chunk, state);
		if (new_row_group_idx.IsValid()) {
			writer->WriteNewRowGroup(*coll,
			                         new_row_group_idx.GetIndex()); // <-- on buggy code this frees the active row group
		}

		// On buggy code, this Append reads the dangling RowGroup* -> ASAN heap-use-after-free.
		FillChunk(chunk, NumericCast<int64_t>(STANDARD_VECTOR_SIZE));
		collection.Append(chunk, state);

		collection.FinalizeAppend(tdata, state);
	}
	writer->FinalFlush();

	con.Rollback();
	REQUIRE(true); // reaching here without an ASAN abort means the bug is fixed
}
