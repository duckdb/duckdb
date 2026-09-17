#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/optimistic_data_writer.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/transaction/transaction_data.hpp"

using namespace duckdb;

// Regression test for the row-group-append corruption behind duckdb/duckdb#22823
// (deterministic bitpacking/RLE segment corruption during CHECKPOINT on 1.5.2-1.5.4).
//
// Root cause: after the optimistic writer flushed row groups to disk
// (OptimisticDataWriter::WriteUnflushedRowGroups / WriteNewRowGroup -> FlushToDisk ->
// RowGroupCollection::SetRowGroup replaces the shared_ptr<RowGroup> in the segment
// tree), subsequent appends could still land in - or point at - the replaced row
// groups:
//  1. appends continued into an already-flushed row group (fixed by #22997,
//     RowGroupAppendMode::REQUIRE_NEW after WriteUnflushedRowGroups), and
//  2. RowGroupAppendState::row_group is a raw RowGroup*; flushing the row group that
//     is the current append target freed it, and the next Append() dereferenced the
//     dangling pointer (heap-use-after-free, fixed by #23103, which tracks the exact
//     flushed row groups instead of assuming only trailing row groups are unflushed).
// Both paths feed garbage into the compression state, which then writes segments that
// the same version fails to read back ("Invalid bitpacking mode", "Bitpacking offset
// is out of range", "Corrupted RLE segment: rle_count_offset is corrupted").
//
// This test drives the OptimisticDataWriter API directly into the pre-condition
// (all row groups of the collection flushed, append mode still allowing appends into
// the collection) and then keeps appending across further row-group boundaries while
// flushes happen. On the buggy code this is a heap-use-after-free that
// AddressSanitizer reports; on fixed code it must complete cleanly.
//
// Must run on a persistent (on-disk) database: OptimisticDataWriter::PrepareWrite()
// is a no-op for in-memory / temporary / read-only tables, so no flush (and no bug)
// occurs.

static void FillChunk(DataChunk &chunk, int64_t base) {
	auto writer = FlatVector::Writer<int64_t>(chunk.data[0], STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		writer.WriteValue(base + NumericCast<int64_t>(i));
	}
	chunk.SetCardinalityUnsafe(STANDARD_VECTOR_SIZE);
}

TEST_CASE("Appending after the optimistic writer flushed row groups must not corrupt storage",
          "[storage][rowgroup_append_flush]") {
	auto db_path = TestCreatePath("rowgroup_append_uaf.db");
	DeleteDatabase(db_path);

	DuckDB db(db_path);
	Connection con(db);
	// flush as soon as a single row group is buffered, so flushes happen mid-append
	REQUIRE_NO_FAIL(con.Query("SET write_buffer_row_group_count=1;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (a BIGINT);"));

	auto &context = *con.context;
	con.BeginTransaction();

	auto default_db = DatabaseManager::GetDefaultDatabase(context);
	auto &table_entry =
	    Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(default_db, Identifier::DefaultSchema(), "t"))
	        .Cast<DuckTableEntry>();
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

	TransactionData tdata(0, VisibilityBound::AllCommitted());

	// Phase 1: append exactly one full row group, then flush everything to disk.
	// After WriteUnflushedRowGroups every row group of the collection is persistent,
	// while the collection remains appendable.
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
	REQUIRE(coll->flushed_row_groups.size() == collection.GetRowGroupCount());

	// Phase 2: a fresh append into the same collection (mimicking a subsequent
	// PhysicalInsert::Combine re-append / CollectionMerger re-append). Keep appending
	// across two more row-group boundaries so WriteNewRowGroup flushes row groups
	// while the append is in flight. On buggy code the flush frees the row group the
	// append state points at, and the next Append dereferences the dangling pointer.
	{
		TableAppendState state;
		collection.InitializeAppend(tdata, state);
		for (idx_t c = 0; c < 2 * chunks_per_row_group + 1; c++) {
			FillChunk(chunk, NumericCast<int64_t>(c * STANDARD_VECTOR_SIZE));
			auto flushed_row_group_idx = collection.Append(chunk, state);
			if (flushed_row_group_idx.IsValid()) {
				writer->WriteNewRowGroup(*coll, flushed_row_group_idx.GetIndex());
			}
		}
		collection.FinalizeAppend(tdata, state);
	}
	writer->FinalFlush();

	// all rows from both phases must be accounted for
	REQUIRE(collection.GetTotalRows() == row_group_size + 2 * row_group_size + STANDARD_VECTOR_SIZE);

	con.Rollback();
}
