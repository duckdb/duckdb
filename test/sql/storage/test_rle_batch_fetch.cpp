#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/transaction_data.hpp"

using namespace duckdb;

TEST_CASE("RLE batch fetch preserves arbitrary offsets and output boundaries", "[storage][rle]") {
	auto config = GetTestConfig();
	auto path = TestCreatePath("rle_batch_offsets.db");
	DeleteDatabase(path);
	{
		DuckDB db(path, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(v BIGINT USING COMPRESSION rle)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t SELECT i//4 FROM range(20000) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		REQUIRE_NO_FAIL(con.Query("BEGIN"));
		auto &context = *con.context;
		auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName("t")).Cast<DuckTableEntry>();
		auto groups = table.GetStorage().GetRowGroupCollection()->GetRowGroups();
		auto group = groups->GetRootSegment();
		REQUIRE(group);
		auto &column = group->GetNode().GetRawColumnData(storage_t(0));
		auto node = column.GetSegmentTree().GetRootSegment();
		REQUIRE(node);
		auto &segment = node->GetNode();
		REQUIRE(segment.GetCompressionFunction().type == CompressionType::COMPRESSION_RLE);
		REQUIRE(segment.count > STANDARD_VECTOR_SIZE + 1);
		ColumnFetchState state;
		state.context = context;

		// Keep sentinel values around each output range to detect incorrect result offsets and overruns.
		vector<row_t> ids {0, 5, 5, 2, 9, row_t(segment.count - 1), 12, 8, 4, 0, 0};
		Vector actual(LogicalType::BIGINT, ids.size() + 2);
		Vector expected(LogicalType::BIGINT, ids.size() + 2);
		auto output = FlatVector::GetDataMutable<int64_t>(actual);
		for (idx_t i = 0; i < ids.size() + 2; i++) {
			output[i] = -1;
		}
		segment.FetchRows(state, unsafe_array_ptr<row_t>(ids.data(), ids.size()), ids.size(), actual, 1);
		for (idx_t i = 0; i < ids.size(); i++) {
			segment.FetchRow(state, ids[i], expected, i + 1);
			REQUIRE(actual.GetValue(i + 1) == expected.GetValue(i + 1));
		}
		REQUIRE(output[0] == -1);
		REQUIRE(output[ids.size() + 1] == -1);
		segment.FetchRows(state, unsafe_array_ptr<row_t>(ids.data(), 0), 0, actual, 1);
		REQUIRE(output[0] == -1);
		row_t invalid = -1;
		REQUIRE_THROWS(segment.FetchRows(state, unsafe_array_ptr<row_t>(invalid), 1, actual, 1));
		invalid = row_t(segment.count);
		REQUIRE_THROWS(segment.FetchRows(state, unsafe_array_ptr<row_t>(invalid), 1, actual, 1));

		// Repeated fetches at a run boundary must not consume the current row.
		vector<row_t> boundary_ids {3, 3, 4};
		Vector boundary_result(LogicalType::BIGINT, boundary_ids.size());
		segment.FetchRows(state, unsafe_array_ptr<row_t>(boundary_ids.data(), boundary_ids.size()), boundary_ids.size(),
		                  boundary_result, 0);
		REQUIRE(boundary_result.GetValue(0) == Value::BIGINT(0));
		REQUIRE(boundary_result.GetValue(1) == Value::BIGINT(0));
		REQUIRE(boundary_result.GetValue(2) == Value::BIGINT(1));

		// More than one vector of requests exercises ColumnData's bounded batching and non-identity selection.
		const idx_t count = STANDARD_VECTOR_SIZE + 1;
		vector<idx_t> offsets(count);
		SelectionVector selection(count);
		for (idx_t i = 0; i < count; i++) {
			offsets[i] = i;
			selection.set_index(i, count - i - 1);
		}
		Vector batched(LogicalType::BIGINT, count + 2);
		auto data = FlatVector::GetDataMutable<int64_t>(batched);
		data[0] = data[count + 1] = -1;
		auto &transaction = DuckTransaction::Get(context, table.catalog);
		column.FetchRowsAtSegmentLevel(transaction, state, offsets.data(), selection, count, batched, 1);
		for (idx_t i = 0; i < count; i++) {
			REQUIRE(data[i + 1] == int64_t((count - i - 1) / 4));
		}
		REQUIRE(data[0] == -1);
		REQUIRE(data[count + 1] == -1);
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
	}
	DeleteDatabase(path);
}
