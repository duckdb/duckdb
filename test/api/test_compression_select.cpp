#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/compression/dict_fsst/decompression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"

using namespace duckdb;

TEST_CASE("Sparse DICT_FSST filtering retains strings from an already decoded dictionary",
          "[storage][compression][dict_fsst]") {
	if (STANDARD_VECTOR_SIZE < 128) {
		return;
	}
	auto path = TestCreatePath("dict_fsst_filter_ownership.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET force_compression='dict_fsst'"));
	const string suffix = "/special requests packages regular accounts";
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings AS SELECT 'key=' || lpad((i // 2)::VARCHAR, 6, '0') || "
	                          "repeat('" +
	                          suffix + "', 4) AS v FROM range(16387) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	con.BeginTransaction();
	auto &table =
	    Catalog::GetEntry<TableCatalogEntry>(*con.context, QualifiedName(INVALID_CATALOG, DEFAULT_SCHEMA, "strings"))
	        .Cast<DuckTableEntry>();
	auto &column = table.GetStorage().GetRowGroupCollection()->GetRowGroup(0)->GetRawColumnData(0);
	auto node = column.GetSegmentTree().GetRootSegment();
	REQUIRE(node);
	auto &segment = node->GetNode();
	const idx_t vector_count = STANDARD_VECTOR_SIZE;
	REQUIRE(segment.count >= vector_count + 1);
	Vector result(LogicalType::VARCHAR);
	SelectionVector selection(vector_count);
	idx_t selected_count = 0;
	for (idx_t row = 1; row < vector_count; row += 128) {
		selection.set_index(selected_count++, row);
	}
	const auto input_count = selected_count;
	{
		ColumnScanState state(nullptr);
		state.current = node;
		state.offset_in_column = node->GetRowStart();
		segment.InitializeScan(state);
		auto &scan = state.scan_state->Cast<dict_fsst::CompressedStringScanState>();
		REQUIRE(scan.layout.mode == dict_fsst::DictFSSTMode::DICT_FSST);
		REQUIRE(scan.layout.dict_count >= 512);
		REQUIRE(idx_t(scan.layout.dict_count) * 8 >= segment.count);
		// A partial scan can prepare the dictionary without preparing a filter cache.
		{
			Vector prefix(LogicalType::VARCHAR);
			segment.Scan(state, 1, prefix, 0, ScanVectorType::SCAN_FLAT_VECTOR);
		}
		REQUIRE(scan.dictionary);
		REQUIRE_FALSE(scan.filter_cache);
		state.offset_in_column++;
		auto filter =
		    ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHANOREQUALTO, Value("key="));
		auto filter_state = TableFilterState::Initialize(*con.context, *filter);
		segment.Filter(state, vector_count, result, selection, selected_count, *filter, *filter_state);
		REQUIRE(selected_count == input_count);
		// Check ownership before freeing the scan state, so a failure never reads dangling strings.
		auto &dictionary_heap = scan.dictionary->data.GetBufferRef()->GetAuxiliaryData();
		REQUIRE(dictionary_heap);
		REQUIRE((scan.dictionary.use_count() > 1 || dictionary_heap.use_count() > 1));
	}
	for (idx_t i = 0; i < selected_count; i++) {
		const auto row = 1 + i * 128;
		REQUIRE(selection.get_index(i) == row);
		const auto key = std::to_string((row + 1) / 2);
		const auto expected = "key=" + string(6 - key.size(), '0') + key + suffix + suffix + suffix + suffix;
		REQUIRE(result.GetValue(row) == Value(expected));
	}
	con.Commit();
}

static unique_ptr<ColumnSegment> MakeRLETestSegment(DuckDB &db, const CompressionFunction &function,
                                                    const vector<uint16_t> &counts, idx_t row_count) {
	auto &buffers = BufferManager::GetBufferManager(*db.instance);
	auto &blocks = buffers.GetTemporaryBlockManager();
	auto segment = ColumnSegment::CreateTransientSegment(*db.instance, function, LogicalType::INTEGER,
	                                                     blocks.GetBlockSize(), blocks);
	const auto count_offset = sizeof(uint64_t) + counts.size() * sizeof(int32_t);
	const auto byte_size = count_offset + counts.size() * sizeof(uint16_t);
	segment->count = row_count;
	// Bound the reader to the encoded bytes, excluding allocator padding.
	segment->SetByteSize(NumericCast<uint32_t>(byte_size));
	auto handle = buffers.Pin(segment->GetBlockHandle());
	auto data = handle.GetDataMutable();
	Store<uint64_t>(count_offset, data);
	for (idx_t i = 0; i < counts.size(); i++) {
		Store<int32_t>(NumericCast<int32_t>(i + 1), data + sizeof(uint64_t) + i * sizeof(int32_t));
		Store<uint16_t>(counts[i], data + count_offset + i * sizeof(uint16_t));
	}
	return segment;
}

TEST_CASE("RLE validation covers batch boundaries before decoding", "[storage][compression][rle]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto function = RLEFun::GetFunction(PhysicalType::INT32);
	const auto entries = GENERATE(31, 32, 33, 64, 65);
	const auto zeros = GENERATE(false, true);
	vector<uint16_t> counts(entries, 1);
	if (zeros) {
		counts[0] = 0;
		counts[entries - 1] = 0;
		if (entries > 32) {
			counts[31] = counts[32] = 0;
		}
	}
	vector<int32_t> expected;
	for (idx_t i = 0; i < counts.size(); i++) {
		if (counts[i]) {
			expected.push_back(NumericCast<int32_t>(i + 1));
		}
	}
	// Bytes after the last logical row are not another run to validate.
	counts.push_back(0);
	counts.push_back(65535);
	const auto row_count = expected.size();
	auto segment = MakeRLETestSegment(db, function, counts, row_count);
	ColumnScanState state(nullptr);
	segment->InitializeScan(state);
	Vector result(LogicalType::INTEGER, row_count + 3);
	auto check = [&](idx_t offset, idx_t count, idx_t expected_offset) {
		auto values = result.Values<int32_t>();
		for (idx_t i = 0; i < count; i++) {
			REQUIRE(values[offset + i].GetValue() == expected[expected_offset + i]);
		}
	};

	SECTION("scan") {
		segment->Scan(state, row_count, result, 0, ScanVectorType::SCAN_ENTIRE_VECTOR);
		check(0, row_count, 0);
	}
	SECTION("partial scans reuse the validation frontier") {
		const auto first = MinValue<idx_t>(31, row_count - 1);
		segment->Scan(state, first, result, 0, ScanVectorType::SCAN_FLAT_VECTOR);
		state.offset_in_column = first;
		segment->Scan(state, row_count - first, result, first, ScanVectorType::SCAN_FLAT_VECTOR);
		check(0, row_count, 0);
	}
	SECTION("selection advances over the unselected tail") {
		SelectionVector selection(1);
		selection.set_index(0, 0);
		segment->Select(state, row_count - 1, result, selection, 1, 0, ScanVectorType::SCAN_FLAT_VECTOR);
		check(0, 1, 0);
		state.offset_in_column = row_count - 1;
		segment->Scan(state, 1, result, 1, ScanVectorType::SCAN_FLAT_VECTOR);
		check(1, 1, row_count - 1);
	}
	SECTION("skip then scan") {
		state.offset_in_column = row_count - 1;
		segment->Skip(state);
		segment->Scan(state, 1, result, 0, ScanVectorType::SCAN_ENTIRE_VECTOR);
		check(0, 1, row_count - 1);
	}
	SECTION("filter validates the complete dictionary") {
		auto filter = ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(0));
		auto filter_state = TableFilterState::Initialize(*con.context, *filter);
		FlatVector::SetSize(result, count_t(row_count));
		SelectionVector selection;
		idx_t selected_count = row_count;
		segment->Filter(state, row_count, result, selection, selected_count, *filter, *filter_state);
		REQUIRE(selected_count == row_count);
		check(0, row_count, 0);
	}
}

TEST_CASE("RLE filtering advances past a partially selected batch", "[storage][compression][rle]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto function = RLEFun::GetFunction(PhysicalType::INT32);
	vector<uint16_t> counts {0, 1, 9, 0, 1, 9, 0};
	auto segment = MakeRLETestSegment(db, function, counts, 20);
	ColumnScanState state(nullptr);
	segment->InitializeScan(state);
	auto filter =
	    ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHANOREQUALTO, Value::INTEGER(3));
	const auto adaptive = GENERATE(false, true);
	if (adaptive) {
		filter = make_uniq<ExpressionFilter>(
		    CreateSelectivityOptionalFilterExpression(filter->expr->Copy(), LogicalType::INTEGER, 1.0f, 1));
	}
	auto filter_state = TableFilterState::Initialize(*con.context, *filter);
	REQUIRE(filter_state->can_cache);
	REQUIRE(filter_state->Cast<ExpressionFilterState>().can_cache_result == !adaptive);
	const idx_t prefix = GENERATE(0, 1, 5, 10);
	const auto incoming_selection = GENERATE(false, true);
	Vector result(LogicalType::INTEGER, 10);
	SelectionVector selection;
	if (incoming_selection) {
		selection.Initialize(10);
		for (idx_t row = 0; row < prefix; row++) {
			selection.set_index(row, row);
		}
	}
	idx_t selected_count = prefix;
	segment->Filter(state, 10, result, selection, selected_count, *filter, *filter_state);
	REQUIRE(selected_count == (prefix == 0 ? 0 : prefix - 1));
	{
		auto values = result.Values<int32_t>();
		for (idx_t row = 0; row < selected_count; row++) {
			REQUIRE(selection.get_index(row) == row + 1);
			REQUIRE(values[selection.get_index(row)].GetValue() == 3);
		}
	}
	state.offset_in_column = 10;
	segment->Scan(state, 10, result, 0, ScanVectorType::SCAN_FLAT_VECTOR);
	auto values = result.Values<int32_t>();
	for (idx_t row = 0; row < 10; row++) {
		REQUIRE(values[row].GetValue() == (row == 0 ? 5 : 6));
	}
}

TEST_CASE("RLE filtering preserves selections retained by dictionary results",
          "[storage][compression][rle][buffer_ptr]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto function = RLEFun::GetFunction(PhysicalType::INT32);
	const idx_t batch_size = STANDARD_VECTOR_SIZE;
	const auto half_batch = NumericCast<uint16_t>(batch_size / 2);
	vector<uint16_t> counts {half_batch, half_batch, half_batch, half_batch};
	auto segment = MakeRLETestSegment(db, function, counts, 2 * batch_size);
	ColumnScanState state(nullptr);
	segment->InitializeScan(state);
	auto filter = ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(1));
	auto filter_state = TableFilterState::Initialize(*con.context, *filter);
	Vector retained(LogicalType::INTEGER);
	{
		Vector first(LogicalType::INTEGER, batch_size);
		FlatVector::SetSize(first, count_t(batch_size));
		SelectionVector selection;
		idx_t selected_count = batch_size;
		segment->Filter(state, batch_size, first, selection, selected_count, *filter, *filter_state);
		REQUIRE(selected_count == half_batch);
		// Retain the filter's selection as a dictionary result, just like a row-group scan.
		retained.Slice(first, selection, selected_count);
	}
	auto &retained_selection = DictionaryVector::SelVector(retained);
	REQUIRE(retained_selection.Capacity() >= batch_size);
	// Only the scan state and the dictionary result still own this selection.
	REQUIRE(retained_selection.sel_data().use_count() == 2);
	auto check_retained = [&]() {
		REQUIRE(retained.size() == half_batch);
		for (idx_t row = 0; row < half_batch; row++) {
			REQUIRE(retained_selection.get_index(row) == half_batch + row);
			REQUIRE(retained.GetValue(row) == Value::INTEGER(2));
		}
	};
	check_retained();

	state.offset_in_column = batch_size;
	Vector second(LogicalType::INTEGER, batch_size);
	FlatVector::SetSize(second, count_t(batch_size));
	SelectionVector selection;
	idx_t selected_count = batch_size;
	segment->Filter(state, batch_size, second, selection, selected_count, *filter, *filter_state);
	REQUIRE(selected_count == batch_size);
	for (idx_t row = 0; row < batch_size; row++) {
		REQUIRE(second.GetValue(row) == Value::INTEGER(row < half_batch ? 3 : 4));
	}
	// Capacity was sufficient: the scan replaced its selection because it was shared.
	REQUIRE(retained_selection.sel_data().use_count() == 1);
	check_retained();
}

TEST_CASE("Malformed RLE batches fail before unchecked run access", "[storage][compression][rle]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto function = RLEFun::GetFunction(PhysicalType::INT32);
	const auto entries = GENERATE(31, 32, 33, 64, 65);
	const auto corruption = GENERATE(0, 1, 2);
	vector<uint16_t> counts(entries, 1);
	idx_t row_count = entries + 1;
	if (corruption == 1) {
		// A nonnegative batch cannot hide a run exceeding the logical remainder.
		counts[MinValue<idx_t>(31, entries - 1)] = 65535;
	} else if (corruption == 2) {
		std::fill(counts.begin(), counts.end(), 0);
	}
	auto segment = MakeRLETestSegment(db, function, counts, row_count);
	ColumnScanState state(nullptr);
	segment->InitializeScan(state);
	Vector result(LogicalType::INTEGER, row_count);

	SECTION("scan") {
		REQUIRE_THROWS_WITH(segment->Scan(state, row_count, result, 0, ScanVectorType::SCAN_ENTIRE_VECTOR),
		                    Catch::Contains("Corrupted RLE segment:"));
	}
	SECTION("partial scan") {
		REQUIRE_THROWS_WITH(segment->Scan(state, row_count, result, 0, ScanVectorType::SCAN_FLAT_VECTOR),
		                    Catch::Contains("Corrupted RLE segment:"));
	}
	SECTION("selection validates its unselected tail") {
		SelectionVector selection(1);
		selection.set_index(0, 0);
		REQUIRE_THROWS_WITH(
		    segment->Select(state, row_count, result, selection, 1, 0, ScanVectorType::SCAN_FLAT_VECTOR),
		    Catch::Contains("Corrupted RLE segment:"));
	}
	SECTION("skip") {
		state.offset_in_column = row_count;
		REQUIRE_THROWS_WITH(segment->Skip(state), Catch::Contains("Corrupted RLE segment:"));
	}
	SECTION("filter validates beyond the requested vector") {
		auto filter = ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(0));
		auto filter_state = TableFilterState::Initialize(*con.context, *filter);
		SelectionVector selection;
		idx_t selected_count = 1;
		REQUIRE_THROWS_WITH(segment->Filter(state, 1, result, selection, selected_count, *filter, *filter_state),
		                    Catch::Contains("Corrupted RLE segment:"));
	}
}

TEST_CASE("Constant selection preserves the surrounding flat result", "[storage][compression]") {
	DuckDB db(nullptr);
	Vector result(LogicalType::INTEGER, 8);
	{
		auto writer = FlatVector::Writer<int32_t>(result, 8);
		for (int32_t i = 0; i < 8; i++) {
			writer.WriteValue(i);
		}
	}
	SelectionVector selection(2);
	selection.set_index(0, 1);
	selection.set_index(1, 6);
	ColumnScanState state(nullptr);

	SECTION("values") {
		auto function = ConstantFun::GetFunction(PhysicalType::INT32);
		auto stats = NumericStats::CreateEmpty(LogicalType::INTEGER);
		NumericStats::SetMin(stats, Value::INTEGER(42));
		NumericStats::SetMax(stats, Value::INTEGER(42));
		stats.SetHasNoNull();
		ColumnSegment segment(*db.instance, nullptr, ColumnSegmentType::PERSISTENT, 8, function, std::move(stats),
		                      INVALID_BLOCK, 0, 0);
		segment.Select(state, 8, result, selection, 2, 3, ScanVectorType::SCAN_FLAT_VECTOR);
		REQUIRE(result.GetVectorType() == VectorType::FLAT_VECTOR);
		auto values = result.Values<int32_t>();
		REQUIRE(values.size() == 8);
		for (idx_t i = 0; i < values.size(); i++) {
			REQUIRE(values[i].IsValid());
			REQUIRE(values[i].GetValue() == (i == 3 || i == 4 ? 42 : int32_t(i)));
		}
	}

	SECTION("validity") {
		auto function = ConstantFun::GetFunction(PhysicalType::BIT);
		auto stats = BaseStatistics::CreateEmpty(LogicalTypeId::VALIDITY);
		stats.SetHasNull();
		ColumnSegment segment(*db.instance, nullptr, ColumnSegmentType::PERSISTENT, 8, function, std::move(stats),
		                      INVALID_BLOCK, 0, 0);
		segment.Select(state, 8, result, selection, 2, 3, ScanVectorType::SCAN_FLAT_VECTOR);
		REQUIRE(result.GetVectorType() == VectorType::FLAT_VECTOR);
		auto values = result.Values<int32_t>();
		REQUIRE(values.size() == 8);
		for (idx_t i = 0; i < values.size(); i++) {
			REQUIRE(values[i].IsValid() == (i != 3 && i != 4));
			if (values[i].IsValid()) {
				REQUIRE(values[i].GetValue() == int32_t(i));
			}
		}
	}
}
