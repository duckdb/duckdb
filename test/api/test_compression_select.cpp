#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
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

TEST_CASE("Retained RLE dictionaries survive selection reuse and scan destruction", "[storage][compression][rle]") {
	DuckDB db(nullptr);
	auto function = RLEFun::GetFunction(PhysicalType::INT32);
	const auto nullable = GENERATE(false, true);
	Vector first(LogicalType::INTEGER, 28), second(LogicalType::INTEGER, 24);
	{
		auto segment = MakeRLETestSegment(db, function, {0, 13, 0, 15, 12, 0, 16}, 56);
		if (nullable) {
			segment->GetStatsMutable().SetHasNull();
		} else {
			segment->GetStatsMutable().SetHasNoNull();
		}
		ColumnScanState state(nullptr);
		segment->InitializeScan(state);
		segment->Scan(state, 28, first, 0, ScanVectorType::SCAN_ENTIRE_VECTOR);
		REQUIRE(first.GetVectorType() == VectorType::DICTIONARY_VECTOR);
		const auto id = DictionaryVector::DictionaryId(first);
		REQUIRE(!id.empty());
		const auto &child = DictionaryVector::Child(first);
		REQUIRE(FlatVector::Validity(child).RowIsValid(child.size() - 1) == !nullable);
		state.offset_in_column = 28;
		segment->Scan(state, 24, second, 0, ScanVectorType::SCAN_ENTIRE_VECTOR);
		REQUIRE(second.GetVectorType() == VectorType::DICTIONARY_VECTOR);
		REQUIRE(DictionaryVector::DictionaryId(second) == id);
	}
	for (idx_t i = 0; i < 28; i++) {
		REQUIRE(first.GetValue(i) == Value::INTEGER(i < 13 ? 2 : 4));
	}
	for (idx_t i = 0; i < 24; i++) {
		REQUIRE(second.GetValue(i) == Value::INTEGER(i < 12 ? 5 : 7));
	}
}

template <class T>
static unique_ptr<ColumnSegment> MakeRLEPlaceholderSegment(DuckDB &db, const CompressionFunction &function,
                                                           const LogicalType &type, idx_t run_length) {
	auto &buffers = BufferManager::GetBufferManager(*db.instance);
	auto &blocks = buffers.GetTemporaryBlockManager();
	auto segment = ColumnSegment::CreateTransientSegment(*db.instance, function, type, blocks.GetBlockSize(), blocks);
	const idx_t entries = 21;
	const idx_t count_offset = AlignValue(sizeof(uint64_t) + entries * sizeof(T));
	segment->count = (entries - 1) * run_length;
	segment->SetByteSize(NumericCast<uint32_t>(count_offset + entries * sizeof(uint16_t)));
	segment->GetStatsMutable().SetHasNoNull();
	auto handle = buffers.Pin(segment->GetBlockHandle());
	auto data = handle.GetDataMutable();
	Store<uint64_t>(count_offset, data);
	for (idx_t i = 0; i < entries; i++) {
		Store<T>(i ? T((i - 1) % 2) : NullValue<T>(), data + sizeof(uint64_t) + i * sizeof(T));
		Store<uint16_t>(NumericCast<uint16_t>(i ? run_length : 0), data + count_offset + i * sizeof(uint16_t));
	}
	return segment;
}

static void CheckRLEFilterValues(DataChunk &args, ExpressionState &, Vector &result) {
	auto writer = FlatVector::Writer<bool>(result, args.size());
	for (idx_t i = 0; i < args.size(); i++) {
		const auto value = args.data[0].GetValue(i);
		REQUIRE(value.IsValid());
		if (value.IsNull()) {
			writer.WriteNull();
		} else {
			writer.WriteValue(true);
		}
	}
}

template <class T>
static void CheckRLEPlaceholder(DuckDB &db, Connection &con, const LogicalType &type) {
	INFO(type.ToString());
	auto compression = RLEFun::GetFunction(type.InternalType());
	ScalarFunction function("check_rle_values", {}, LogicalType::BOOLEAN, CheckRLEFilterValues);
	function.GetSignature().AddParameter("value", type);
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(make_uniq<BoundReferenceExpression>(type, 0));
	ExpressionFilter filter(
	    make_uniq<BoundFunctionExpression>(BoundScalarFunction(function), std::move(arguments), nullptr));
	for (const idx_t run_length : {1, 37}) {
		// A zero-length legacy run is unused even when statistics exclude row NULLs.
		auto segment = MakeRLEPlaceholderSegment<T>(db, compression, type, run_length);
		if (run_length == 37) {
			ColumnScanState scan(nullptr);
			segment->InitializeScan(scan);
			Vector result(type, 128);
			segment->Scan(scan, 128, result, 0, ScanVectorType::SCAN_ENTIRE_VECTOR);
			REQUIRE(result.GetVectorType() == VectorType::DICTIONARY_VECTOR);
			const auto &child = DictionaryVector::Child(result);
			REQUIRE_FALSE(FlatVector::Validity(child).RowIsValid(0));
			for (idx_t i = 1; i < child.size(); i++) {
				if (type.id() == LogicalTypeId::BOOLEAN) {
					// Check raw bytes before GetValue can read them as bool.
					const auto byte = FlatVector::GetData<int8_t>(child)[i];
					REQUIRE((byte == 0 || byte == 1));
				}
				REQUIRE(child.GetValue(i).IsValid());
			}
		}
		if (!compression.filter) {
			continue;
		}
		auto filter_state = TableFilterState::Initialize(*con.context, filter);
		REQUIRE(filter_state->can_cache);
		REQUIRE_FALSE(filter_state->Cast<ExpressionFilterState>().fast_executor);
		ColumnScanState state(nullptr);
		segment->InitializeScan(state);
		const auto count = 20 * run_length;
		Vector result(type, count);
		FlatVector::SetSize(result, count_t(count));
		SelectionVector selection;
		auto selected_count = count;
		segment->Filter(state, count, result, selection, selected_count, filter, *filter_state);
		REQUIRE(selected_count == count);
		auto values = result.Values<T>();
		for (idx_t i = 0; i < selected_count; i++) {
			REQUIRE(selection.get_index(i) == i);
			REQUIRE(values[i].IsValid());
			REQUIRE(values[i].GetValue() == T((i / run_length) % 2));
		}
	}
}

TEST_CASE("RLE legacy placeholders remain safe in scans and filters", "[storage][compression][rle]") {
	DuckDB db(nullptr);
	Connection con(db);
	CheckRLEPlaceholder<int8_t>(db, con, LogicalType::BOOLEAN);
	CheckRLEPlaceholder<int16_t>(db, con, LogicalType::DECIMAL(4, 0));
	for (const auto &type : {LogicalType::TIME, LogicalType::TIME_NS, LogicalType::TIME_TZ}) {
		CheckRLEPlaceholder<int64_t>(db, con, type);
	}
}

TEST_CASE("Dictionary validity preserves ownership and dictionary entries", "[storage][compression][rle]") {
	SECTION("Shared and borrowed selections survive remapping") {
		const auto borrowed_selection = GENERATE(false, true);
		Vector result(LogicalType::INTEGER);
		{
			auto dictionary = DictionaryVector::CreateReusableDictionary(LogicalType::INTEGER, 3);
			dictionary->data.SetValue(0, Value::INTEGER(10));
			dictionary->data.SetValue(1, Value::INTEGER(20));
			dictionary->data.SetValue(2, Value(LogicalType::INTEGER));
			sel_t borrowed[3] = {0, 1, 2};
			SelectionVector indices(3);
			if (borrowed_selection) {
				indices.Initialize(borrowed, 3);
			}
			for (idx_t i = 0; i < 3; i++) {
				indices.set_index(i, i);
			}
			result.Dictionary(dictionary, indices, 3);
			ValidityMask validity(3);
			validity.SetInvalid(1);
			DictionaryVector::ApplyValidity(result, validity);
			REQUIRE(result.GetVectorType() == VectorType::DICTIONARY_VECTOR);
			REQUIRE(DictionaryVector::DictionaryId(result) == dictionary->id);
			REQUIRE(indices.get_index(1) == 1);
		}
		REQUIRE(result.GetValue(0) == Value::INTEGER(10));
		REQUIRE(result.GetValue(1).IsNull());
		REQUIRE(result.GetValue(2).IsNull());
	}

	SECTION("Reusing or extending a dictionary preserves the original entry and caches") {
		const auto null_index = GENERATE(0, 3);
		const auto value = Value("a string longer than the inline string storage");
		auto dictionary = DictionaryVector::CreateReusableGlobalDictionary(value.type(), 3);
		for (idx_t i = 0; i < 3; i++) {
			dictionary->data.SetValue(i, i == idx_t(null_index) ? Value(value.type()) : value);
		}
		SelectionVector indices(3);
		for (idx_t i = 0; i < 3; i++) {
			indices.set_index(i, i);
		}
		Vector result(value.type());
		result.Dictionary(dictionary, indices, 3);
		Vector original(value.type());
		original.Reference(result);
		REQUIRE(DictionaryVector::CanCacheHashes(result));
		const auto hashes = &DictionaryVector::GetCachedHashes(result);
		ValidityMask validity(3);
		validity.SetAllValid(3);
		DictionaryVector::ApplyValidity(result, validity);
		REQUIRE(result.GetBufferRef() == original.GetBufferRef());
		validity.SetInvalid(1);
		DictionaryVector::ApplyValidity(result, validity);
		REQUIRE(result.GetVectorType() == VectorType::DICTIONARY_VECTOR);
		result.Verify();
		if (null_index != 3) {
			REQUIRE(DictionaryVector::DictionaryId(result) == dictionary->id);
			REQUIRE(DictionaryVector::IsGlobalDictionary(result));
			REQUIRE(&DictionaryVector::GetCachedHashes(result) == hashes);
		} else {
			REQUIRE(DictionaryVector::Child(result).size() == 4);
			REQUIRE(DictionaryVector::DictionaryId(result).empty());
			REQUIRE_FALSE(DictionaryVector::IsGlobalDictionary(result));
		}
		REQUIRE(Value::NotDistinctFrom(result.GetValue(0), original.GetValue(0)));
		REQUIRE(result.GetValue(1).IsNull());
		REQUIRE(result.GetValue(2) == value);
		REQUIRE(original.GetValue(1) == value);
		REQUIRE(&DictionaryVector::GetCachedHashes(original) == hashes);
		REQUIRE(dictionary->data.size() == 3);
	}

	SECTION("An owned selection preserves the padding after a partial validity word") {
		auto dictionary = DictionaryVector::CreateReusableDictionary(LogicalType::INTEGER, 2);
		dictionary->data.SetValue(0, Value::INTEGER(42));
		dictionary->data.SetValue(1, Value(LogicalType::INTEGER));
		SelectionVector indices(128);
		for (idx_t i = 0; i < 128; i++) {
			indices.set_index(i, i < 65 ? 0 : 99);
		}
		const auto data = indices.data();
		Vector result(LogicalType::INTEGER);
		result.Dictionary(dictionary, indices, 65);
		indices = SelectionVector();
		ValidityMask validity(128);
		validity.SetAllInvalid(128);
		validity.SetValid(1);
		DictionaryVector::ApplyValidity(result, validity);
		REQUIRE(result.GetVectorType() == VectorType::DICTIONARY_VECTOR);
		REQUIRE(DictionaryVector::SelVector(result).data() == data);
		for (idx_t i = 0; i < 128; i++) {
			REQUIRE(data[i] == (i < 65 ? (i == 1 ? 0 : 1) : 99));
		}
	}
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
	result.Initialize();
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
		for (idx_t row = 0; row < selected_count; row++) {
			REQUIRE(first.GetValue(selection.get_index(row)) == Value::INTEGER(2));
		}
		// A flat companion column retains the filter's selection even when the filter emits a dictionary.
		Vector payload(LogicalType::INTEGER, batch_size);
		auto writer = FlatVector::Writer<int32_t>(payload, batch_size);
		for (idx_t row = 0; row < batch_size; row++) {
			writer.WriteValue(NumericCast<int32_t>(row));
		}
		retained.Slice(payload, selection, selected_count);
	}
	auto &retained_selection = DictionaryVector::SelVector(retained);
	REQUIRE(retained_selection.Capacity() >= batch_size);
	auto check_retained = [&]() {
		REQUIRE(retained.size() == half_batch);
		for (idx_t row = 0; row < half_batch; row++) {
			REQUIRE(retained_selection.get_index(row) == half_batch + row);
			REQUIRE(retained.GetValue(row) == Value::INTEGER(NumericCast<int32_t>(half_batch + row)));
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
