#include "duckdb/function/window/window_collection.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowCollection
//===--------------------------------------------------------------------===//
WindowCollection::WindowCollection(BufferManager &buffer_manager, idx_t count, const vector<LogicalType> &types)
    : all_valids(types.size()), types(types), count(count), buffer_manager(buffer_manager) {
	if (!types.empty()) {
		inputs = make_uniq<ColumnDataCollection>(buffer_manager, types);
	}

	validities.resize(types.size());

	// Atomic vectors can't be constructed with a given value
	for (auto &all_valid : all_valids) {
		all_valid = true;
	}
}

void WindowCollection::GetCollection(idx_t row_idx, ColumnDataCollectionSpec &spec) {
	if (spec.second && row_idx == spec.first + spec.second->Count()) {
		return;
	}

	lock_guard<mutex> collection_guard(lock);

	auto collection = make_uniq<ColumnDataCollection>(buffer_manager, types);
	spec = {row_idx, collection.get()};
	Range probe {row_idx, collections.size()};
	auto i = std::upper_bound(ranges.begin(), ranges.end(), probe);
	ranges.insert(i, probe);
	collections.emplace_back(std::move(collection));
}

void WindowCollection::Combine(const ColumnSet &validity_cols) {
	lock_guard<mutex> collection_guard(lock);

	// If there are no columns (COUNT(*)) then this is a NOP
	if (types.empty()) {
		return;
	}

	// Have we already combined?
	if (inputs->Count()) {
		D_ASSERT(collections.empty());
		D_ASSERT(ranges.empty());
		return;
	}

	// If there are columns, we should have data
	D_ASSERT(!collections.empty());
	D_ASSERT(!ranges.empty());

	for (auto &range : ranges) {
		inputs->Combine(*collections[range.second]);
	}
	collections.clear();
	ranges.clear();

	if (validity_cols.empty()) {
		return;
	}

	D_ASSERT(inputs.get());

	//	Find all columns with NULLs
	vector<column_t> invalid_cols;
	for (auto &col_idx : validity_cols) {
		if (!all_valids[col_idx]) {
			invalid_cols.emplace_back(col_idx);
			validities[col_idx].Initialize(inputs->Count());
		}
	}

	if (invalid_cols.empty()) {
		return;
	}

	WindowCursor cursor(*this, invalid_cols);
	idx_t target_offset = 0;
	while (cursor.Scan()) {
		const auto count = cursor.chunk.size();
		for (idx_t i = 0; i < invalid_cols.size(); ++i) {
			auto &other = FlatVector::Validity(cursor.chunk.data[i]);
			const auto col_idx = invalid_cols[i];
			validities[col_idx].SliceInPlace(other, target_offset, 0, count);
		}
		target_offset += count;
	}
}

WindowBuilder::WindowBuilder(WindowCollection &collection) : collection(collection) {
}

void WindowBuilder::Sink(DataChunk &chunk, idx_t input_idx) {
	// Check whether we need a a new collection
	if (!sink.second || input_idx < sink.first || sink.first + sink.second->Count() < input_idx) {
		collection.GetCollection(input_idx, sink);
		D_ASSERT(sink.second);
		sink.second->InitializeAppend(appender);
	}
	sink.second->Append(appender, chunk);

	// Record NULLs
	for (column_t col_idx = 0; col_idx < chunk.ColumnCount(); ++col_idx) {
		if (!collection.all_valids[col_idx]) {
			continue;
		}

		// Column was valid, make sure it still is.
		UnifiedVectorFormat data;
		chunk.data[col_idx].ToUnifiedFormat(chunk.size(), data);
		if (!data.validity.AllValid()) {
			collection.all_valids[col_idx] = false;
		}
	}
}

WindowCursor::WindowCursor(const WindowCollection &paged, vector<column_t> column_ids) : paged(paged) {
	D_ASSERT(paged.collections.empty());
	D_ASSERT(paged.ranges.empty());
	if (column_ids.empty()) {
		//	For things like COUNT(*) set the state up to contain the whole range
		state.segment_index = 0;
		state.chunk_index = 0;
		state.current_row_index = 0;
		state.next_row_index = paged.size();
		state.properties = ColumnDataScanProperties::ALLOW_ZERO_COPY;
		chunk.SetCapacity(state.next_row_index);
		chunk.SetCardinality(state.next_row_index);
		return;
	} else if (chunk.data.empty()) {
		auto &inputs = paged.inputs;
		D_ASSERT(inputs.get());
		inputs->InitializeScan(state, std::move(column_ids));
		inputs->InitializeScanChunk(state, chunk);
	}
}

WindowCursor::WindowCursor(const WindowCollection &paged, column_t col_idx)
    : WindowCursor(paged, vector<column_t>(1, col_idx)) {
}

} // namespace duckdb
