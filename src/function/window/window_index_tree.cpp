#include "duckdb/function/window/window_index_tree.hpp"
#include "duckdb/function/window/window_collection.hpp"

#include <utility>

namespace duckdb {

WindowIndexTree::WindowIndexTree(ClientContext &context, const vector<BoundOrderByNode> &orders,
                                 const vector<column_t> &sort_idx, const idx_t count)
    : WindowMergeSortTree(context, orders, sort_idx, count) {
}

WindowIndexTree::WindowIndexTree(ClientContext &context, const BoundOrderModifier &order_bys,
                                 const vector<column_t> &sort_idx, const idx_t count)
    : WindowIndexTree(context, order_bys.orders, sort_idx, count) {
}

unique_ptr<LocalSinkState> WindowIndexTree::GetLocalState(ExecutionContext &context) {
	return make_uniq<WindowIndexTreeLocalState>(context, *this);
}

WindowIndexTreeLocalState::WindowIndexTreeLocalState(ExecutionContext &context, WindowIndexTree &index_tree)
    : WindowMergeSortTreeLocalState(context, index_tree), index_tree(index_tree) {
}

void WindowIndexTreeLocalState::BuildLeaves() {
	auto &collection = *window_tree.sorted;
	if (!collection.Count()) {
		return;
	}

	// Find our chunk range
	const auto block_begin = (build_task * collection.ChunkCount()) / window_tree.total_tasks;
	const auto block_end = ((build_task + 1) * collection.ChunkCount()) / window_tree.total_tasks;

	//	Scan the index column (the last one)
	vector<column_t> index_ids(1, window_tree.scan_cols.size() - 1);
	WindowCollectionChunkScanner scanner(collection, index_ids, block_begin);
	auto &payload_chunk = scanner.chunk;

	idx_t row_idx = scanner.Scanned();
	for (auto block_curr = block_begin; block_curr < block_end; ++block_curr) {
		if (!scanner.Scan()) {
			break;
		}
		const auto count = payload_chunk.size();
		if (count == 0) {
			break;
		}
		auto &indices = payload_chunk.data[0];
		if (window_tree.mst32) {
			auto &sorted = window_tree.mst32->LowestLevel();
			auto data = FlatVector::GetData<int32_t>(indices);
			std::copy(data, data + count, sorted.data() + row_idx);
		} else {
			auto &sorted = window_tree.mst64->LowestLevel();
			auto data = FlatVector::GetData<int64_t>(indices);
			std::copy(data, data + count, sorted.data() + row_idx);
		}
		row_idx += count;
	}
}

pair<idx_t, idx_t> WindowIndexTree::SelectNth(const SubFrames &frames, idx_t n) const {
	if (mst32) {
		const auto nth = mst32->SelectNth(frames, n);
		if (nth.second) {
			return nth;
		} else {
			return {mst32->NthElement(nth.first), 0};
		}
	} else {
		const auto nth = mst64->SelectNth(frames, n);
		if (nth.second) {
			return nth;
		} else {
			return {mst64->NthElement(nth.first), 0};
		}
	}
}

} // namespace duckdb
