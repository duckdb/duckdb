#include "duckdb/function/window/window_index_tree.hpp"

#include <thread>
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

unique_ptr<WindowAggregatorState> WindowIndexTree::GetLocalState() {
	return make_uniq<WindowIndexTreeLocalState>(*this);
}

WindowIndexTreeLocalState::WindowIndexTreeLocalState(WindowIndexTree &index_tree)
    : WindowMergeSortTreeLocalState(index_tree), index_tree(index_tree) {
}

void WindowIndexTreeLocalState::BuildLeaves() {
	auto &global_sort = *index_tree.global_sort;
	if (global_sort.sorted_blocks.empty()) {
		return;
	}

	PayloadScanner scanner(global_sort, build_task);
	idx_t row_idx = index_tree.block_starts[build_task];
	for (;;) {
		payload_chunk.Reset();
		scanner.Scan(payload_chunk);
		const auto count = payload_chunk.size();
		if (count == 0) {
			break;
		}
		auto &indices = payload_chunk.data[0];
		if (index_tree.mst32) {
			auto &sorted = index_tree.mst32->LowestLevel();
			auto data = FlatVector::GetData<uint32_t>(indices);
			std::copy(data, data + count, sorted.data() + row_idx);
		} else {
			auto &sorted = index_tree.mst64->LowestLevel();
			auto data = FlatVector::GetData<uint64_t>(indices);
			std::copy(data, data + count, sorted.data() + row_idx);
		}
		row_idx += count;
	}
}

idx_t WindowIndexTree::SelectNth(const SubFrames &frames, idx_t n) const {
	if (mst32) {
		return mst32->NthElement(mst32->SelectNth(frames, n));
	} else {
		return mst64->NthElement(mst64->SelectNth(frames, n));
	}
}

} // namespace duckdb
