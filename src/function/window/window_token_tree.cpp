#include "duckdb/function/window/window_token_tree.hpp"

namespace duckdb {

class WindowTokenTreeLocalState : public WindowMergeSortTreeLocalState {
public:
	explicit WindowTokenTreeLocalState(WindowTokenTree &token_tree)
	    : WindowMergeSortTreeLocalState(token_tree), token_tree(token_tree) {
	}
	//! Process sorted leaf data
	void BuildLeaves() override;

	WindowTokenTree &token_tree;
};

void WindowTokenTreeLocalState::BuildLeaves() {
	auto &global_sort = *token_tree.global_sort;
	if (global_sort.sorted_blocks.empty()) {
		return;
	}

	//	Scan the sort keys and note deltas
	SBIterator curr(global_sort, ExpressionType::COMPARE_LESSTHAN);
	SBIterator prev(global_sort, ExpressionType::COMPARE_LESSTHAN);
	const auto &sort_layout = global_sort.sort_layout;

	const auto block_begin = token_tree.block_starts.at(build_task);
	const auto block_end = token_tree.block_starts.at(build_task + 1);
	auto &deltas = token_tree.deltas;
	if (!block_begin) {
		// First block, so set up initial delta
		deltas[0] = 0;
	} else {
		// Move to the to end of the previous block
		// so we can record the comparison result for the first row
		curr.SetIndex(block_begin - 1);
		prev.SetIndex(block_begin - 1);
	}

	for (++curr; curr.GetIndex() < block_end; ++curr, ++prev) {
		int lt = 0;
		if (sort_layout.all_constant) {
			lt = FastMemcmp(prev.entry_ptr, curr.entry_ptr, sort_layout.comparison_size);
		} else {
			lt = Comparators::CompareTuple(prev.scan, curr.scan, prev.entry_ptr, curr.entry_ptr, sort_layout,
			                               prev.external);
		}

		deltas[curr.GetIndex()] = (lt != 0);
	}
}

idx_t WindowTokenTree::MeasurePayloadBlocks() {
	const auto count = WindowMergeSortTree::MeasurePayloadBlocks();

	deltas.resize(count);

	return count;
}

template <typename T>
static void BuildTokens(WindowTokenTree &token_tree, vector<T> &tokens) {
	PayloadScanner scanner(*token_tree.global_sort);
	DataChunk payload_chunk;
	payload_chunk.Initialize(token_tree.context, token_tree.global_sort->payload_layout.GetTypes());
	const T *row_idx = nullptr;
	idx_t i = 0;

	T token = 0;
	for (auto &d : token_tree.deltas) {
		if (i >= payload_chunk.size()) {
			payload_chunk.Reset();
			scanner.Scan(payload_chunk);
			if (!payload_chunk.size()) {
				break;
			}
			row_idx = FlatVector::GetData<T>(payload_chunk.data[0]);
			i = 0;
		}

		token += d;
		tokens[row_idx[i++]] = token;
	}
}

unique_ptr<WindowAggregatorState> WindowTokenTree::GetLocalState() {
	return make_uniq<WindowTokenTreeLocalState>(*this);
}

void WindowTokenTree::CleanupSort() {
	//	Convert the deltas to tokens
	if (mst64) {
		BuildTokens(*this, mst64->LowestLevel());
	} else {
		BuildTokens(*this, mst32->LowestLevel());
	}

	// Deallocate memory
	vector<uint8_t> empty;
	deltas.swap(empty);

	WindowMergeSortTree::CleanupSort();
}

template <typename TREE>
static idx_t TokenRank(const TREE &tree, const idx_t lower, const idx_t upper, const idx_t row_idx) {
	idx_t rank = 1;
	const auto needle = tree.LowestLevel()[row_idx];
	tree.AggregateLowerBound(lower, upper, needle, [&](idx_t level, const idx_t run_begin, const idx_t run_pos) {
		rank += run_pos - run_begin;
	});
	return rank;
}

idx_t WindowTokenTree::Rank(const idx_t lower, const idx_t upper, const idx_t row_idx) const {
	if (mst64) {
		return TokenRank(*mst64, lower, upper, row_idx);
	} else {
		return TokenRank(*mst32, lower, upper, row_idx);
	}
}

template <typename TREE>
static idx_t NextPeer(const TREE &tree, const idx_t lower, const idx_t upper, const idx_t row_idx) {
	// We return an index, not a relative position
	idx_t idx = lower;
	// Because tokens are dense, we can find the next peer by adding 1 to the probed token value
	const auto needle = tree.LowestLevel()[row_idx] + 1;
	tree.AggregateLowerBound(lower, upper, needle, [&](idx_t level, const idx_t run_begin, const idx_t run_pos) {
		idx += run_pos - run_begin;
	});
	return idx;
}

idx_t WindowTokenTree::PeerEnd(const idx_t lower, const idx_t upper, const idx_t row_idx) const {
	if (mst64) {
		return NextPeer(*mst64, lower, upper, row_idx);
	} else {
		return NextPeer(*mst32, lower, upper, row_idx);
	}
}

} // namespace duckdb
