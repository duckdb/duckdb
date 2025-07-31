#include "duckdb/function/window/window_token_tree.hpp"

namespace duckdb {

class WindowTokenTreeLocalState : public WindowMergeSortTreeLocalState {
public:
	WindowTokenTreeLocalState(ExecutionContext &context, WindowTokenTree &token_tree)
	    : WindowMergeSortTreeLocalState(context, token_tree), token_tree(token_tree) {
	}
	//! Process sorted leaf data
	void BuildLeaves() override;

	WindowTokenTree &token_tree;
};

void WindowTokenTreeLocalState::BuildLeaves() {
	auto &collection = *token_tree.sorted;
	if (!collection.Count()) {
		return;
	}

	// Find our chunk range
	const auto block_begin = (build_task * collection.ChunkCount()) / token_tree.total_tasks;
	const auto block_end = ((build_task + 1) * collection.ChunkCount()) / token_tree.total_tasks;

	//	Start back one to get the overlap
	idx_t block_curr = block_begin ? block_begin - 1 : 0;

	//	Scan the sort columns
	WindowCollectionChunkScanner scanner(collection, token_tree.key_cols, block_curr);
	auto &scanned = scanner.chunk;

	//	Shifted buffer for the next values
	DataChunk next;
	collection.InitializeScanChunk(scanner.state, next);

	//	Delay buffer for the previous row
	DataChunk delayed;
	collection.InitializeScanChunk(scanner.state, delayed);

	//	Only compare the sort arguments.
	const auto compare_count = token_tree.key_cols.size();
	const auto compare_type = scanner.PrefixStructType(compare_count);
	Vector compare_curr(compare_type);
	Vector compare_prev(compare_type);

	auto &deltas = token_tree.deltas;
	bool boundary_compare = false;
	idx_t row_idx = 1;
	if (!block_begin) {
		// First block, so set up initial delta
		deltas[0] = 0;
	} else {
		// Move to the to end of the previous block
		// so we can record the comparison result for the first row
		boundary_compare = true;
	}
	if (!scanner.Scan()) {
		return;
	}

	//	Process chunks offset by 1
	SelectionVector next_sel(1, STANDARD_VECTOR_SIZE);
	SelectionVector distinct(STANDARD_VECTOR_SIZE);
	SelectionVector matching(STANDARD_VECTOR_SIZE);

	while (block_curr < block_end) {
		//	Compare the current to the previous;
		DataChunk *curr = nullptr;
		DataChunk *prev = nullptr;

		idx_t prev_count = 0;
		if (boundary_compare) {
			//	Save the last row of the scanned chunk
			prev_count = 1;
			sel_t last = UnsafeNumericCast<sel_t>(scanned.size() - 1);
			SelectionVector sel(&last);
			delayed.Reset();
			scanned.Copy(delayed, sel, prev_count);
			prev = &delayed;

			// Try to read the next chunk
			++block_curr;
			row_idx = scanner.Scanned();
			if (block_curr >= block_end || !scanner.Scan()) {
				break;
			}
			curr = &scanned;
		} else {
			//	Compare the [1..size) values with the [0..size-1) values
			prev_count = scanned.size() - 1;
			if (!prev_count) {
				//	1 row scanned, so just skip the rest of the loop.
				boundary_compare = true;
				continue;
			}
			prev = &scanned;

			// Slice the current back one into the previous
			next.Slice(scanned, next_sel, prev_count);
			curr = &next;
		}

		//	Reference the comparison prefix as a struct to simplify the compares.
		scanner.ReferenceStructColumns(*prev, compare_prev, compare_count);
		scanner.ReferenceStructColumns(*curr, compare_curr, compare_count);

		const auto n =
		    VectorOperations::DistinctFrom(compare_curr, compare_prev, nullptr, prev_count, &distinct, &matching);

		//	If n is 0, neither SV has been filled in?
		const auto remaining = prev_count - n;
		auto match_sel = n ? &matching : FlatVector::IncrementalSelectionVector();

		//	Same as previous - token delta is 0
		for (idx_t j = 0; j < remaining; ++j) {
			auto scan_idx = match_sel->get_index(j);
			deltas[scan_idx + row_idx] = 0;
		}

		//	Different value - token delta is 1
		for (idx_t j = 0; j < n; ++j) {
			auto scan_idx = distinct.get_index(j);
			deltas[scan_idx + row_idx] = 1;
		}

		//	Transition between comparison ranges.
		boundary_compare = !boundary_compare;
		row_idx += prev_count;
	}
}

idx_t WindowTokenTree::MeasurePayloadBlocks() {
	const auto count = WindowMergeSortTree::MeasurePayloadBlocks();

	deltas.resize(count);

	return count;
}

template <typename T>
static void BuildTokens(WindowTokenTree &token_tree, vector<T> &tokens) {
	auto &collection = *token_tree.sorted;
	if (!collection.Count()) {
		return;
	}
	//	Scan the index column
	vector<column_t> scan_ids(1, token_tree.scan_cols.size() - 1);
	WindowCollectionChunkScanner scanner(collection, scan_ids, 0);
	auto &payload_chunk = scanner.chunk;

	const T *row_idx = nullptr;
	idx_t i = 0;

	T token = 0;
	for (auto &d : token_tree.deltas) {
		if (i >= payload_chunk.size()) {
			if (!scanner.Scan()) {
				break;
			}
			row_idx = FlatVector::GetDataUnsafe<T>(payload_chunk.data[0]);
			i = 0;
		}

		token += d;
		tokens[row_idx[i++]] = token;
	}
}

unique_ptr<WindowAggregatorState> WindowTokenTree::GetLocalState(ExecutionContext &context) {
	return make_uniq<WindowTokenTreeLocalState>(context, *this);
}

void WindowTokenTree::Finished() {
	//	Convert the deltas to tokens
	if (mst64) {
		BuildTokens(*this, mst64->LowestLevel());
	} else {
		BuildTokens(*this, mst32->LowestLevel());
	}
	/*
	    for (const auto &d : deltas) {
	        Printer::Print(StringUtil::Format("%lld", d));
	    }
	*/
	// Deallocate memory
	vector<uint8_t> empty;
	deltas.swap(empty);

	WindowMergeSortTree::Finished();
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
