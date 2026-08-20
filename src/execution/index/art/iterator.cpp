#include "duckdb/execution/index/art/iterator.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/const_prefix_handle.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/node_handle.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

#include "pdqsort.h"

namespace duckdb {

//! Limit sorted duplicate lookups to the final capacity slots.
static constexpr idx_t SORTED_DUPLICATE_SEARCH_WINDOW = 16;

//! Limit the temporary bitmap to larger inputs and one quarter of the Row-ID array.
static constexpr idx_t BITMAP_MIN_ROW_COUNT = static_cast<idx_t>(STANDARD_VECTOR_SIZE) * 4;
static constexpr idx_t BITMAP_MAX_BYTES_PER_ROW_ID = sizeof(row_t) / 4;

static bool TryGetBitmapLayout(const unsafe_vector<row_t> &row_ids, row_t &min_row_id, idx_t &span, idx_t &word_count) {
	const auto row_count = row_ids.size();
	if (row_count < BITMAP_MIN_ROW_COUNT) {
		return false;
	}

	const auto minmax = std::minmax_element(row_ids.begin(), row_ids.end());
	min_row_id = *minmax.first;
	if (min_row_id < 0) {
		return false;
	}
	const auto max_row_id = *minmax.second;
	const auto unsigned_min = static_cast<uint64_t>(min_row_id);
	const auto unsigned_max = static_cast<uint64_t>(max_row_id);
	uint64_t unsigned_span;
	if (!TryAddOperator::Operation<uint64_t, uint64_t, uint64_t>(unsigned_max - unsigned_min, 1, unsigned_span) ||
	    unsigned_span > NumericLimits<idx_t>::Maximum()) {
		return false;
	}
	span = static_cast<idx_t>(unsigned_span);

	word_count = span / 64;
	if (span % 64 != 0 && !TryAddOperator::Operation<idx_t, idx_t, idx_t>(word_count, 1, word_count)) {
		return false;
	}

	idx_t bitmap_bytes;
	if (!TryMultiplyOperator::Operation<idx_t, idx_t, idx_t>(word_count, sizeof(uint64_t), bitmap_bytes)) {
		return false;
	}

	idx_t relative_limit;
	if (!TryMultiplyOperator::Operation<idx_t, idx_t, idx_t>(row_count, BITMAP_MAX_BYTES_PER_ROW_ID, relative_limit)) {
		relative_limit = NumericLimits<idx_t>::Maximum();
	}
	return bitmap_bytes <= relative_limit;
}

ARTRowIdCollection::ARTRowIdCollection(const idx_t capacity_p) : capacity(capacity_p) {
	row_ids.reserve(MinValue<idx_t>(capacity, STANDARD_VECTOR_SIZE));
}

bool ARTRowIdCollection::TryAdd(const row_t row_id) {
	D_ASSERT(row_ids.size() <= capacity);
	if (row_ids.size() >= capacity) {
		Normalize();
		if (row_ids.size() >= capacity) {
			return false;
		}
	}
	if (!row_ids.empty() && non_decreasing && row_ids.back() >= row_id) {
		const auto binary_search_threshold = capacity - MinValue<idx_t>(capacity, SORTED_DUPLICATE_SEARCH_WINDOW);
		if (row_ids.back() == row_id ||
		    (row_ids.size() >= binary_search_threshold && std::binary_search(row_ids.begin(), row_ids.end(), row_id))) {
			return true;
		}
		non_decreasing = false;
	}

	row_ids.push_back(row_id);
	return true;
}

void ARTRowIdCollection::Reset() {
	row_ids.clear();
	if (row_ids.capacity() == 0) {
		row_ids.reserve(MinValue<idx_t>(capacity, STANDARD_VECTOR_SIZE));
	}
	non_decreasing = true;
}

void ARTRowIdCollection::Normalize() {
	if (row_ids.size() <= 1 || non_decreasing) {
		row_ids.erase(std::unique(row_ids.begin(), row_ids.end()), row_ids.end());
	} else if (std::is_sorted(row_ids.rbegin(), row_ids.rend())) {
		std::reverse(row_ids.begin(), row_ids.end());
		row_ids.erase(std::unique(row_ids.begin(), row_ids.end()), row_ids.end());
	} else {
		row_t min_row_id;
		idx_t span;
		idx_t word_count;
		if (TryGetBitmapLayout(row_ids, min_row_id, span, word_count)) {
			unsafe_vector<uint64_t> bitmap(word_count, 0);
			const auto unsigned_min = static_cast<uint64_t>(min_row_id);
			for (const auto row_id : row_ids) {
				const auto offset = static_cast<idx_t>(static_cast<uint64_t>(row_id) - unsigned_min);
				D_ASSERT(offset < span);
				bitmap[offset >> 6] |= uint64_t(1) << (offset & 63);
			}

			idx_t result_count = 0;
			for (idx_t word_idx = 0; word_idx < bitmap.size(); word_idx++) {
				auto word = bitmap[word_idx];
				while (word != 0) {
					const auto bit_offset = CountZeros<uint64_t>::Trailing(word);
					const auto offset = (word_idx << 6) + bit_offset;
					row_ids[result_count++] = static_cast<row_t>(unsigned_min + offset);
					word &= word - 1;
				}
			}
			row_ids.resize(result_count);
		} else {
			duckdb_pdqsort::pdqsort_branchless(row_ids.begin(), row_ids.end());
			row_ids.erase(std::unique(row_ids.begin(), row_ids.end()), row_ids.end());
		}
	}

	non_decreasing = true;
}

unsafe_vector<row_t> ARTRowIdCollection::TakeRows() {
	Normalize();
	return std::move(row_ids);
}

//===--------------------------------------------------------------------===//
// IteratorKey
//===--------------------------------------------------------------------===//

bool IteratorKey::Contains(const ARTKey &key) const {
	if (Size() < key.len) {
		return false;
	}
	for (idx_t i = 0; i < key.len; i++) {
		if (key_bytes[i] != key.data[i]) {
			return false;
		}
	}
	return true;
}

bool IteratorKey::GreaterThan(const ARTKey &key, const bool equal, const uint8_t nested_depth) const {
	for (idx_t i = 0; i < MinValue<idx_t>(Size(), key.len); i++) {
		if (key_bytes[i] > key.data[i]) {
			return true;
		} else if (key_bytes[i] < key.data[i]) {
			return false;
		}
	}

	// Returns true, if current_key is greater than (or equal to) key.
	D_ASSERT(Size() >= nested_depth);
	auto this_len = Size() - nested_depth;
	return equal ? this_len > key.len : this_len >= key.len;
}

//===--------------------------------------------------------------------===//
// Iterator
//===--------------------------------------------------------------------===//

template <typename Output>
ARTScanResult Iterator::Scan(const ARTKey &upper_bound, Output &output, bool equal) {
	bool has_next;
	do {
		// An empty upper bound indicates that no upper bound exists.
		if (!upper_bound.Empty()) {
			if (status == GateStatus::GATE_NOT_SET || entered_nested_leaf) {
				if (current_key.GreaterThan(upper_bound, equal, nested_depth)) {
					return ARTScanResult::COMPLETED;
				}
			}
		}

		// Set the current key in the output policy.
		D_ASSERT(current_key.Size() >= nested_depth);
		auto key_len = current_key.Size() - nested_depth;
		output.SetKey(current_key, key_len);

		switch (last_leaf.GetType()) {
		case NType::LEAF_INLINED: {
			if (!output.TryAdd(last_leaf.GetRowId())) {
				return ARTScanResult::PAUSED;
			}
			break;
		}
		case NType::LEAF: {
			D_ASSERT(nested_depth == 0);
			if (!resume_state.has_cached_row_ids) {
				resume_state.cached_row_ids.clear();
				Leaf::DeprecatedGetRowIds(art, last_leaf, resume_state.cached_row_ids, NumericLimits<idx_t>::Maximum());
				resume_state.cached_row_ids_it = resume_state.cached_row_ids.begin();
				resume_state.has_cached_row_ids = true;
			}
			// Try to output the next entry in the deprecated leaf chain.
			while (resume_state.cached_row_ids_it != resume_state.cached_row_ids.end()) {
				if (!output.TryAdd(*resume_state.cached_row_ids_it)) {
					// If we pause here, then scanning will resume at cached_row_ids_it.
					return ARTScanResult::PAUSED;
				}
				++resume_state.cached_row_ids_it;
			}
			resume_state.has_cached_row_ids = false;
			break;
		}
		case NType::NODE_7_LEAF:
		case NType::NODE_15_LEAF:
		case NType::NODE_256_LEAF: {
			// If we haven't traversed this leaf yet, set nested_started to true (allows us to pick up iteration again
			// in case we fill the output with capacity.
			if (!resume_state.nested_started) {
				resume_state.nested_byte = 0;
				resume_state.nested_started = true;
			}
			// Try to output the next inlined leaf.
			while (last_leaf.GetNextByte(art, resume_state.nested_byte)) {
				row_id[ROW_ID_SIZE - 1] = resume_state.nested_byte;
				ARTKey rid_key(&row_id[0], ROW_ID_SIZE);
				if (!output.TryAdd(rid_key.GetRowId())) {
					// If we pause here, then scanning will resume at nested_byte in the current leaf.
					return ARTScanResult::PAUSED;
				}

				if (resume_state.nested_byte == NumericLimits<uint8_t>::Maximum()) {
					break;
				}
				resume_state.nested_byte++;
			}
			resume_state.nested_started = false;
			break;
		}
		default:
			throw InternalException("Invalid leaf type for index scan.");
		}

		entered_nested_leaf = false;
		has_next = Next();
	} while (has_next);
	return ARTScanResult::COMPLETED;
}

// Explicit template instantiations for the output policies.
template ARTScanResult Iterator::Scan<RowIdSetOutput>(const ARTKey &, RowIdSetOutput &, bool);
template ARTScanResult Iterator::Scan<ARTRowIdCollection>(const ARTKey &, ARTRowIdCollection &, bool);
template ARTScanResult Iterator::Scan<KeyRowIdOutput>(const ARTKey &, KeyRowIdOutput &, bool);

void Iterator::FindMinimum(NodePtr current) {
	while (current.HasMetadata()) {
		// Found the minimum.
		if (current.IsAnyLeaf()) {
			last_leaf = current;
			return;
		}

		// We are passing a gate node.
		if (current.GetGateStatus() == GateStatus::GATE_SET) {
			D_ASSERT(status == GateStatus::GATE_NOT_SET);
			status = GateStatus::GATE_SET;
			entered_nested_leaf = true;
			nested_depth = 0;
		}

		// Traverse the prefix.
		if (current.GetType() == NType::PREFIX) {
			NodePtr child;
			{
				ConstNodeHandle handle(art, current);
				auto data = handle.GetPtr();
				auto count = data[art.PrefixCount()];

				for (idx_t i = 0; i < count; i++) {
					current_key.Push(data[i]);
					if (status == GateStatus::GATE_SET) {
						row_id[nested_depth] = data[i];
						nested_depth++;
						D_ASSERT(nested_depth < Prefix::ROW_ID_SIZE);
					}
				}
				// Copy the child locator before releasing the prefix handle.
				child = ConstPrefixHandle::ChildRef(art, handle);
			}
			nodes.emplace(current, 0);
			current = child;
			continue;
		}

		// Go to the leftmost entry in the current node.
		uint8_t byte = 0;
		auto child = current.GetNextChildNode(art, byte);
		D_ASSERT(child);

		// Move to the leftmost node.
		current_key.Push(byte);
		if (status == GateStatus::GATE_SET) {
			row_id[nested_depth] = byte;
			nested_depth++;
			D_ASSERT(nested_depth < Prefix::ROW_ID_SIZE);
		}
		nodes.emplace(current, byte);
		current = child.Get();
	}
	// Should always have a node with metadata.
	throw InternalException("ART Iterator::FindMinimum: Reached node without metadata");
}

bool Iterator::LowerBound(NodePtr current, const ARTKey &key, const bool equal) {
	idx_t depth = 0;

	while (current.HasMetadata()) {
		// We found any leaf node, or a gate.
		if (current.IsAnyLeaf() || current.GetGateStatus() == GateStatus::GATE_SET) {
			D_ASSERT(status == GateStatus::GATE_NOT_SET);
			D_ASSERT(current_key.Size() == key.len);
			if (!equal && current_key.Contains(key)) {
				return Next();
			}

			if (current.GetGateStatus() == GateStatus::GATE_SET) {
				FindMinimum(current);
			} else {
				last_leaf = current;
			}
			return true;
		}

		D_ASSERT(current.GetGateStatus() == GateStatus::GATE_NOT_SET);
		if (current.GetType() != NType::PREFIX) {
			auto next_byte = key[depth];
			auto child = current.GetNextChildNode(art, next_byte);

			// The key is greater than any key in this subtree.
			if (!child) {
				return Next();
			}

			current_key.Push(next_byte);
			nodes.emplace(current, next_byte);

			// We return the minimum because all keys are greater than the lower bound.
			if (next_byte > key[depth]) {
				FindMinimum(child.Get());
				return true;
			}

			// Move to the child and increment depth.
			current = child.Get();
			depth++;
			continue;
		}

		// Copy the prefix bytes and child while the prefix is pinned.
		uint8_t prefix_count;
		NodePtr prefix_child;
		const auto prefix_offset = current_key.Size();
		{
			ConstNodeHandle handle(art, current);
			auto data = handle.GetPtr();
			prefix_count = data[art.PrefixCount()];
			// Copy the child locator before releasing the prefix handle.
			prefix_child = ConstPrefixHandle::ChildRef(art, handle);

			for (idx_t i = 0; i < prefix_count; i++) {
				current_key.Push(data[i]);
			}
			nodes.emplace(current, 0);
		}

		// Compare the copied prefix bytes with the key bytes.
		for (idx_t i = 0; i < prefix_count; i++) {
			// We found a prefix byte that is less than its corresponding key byte.
			// I.e., the subsequent node is lesser than the key. Thus, the next node
			// is the lower bound.
			if (current_key[prefix_offset + i] < key[depth + i]) {
				return Next();
			}

			// We found a prefix byte that is greater than its corresponding key byte.
			// I.e., the subsequent node is greater than the key. Thus, the minimum is
			// the lower bound.
			if (current_key[prefix_offset + i] > key[depth + i]) {
				FindMinimum(prefix_child);
				return true;
			}
		}

		// The prefix matches the key.
		depth += prefix_count;
		current = prefix_child;
	}
	// Should always have a node with metadata.
	throw InternalException("ART Iterator::LowerBound: Reached node without metadata");
}

bool Iterator::Next() {
	while (!nodes.empty()) {
		auto &top = nodes.top();
		D_ASSERT(!top.node.IsAnyLeaf());

		if (top.node.GetType() == NType::PREFIX) {
			PopNode();
			continue;
		}

		if (top.byte == NumericLimits<uint8_t>::Maximum()) {
			// No more children of this node.
			// Move up the tree by popping the key byte of the current node.
			PopNode();
			continue;
		}

		top.byte++;
		auto child = top.node.GetNextChildNode(art, top.byte);
		if (!child) {
			// No more children of this node.
			// Move up the tree by popping the key byte of the current node.
			PopNode();
			continue;
		}

		current_key.Pop(1);
		current_key.Push(top.byte);
		if (status == GateStatus::GATE_SET) {
			row_id[nested_depth - 1] = top.byte;
		}

		FindMinimum(child.Get());
		return true;
	}
	return false;
}

void Iterator::PopNode() {
	auto gate_status = nodes.top().node.GetGateStatus();

	// Pop the byte and the node.
	if (nodes.top().node.GetType() != NType::PREFIX) {
		current_key.Pop(1);
		if (status == GateStatus::GATE_SET) {
			nested_depth--;
			D_ASSERT(nested_depth < Prefix::ROW_ID_SIZE);
		}

	} else {
		// Pop all prefix bytes and the node.
		ConstNodeHandle handle(art, nodes.top().node);
		auto data = handle.GetPtr();
		auto prefix_byte_count = data[art.PrefixCount()];
		current_key.Pop(prefix_byte_count);

		if (status == GateStatus::GATE_SET) {
			nested_depth -= prefix_byte_count;
			D_ASSERT(nested_depth < Prefix::ROW_ID_SIZE);
		}
	}
	nodes.pop();

	// We are popping a gate node.
	if (gate_status == GateStatus::GATE_SET) {
		D_ASSERT(status == GateStatus::GATE_SET);
		status = GateStatus::GATE_NOT_SET;
	}
}

} // namespace duckdb
