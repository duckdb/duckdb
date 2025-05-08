//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/tournament_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/pair.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

template <class ITER>
class tournament_tree_t { // NOLINT: match stl case
	using iterator = ITER;
	using value_type = typename std::iterator_traits<iterator>::value_type;

public:
	struct leaf_node_t { // NOLINT: match stl case
	public:
		explicit leaf_node_t(const pair<iterator, iterator> &sorted_run)
		    : current(sorted_run.first), end(sorted_run.second) {
		}

	public:
		bool has_value() const { // NOLINT: match stl case
			return current != end;
		}

		void advance() { // NOLINT: match stl case
			++current;
		}

		value_type &value() const { // NOLINT: match stl case
			D_ASSERT(has_value());
			return *current;
		}

		friend bool operator<(const leaf_node_t &lhs, const leaf_node_t &rhs) {
			return lhs.has_value() ? (rhs.has_value() ? lhs.value() < rhs.value() : true) : false;
		}

	private:
		iterator current;
		iterator end;
	};

public:
	explicit tournament_tree_t(const vector<pair<iterator, iterator>> &sorted_runs) // NOLINT: match stl case
	    : num_leaf_nodes(sorted_runs.size()), num_internal_nodes(NextPowerOfTwo(num_leaf_nodes) - 1) {
		leaf_nodes.reserve(num_leaf_nodes);
		internal_nodes.resize(num_internal_nodes, num_leaf_nodes - 1);

		for (idx_t leaf_idx = 0; leaf_idx < num_leaf_nodes; leaf_idx++) {
			leaf_nodes.emplace_back(sorted_runs[leaf_idx]);
		}

		if (num_internal_nodes > 2) {
			for (idx_t leaf_idx = 0; leaf_idx < num_leaf_nodes; leaf_idx++) {
				update(leaf_idx);
			}
		}
	}

public:
	bool empty() const { // NOLINT: match stl case
		if (num_leaf_nodes == 1) {
			return !leaf_nodes[0].has_value();
		}
		if (num_leaf_nodes == 2) {
			return !leaf_nodes[0].has_value() && !leaf_nodes[1].has_value();
		}
		return !leaf_nodes[internal_nodes[0]].has_value();
	}

	idx_t get_batch(value_type **const values, const idx_t &batch_size) { // NOLINT: match stl case
		if (empty()) {
			VerifyEmpty();
			return 0;
		}
		idx_t count;
		if (num_leaf_nodes == 1) {
			count = get_batch_one_leaf(values, batch_size);
		} else if (num_leaf_nodes == 2) {
			count = get_batch_two_leaves(values, batch_size);
		} else {
			count = get_batch_internal(values, batch_size);
		}
		if (count != batch_size) {
			VerifyEmpty();
		}
		return count;
	}

private:
	idx_t get_batch_one_leaf(value_type **const values, const idx_t &batch_size) { // NOLINT: match stl case
		auto &leaf_node = leaf_nodes[0];
		idx_t count = 0;
		while (leaf_node.has_value() && count < batch_size) {
			values[count++] = &leaf_node.value();
			leaf_node.advance();
		}
		return count;
	}

	idx_t get_batch_two_leaves(value_type **const values, const idx_t &batch_size) { // NOLINT: match stl case
		auto &left_leaf_node = leaf_nodes[0];
		auto &right_leaf_node = leaf_nodes[1];
		idx_t count = 0;
		while (count < batch_size) {
			auto &winning_leaf_node = left_leaf_node < right_leaf_node ? left_leaf_node : right_leaf_node;
			if (!winning_leaf_node.has_value()) {
				break;
			}
			values[count++] = &winning_leaf_node.value();
			winning_leaf_node.advance();
		}
		return count;
	}

	idx_t get_batch_internal(value_type **const values, const idx_t &batch_size) { // NOLINT: match stl case
		idx_t count = 0;
		while (count < batch_size) {
			const auto &winning_leaf_idx = internal_nodes[0];
			auto &top = leaf_nodes[winning_leaf_idx];
			if (!top.has_value()) {
				break;
			}
			values[count++] = &top.value();
			top.advance();
			update(winning_leaf_idx);
		}
		return count;
	}

	static inline idx_t parent(const idx_t &i) { // NOLINT: match stl case
		return (i - 1) / 2;
	}
	static inline idx_t left_child(const idx_t &i) { // NOLINT: match stl case
		return 2 * i + 1;
	}
	static inline idx_t right_child(const idx_t &i) { // NOLINT: match stl case
		return 2 * i + 2;
	}

	inline void update(const idx_t &leaf_idx) { // NOLINT: match stl case
		idx_t internal_idx = num_internal_nodes / 2 + leaf_idx / 2;
		idx_t left_leaf_idx = leaf_idx & 1 ? leaf_idx - 1 : leaf_idx;
		idx_t right_leaf_idx = MinValue(left_leaf_idx + 1, num_leaf_nodes - 1);
		while (true) {
			internal_nodes[internal_idx] =
			    leaf_nodes[left_leaf_idx] < leaf_nodes[right_leaf_idx] ? left_leaf_idx : right_leaf_idx;
			if (internal_idx == 0) {
				break;
			}
			internal_idx = parent(internal_idx);
			left_leaf_idx = internal_nodes[left_child(internal_idx)];
			right_leaf_idx = internal_nodes[right_child(internal_idx)];
		}
	}

	void VerifyEmpty() {
#ifdef D_ASSERT_IS_ENABLED
		for (const auto &leaf_node : leaf_nodes) {
			if (leaf_node.has_value()) {
				throw InternalException("oops");
			}
		}
#endif
	}

private:
	const idx_t num_leaf_nodes;
	unsafe_vector<leaf_node_t> leaf_nodes;

	const idx_t num_internal_nodes;
	unsafe_vector<idx_t> internal_nodes;
};

} // namespace duckdb
