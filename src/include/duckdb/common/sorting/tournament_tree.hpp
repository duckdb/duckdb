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

template <class ITER, class COMPARE = std::less<typename std::iterator_traits<ITER>::value_type>>
class tournament_tree_t { // NOLINT: match stl case
	using iterator = ITER;
	using value_type = typename std::iterator_traits<iterator>::value_type;
	using comp = COMPARE;

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
			current += current != end;
		}

		value_type &value() const { // NOLINT: match stl case
			D_ASSERT(has_value());
			return *current;
		}

	private:
		iterator current;
		const iterator end;
	};

public:
	explicit tournament_tree_t(const vector<pair<iterator, iterator>> &sorted_runs) // NOLINT: match stl case
	    : num_leaf_nodes(sorted_runs.size()), num_internal_nodes(num_leaf_nodes - 1) {
		leaf_nodes.resize(num_leaf_nodes);
		internal_nodes.resize(num_internal_nodes);

		for (idx_t leaf_idx = 0; leaf_idx < num_leaf_nodes; leaf_idx++) {
			leaf_nodes[leaf_idx] = leaf_node_t(sorted_runs[leaf_idx]);
		}

		for (idx_t leaf_idx = 0; leaf_idx < num_leaf_nodes; leaf_idx++) {
			update(leaf_idx);
		}
	}

public:
	bool empty() const { // NOLINT: match stl case
		return leaf_nodes[internal_nodes[0]].has_value();
	}

	value_type &top() const { // NOLINT: match stl case
		return leaf_nodes[internal_nodes[0]].value();
	}

	void pop() { // NOLINT: match stl case
		const auto winning_leaf_idx = internal_nodes[0];
		leaf_nodes[winning_leaf_idx].advance();
		update(winning_leaf_idx);
	}

private:
	// Utility functions for computing parent/child indices
	static idx_t parent(const idx_t &i) { // NOLINT: match stl case
		return (i - 1) / 2;
	}
	static idx_t left_child(const idx_t &i) { // NOLINT: match stl case
		return 2 * i + 1;
	}
	static idx_t right_child(const idx_t &i) { // NOLINT: match stl case
		return 2 * i + 2;
	}

	// Update tree from given node upwards
	void update(const idx_t &leaf_idx) { // NOLINT: match stl case
		auto internal_idx = leaf_idx;
		while (internal_idx < num_internal_nodes) {
			const auto left_leaf_idx = internal_nodes[left_child(internal_idx)];
			const auto right_leaf_idx = internal_nodes[right_child(internal_idx)];

			internal_nodes[internal_idx] = comp(leaf_nodes[left_leaf_idx].value(), leaf_nodes[right_leaf_idx].value())
			                                   ? left_leaf_idx
			                                   : right_leaf_idx;

			if (internal_idx == 0) {
				break;
			}
			internal_idx = parent(internal_idx);
		}
	}

private:
	const idx_t num_leaf_nodes;
	unsafe_vector<leaf_node_t> leaf_nodes;

	const idx_t num_internal_nodes;
	unsafe_vector<uint32_t> internal_nodes;
};

} // namespace duckdb
