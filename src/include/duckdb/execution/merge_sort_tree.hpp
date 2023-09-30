//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/merge_sort_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/array.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

// Implementation of a generic merge-sort-tree
template <typename E = idx_t, typename O = idx_t, typename CMP = std::less<E>, uint64_t F = 64, uint64_t C = 64>
struct MergeSortTree {
	using ElementType = E;
	using OffsetType = O;
	using Elements = vector<ElementType>;
	using Offsets = vector<OffsetType>;
	using Level = pair<Elements, Offsets>;
	using Tree = vector<Level>;

	using RunElement = pair<ElementType, size_t>;
	using RunElements = array<RunElement, F>;
	using Games = array<RunElement, F - 1>;

	struct CompareElements {
		explicit CompareElements(const CMP &cmp) : cmp(cmp) {
		}

		bool operator()(const RunElement &lhs, const RunElement &rhs) {
			if (cmp(lhs.first, rhs.first)) {
				return true;
			}
			if (cmp(rhs.first, lhs.first)) {
				return false;
			}
			return lhs.second < rhs.second;
		}

		CMP cmp;
	};

	MergeSortTree() {
	}
	explicit MergeSortTree(Elements &&lowest_level, const CMP &cmp = CMP());

	RunElement StartGames(Games &losers, const RunElements &elements, const RunElement &sentinel) {
		const auto elem_nodes = elements.size();
		const auto game_nodes = losers.size();
		Games winners;

		//	Play the first round of games,
		//	placing the losers at the bottom of the game
		const auto base_offset = game_nodes / 2;
		auto losers_base = losers.data() + base_offset;
		auto winners_base = winners.data() + base_offset;

		const auto base_count = elem_nodes / 2;
		for (size_t i = 0; i < base_count; ++i) {
			const auto &e0 = elements[i * 2 + 0];
			const auto &e1 = elements[i * 2 + 1];
			if (cmp(e0, e1)) {
				losers_base[i] = e1;
				winners_base[i] = e0;
			} else {
				losers_base[i] = e0;
				winners_base[i] = e1;
			}
		}

		//	Fill in any byes
		if (elem_nodes % 2) {
			winners_base[base_count] = elements.back();
			losers_base[base_count] = sentinel;
		}

		//	Pad to a power of 2
		const auto base_size = (game_nodes + 1) / 2;
		for (size_t i = (elem_nodes + 1) / 2; i < base_size; ++i) {
			winners_base[i] = sentinel;
			losers_base[i] = sentinel;
		}

		//	Play the winners against each other
		//	and stick the losers in the upper levels of the tournament tree
		for (size_t i = base_offset; i-- > 0;) {
			//	Indexing backwards
			const auto &e0 = winners[i * 2 + 1];
			const auto &e1 = winners[i * 2 + 2];
			if (cmp(e0, e1)) {
				losers[i] = e1;
				winners[i] = e0;
			} else {
				losers[i] = e0;
				winners[i] = e1;
			}
		}

		//	Return the final winner
		return winners[0];
	}

	RunElement ReplayGames(Games &losers, size_t slot_idx, const RunElement &insert_val) {
		RunElement smallest = insert_val;
		//	Start at a fake level below
		auto idx = slot_idx + losers.size();
		do {
			// Parent index
			idx = (idx - 1) / 2;
			// swap if out of order
			if (cmp(losers[idx], smallest)) {
				std::swap(losers[idx], smallest);
			}
		} while (idx);

		return smallest;
	}

	Tree tree;
	CompareElements cmp;

	static constexpr auto FANOUT = F;
	static constexpr auto CASCADING = C;

	static size_t LowestCascadingLevel() {
		size_t level = 0;
		size_t level_width = 1;
		while (level_width <= CASCADING) {
			++level;
			level_width *= FANOUT;
		}
		return level;
	}
};

template <typename E, typename P, typename CMP, uint64_t F, uint64_t C>
MergeSortTree<E, P, CMP, F, C>::MergeSortTree(Elements &&lowest_level, const CMP &cmp) : cmp(cmp) {
	const auto fanout = F;
	const auto cascading = C;
	const auto count = lowest_level.size();
	tree.emplace_back(Level(lowest_level, Offsets()));

	const RunElement SENTINEL(std::numeric_limits<ElementType>::max(), std::numeric_limits<size_t>::max());

	//	Fan in parent levels until we are at the top
	//	Note that we don't build the top layer as that would just be all the data.
	for (size_t child_run_length = 1; child_run_length < count;) {
		const auto run_length = child_run_length * fanout;
		const auto num_runs = (count + run_length - 1) / run_length;

		Elements elements;
		elements.reserve(count);

		//	Allocate cascading pointers only if there is room
		Offsets cascades;
		if (cascading > 0 && run_length > cascading) {
			const auto num_cascades = fanout * num_runs * (run_length / cascading + 2);
			cascades.reserve(num_cascades);
		}

		//	Create each parent run by merging the child runs using a tournament tree
		// 	https://en.wikipedia.org/wiki/K-way_merge_algorithm
		//	TODO: Because the runs are independent, they can be parallelised with parallel_for
		const auto &child_level = tree.back();
		for (size_t run_idx = 0; run_idx < num_runs; ++run_idx) {
			//	Position markers for scanning the children.
			using Bounds = pair<size_t, size_t>;
			array<Bounds, fanout> bounds;
			//	Start with first element of each (sorted) child run
			RunElements players;
			const auto child_base = run_idx * run_length;
			for (size_t child_run = 0; child_run < fanout; ++child_run) {
				const auto child_idx = child_base + child_run * child_run_length;
				bounds[child_run] = {MinValue<size_t>(child_idx, count),
				                     MinValue<size_t>(child_idx + child_run_length, count)};
				if (bounds[child_run].first != bounds[child_run].second) {
					players[child_run] = {child_level.first[child_idx], child_run};
				} else {
					//	Empty child
					players[child_run] = SENTINEL;
				}
			}

			//	Play the first round and extract the winner
			Games games;
			auto winner = StartGames(games, players, SENTINEL);
			while (winner != SENTINEL) {
				// Add fractional cascading pointers
				// if we are on a fraction boundary
				if (cascading > 0 && run_length > cascading && elements.size() % cascading == 0) {
					for (size_t i = 0; i < fanout; ++i) {
						cascades.emplace_back(bounds[i].first);
					}
				}

				//	Insert new winner element into the current run
				elements.emplace_back(winner.first);
				const auto child_run = winner.second;
				auto &child_idx = bounds[child_run].first;
				++child_idx;

				//	Move to the next entry in the child run (if any)
				if (child_idx < bounds[child_run].second) {
					winner = ReplayGames(games, child_run, {child_level.first[child_idx], child_run});
				} else {
					winner = ReplayGames(games, child_run, SENTINEL);
				}
			}

			// Add terminal cascade pointers to the end
			if (cascading > 0 && run_length > cascading) {
				for (size_t j = 0; j < 2; ++j) {
					for (size_t i = 0; i < fanout; ++i) {
						cascades.emplace_back(bounds[i].first);
					}
				}
			}
		}

		//	Insert completed level and move up to the next one
		tree.emplace_back(std::move(elements), std::move(cascades));
		child_run_length = run_length;
	}
}

} // namespace duckdb
