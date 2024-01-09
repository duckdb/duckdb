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
#include "duckdb/common/vector_operations/aggregate_executor.hpp"

namespace duckdb {

// MIT License Text:
//
// Copyright 2022 salesforce.com, inc.
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
// BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
// ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Implementation of a generic merge-sort-tree
// Rewrite of the original, which was in C++17 and targeted for research,
// instead of deployment.
template <typename E = idx_t, typename O = idx_t, typename CMP = std::less<E>, uint64_t F = 32, uint64_t C = 32>
struct MergeSortTree {
	using ElementType = E;
	using OffsetType = O;
	using Elements = vector<ElementType>;
	using Offsets = vector<OffsetType>;
	using Level = pair<Elements, Offsets>;
	using Tree = vector<Level>;

	using RunElement = pair<ElementType, idx_t>;
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

	idx_t SelectNth(const SubFrames &frames, idx_t n) const;

	inline ElementType NthElement(idx_t i) const {
		return tree.front().first[i];
	}

protected:
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
		for (idx_t i = 0; i < base_count; ++i) {
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
		for (idx_t i = (elem_nodes + 1) / 2; i < base_size; ++i) {
			winners_base[i] = sentinel;
			losers_base[i] = sentinel;
		}

		//	Play the winners against each other
		//	and stick the losers in the upper levels of the tournament tree
		for (idx_t i = base_offset; i-- > 0;) {
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

	RunElement ReplayGames(Games &losers, idx_t slot_idx, const RunElement &insert_val) {
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

	static idx_t LowestCascadingLevel() {
		idx_t level = 0;
		idx_t level_width = 1;
		while (level_width <= CASCADING) {
			++level;
			level_width *= FANOUT;
		}
		return level;
	}
};

template <typename E, typename O, typename CMP, uint64_t F, uint64_t C>
MergeSortTree<E, O, CMP, F, C>::MergeSortTree(Elements &&lowest_level, const CMP &cmp) : cmp(cmp) {
	const auto fanout = F;
	const auto cascading = C;
	const auto count = lowest_level.size();
	tree.emplace_back(Level(lowest_level, Offsets()));

	const RunElement SENTINEL(std::numeric_limits<ElementType>::max(), std::numeric_limits<idx_t>::max());

	//	Fan in parent levels until we are at the top
	//	Note that we don't build the top layer as that would just be all the data.
	for (idx_t child_run_length = 1; child_run_length < count;) {
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
		for (idx_t run_idx = 0; run_idx < num_runs; ++run_idx) {
			//	Position markers for scanning the children.
			using Bounds = pair<idx_t, idx_t>;
			array<Bounds, fanout> bounds;
			//	Start with first element of each (sorted) child run
			RunElements players;
			const auto child_base = run_idx * run_length;
			for (idx_t child_run = 0; child_run < fanout; ++child_run) {
				const auto child_idx = child_base + child_run * child_run_length;
				bounds[child_run] = {MinValue<idx_t>(child_idx, count),
				                     MinValue<idx_t>(child_idx + child_run_length, count)};
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
					for (idx_t i = 0; i < fanout; ++i) {
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
				for (idx_t j = 0; j < 2; ++j) {
					for (idx_t i = 0; i < fanout; ++i) {
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

template <typename E, typename O, typename CMP, uint64_t F, uint64_t C>
idx_t MergeSortTree<E, O, CMP, F, C>::SelectNth(const SubFrames &frames, idx_t n) const {
	// Handle special case of a one-element tree
	if (tree.size() < 2) {
		return 0;
	}

	// 	The first level contains a single run,
	//	so the only thing we need is any cascading pointers
	auto level_no = tree.size() - 2;
	auto level_width = 1;
	for (idx_t i = 0; i < level_no; ++i) {
		level_width *= FANOUT;
	}

	// Find Nth element in a top-down traversal
	idx_t result = 0;

	// First, handle levels with cascading pointers
	const auto min_cascaded = LowestCascadingLevel();
	if (level_no > min_cascaded) {
		//	Initialise the cascade indicies from the previous level
		using CascadeRange = pair<idx_t, idx_t>;
		std::array<CascadeRange, 3> cascades;
		const auto &level = tree[level_no + 1].first;
		for (idx_t f = 0; f < frames.size(); ++f) {
			const auto &frame = frames[f];
			auto &cascade_idx = cascades[f];
			const auto lower_idx = std::lower_bound(level.begin(), level.end(), frame.start) - level.begin();
			cascade_idx.first = lower_idx / CASCADING * FANOUT;
			const auto upper_idx = std::lower_bound(level.begin(), level.end(), frame.end) - level.begin();
			cascade_idx.second = upper_idx / CASCADING * FANOUT;
		}

		// 	Walk the cascaded levels
		for (; level_no >= min_cascaded; --level_no) {
			//	The cascade indicies into this level are in the previous level
			const auto &level_cascades = tree[level_no + 1].second;

			// Go over all children until we found enough in range
			const auto *level_data = tree[level_no].first.data();
			while (true) {
				idx_t matched = 0;
				std::array<CascadeRange, 3> matches;
				for (idx_t f = 0; f < frames.size(); ++f) {
					const auto &frame = frames[f];
					auto &cascade_idx = cascades[f];
					auto &match = matches[f];

					const auto lower_begin = level_data + level_cascades[cascade_idx.first];
					const auto lower_end = level_data + level_cascades[cascade_idx.first + FANOUT];
					match.first = std::lower_bound(lower_begin, lower_end, frame.start) - level_data;

					const auto upper_begin = level_data + level_cascades[cascade_idx.second];
					const auto upper_end = level_data + level_cascades[cascade_idx.second + FANOUT];
					match.second = std::lower_bound(upper_begin, upper_end, frame.end) - level_data;

					matched += idx_t(match.second - match.first);
				}
				if (matched > n) {
					// 	Too much in this level, so move down to leftmost child candidate within the cascade range
					for (idx_t f = 0; f < frames.size(); ++f) {
						auto &cascade_idx = cascades[f];
						auto &match = matches[f];
						cascade_idx.first = (match.first / CASCADING + 2 * result) * FANOUT;
						cascade_idx.second = (match.second / CASCADING + 2 * result) * FANOUT;
					}
					break;
				}

				//	Not enough in this child, so move right
				for (idx_t f = 0; f < frames.size(); ++f) {
					auto &cascade_idx = cascades[f];
					++cascade_idx.first;
					++cascade_idx.second;
				}
				++result;
				n -= matched;
			}
			result *= FANOUT;
			level_width /= FANOUT;
		}
	}

	//	Continue with the uncascaded levels (except the first)
	for (; level_no > 0; --level_no) {
		const auto &level = tree[level_no].first;
		auto range_begin = level.begin() + result * level_width;
		auto range_end = range_begin + level_width;
		while (range_end < level.end()) {
			idx_t matched = 0;
			for (idx_t f = 0; f < frames.size(); ++f) {
				const auto &frame = frames[f];
				const auto lower_match = std::lower_bound(range_begin, range_end, frame.start);
				const auto upper_match = std::lower_bound(lower_match, range_end, frame.end);
				matched += idx_t(upper_match - lower_match);
			}
			if (matched > n) {
				// 	Too much in this level, so move down to leftmost child candidate
				//	Since we have no cascade pointers left, this is just the start of the next level.
				break;
			}
			//	Not enough in this child, so move right
			range_begin = range_end;
			range_end += level_width;
			++result;
			n -= matched;
		}
		result *= FANOUT;
		level_width /= FANOUT;
	}

	// The last level
	const auto *level_data = tree[level_no].first.data();
	++n;

	const auto count = tree[level_no].first.size();
	for (const auto limit = MinValue<idx_t>(result + FANOUT, count); result < limit; ++result) {
		const auto v = level_data[result];
		for (const auto &frame : frames) {
			n -= (v >= frame.start) && (v < frame.end);
		}
		if (!n) {
			break;
		}
	}

	return result;
}

} // namespace duckdb
