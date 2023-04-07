/// @file
/// @brief randomness

#ifndef RANDOM_HH
#define RANDOM_HH

#include <iterator>
#include <random>
#include <stdexcept>
#include <utility>
#include "duckdb/common/vector.hpp"

namespace smith {
extern std::mt19937_64 rng;
}

using duckdb::vector;

template <typename T>
T &random_pick(vector<T> &container) {
	if (!container.size())
		throw std::runtime_error("No candidates available");

	std::uniform_int_distribution<int> pick(0, container.size() - 1);
	return container[pick(smith::rng)];
}

template <typename I>
I random_pick(I beg, I end) {
	if (beg == end)
		throw std::runtime_error("No candidates available");

	std::uniform_int_distribution<> pick(0, std::distance(beg, end) - 1);
	std::advance(beg, pick(smith::rng));
	return beg;
}

template <typename I>
I random_pick(std::pair<I, I> iters) {
	return random_pick(iters.first, iters.second);
}

int d6(), d9(), d12(), d20(), d42(), d100();

#endif
