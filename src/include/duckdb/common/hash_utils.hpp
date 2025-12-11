//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/hash_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <functional>

namespace duckdb {

// Util for reference wrapper hashing
template <typename Hash>
struct RefHash : Hash {
	RefHash() = default;
	template <typename H>
	RefHash(H &&h) : Hash(std::forward<H>(h)) { // NOLINT
	}

	template <typename T>
	std::size_t operator()(std::reference_wrapper<const T> val) const {
		return Hash::operator()(val.get());
	}
	template <typename T>
	std::size_t operator()(const T &val) const {
		return Hash::operator()(val);
	}
};

template <typename Equal>
struct RefEq : Equal {
	RefEq() = default;
	template <typename Eq>
	RefEq(Eq &&eq) : Equal(std::forward<Eq>(eq)) { // NOLINT
	}

	template <typename T1, typename T2>
	bool operator()(std::reference_wrapper<const T1> lhs, std::reference_wrapper<const T2> rhs) const {
		return Equal::operator()(lhs.get(), rhs.get());
	}
	template <typename T1, typename T2>
	bool operator()(const T1 &lhs, std::reference_wrapper<const T2> rhs) const {
		return Equal::operator()(lhs, rhs.get());
	}
	template <typename T1, typename T2>
	bool operator()(std::reference_wrapper<const T1> lhs, const T2 &rhs) const {
		return Equal::operator()(lhs.get(), rhs);
	}
	template <typename T1, typename T2>
	bool operator()(const T1 &lhs, const T2 &rhs) const {
		return Equal::operator()(lhs, rhs);
	}
};

} // namespace duckdb
