//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/no_destructor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <new>
#include <utility>

namespace duckdb {

template <class T>
class NoDestructor {
public:
	template <class... ARGS>
	explicit NoDestructor(ARGS &&... args) {
		new (static_cast<void *>(storage)) T(std::forward<ARGS>(args)...);
	}

	NoDestructor(const NoDestructor &) = delete;
	NoDestructor &operator=(const NoDestructor &) = delete;

	T &Get() {
		return *reinterpret_cast<T *>(storage);
	}

	const T &Get() const {
		return *reinterpret_cast<const T *>(storage);
	}

	T &operator*() {
		return Get();
	}

	const T &operator*() const {
		return Get();
	}

	T *operator->() {
		return &Get();
	}

	const T *operator->() const {
		return &Get();
	}

private:
	alignas(T) unsigned char storage[sizeof(T)];
};

} // namespace duckdb
