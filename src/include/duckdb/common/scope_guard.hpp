//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/scope_guard.hpp
//
//
//===----------------------------------------------------------------------===//
//
// SCOPE_EXIT is used to execute a series of registered functions when it goes
// out of scope.
// For details, refer to Andrei Alexandrescu's CppCon 2015 talk "Declarative
// Control Flow"
//
// Example usage:
//   void Function() {
//     FILE* fp = fopen("my_file.txt", "r");
//     SCOPE_EXIT { fclose(fp); };
//     // Do something.
//   }  // fp will be closed at exit.
//

#pragma once

#include <functional>
#include <utility>

// Need to have two macro invocation to allow [x] and [y] to be replaced.
#define __DUCKDB_CONCAT(x, y) x##y

#define DUCKDB_CONCAT(x, y) __DUCKDB_CONCAT(x, y)

// Macros which gets unique variable name.
#define DUCKDB_UNIQUE_VARIABLE(base) DUCKDB_CONCAT(base, __LINE__)

namespace duckdb {

class ScopeGuard {
private:
	using Func = std::function<void(void)>;

public:
	ScopeGuard() : func_([]() {}) {
	}
	explicit ScopeGuard(Func &&func) : func_(std::forward<Func>(func)) {
	}
	~ScopeGuard() noexcept {
		func_();
	}

	// Register a new function to be invoked at destruction.
	// Execution will be performed at the reversed order they're registered.
	ScopeGuard &operator+=(Func &&another_func) {
		func_ = [cur_func = std::move(func_), another_func = std::move(another_func)]() {
			// Executed in the reverse order functions are registered.
			another_func();
			cur_func();
		};
		return *this;
	}

private:
	Func func_;
};

namespace internal {

using ScopeGuardFunc = std::function<void(void)>;

// Constructs a scope guard that calls 'fn' when it exits.
enum class ScopeGuardOnExit {};
inline ScopeGuard operator+(ScopeGuardOnExit /*unused*/, ScopeGuardFunc fn) {
	return ScopeGuard {std::move(fn)};
}

} // namespace internal

} // namespace duckdb

#define SCOPE_EXIT auto DUCKDB_UNIQUE_VARIABLE(SCOPE_EXIT_TEMP_EXIT) = duckdb::internal::ScopeGuardOnExit {} + [&]()
