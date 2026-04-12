//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/scope_exit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <utility>

#define DUCKDB_SCOPE_CONCAT_INNER(x, y) x##y
#define DUCKDB_SCOPE_CONCAT(x, y)       DUCKDB_SCOPE_CONCAT_INNER(x, y)
#define DUCKDB_SCOPE_UNIQUE_VAR(base)   DUCKDB_SCOPE_CONCAT(base, __LINE__)

namespace duckdb {

class ScopeGuard {
private:
	using Func = std::function<void(void)>;

public:
	ScopeGuard() : func([]() {}) {
	}
	explicit ScopeGuard(Func &&func_p) : func(std::move(func_p)) {
	}
	// Disable copy and move.
	ScopeGuard(const ScopeGuard &) = delete;
	ScopeGuard &operator=(const ScopeGuard &) = delete;

	~ScopeGuard() noexcept {
		func();
	}

	// Register a new function to be invoked at destruction.
	// Execution will be performed at the reversed order they're registered.
	ScopeGuard &operator+=(Func &&another_func) {
		Func cur_func = std::move(func);
		func = [cur_func = std::move(cur_func), another_func = std::move(another_func)]() {
			// Executed in the reverse order functions are registered.
			another_func();
			cur_func();
		};
		return *this;
	}

private:
	Func func;
};

namespace internal {

using ScopeGuardFunc = std::function<void(void)>;

// Constructs a scope guard that calls 'fn' when it exits.
enum class ScopeGuardOnExit {};
inline auto operator+(ScopeGuardOnExit /*unused*/, ScopeGuardFunc fn) {
	return ScopeGuard {std::move(fn)};
}

} // namespace internal

} // namespace duckdb

// SCOPE_EXIT is used to execute a series of registered functions when it goes
// out of scope.
// For details, refer to Andrei Alexandrescu's CppCon 2015 talk "Declarative
// Control Flow"
//
// Examples:
//   void Function() {
//     FILE* fp = fopen("my_file.txt", "r");
//     SCOPE_EXIT { fclose(fp); };
//     // Do something.
//   }  // fp will be closed at exit.
#define SCOPE_EXIT auto DUCKDB_SCOPE_UNIQUE_VAR(SCOPE_EXIT_TEMP_EXIT) = duckdb::internal::ScopeGuardOnExit {} + [&]()
