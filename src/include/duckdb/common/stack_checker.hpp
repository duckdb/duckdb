//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/stack_checker.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class NativeStackChecker {
public:
	//! On Linux, returns whether the current thread has less than the reserved stack space remaining.
	//! Returns false on unsupported platforms.
	static bool IsStackNearLimit();
};

template <class RECURSIVE_CLASS>
class StackChecker {
public:
	StackChecker(RECURSIVE_CLASS &recursive_class_p, idx_t stack_usage_p)
	    : recursive_class(recursive_class_p), stack_usage(stack_usage_p) {
		if (NativeStackChecker::IsStackNearLimit()) {
			throw InvalidInputException("Insufficient stack space to process the query");
		}
		recursive_class.stack_depth += stack_usage;
	}
	~StackChecker() {
		recursive_class.stack_depth -= stack_usage;
	}
	StackChecker(StackChecker &&other) noexcept
	    : recursive_class(other.recursive_class), stack_usage(other.stack_usage) {
		other.stack_usage = 0;
	}
	StackChecker(const StackChecker &) = delete;

private:
	RECURSIVE_CLASS &recursive_class;
	idx_t stack_usage;
};

} // namespace duckdb
