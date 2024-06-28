//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/stack_checker.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

template <class RECURSIVE_CLASS>
class StackChecker {
public:
	StackChecker(RECURSIVE_CLASS &recursive_class_p, idx_t stack_usage_p)
	    : recursive_class(recursive_class_p), stack_usage(stack_usage_p) {
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
