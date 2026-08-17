//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/binary_executor.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/smaller_binary.hpp"
#include "duckdb/common/vector_operations/scalar_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <functional>
#include <type_traits>

namespace duckdb {

template <class OP>
struct ComparisonSelectComplement {
	static constexpr bool FOLD = false;
};

template <>
struct ComparisonSelectComplement<NotEquals> {
	static constexpr bool FOLD = true;
	using COMPLEMENT = Equals;
	static constexpr bool SWAP_OPERANDS = false;
};

template <>
struct ComparisonSelectComplement<GreaterThanEquals> {
	static constexpr bool FOLD = true;
	using COMPLEMENT = GreaterThan;
	static constexpr bool SWAP_OPERANDS = true;
};

struct DefaultNullCheckOperator {
	template <class LEFT_TYPE, class RIGHT_TYPE>
	static inline bool Operation(LEFT_TYPE left, RIGHT_TYPE right) {
		return false;
	}
};

struct BinaryStandardOperatorWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC &fun, LEFT_TYPE left, RIGHT_TYPE right, ValidityMask &mask, idx_t idx) {
		return OP::template Operation<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(left, right);
	}

	static constexpr bool AddsNulls() {
		return false;
	}
};

struct BinarySingleArgumentOperatorWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC &fun, LEFT_TYPE left, RIGHT_TYPE right, ValidityMask &mask, idx_t idx) {
		return OP::template Operation<LEFT_TYPE>(left, right);
	}

	static constexpr bool AddsNulls() {
		return false;
	}
};

template <bool ADDS_NULLS>
struct BinaryLambdaWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC &fun, LEFT_TYPE left, RIGHT_TYPE right, ValidityMask &mask, idx_t idx) {
		if constexpr (ADDS_NULLS) {
			auto result = fun(left, right);
			if (!result.has_value()) {
				mask.SetInvalid(idx);
				return RESULT_TYPE();
			}
			return result.value();
		} else {
			return fun(left, right);
		}
	}

	static constexpr bool AddsNulls() {
		return ADDS_NULLS;
	}
};

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC,
          bool CAN_ADD_NULLS>
struct BinaryScalarAdapter {
	static constexpr bool ADDS_NULLS = CAN_ADD_NULLS;

	explicit BinaryScalarAdapter(FUNC &fun_p) : fun(fun_p) {
	}

	inline RESULT_TYPE Operation(ValidityMask &mask, idx_t idx, LEFT_TYPE left, RIGHT_TYPE right) {
		return OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(fun, left, right, mask, idx);
	}

	FUNC &fun;
};

template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
struct BinarySelectAdapter {
	inline bool Operation(LEFT_TYPE left, RIGHT_TYPE right) {
		return OP::Operation(left, right);
	}

	inline bool OperationNoNull(LEFT_TYPE left, RIGHT_TYPE right) {
		if constexpr (ComparisonSelectComplement<OP>::FOLD) {
			using FOLDED = ComparisonSelectComplement<OP>;
			using COMPLEMENT = typename FOLDED::COMPLEMENT;
			if constexpr (FOLDED::SWAP_OPERANDS) {
				return !COMPLEMENT::Operation(right, left);
			}
			return !COMPLEMENT::Operation(left, right);
		}
		return Operation(left, right);
	}
};

struct BinaryExecutor {
private:
	struct ExecutePolicy {
#if !DUCKDB_SMALLER_BINARY(binary_executor_flat)
		static constexpr bool SPECIALIZE_FLAT = true;
#else
		static constexpr bool SPECIALIZE_FLAT = false;
#endif
#if !DUCKDB_SMALLER_BINARY(binary_executor_generic_nullable)
		static constexpr bool SPECIALIZE_NULLABLE_GENERIC_SELECTIONS = true;
#else
		static constexpr bool SPECIALIZE_NULLABLE_GENERIC_SELECTIONS = false;
#endif
		static constexpr bool PRESERVE_RESULT_VALIDITY = false;
	};

	struct SelectPolicy {
#if !DUCKDB_SMALLER_BINARY(binary_executor_select_flat)
		static constexpr uint64_t SPECIALIZED_MASKS = 0x7;
		static constexpr uint64_t DIRECT_TRUE_FLAT_MASKS = 0x7;
#else
		static constexpr uint64_t SPECIALIZED_MASKS = 0;
		static constexpr uint64_t DIRECT_TRUE_FLAT_MASKS = 0;
#endif
#if !DUCKDB_SMALLER_BINARY(binary_executor_select_flags)
		static constexpr bool SPECIALIZE_OUTPUTS = true;
#else
		static constexpr bool SPECIALIZE_OUTPUTS = false;
#endif
	};

	template <bool ADDS_NULLS, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP,
	          class FUNC>
	static void ExecuteSwitchInternal(const Vector &left, const Vector &right, Vector &result, idx_t count, FUNC &fun) {
		std::array<ScalarExecutor::VectorRef, 2> inputs = {{left, right}};
		BinaryScalarAdapter<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, ADDS_NULLS> adapter(fun);
		ScalarExecutor::Execute<ExecutePolicy, RESULT_TYPE, decltype(adapter), LEFT_TYPE, RIGHT_TYPE>(inputs, result,
		                                                                                              count, adapter);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC>
	static void ExecuteSwitch(const Vector &left, const Vector &right, Vector &result, idx_t count, FUNC &fun) {
		if (OPWRAPPER::AddsNulls()) {
			ExecuteSwitchInternal<true, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP>(left, right, result, count,
			                                                                               fun);
		} else {
			ExecuteSwitchInternal<false, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP>(left, right, result, count,
			                                                                                fun);
		}
	}

	static idx_t CheckExecuteCount(const Vector &left, const Vector &right) {
		if (left.size() != right.size()) {
			throw InternalException(
			    "Mismatch in input vector sizes for BinaryExecutor - left has %d rows but right has %d", left.size(),
			    right.size());
		}
		return left.size();
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
	static idx_t SelectShared(const std::array<ScalarExecutor::VectorRef, 2> &inputs, const SelectionVector *sel,
	                          idx_t count, SelectionVector *true_sel, SelectionVector *false_sel) {
		BinarySelectAdapter<LEFT_TYPE, RIGHT_TYPE, OP> adapter;
		return ScalarExecutor::Select<SelectPolicy, decltype(adapter), LEFT_TYPE, RIGHT_TYPE>(
		    inputs, sel, count, true_sel, false_sel, adapter);
	}

public:
	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE,
	          class FUNC = std::function<RESULT_TYPE(LEFT_TYPE, RIGHT_TYPE)>>
	static void Execute(const Vector &left, const Vector &right, Vector &result, idx_t count, FUNC fun) {
		constexpr bool adds_nulls =
		    std::is_same<std::invoke_result_t<FUNC &, LEFT_TYPE, RIGHT_TYPE>, optional<RESULT_TYPE>>::value;
		ExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, BinaryLambdaWrapper<adds_nulls>, bool>(left, right, result,
		                                                                                         count, fun);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP,
	          class OPWRAPPER = BinarySingleArgumentOperatorWrapper>
	static void Execute(const Vector &left, const Vector &right, Vector &result, idx_t count) {
		bool dummy = false;
		ExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP>(left, right, result, count, dummy);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
	static void ExecuteStandard(const Vector &left, const Vector &right, Vector &result, idx_t count) {
		bool dummy = false;
		ExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, BinaryStandardOperatorWrapper, OP>(left, right, result, count,
		                                                                                     dummy);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE,
	          class FUNC = std::function<RESULT_TYPE(LEFT_TYPE, RIGHT_TYPE)>>
	static void Execute(const Vector &left, const Vector &right, Vector &result, FUNC fun) {
		Execute<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, right, result, CheckExecuteCount(left, right), fun);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP,
	          class OPWRAPPER = BinarySingleArgumentOperatorWrapper>
	static void Execute(const Vector &left, const Vector &right, Vector &result) {
		Execute<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP, OPWRAPPER>(left, right, result, CheckExecuteCount(left, right));
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
	static void ExecuteStandard(const Vector &left, const Vector &right, Vector &result) {
		ExecuteStandard<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP>(left, right, result, CheckExecuteCount(left, right));
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
	static idx_t Select(const Vector &left, const Vector &right, const SelectionVector *sel, idx_t count,
	                    SelectionVector *true_sel, SelectionVector *false_sel) {
		std::array<ScalarExecutor::VectorRef, 2> inputs = {{left, right}};
		return SelectShared<LEFT_TYPE, RIGHT_TYPE, OP>(inputs, sel, count, true_sel, false_sel);
	}
};

} // namespace duckdb
