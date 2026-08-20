//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/variadic_executor.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/smaller_binary.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/scalar_executor.hpp"

#include <array>
#include <functional>
#include <tuple>
#include <type_traits>

namespace duckdb {

//! Wrappers that adapt different calling conventions to a uniform interface.
//! Each wrapper's Operation method takes: (FUN, ValidityMask&, idx_t, ARGS...)
struct VariadicLambdaWrapper {
	template <class FUN, class RESULT_TYPE, class... ARGS>
	static inline RESULT_TYPE Operation(FUN &fun, ValidityMask &mask, idx_t idx, ARGS... args) {
		if constexpr (std::is_same<decltype(fun(args...)), optional<RESULT_TYPE>>::value) {
			auto result = fun(args...);
			if (!result.has_value()) {
				mask.SetInvalid(idx);
				return RESULT_TYPE();
			}
			return result.value();
		} else {
			return fun(args...);
		}
	}
};

template <class OP>
struct VariadicStandardOperatorWrapper {
	template <class FUN, class RESULT_TYPE, class... ARGS>
	static inline RESULT_TYPE Operation(FUN &fun, ValidityMask &mask, idx_t idx, ARGS... args) {
		return OP::template Operation<ARGS..., RESULT_TYPE>(args...);
	}
};

template <class RESULT_TYPE, class FUN, class... ARGS>
struct VariadicLambdaAdapter {
	static constexpr bool ADDS_NULLS =
	    std::is_same<std::invoke_result_t<FUN &, ARGS &...>, optional<RESULT_TYPE>>::value;

	explicit VariadicLambdaAdapter(FUN &fun_p) : fun(fun_p) {
	}

	inline RESULT_TYPE Operation(ValidityMask &mask, idx_t idx, ARGS... args) {
		return VariadicLambdaWrapper::template Operation<FUN, RESULT_TYPE, ARGS...>(fun, mask, idx, args...);
	}

	FUN &fun;
};

template <class RESULT_TYPE, class OP, class... ARGS>
struct VariadicStandardAdapter {
	static constexpr bool ADDS_NULLS = false;

	inline RESULT_TYPE Operation(ValidityMask &, idx_t, ARGS... args) {
		return OP::template Operation<ARGS..., RESULT_TYPE>(args...);
	}
};

template <class OP, class... ARGS>
struct VariadicSelectAdapter {
	inline bool Operation(ARGS... args) {
		return OP::Operation(args...);
	}

	inline bool OperationNoNull(ARGS... args) {
		return Operation(args...);
	}
};

//! VariadicExecutor is the generic public facade over ScalarExecutor.
//! Template parameter ordering remains <RESULT_TYPE, INPUT_TYPES...>.
struct VariadicExecutor {
	using VectorRef = ScalarExecutor::VectorRef;

private:
	template <class... ARGS>
	static constexpr bool SpecializeFlat() {
		return sizeof...(ARGS) <= 3 && (... && std::is_arithmetic<ARGS>::value);
	}

	template <class... ARGS>
	struct ExecutePolicy {
#if !DUCKDB_SMALLER_BINARY(variadic_executor_flat)
		static constexpr bool SPECIALIZE_FLAT = SpecializeFlat<ARGS...>();
#else
		static constexpr bool SPECIALIZE_FLAT = false;
#endif
		static constexpr bool SPECIALIZE_NULLABLE_GENERIC_SELECTIONS = false;
		static constexpr bool PRESERVE_RESULT_VALIDITY = false;
	};

	template <class... ARGS>
	struct SelectPolicy {
#if !DUCKDB_SMALLER_BINARY(variadic_executor_select_flat)
		static constexpr uint64_t SPECIALIZED_MASKS = SpecializeFlat<ARGS...>() ? 1 : 0;
		static constexpr uint64_t DIRECT_TRUE_FLAT_MASKS =
		    sizeof...(ARGS) == 3 && SpecializeFlat<ARGS...>() ? uint64_t(1) << 6 : 0;
#else
		static constexpr uint64_t SPECIALIZED_MASKS = 0;
		static constexpr uint64_t DIRECT_TRUE_FLAT_MASKS = 0;
#endif
#if !DUCKDB_SMALLER_BINARY(variadic_executor_select_flags)
		static constexpr bool SPECIALIZE_OUTPUTS = true;
#else
		static constexpr bool SPECIALIZE_OUTPUTS = false;
#endif
	};

	template <size_t N, size_t... Is>
	static std::array<VectorRef, N> MakeInputArrayImpl(DataChunk &input, std::index_sequence<Is...>) {
		return {{std::cref(input.data[Is])...}};
	}

	template <size_t N>
	static std::array<VectorRef, N> MakeInputArray(DataChunk &input) {
		D_ASSERT(input.ColumnCount() >= N);
		return MakeInputArrayImpl<N>(input, std::make_index_sequence<N> {});
	}

	template <size_t N>
	static idx_t CheckExecuteCount(const std::array<VectorRef, N> &inputs) {
		static_assert(N > 0, "VariadicExecutor requires at least one input");
		idx_t count = inputs[0].get().size();
		for (idx_t i = 1; i < N; i++) {
			if (inputs[i].get().size() != count) {
				throw InternalException(
				    "Mismatch in input vector sizes for VariadicExecutor - expected %d rows but got %d", count,
				    inputs[i].get().size());
			}
		}
		return count;
	}

public:
	template <class RESULT_TYPE, class... ARGS, class FUN>
	static void Execute(std::array<VectorRef, sizeof...(ARGS)> inputs, Vector &result, FUN fun) {
		auto count = CheckExecuteCount(inputs);
		VariadicLambdaAdapter<RESULT_TYPE, FUN, ARGS...> adapter(fun);
		ScalarExecutor::Execute<ExecutePolicy<ARGS...>, RESULT_TYPE, decltype(adapter), ARGS...>(inputs, result, count,
		                                                                                         adapter);
	}

	template <class RESULT_TYPE, class... ARGS, class FUN>
	static void Execute(DataChunk &input, Vector &result, FUN fun) {
		auto inputs = MakeInputArray<sizeof...(ARGS)>(input);
		VariadicLambdaAdapter<RESULT_TYPE, FUN, ARGS...> adapter(fun);
		ScalarExecutor::Execute<ExecutePolicy<ARGS...>, RESULT_TYPE, decltype(adapter), ARGS...>(inputs, result,
		                                                                                         input.size(), adapter);
	}

	template <class RESULT_TYPE, class OP, class... ARGS>
	static void ExecuteStandard(std::array<VectorRef, sizeof...(ARGS)> inputs, Vector &result) {
		auto count = CheckExecuteCount(inputs);
		VariadicStandardAdapter<RESULT_TYPE, OP, ARGS...> adapter;
		ScalarExecutor::Execute<ExecutePolicy<ARGS...>, RESULT_TYPE, decltype(adapter), ARGS...>(inputs, result, count,
		                                                                                         adapter);
	}

	template <class OP, class... ARGS>
	static idx_t Select(std::array<VectorRef, sizeof...(ARGS)> inputs, const SelectionVector *sel, idx_t count,
	                    SelectionVector *true_sel, SelectionVector *false_sel) {
		VariadicSelectAdapter<OP, ARGS...> adapter;
		return ScalarExecutor::Select<SelectPolicy<ARGS...>, decltype(adapter), ARGS...>(inputs, sel, count, true_sel,
		                                                                                 false_sel, adapter);
	}
};

} // namespace duckdb
