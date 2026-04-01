//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/variadic_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

#include <array>
#include <functional>
#include <tuple>

namespace duckdb {

//! Wrappers that adapt different calling conventions to a uniform interface.
//! Each wrapper's Operation method takes: (FUN, ValidityMask&, idx_t, ARGS...)

struct VariadicLambdaWrapper {
	template <class FUN, class RESULT_TYPE, class... ARGS>
	static inline RESULT_TYPE Operation(FUN &fun, ValidityMask &mask, idx_t idx, ARGS... args) {
		return fun(args...);
	}
};

struct VariadicLambdaWrapperWithNulls {
	template <class FUN, class RESULT_TYPE, class... ARGS>
	static inline RESULT_TYPE Operation(FUN &fun, ValidityMask &mask, idx_t idx, ARGS... args) {
		return fun(args..., mask, idx);
	}
};

template <class OP>
struct VariadicStandardOperatorWrapper {
	template <class FUN, class RESULT_TYPE, class... ARGS>
	static inline RESULT_TYPE Operation(FUN &fun, ValidityMask &mask, idx_t idx, ARGS... args) {
		return OP::template Operation<ARGS..., RESULT_TYPE>(args...);
	}
};

//! VariadicExecutor: a unified executor for any number of input vectors.
//! Uses C++17 fold expressions and std::index_sequence to generalize the
//! pattern shared by TernaryExecutor, SenaryExecutor, and SeptenaryExecutor.
//!
//! Template parameter ordering: <RESULT_TYPE, INPUT_TYPES...>
//! This differs from TernaryExecutor's <INPUT_TYPES..., RESULT_TYPE> because
//! a parameter pack must be last (or followed only by deducible params).
//! The named executors (TernaryExecutor, etc.) provide backward-compatible APIs.
struct VariadicExecutor {
	using VectorRef = std::reference_wrapper<Vector>;

private:
	template <size_t N, size_t... Is>
	static std::array<VectorRef, N> MakeInputArrayImpl(DataChunk &input, std::index_sequence<Is...>) {
		return {{std::ref(input.data[Is])...}};
	}

	template <size_t N>
	static std::array<VectorRef, N> MakeInputArray(DataChunk &input) {
		D_ASSERT(input.ColumnCount() >= N);
		return MakeInputArrayImpl<N>(input, std::make_index_sequence<N> {});
	}

	template <size_t N>
	static bool AllConstant(const std::array<VectorRef, N> &inputs) {
		for (size_t i = 0; i < N; i++) {
			if (inputs[i].get().GetVectorType() != VectorType::CONSTANT_VECTOR) {
				return false;
			}
		}
		return true;
	}

	template <size_t N>
	static bool AnyConstantNull(const std::array<VectorRef, N> &inputs) {
		for (size_t i = 0; i < N; i++) {
			if (ConstantVector::IsNull(inputs[i].get())) {
				return true;
			}
		}
		return false;
	}

	//-------------------------------------------------------------------
	// Execute implementation
	//-------------------------------------------------------------------
	template <class RESULT_TYPE, class OPWRAPPER, class FUN, class... ARGS, size_t... Is>
	static void ExecuteImplWithIndices(std::array<VectorRef, sizeof...(ARGS)> &inputs, Vector &result, idx_t count,
	                                   FUN fun, std::index_sequence<Is...>) {
		constexpr size_t N = sizeof...(ARGS);

		if (AllConstant<N>(inputs)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			if (AnyConstantNull<N>(inputs)) {
				ConstantVector::SetNull(result, true);
			} else {
				auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);
				result_data[0] = OPWRAPPER::template Operation<FUN, RESULT_TYPE, ARGS...>(
				    fun, ConstantVector::Validity(result), 0,
				    *ConstantVector::GetData<ARGS>(inputs[Is].get())...);
			}
		} else {
			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
			auto &result_validity = FlatVector::Validity(result);

			std::array<UnifiedVectorFormat, N> vdata;
			for (size_t i = 0; i < N; i++) {
				inputs[i].get().ToUnifiedFormat(count, vdata[i]);
			}

			auto data_ptrs = std::make_tuple(UnifiedVectorFormat::GetData<ARGS>(vdata[Is])...);

			if ((... || vdata[Is].validity.CanHaveNull())) {
				for (idx_t i = 0; i < count; i++) {
					std::array<idx_t, N> idxs = {{vdata[Is].sel->get_index(i)...}};
					if ((... && vdata[Is].validity.RowIsValid(idxs[Is]))) {
						result_data[i] = OPWRAPPER::template Operation<FUN, RESULT_TYPE, ARGS...>(
						    fun, result_validity, i, std::get<Is>(data_ptrs)[idxs[Is]]...);
					} else {
						result_validity.SetInvalid(i);
					}
				}
			} else {
				for (idx_t i = 0; i < count; i++) {
					result_data[i] = OPWRAPPER::template Operation<FUN, RESULT_TYPE, ARGS...>(
					    fun, result_validity, i, std::get<Is>(data_ptrs)[vdata[Is].sel->get_index(i)]...);
				}
			}
		}
	}

	//-------------------------------------------------------------------
	// Select implementation
	//-------------------------------------------------------------------
	template <class OP, bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL, class... ARGS, size_t... Is>
	static idx_t SelectLoopImpl(std::tuple<const ARGS *...> &data_ptrs,
	                            std::array<UnifiedVectorFormat, sizeof...(ARGS)> &vdata,
	                            const SelectionVector *result_sel, idx_t count, SelectionVector *true_sel,
	                            SelectionVector *false_sel, std::index_sequence<Is...>) {
		constexpr size_t N = sizeof...(ARGS);
		idx_t true_count = 0, false_count = 0;
		for (idx_t i = 0; i < count; i++) {
			auto result_idx = result_sel->get_index(i);
			std::array<idx_t, N> idxs = {{vdata[Is].sel->get_index(i)...}};
			bool comparison_result = (NO_NULL || (... && vdata[Is].validity.RowIsValid(idxs[Is]))) &&
			                         OP::Operation(std::get<Is>(data_ptrs)[idxs[Is]]...);
			if (HAS_TRUE_SEL) {
				true_sel->set_index(true_count, result_idx);
				true_count += comparison_result;
			}
			if (HAS_FALSE_SEL) {
				false_sel->set_index(false_count, result_idx);
				false_count += !comparison_result;
			}
		}
		if (HAS_TRUE_SEL) {
			return true_count;
		}
		return count - false_count;
	}

	template <class OP, bool NO_NULL, class... ARGS, size_t... Is>
	static idx_t SelectLoopSelSwitch(std::tuple<const ARGS *...> &data_ptrs,
	                                 std::array<UnifiedVectorFormat, sizeof...(ARGS)> &vdata,
	                                 const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                                 SelectionVector *false_sel, std::index_sequence<Is...> indices) {
		if (true_sel && false_sel) {
			return SelectLoopImpl<OP, NO_NULL, true, true, ARGS...>(data_ptrs, vdata, sel, count, true_sel, false_sel,
			                                                        indices);
		} else if (true_sel) {
			return SelectLoopImpl<OP, NO_NULL, true, false, ARGS...>(data_ptrs, vdata, sel, count, true_sel, false_sel,
			                                                         indices);
		} else {
			D_ASSERT(false_sel);
			return SelectLoopImpl<OP, NO_NULL, false, true, ARGS...>(data_ptrs, vdata, sel, count, true_sel, false_sel,
			                                                         indices);
		}
	}

	template <class OP, class... ARGS, size_t... Is>
	static idx_t SelectImplWithIndices(std::array<VectorRef, sizeof...(ARGS)> &inputs, const SelectionVector *sel,
	                                   idx_t count, SelectionVector *true_sel, SelectionVector *false_sel,
	                                   std::index_sequence<Is...> indices) {
		constexpr size_t N = sizeof...(ARGS);
		if (!sel) {
			sel = FlatVector::IncrementalSelectionVector();
		}

		std::array<UnifiedVectorFormat, N> vdata;
		for (size_t i = 0; i < N; i++) {
			inputs[i].get().ToUnifiedFormat(count, vdata[i]);
		}

		auto data_ptrs = std::make_tuple(UnifiedVectorFormat::GetData<ARGS>(vdata[Is])...);

		if ((... || vdata[Is].validity.CanHaveNull())) {
			return SelectLoopSelSwitch<OP, false, ARGS...>(data_ptrs, vdata, sel, count, true_sel, false_sel, indices);
		} else {
			return SelectLoopSelSwitch<OP, true, ARGS...>(data_ptrs, vdata, sel, count, true_sel, false_sel, indices);
		}
	}

public:
	//-------------------------------------------------------------------
	// Execute: lambda-based, with Vector array
	//-------------------------------------------------------------------
	template <class RESULT_TYPE, class... ARGS, class FUN>
	static void Execute(std::array<VectorRef, sizeof...(ARGS)> inputs, Vector &result, idx_t count, FUN fun) {
		ExecuteImplWithIndices<RESULT_TYPE, VariadicLambdaWrapper, FUN, ARGS...>(inputs, result, count, fun,
		                                                                         std::index_sequence_for<ARGS...> {});
	}

	//-------------------------------------------------------------------
	// Execute: lambda-based, with DataChunk
	//-------------------------------------------------------------------
	template <class RESULT_TYPE, class... ARGS, class FUN>
	static void Execute(DataChunk &input, Vector &result, FUN fun) {
		auto inputs = MakeInputArray<sizeof...(ARGS)>(input);
		ExecuteImplWithIndices<RESULT_TYPE, VariadicLambdaWrapper, FUN, ARGS...>(inputs, result, input.size(), fun,
		                                                                         std::index_sequence_for<ARGS...> {});
	}

	//-------------------------------------------------------------------
	// ExecuteStandard: static OP::Operation, with Vector array
	//-------------------------------------------------------------------
	template <class RESULT_TYPE, class OP, class... ARGS>
	static void ExecuteStandard(std::array<VectorRef, sizeof...(ARGS)> inputs, Vector &result, idx_t count) {
		bool dummy = false;
		ExecuteImplWithIndices<RESULT_TYPE, VariadicStandardOperatorWrapper<OP>, bool, ARGS...>(
		    inputs, result, count, dummy, std::index_sequence_for<ARGS...> {});
	}

	//-------------------------------------------------------------------
	// ExecuteWithNulls: lambda receives (args..., ValidityMask&, idx_t)
	//-------------------------------------------------------------------
	template <class RESULT_TYPE, class... ARGS, class FUN>
	static void ExecuteWithNulls(std::array<VectorRef, sizeof...(ARGS)> inputs, Vector &result, idx_t count, FUN fun) {
		ExecuteImplWithIndices<RESULT_TYPE, VariadicLambdaWrapperWithNulls, FUN, ARGS...>(
		    inputs, result, count, fun, std::index_sequence_for<ARGS...> {});
	}

	//-------------------------------------------------------------------
	// Select: OP::Operation returns bool, with Vector array
	//-------------------------------------------------------------------
	template <class OP, class... ARGS>
	static idx_t Select(std::array<VectorRef, sizeof...(ARGS)> inputs, const SelectionVector *sel, idx_t count,
	                    SelectionVector *true_sel, SelectionVector *false_sel) {
		return SelectImplWithIndices<OP, ARGS...>(inputs, sel, count, true_sel, false_sel,
		                                          std::index_sequence_for<ARGS...> {});
	}
};

} // namespace duckdb
