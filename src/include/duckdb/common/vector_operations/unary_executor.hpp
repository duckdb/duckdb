//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/unary_executor.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/function_errors.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/smaller_binary.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector_operations/scalar_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <functional>
#include <type_traits>

namespace duckdb {

struct UnaryOperatorWrapper {
	template <class OP, class INPUT_TYPE, class RESULT_TYPE, class DATA_TYPE>
	static inline RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, DATA_TYPE &data) {
		return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input);
	}
};

struct UnaryLambdaWrapper {
	template <class FUNC, class INPUT_TYPE, class RESULT_TYPE, class DATA_TYPE>
	static inline RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, DATA_TYPE &data) {
		if constexpr (std::is_same<decltype(data(input)), optional<RESULT_TYPE>>::value) {
			auto result = data(input);
			if (!result.has_value()) {
				mask.SetInvalid(idx);
				return RESULT_TYPE();
			}
			return result.value();
		} else {
			return data(input);
		}
	}
};

struct GenericUnaryWrapper {
	template <class OP, class INPUT_TYPE, class RESULT_TYPE, class DATA_TYPE>
	static inline RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, DATA_TYPE &data) {
		return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, mask, idx, data);
	}
};

template <class OP>
struct UnaryStringOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, StringHeap &heap) {
		return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, heap);
	}
};

template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class DATA_TYPE, bool CAN_ADD_NULLS>
struct UnaryScalarAdapter {
	static constexpr bool ADDS_NULLS = CAN_ADD_NULLS;

	explicit UnaryScalarAdapter(DATA_TYPE &data_p) : data(data_p) {
	}

	inline RESULT_TYPE Operation(ValidityMask &mask, idx_t idx, INPUT_TYPE input) {
		return OPWRAPPER::template Operation<OP, INPUT_TYPE, RESULT_TYPE>(input, mask, idx, data);
	}

	DATA_TYPE &data;
};

template <class INPUT_TYPE, class FUNC>
struct UnarySelectAdapter {
	explicit UnarySelectAdapter(FUNC &fun_p) : fun(fun_p) {
	}

	inline bool Operation(INPUT_TYPE input) {
		return fun(input);
	}

	inline bool OperationNoNull(INPUT_TYPE input) {
		return Operation(input);
	}

	FUNC &fun;
};

struct UnaryExecutor {
private:
	struct ExecutePolicy {
#if !DUCKDB_SMALLER_BINARY(unary_executor_flat)
		static constexpr bool SPECIALIZE_FLAT = true;
#else
		static constexpr bool SPECIALIZE_FLAT = false;
#endif
		static constexpr bool SPECIALIZE_NULLABLE_GENERIC_SELECTIONS = false;
		static constexpr bool SPECIALIZE_NON_NULL_GENERIC_SELECTIONS = false;
		static constexpr bool PRESERVE_RESULT_VALIDITY = true;
	};

	struct SelectPolicy {
#if !DUCKDB_SMALLER_BINARY(unary_executor_select_flat)
		static constexpr uint64_t SPECIALIZED_MASKS = 1;
#else
		static constexpr uint64_t SPECIALIZED_MASKS = 0;
#endif
#if !DUCKDB_SMALLER_BINARY(unary_executor_select_flags)
		static constexpr bool SPECIALIZE_OUTPUTS = true;
#else
		static constexpr bool SPECIALIZE_OUTPUTS = false;
#endif
		static constexpr uint64_t DIRECT_TRUE_FLAT_MASKS = 0;
	};

	template <bool ADDS_NULLS, class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class DATA_TYPE>
	static inline void ExecuteInternal(const Vector &input, Vector &result, idx_t count, DATA_TYPE &data,
	                                   FunctionErrors errors) {
		UnaryScalarAdapter<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, DATA_TYPE, ADDS_NULLS> adapter(data);

#if !DUCKDB_SMALLER_BINARY(unary_executor_flat)
		if (input.GetVectorType() == VectorType::DICTIONARY_VECTOR && errors == FunctionErrors::CANNOT_ERROR) {
			static constexpr idx_t DICTIONARY_THRESHOLD = 2;
			auto dictionary_size = DictionaryVector::DictionarySize(input);
			if (dictionary_size.IsValid() && dictionary_size.GetIndex() * DICTIONARY_THRESHOLD <= count) {
				auto &dictionary_values = DictionaryVector::Child(input);
				if (dictionary_values.GetVectorType() == VectorType::FLAT_VECTOR) {
					std::array<ScalarExecutor::VectorRef, 1> dictionary_input = {{dictionary_values}};
					ScalarExecutor::Execute<ExecutePolicy, RESULT_TYPE, decltype(adapter), INPUT_TYPE>(
					    dictionary_input, result, dictionary_size.GetIndex(), adapter);
					auto &offsets = DictionaryVector::SelVector(input);
					FlatVector::SetSize(result, dictionary_size.GetIndex());
					result.Dictionary(result, dictionary_size.GetIndex(), offsets, count);
					return;
				}
			}
		}
#endif

		std::array<ScalarExecutor::VectorRef, 1> inputs = {{input}};
		ScalarExecutor::Execute<ExecutePolicy, RESULT_TYPE, decltype(adapter), INPUT_TYPE>(inputs, result, count,
		                                                                                   adapter);
	}

public:
	template <class INPUT_TYPE, class RESULT_TYPE, class OP>
	static void Execute(const Vector &input, Vector &result, idx_t count) {
		std::nullptr_t no_data = nullptr;
		ExecuteInternal<false, INPUT_TYPE, RESULT_TYPE, UnaryOperatorWrapper, OP>(
		    input, result, count, no_data, FunctionErrors::CAN_THROW_RUNTIME_ERROR);
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class FUNC = std::function<RESULT_TYPE(INPUT_TYPE)>>
	static void Execute(const Vector &input, Vector &result, idx_t count, FUNC fun,
	                    FunctionErrors errors = FunctionErrors::CAN_THROW_RUNTIME_ERROR) {
		constexpr bool adds_nulls =
		    std::is_same<std::invoke_result_t<FUNC &, INPUT_TYPE>, optional<RESULT_TYPE>>::value;
		ExecuteInternal<adds_nulls, INPUT_TYPE, RESULT_TYPE, UnaryLambdaWrapper, FUNC>(input, result, count, fun,
		                                                                               errors);
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OP, class DATA_TYPE>
	static void GenericExecute(const Vector &input, Vector &result, idx_t count, DATA_TYPE &data, bool = false) {
		// Generic operations own the result mask so they can invalidate rows at runtime.
		ExecuteInternal<true, INPUT_TYPE, RESULT_TYPE, GenericUnaryWrapper, OP>(
		    input, result, count, data, FunctionErrors::CAN_THROW_RUNTIME_ERROR);
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OP>
	static void ExecuteString(const Vector &input, Vector &result, idx_t count) {
		auto &heap = StringVector::GetStringHeap(result);
		GenericExecute<INPUT_TYPE, RESULT_TYPE, UnaryStringOperator<OP>>(input, result, count, heap);
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OP>
	static void Execute(const Vector &input, Vector &result) {
		Execute<INPUT_TYPE, RESULT_TYPE, OP>(input, result, input.size());
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class FUNC = std::function<RESULT_TYPE(INPUT_TYPE)>>
	static void Execute(const Vector &input, Vector &result, FUNC fun,
	                    FunctionErrors errors = FunctionErrors::CAN_THROW_RUNTIME_ERROR) {
		Execute<INPUT_TYPE, RESULT_TYPE, FUNC>(input, result, input.size(), fun, errors);
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OP, class DATA_TYPE>
	static void GenericExecute(const Vector &input, Vector &result, DATA_TYPE &data, bool adds_nulls = false) {
		GenericExecute<INPUT_TYPE, RESULT_TYPE, OP, DATA_TYPE>(input, result, input.size(), data, adds_nulls);
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OP>
	static void ExecuteString(const Vector &input, Vector &result) {
		ExecuteString<INPUT_TYPE, RESULT_TYPE, OP>(input, result, input.size());
	}

	template <class INPUT_TYPE, class FUNC = std::function<bool(INPUT_TYPE)>>
	static idx_t Select(const Vector &input, const SelectionVector *sel, idx_t count, FUNC fun,
	                    SelectionVector *true_sel, SelectionVector *false_sel) {
		std::array<ScalarExecutor::VectorRef, 1> inputs = {{input}};
		UnarySelectAdapter<INPUT_TYPE, FUNC> adapter(fun);
		return ScalarExecutor::Select<SelectPolicy, decltype(adapter), INPUT_TYPE>(inputs, sel, count, true_sel,
		                                                                           false_sel, adapter);
	}
};

} // namespace duckdb
