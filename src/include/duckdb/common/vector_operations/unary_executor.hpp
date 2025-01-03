//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/unary_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/enums/function_errors.hpp"

#include <functional>

namespace duckdb {

struct UnaryOperatorWrapper {
	template <class OP, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input);
	}
};

struct UnaryLambdaWrapper {
	template <class FUNC, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto fun = (FUNC *)dataptr;
		return (*fun)(input);
	}
};

struct GenericUnaryWrapper {
	template <class OP, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, mask, idx, dataptr);
	}
};

struct UnaryLambdaWrapperWithNulls {
	template <class FUNC, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto fun = (FUNC *)dataptr;
		return (*fun)(input, mask, idx);
	}
};

template <class OP>
struct UnaryStringOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto vector = reinterpret_cast<Vector *>(dataptr);
		return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, *vector);
	}
};

struct UnaryExecutor {
private:
	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP>
	static inline void ExecuteLoop(const INPUT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data, idx_t count,
	                               const SelectionVector *__restrict sel_vector, ValidityMask &mask,
	                               ValidityMask &result_mask, void *dataptr, bool adds_nulls) {
#ifdef DEBUG
		// ldata may point to a compressed dictionary buffer which can be smaller than ldata + count
		idx_t max_index = 0;
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel_vector->get_index(i);
			max_index = MaxValue(max_index, idx);
		}
		ASSERT_RESTRICT(ldata, ldata + max_index, result_data, result_data + count);
#endif

		if (!mask.AllValid()) {
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel_vector->get_index(i);
				if (mask.RowIsValidUnsafe(idx)) {
					result_data[i] =
					    OPWRAPPER::template Operation<OP, INPUT_TYPE, RESULT_TYPE>(ldata[idx], result_mask, i, dataptr);
				} else {
					result_mask.SetInvalid(i);
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel_vector->get_index(i);
				result_data[i] =
				    OPWRAPPER::template Operation<OP, INPUT_TYPE, RESULT_TYPE>(ldata[idx], result_mask, i, dataptr);
			}
		}
	}

#ifndef DUCKDB_SMALLER_BINARY
	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP>
	static inline void ExecuteFlat(const INPUT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data, idx_t count,
	                               ValidityMask &mask, ValidityMask &result_mask, void *dataptr, bool adds_nulls) {
		ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);

		if (!mask.AllValid()) {
			if (!adds_nulls) {
				result_mask.Initialize(mask);
			} else {
				result_mask.Copy(mask, count);
			}
			idx_t base_idx = 0;
			auto entry_count = ValidityMask::EntryCount(count);
			for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
				auto validity_entry = mask.GetValidityEntry(entry_idx);
				idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
				if (ValidityMask::AllValid(validity_entry)) {
					// all valid: perform operation
					for (; base_idx < next; base_idx++) {
						result_data[base_idx] = OPWRAPPER::template Operation<OP, INPUT_TYPE, RESULT_TYPE>(
						    ldata[base_idx], result_mask, base_idx, dataptr);
					}
				} else if (ValidityMask::NoneValid(validity_entry)) {
					// nothing valid: skip all
					base_idx = next;
					continue;
				} else {
					// partially valid: need to check individual elements for validity
					idx_t start = base_idx;
					for (; base_idx < next; base_idx++) {
						if (ValidityMask::RowIsValid(validity_entry, base_idx - start)) {
							D_ASSERT(mask.RowIsValid(base_idx));
							result_data[base_idx] = OPWRAPPER::template Operation<OP, INPUT_TYPE, RESULT_TYPE>(
							    ldata[base_idx], result_mask, base_idx, dataptr);
						}
					}
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				result_data[i] =
				    OPWRAPPER::template Operation<OP, INPUT_TYPE, RESULT_TYPE>(ldata[i], result_mask, i, dataptr);
			}
		}
	}
#endif

	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP>
	static inline void ExecuteStandard(Vector &input, Vector &result, idx_t count, void *dataptr, bool adds_nulls,
	                                   FunctionErrors errors = FunctionErrors::CAN_THROW_RUNTIME_ERROR) {
		switch (input.GetVectorType()) {
		case VectorType::CONSTANT_VECTOR: {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);
			auto ldata = ConstantVector::GetData<INPUT_TYPE>(input);

			if (ConstantVector::IsNull(input)) {
				ConstantVector::SetNull(result, true);
			} else {
				ConstantVector::SetNull(result, false);
				*result_data = OPWRAPPER::template Operation<OP, INPUT_TYPE, RESULT_TYPE>(
				    *ldata, ConstantVector::Validity(result), 0, dataptr);
			}
			break;
		}
#ifndef DUCKDB_SMALLER_BINARY
		case VectorType::FLAT_VECTOR: {
			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
			auto ldata = FlatVector::GetData<INPUT_TYPE>(input);

			ExecuteFlat<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP>(ldata, result_data, count, FlatVector::Validity(input),
			                                                    FlatVector::Validity(result), dataptr, adds_nulls);
			break;
		}
		case VectorType::DICTIONARY_VECTOR: {
			// dictionary vector - we can run the function ONLY on the dictionary in some cases
			// we can only do this if the function does not throw errors
			// we can execute the function on a value that is in the dictionary but that is not referenced
			// if the function can throw errors - this will result in us (incorrectly) throwing an error
			if (errors == FunctionErrors::CANNOT_ERROR) {
				static constexpr idx_t DICTIONARY_THRESHOLD = 2;
				auto dict_size = DictionaryVector::DictionarySize(input);
				if (dict_size.IsValid() && dict_size.GetIndex() * DICTIONARY_THRESHOLD <= count) {
					// we can operate directly on the dictionary if we have a dictionary size
					// but this only makes sense if the dictionary size is smaller than the count by some factor
					auto &dictionary_values = DictionaryVector::Child(input);
					if (dictionary_values.GetVectorType() == VectorType::FLAT_VECTOR) {
						// execute the function over the dictionary
						auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
						auto ldata = FlatVector::GetData<INPUT_TYPE>(dictionary_values);
						ExecuteFlat<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP>(
						    ldata, result_data, dict_size.GetIndex(), FlatVector::Validity(dictionary_values),
						    FlatVector::Validity(result), dataptr, adds_nulls);
						// slice the result with the original offsets
						auto &offsets = DictionaryVector::SelVector(input);
						result.Dictionary(result, dict_size.GetIndex(), offsets, count);
						break;
					}
				}
			}
			DUCKDB_EXPLICIT_FALLTHROUGH;
		}
#endif
		default: {
			UnifiedVectorFormat vdata;
			input.ToUnifiedFormat(count, vdata);

			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
			auto ldata = UnifiedVectorFormat::GetData<INPUT_TYPE>(vdata);

			ExecuteLoop<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP>(ldata, result_data, count, vdata.sel, vdata.validity,
			                                                    FlatVector::Validity(result), dataptr, adds_nulls);
			break;
		}
		}
	}

public:
	template <class INPUT_TYPE, class RESULT_TYPE, class OP>
	static void Execute(Vector &input, Vector &result, idx_t count) {
		ExecuteStandard<INPUT_TYPE, RESULT_TYPE, UnaryOperatorWrapper, OP>(input, result, count, nullptr, false);
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class FUNC = std::function<RESULT_TYPE(INPUT_TYPE)>>
	static void Execute(Vector &input, Vector &result, idx_t count, FUNC fun,
	                    FunctionErrors errors = FunctionErrors::CAN_THROW_RUNTIME_ERROR) {
		ExecuteStandard<INPUT_TYPE, RESULT_TYPE, UnaryLambdaWrapper, FUNC>(
		    input, result, count, reinterpret_cast<void *>(&fun), false, errors);
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OP>
	static void GenericExecute(Vector &input, Vector &result, idx_t count, void *dataptr, bool adds_nulls = false) {
		ExecuteStandard<INPUT_TYPE, RESULT_TYPE, GenericUnaryWrapper, OP>(input, result, count, dataptr, adds_nulls);
	}

	template <class INPUT_TYPE, class RESULT_TYPE,
	          class FUNC = std::function<RESULT_TYPE(INPUT_TYPE, ValidityMask &, idx_t)>>
	static void ExecuteWithNulls(Vector &input, Vector &result, idx_t count, FUNC fun) {
		ExecuteStandard<INPUT_TYPE, RESULT_TYPE, UnaryLambdaWrapperWithNulls, FUNC>(input, result, count, (void *)&fun,
		                                                                            true);
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OP>
	static void ExecuteString(Vector &input, Vector &result, idx_t count) {
		UnaryExecutor::GenericExecute<INPUT_TYPE, RESULT_TYPE, UnaryStringOperator<OP>>(input, result, count,
		                                                                                (void *)&result);
	}

private:
	// Select logic copied from TernaryExecutor, but with a lambda instead of a static functor
	template <class INPUT_TYPE, class FUNC = std::function<bool(INPUT_TYPE)>, bool NO_NULL, bool HAS_TRUE_SEL,
	          bool HAS_FALSE_SEL>
	static inline idx_t SelectLoop(const INPUT_TYPE *__restrict input_data, const SelectionVector *result_sel,
	                               const idx_t count, FUNC fun, const SelectionVector &input_sel,
	                               ValidityMask &input_validity, SelectionVector *true_sel,
	                               SelectionVector *false_sel) {
		idx_t true_count = 0, false_count = 0;
		for (idx_t i = 0; i < count; i++) {
			const auto result_idx = result_sel->get_index(i);
			const auto idx = input_sel.get_index(i);
			const bool comparison_result = (NO_NULL || input_validity.RowIsValid(idx)) && fun(input_data[idx]);
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
		} else {
			return count - false_count;
		}
	}

	template <class INPUT_TYPE, class FUNC = std::function<bool(INPUT_TYPE)>, bool NO_NULL>
	static inline idx_t SelectLoopSelSwitch(UnifiedVectorFormat &input_data, const SelectionVector *sel,
	                                        const idx_t count, FUNC fun, SelectionVector *true_sel,
	                                        SelectionVector *false_sel) {
		if (true_sel && false_sel) {
			return SelectLoop<INPUT_TYPE, FUNC, NO_NULL, true, true>(
			    UnifiedVectorFormat::GetData<INPUT_TYPE>(input_data), sel, count, fun, *input_data.sel,
			    input_data.validity, true_sel, false_sel);
		} else if (true_sel) {
			return SelectLoop<INPUT_TYPE, FUNC, NO_NULL, true, false>(
			    UnifiedVectorFormat::GetData<INPUT_TYPE>(input_data), sel, count, fun, *input_data.sel,
			    input_data.validity, true_sel, false_sel);
		} else {
			D_ASSERT(false_sel);
			return SelectLoop<INPUT_TYPE, FUNC, NO_NULL, false, true>(
			    UnifiedVectorFormat::GetData<INPUT_TYPE>(input_data), sel, count, fun, *input_data.sel,
			    input_data.validity, true_sel, false_sel);
		}
	}

	template <class INPUT_TYPE, class FUNC = std::function<bool(INPUT_TYPE)>>
	static inline idx_t SelectLoopSwitch(UnifiedVectorFormat &input_data, const SelectionVector *sel, const idx_t count,
	                                     FUNC fun, SelectionVector *true_sel, SelectionVector *false_sel) {
		if (!input_data.validity.AllValid()) {
			return SelectLoopSelSwitch<INPUT_TYPE, FUNC, false>(input_data, sel, count, fun, true_sel, false_sel);
		} else {
			return SelectLoopSelSwitch<INPUT_TYPE, FUNC, true>(input_data, sel, count, fun, true_sel, false_sel);
		}
	}

public:
	template <class INPUT_TYPE, class FUNC = std::function<bool(INPUT_TYPE)>>
	static idx_t Select(Vector &input, const SelectionVector *sel, const idx_t count, FUNC fun,
	                    SelectionVector *true_sel, SelectionVector *false_sel) {
		if (!sel) {
			sel = FlatVector::IncrementalSelectionVector();
		}
		UnifiedVectorFormat input_data;
		input.ToUnifiedFormat(count, input_data);

		return SelectLoopSwitch<INPUT_TYPE, FUNC>(input_data, sel, count, fun, true_sel, false_sel);
	}
};

} // namespace duckdb
