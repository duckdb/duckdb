//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/scalar_executor.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

#include <array>
#include <functional>
#include <tuple>
#include <type_traits>

namespace duckdb {

//! Internal execution engine shared by the named scalar executor facades.
struct ScalarExecutor {
	using VectorRef = std::reference_wrapper<const Vector>;

private:
	struct InputProfile {
		uint64_t constant_mask = 0;
		bool all_constant = true;
		bool all_flat_or_constant = true;
		bool any_constant_null = false;
	};

	template <size_t N, size_t... Is>
	static inline InputProfile GetInputProfile(const std::array<VectorRef, N> &inputs, std::index_sequence<Is...>) {
		InputProfile result;
		auto classify = [&](auto input_index) {
			constexpr idx_t INPUT_INDEX = decltype(input_index)::value;
			auto vector_type = inputs[INPUT_INDEX].get().GetVectorType();
			if (vector_type == VectorType::CONSTANT_VECTOR) {
				if constexpr (INPUT_INDEX < 64) {
					result.constant_mask |= uint64_t(1) << INPUT_INDEX;
				}
				result.any_constant_null |= ConstantVector::IsNull(inputs[INPUT_INDEX].get());
			} else {
				result.all_constant = false;
				result.all_flat_or_constant &= vector_type == VectorType::FLAT_VECTOR;
			}
		};
		(classify(std::integral_constant<idx_t, Is> {}), ...);
		return result;
	}

	template <idx_t INPUT_INDEX, uint64_t CONSTANT_MASK>
	static inline idx_t InputIndex(idx_t row) {
		if constexpr (CONSTANT_MASK & (uint64_t(1) << INPUT_INDEX)) {
			return 0;
		} else {
			return row;
		}
	}

	template <bool ADDS_NULLS, bool PRESERVE_RESULT_VALIDITY, uint64_t CONSTANT_MASK, size_t N, size_t... Is>
	static inline ValidityMask &PrepareFlatResultValidity(const std::array<VectorRef, N> &inputs, Vector &result,
	                                                      idx_t count, std::index_sequence<Is...>) {
		auto &result_validity = FlatVector::ValidityMutable(result);
		bool initialized = false;
		auto combine = [&](auto input_index) {
			constexpr idx_t INPUT_INDEX = decltype(input_index)::value;
			if constexpr (!(CONSTANT_MASK & (uint64_t(1) << INPUT_INDEX))) {
				auto &input_validity = FlatVector::Validity(inputs[INPUT_INDEX].get());
				if (input_validity.CanHaveNull()) {
					if (!initialized) {
						if constexpr (ADDS_NULLS) {
							result_validity.Copy(input_validity, count);
						} else {
							result_validity.Initialize(input_validity);
						}
						initialized = true;
					} else {
						result_validity.Combine(input_validity, count);
					}
				}
			}
		};
		(combine(std::integral_constant<idx_t, Is> {}), ...);
		if (!initialized) {
			if constexpr (PRESERVE_RESULT_VALIDITY) {
				if constexpr (ADDS_NULLS) {
					ValidityMask preserved(result_validity, count);
					result_validity.Initialize(preserved);
				}
			} else {
				result_validity.Reset(count);
			}
		}
		return result_validity;
	}

	template <uint64_t CONSTANT_MASK, size_t N, size_t... Is>
	static inline ValidityMask PrepareFlatInputValidity(const std::array<VectorRef, N> &inputs, idx_t count,
	                                                    std::index_sequence<Is...>) {
		ValidityMask result(count);
		bool initialized = false;
		auto combine = [&](auto input_index) {
			constexpr idx_t INPUT_INDEX = decltype(input_index)::value;
			if constexpr (!(CONSTANT_MASK & (uint64_t(1) << INPUT_INDEX))) {
				auto &input_validity = FlatVector::Validity(inputs[INPUT_INDEX].get());
				if (!initialized) {
					result.Initialize(input_validity);
					initialized = true;
				} else {
					result.Combine(input_validity, count);
				}
			}
		};
		(combine(std::integral_constant<idx_t, Is> {}), ...);
		return result;
	}

	DUCKDB_API static bool PrepareGenericResultValidity(const UnifiedVectorFormat *formats, idx_t format_count,
	                                                    Vector &result, idx_t count, bool preserve_result_validity,
	                                                    bool adds_nulls, ValidityMask &input_validity);

	template <bool PRESERVE_RESULT_VALIDITY, uint64_t CONSTANT_MASK, class RESULT_TYPE, class ADAPTER, class... ARGS,
	          size_t... Is>
	static void ExecuteFlat(const std::array<VectorRef, sizeof...(ARGS)> &inputs, Vector &result, idx_t count,
	                        ADAPTER &adapter, std::index_sequence<Is...> indices) {
		result.SetVectorType(VectorType::FLAT_VECTOR);
		if (result.size() != count) {
			FlatVector::SetSize(result, count);
		}
		auto result_data = FlatVector::GetDataMutable<RESULT_TYPE>(result);
		auto input_data = std::make_tuple(FlatVector::GetData<ARGS>(inputs[Is].get())...);
		auto &result_validity = PrepareFlatResultValidity<ADAPTER::ADDS_NULLS, PRESERVE_RESULT_VALIDITY, CONSTANT_MASK>(
		    inputs, result, count, indices);

#ifdef DEBUG
		auto assert_restrict = [&](auto input_index) {
			constexpr idx_t INPUT_INDEX = decltype(input_index)::value;
			if constexpr (!(CONSTANT_MASK & (uint64_t(1) << INPUT_INDEX))) {
				auto data = std::get<INPUT_INDEX>(input_data);
				ASSERT_RESTRICT(data, data + count, result_data, result_data + count);
			}
		};
		(assert_restrict(std::integral_constant<idx_t, Is> {}), ...);
#endif

		if (result_validity.CanHaveNull()) {
			idx_t base_idx = 0;
			auto entry_count = ValidityMask::EntryCount(count);
			for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
				auto validity_entry = result_validity.GetValidityEntry(entry_idx);
				auto next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
				if (ValidityMask::AllValid(validity_entry)) {
					for (; base_idx < next; base_idx++) {
						result_data[base_idx] =
						    adapter.Operation(result_validity, base_idx,
						                      std::get<Is>(input_data)[InputIndex<Is, CONSTANT_MASK>(base_idx)]...);
					}
				} else if (ValidityMask::NoneValid(validity_entry)) {
					base_idx = next;
				} else {
					auto start = base_idx;
					for (; base_idx < next; base_idx++) {
						if (ValidityMask::RowIsValid(validity_entry, base_idx - start)) {
							result_data[base_idx] =
							    adapter.Operation(result_validity, base_idx,
							                      std::get<Is>(input_data)[InputIndex<Is, CONSTANT_MASK>(base_idx)]...);
						}
					}
				}
			}
		} else {
			for (idx_t row = 0; row < count; row++) {
				result_data[row] = adapter.Operation(result_validity, row,
				                                     std::get<Is>(input_data)[InputIndex<Is, CONSTANT_MASK>(row)]...);
			}
		}
	}

	template <class RESULT_TYPE, class ADAPTER, class... ARGS, size_t... Is>
	static void ExecuteConstant(const std::array<VectorRef, sizeof...(ARGS)> &inputs, Vector &result, idx_t count,
	                            bool input_is_null, ADAPTER &adapter, std::index_sequence<Is...>) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		if (result.size() != count) {
			FlatVector::SetSize(result, count);
		}
		if (input_is_null) {
			ConstantVector::SetNull(result, true);
			return;
		}
		auto &result_validity = ConstantVector::Validity(result);
		result_validity.SetValid(0);
		auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);
		result_data[0] = adapter.Operation(result_validity, 0, *ConstantVector::GetData<ARGS>(inputs[Is].get())...);
	}

	template <idx_t INPUT_INDEX, uint64_t SELECTION_MASK>
	static inline idx_t GenericInputIndex(const std::array<const sel_t *, 2> &selections, idx_t row) {
		if constexpr (SELECTION_MASK & (uint64_t(1) << INPUT_INDEX)) {
			return selections[INPUT_INDEX][row];
		}
		return row;
	}

	template <uint64_t SELECTION_MASK, class RESULT_TYPE, class ADAPTER, class... ARGS, size_t... Is>
	static void ExecuteGenericNoNullLoop(const std::tuple<const ARGS *...> &input_data,
	                                     const std::array<const sel_t *, 2> &selections, RESULT_TYPE *result_data,
	                                     idx_t count, ValidityMask &result_validity, ADAPTER &adapter,
	                                     std::index_sequence<Is...>) {
		auto result_ptr = result_data;
		auto result_end = result_data + count;
		idx_t result_idx = 0;
		while (result_ptr != result_end) {
			*result_ptr = adapter.Operation(
			    result_validity, result_idx,
			    std::get<Is>(input_data)[GenericInputIndex<Is, SELECTION_MASK>(selections, result_idx)]...);
			result_ptr++;
			result_idx++;
		}
	}

	template <class RESULT_TYPE, class ADAPTER, class... ARGS, size_t... Is>
	static void ExecuteGenericNoNullSwitch(const std::tuple<const ARGS *...> &input_data,
	                                       const std::array<UnifiedVectorFormat, 2> &formats, RESULT_TYPE *result_data,
	                                       idx_t count, ValidityMask &result_validity, ADAPTER &adapter,
	                                       std::index_sequence<Is...> indices) {
		std::array<const sel_t *, 2> selections = {{formats[0].sel->data(), formats[1].sel->data()}};
		uint64_t selection_mask = 0;
		selection_mask |= selections[0] ? 1 : 0;
		selection_mask |= selections[1] ? 2 : 0;
		switch (selection_mask) {
		case 0:
			ExecuteGenericNoNullLoop<0>(input_data, selections, result_data, count, result_validity, adapter, indices);
			return;
		case 1:
			ExecuteGenericNoNullLoop<1>(input_data, selections, result_data, count, result_validity, adapter, indices);
			return;
		case 2:
			ExecuteGenericNoNullLoop<2>(input_data, selections, result_data, count, result_validity, adapter, indices);
			return;
		case 3:
			ExecuteGenericNoNullLoop<3>(input_data, selections, result_data, count, result_validity, adapter, indices);
			return;
		default:
			throw InternalException("Invalid generic scalar executor selection profile");
		}
	}

	template <bool SPECIALIZE_GENERIC_SELECTIONS, bool PRESERVE_RESULT_VALIDITY, class RESULT_TYPE, class ADAPTER,
	          class... ARGS, size_t... Is>
	static void ExecuteGeneric(const std::array<VectorRef, sizeof...(ARGS)> &inputs, Vector &result, idx_t count,
	                           ADAPTER &adapter, std::index_sequence<Is...>) {
		constexpr idx_t N = sizeof...(ARGS);
		std::array<UnifiedVectorFormat, N> formats;
		for (idx_t i = 0; i < N; i++) {
			inputs[i].get().ToUnifiedFormat(formats[i]);
		}
		auto input_data = std::make_tuple(UnifiedVectorFormat::GetData<ARGS>(formats[Is])...);
		result.SetVectorType(VectorType::FLAT_VECTOR);
		if (result.size() != count) {
			FlatVector::SetSize(result, count);
		}
		auto result_data = FlatVector::GetDataMutable<RESULT_TYPE>(result);
		auto &result_validity = FlatVector::ValidityMutable(result);
		ValidityMask input_validity(count);
		auto inputs_can_have_null = PrepareGenericResultValidity(
		    formats.data(), N, result, count, PRESERVE_RESULT_VALIDITY, ADAPTER::ADDS_NULLS, input_validity);

		if (inputs_can_have_null) {
			for (idx_t row = 0; row < count; row++) {
				std::array<idx_t, N> input_indices = {{formats[Is].sel->get_index(row)...}};
				if (input_validity.RowIsValid(row)) {
					result_data[row] =
					    adapter.Operation(result_validity, row, std::get<Is>(input_data)[input_indices[Is]]...);
				} else {
					result_validity.SetInvalid(row);
				}
			}
		} else if constexpr (SPECIALIZE_GENERIC_SELECTIONS && N == 2) {
			ExecuteGenericNoNullSwitch(input_data, formats, result_data, count, result_validity, adapter,
			                           std::index_sequence_for<ARGS...> {});
		} else {
			for (idx_t row = 0; row < count; row++) {
				result_data[row] = adapter.Operation(result_validity, row,
				                                     std::get<Is>(input_data)[formats[Is].sel->get_index(row)]...);
			}
		}
	}

	template <bool SPECIALIZE_FLAT, bool SPECIALIZE_GENERIC_SELECTIONS, bool PRESERVE_RESULT_VALIDITY,
	          class RESULT_TYPE, class ADAPTER, class... ARGS, size_t... Is>
	static void ExecuteInternal(const std::array<VectorRef, sizeof...(ARGS)> &inputs, Vector &result, idx_t count,
	                            ADAPTER &adapter, std::index_sequence<Is...> indices) {
		constexpr idx_t N = sizeof...(ARGS);
		static_assert(N > 0, "ScalarExecutor requires at least one input");
		auto profile = GetInputProfile(inputs, indices);
		if (profile.all_constant) {
			ExecuteConstant<RESULT_TYPE, ADAPTER, ARGS...>(inputs, result, count, profile.any_constant_null, adapter,
			                                               indices);
			return;
		}
		if constexpr (SPECIALIZE_FLAT && N <= 3) {
			if (profile.all_flat_or_constant) {
				if (profile.any_constant_null) {
					result.SetVectorType(VectorType::CONSTANT_VECTOR);
					if (result.size() != count) {
						FlatVector::SetSize(result, count);
					}
					ConstantVector::SetNull(result, true);
					return;
				}
				switch (profile.constant_mask) {
				case 0:
					ExecuteFlat<PRESERVE_RESULT_VALIDITY, 0, RESULT_TYPE, ADAPTER, ARGS...>(inputs, result, count,
					                                                                        adapter, indices);
					return;
				case 1:
					if constexpr (N >= 2) {
						ExecuteFlat<PRESERVE_RESULT_VALIDITY, 1, RESULT_TYPE, ADAPTER, ARGS...>(inputs, result, count,
						                                                                        adapter, indices);
						return;
					}
					break;
				case 2:
					if constexpr (N >= 2) {
						ExecuteFlat<PRESERVE_RESULT_VALIDITY, 2, RESULT_TYPE, ADAPTER, ARGS...>(inputs, result, count,
						                                                                        adapter, indices);
						return;
					}
					break;
				case 3:
					if constexpr (N >= 3) {
						ExecuteFlat<PRESERVE_RESULT_VALIDITY, 3, RESULT_TYPE, ADAPTER, ARGS...>(inputs, result, count,
						                                                                        adapter, indices);
						return;
					}
					break;
				case 4:
					if constexpr (N >= 3) {
						ExecuteFlat<PRESERVE_RESULT_VALIDITY, 4, RESULT_TYPE, ADAPTER, ARGS...>(inputs, result, count,
						                                                                        adapter, indices);
						return;
					}
					break;
				case 5:
					if constexpr (N >= 3) {
						ExecuteFlat<PRESERVE_RESULT_VALIDITY, 5, RESULT_TYPE, ADAPTER, ARGS...>(inputs, result, count,
						                                                                        adapter, indices);
						return;
					}
					break;
				case 6:
					if constexpr (N >= 3) {
						ExecuteFlat<PRESERVE_RESULT_VALIDITY, 6, RESULT_TYPE, ADAPTER, ARGS...>(inputs, result, count,
						                                                                        adapter, indices);
						return;
					}
					break;
				default:
					throw InternalException("Invalid flat/constant scalar executor profile");
				}
			}
		}
		ExecuteGeneric<SPECIALIZE_GENERIC_SELECTIONS, PRESERVE_RESULT_VALIDITY, RESULT_TYPE, ADAPTER, ARGS...>(
		    inputs, result, count, adapter, indices);
	}

	template <bool SPECIALIZE_OUTPUTS, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
	static inline void StoreSelection(bool comparison_result, idx_t result_idx, SelectionVector *true_sel,
	                                  SelectionVector *false_sel, idx_t &true_count, idx_t &false_count) {
		if constexpr (SPECIALIZE_OUTPUTS) {
			if constexpr (HAS_TRUE_SEL) {
				true_sel->set_index(true_count, result_idx);
				true_count += comparison_result;
			}
			if constexpr (HAS_FALSE_SEL) {
				false_sel->set_index(false_count, result_idx);
				false_count += !comparison_result;
			}
		} else {
			if (true_sel) {
				true_sel->set_index(true_count, result_idx);
				true_count += comparison_result;
			}
			if (false_sel) {
				false_sel->set_index(false_count, result_idx);
				false_count += !comparison_result;
			}
		}
	}

	static idx_t FillConstantSelection(bool comparison_result, const SelectionVector &sel, idx_t count,
	                                   SelectionVector *true_sel, SelectionVector *false_sel) {
		if (comparison_result) {
			if (true_sel) {
				for (idx_t i = 0; i < count; i++) {
					true_sel->set_index(i, sel.get_index(i));
				}
			}
			return count;
		}
		if (false_sel) {
			for (idx_t i = 0; i < count; i++) {
				false_sel->set_index(i, sel.get_index(i));
			}
		}
		return 0;
	}

	template <class ADAPTER, class... ARGS, size_t... Is>
	static idx_t SelectConstant(const std::array<VectorRef, sizeof...(ARGS)> &inputs, const SelectionVector &sel,
	                            idx_t count, SelectionVector *true_sel, SelectionVector *false_sel, ADAPTER &adapter,
	                            std::index_sequence<Is...>) {
		bool comparison_result = adapter.Operation(*ConstantVector::GetData<ARGS>(inputs[Is].get())...);
		return FillConstantSelection(comparison_result, sel, count, true_sel, false_sel);
	}

	template <uint64_t CONSTANT_MASK, bool SPECIALIZE_OUTPUTS, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL, class ADAPTER,
	          class... ARGS, size_t... Is>
	static idx_t SelectFlatLoop(const std::tuple<const ARGS *...> &input_data, const ValidityMask &input_validity,
	                            const SelectionVector &sel, idx_t count, SelectionVector *true_sel,
	                            SelectionVector *false_sel, ADAPTER &adapter, std::index_sequence<Is...>) {
		idx_t true_count = 0;
		idx_t false_count = 0;
		idx_t base_idx = 0;
		auto entry_count = ValidityMask::EntryCount(count);
		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			auto validity_entry = input_validity.GetValidityEntry(entry_idx);
			auto next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
			if (ValidityMask::AllValid(validity_entry)) {
				for (; base_idx < next; base_idx++) {
					auto result_idx = sel.get_index(base_idx);
					bool comparison_result =
					    adapter.Operation(std::get<Is>(input_data)[InputIndex<Is, CONSTANT_MASK>(base_idx)]...);
					StoreSelection<SPECIALIZE_OUTPUTS, HAS_TRUE_SEL, HAS_FALSE_SEL>(
					    comparison_result, result_idx, true_sel, false_sel, true_count, false_count);
				}
			} else if (ValidityMask::NoneValid(validity_entry)) {
				if constexpr (SPECIALIZE_OUTPUTS) {
					if constexpr (HAS_FALSE_SEL) {
						for (; base_idx < next; base_idx++) {
							false_sel->set_index(false_count++, sel.get_index(base_idx));
						}
					} else {
						base_idx = next;
					}
				} else {
					if (false_sel) {
						for (; base_idx < next; base_idx++) {
							false_sel->set_index(false_count++, sel.get_index(base_idx));
						}
					} else {
						base_idx = next;
					}
				}
			} else {
				auto start = base_idx;
				for (; base_idx < next; base_idx++) {
					auto result_idx = sel.get_index(base_idx);
					bool comparison_result =
					    ValidityMask::RowIsValid(validity_entry, base_idx - start) &&
					    adapter.Operation(std::get<Is>(input_data)[InputIndex<Is, CONSTANT_MASK>(base_idx)]...);
					StoreSelection<SPECIALIZE_OUTPUTS, HAS_TRUE_SEL, HAS_FALSE_SEL>(
					    comparison_result, result_idx, true_sel, false_sel, true_count, false_count);
				}
			}
		}
		if constexpr (SPECIALIZE_OUTPUTS && HAS_TRUE_SEL) {
			return true_count;
		}
		return true_sel ? true_count : count - false_count;
	}

	template <uint64_t CONSTANT_MASK, bool SPECIALIZE_OUTPUTS, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL, class ADAPTER,
	          class... ARGS, size_t... Is>
	static idx_t SelectFlat(const std::array<VectorRef, sizeof...(ARGS)> &inputs, const SelectionVector &sel,
	                        idx_t count, SelectionVector *true_sel, SelectionVector *false_sel, ADAPTER &adapter,
	                        std::index_sequence<Is...> indices) {
		auto input_data = std::make_tuple(FlatVector::GetData<ARGS>(inputs[Is].get())...);
		auto input_validity = PrepareFlatInputValidity<CONSTANT_MASK>(inputs, count, indices);
		return SelectFlatLoop<CONSTANT_MASK, SPECIALIZE_OUTPUTS, HAS_TRUE_SEL, HAS_FALSE_SEL>(
		    input_data, input_validity, sel, count, true_sel, false_sel, adapter, indices);
	}

	template <uint64_t CONSTANT_MASK, bool SPECIALIZE_OUTPUTS, class ADAPTER, class... ARGS, size_t... Is>
	static idx_t SelectFlatSwitch(const std::array<VectorRef, sizeof...(ARGS)> &inputs, const SelectionVector &sel,
	                              idx_t count, SelectionVector *true_sel, SelectionVector *false_sel, ADAPTER &adapter,
	                              std::index_sequence<Is...> indices) {
		if constexpr (SPECIALIZE_OUTPUTS) {
			if (true_sel && false_sel) {
				return SelectFlat<CONSTANT_MASK, true, true, true, ADAPTER, ARGS...>(inputs, sel, count, true_sel,
				                                                                     false_sel, adapter, indices);
			} else if (true_sel) {
				return SelectFlat<CONSTANT_MASK, true, true, false, ADAPTER, ARGS...>(inputs, sel, count, true_sel,
				                                                                      false_sel, adapter, indices);
			}
			return SelectFlat<CONSTANT_MASK, true, false, true, ADAPTER, ARGS...>(inputs, sel, count, true_sel,
			                                                                      false_sel, adapter, indices);
		}
		return SelectFlat<CONSTANT_MASK, false, false, false, ADAPTER, ARGS...>(inputs, sel, count, true_sel, false_sel,
		                                                                        adapter, indices);
	}

	template <bool NO_NULL, bool SPECIALIZE_OUTPUTS, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL, class ADAPTER,
	          class... ARGS, size_t... Is>
	static idx_t SelectGenericLoop(std::tuple<const ARGS *...> &input_data,
	                               std::array<UnifiedVectorFormat, sizeof...(ARGS)> &formats,
	                               const SelectionVector &sel, idx_t count, SelectionVector *true_sel,
	                               SelectionVector *false_sel, ADAPTER &adapter, std::index_sequence<Is...>) {
		constexpr idx_t N = sizeof...(ARGS);
		idx_t true_count = 0;
		idx_t false_count = 0;
		for (idx_t row = 0; row < count; row++) {
			auto result_idx = sel.get_index(row);
			std::array<idx_t, N> input_indices = {{formats[Is].sel->get_index(row)...}};
			bool comparison_result = (NO_NULL || (... && formats[Is].validity.RowIsValid(input_indices[Is]))) &&
			                         adapter.Operation(std::get<Is>(input_data)[input_indices[Is]]...);
			StoreSelection<SPECIALIZE_OUTPUTS, HAS_TRUE_SEL, HAS_FALSE_SEL>(comparison_result, result_idx, true_sel,
			                                                                false_sel, true_count, false_count);
		}
		if constexpr (SPECIALIZE_OUTPUTS && HAS_TRUE_SEL) {
			return true_count;
		}
		return true_sel ? true_count : count - false_count;
	}

	template <bool NO_NULL, bool SPECIALIZE_OUTPUTS, class ADAPTER, class... ARGS, size_t... Is>
	static idx_t SelectGenericSwitch(std::tuple<const ARGS *...> &input_data,
	                                 std::array<UnifiedVectorFormat, sizeof...(ARGS)> &formats,
	                                 const SelectionVector &sel, idx_t count, SelectionVector *true_sel,
	                                 SelectionVector *false_sel, ADAPTER &adapter, std::index_sequence<Is...> indices) {
		if constexpr (SPECIALIZE_OUTPUTS) {
			if (true_sel && false_sel) {
				return SelectGenericLoop<NO_NULL, true, true, true>(input_data, formats, sel, count, true_sel,
				                                                    false_sel, adapter, indices);
			} else if (true_sel) {
				return SelectGenericLoop<NO_NULL, true, true, false>(input_data, formats, sel, count, true_sel,
				                                                     false_sel, adapter, indices);
			}
			return SelectGenericLoop<NO_NULL, true, false, true>(input_data, formats, sel, count, true_sel, false_sel,
			                                                     adapter, indices);
		}
		return SelectGenericLoop<NO_NULL, false, false, false>(input_data, formats, sel, count, true_sel, false_sel,
		                                                       adapter, indices);
	}

	template <bool SPECIALIZE_OUTPUTS, class ADAPTER, class... ARGS, size_t... Is>
	static idx_t SelectGeneric(const std::array<VectorRef, sizeof...(ARGS)> &inputs, const SelectionVector &sel,
	                           idx_t count, SelectionVector *true_sel, SelectionVector *false_sel, ADAPTER &adapter,
	                           std::index_sequence<Is...> indices) {
		std::array<UnifiedVectorFormat, sizeof...(ARGS)> formats;
		for (idx_t i = 0; i < sizeof...(ARGS); i++) {
			inputs[i].get().ToUnifiedFormat(formats[i]);
		}
		auto input_data = std::make_tuple(UnifiedVectorFormat::GetData<ARGS>(formats[Is])...);
		if ((... || formats[Is].validity.CanHaveNull())) {
			return SelectGenericSwitch<false, SPECIALIZE_OUTPUTS>(input_data, formats, sel, count, true_sel, false_sel,
			                                                      adapter, indices);
		}
		return SelectGenericSwitch<true, SPECIALIZE_OUTPUTS>(input_data, formats, sel, count, true_sel, false_sel,
		                                                     adapter, indices);
	}

	template <uint64_t SPECIALIZED_MASKS, bool SPECIALIZE_OUTPUTS, class ADAPTER, class... ARGS, size_t... Is>
	static idx_t SelectInternal(const std::array<VectorRef, sizeof...(ARGS)> &inputs, const SelectionVector &sel,
	                            idx_t count, SelectionVector *true_sel, SelectionVector *false_sel, ADAPTER &adapter,
	                            std::index_sequence<Is...> indices) {
		constexpr idx_t N = sizeof...(ARGS);
		auto profile = GetInputProfile(inputs, indices);
		if (profile.all_constant) {
			if (profile.any_constant_null) {
				return FillConstantSelection(false, sel, count, true_sel, false_sel);
			}
			return SelectConstant<ADAPTER, ARGS...>(inputs, sel, count, true_sel, false_sel, adapter, indices);
		}
		if constexpr (SPECIALIZED_MASKS != 0 && N <= 3) {
			if (profile.all_flat_or_constant) {
				if (profile.any_constant_null) {
					return FillConstantSelection(false, sel, count, true_sel, false_sel);
				}
				switch (profile.constant_mask) {
				case 0:
					if constexpr (SPECIALIZED_MASKS & (uint64_t(1) << 0)) {
						return SelectFlatSwitch<0, SPECIALIZE_OUTPUTS, ADAPTER, ARGS...>(inputs, sel, count, true_sel,
						                                                                 false_sel, adapter, indices);
					}
					break;
				case 1:
					if constexpr (N >= 2 && (SPECIALIZED_MASKS & (uint64_t(1) << 1))) {
						return SelectFlatSwitch<1, SPECIALIZE_OUTPUTS, ADAPTER, ARGS...>(inputs, sel, count, true_sel,
						                                                                 false_sel, adapter, indices);
					}
					break;
				case 2:
					if constexpr (N >= 2 && (SPECIALIZED_MASKS & (uint64_t(1) << 2))) {
						return SelectFlatSwitch<2, SPECIALIZE_OUTPUTS, ADAPTER, ARGS...>(inputs, sel, count, true_sel,
						                                                                 false_sel, adapter, indices);
					}
					break;
				case 3:
					if constexpr (N >= 3 && (SPECIALIZED_MASKS & (uint64_t(1) << 3))) {
						return SelectFlatSwitch<3, SPECIALIZE_OUTPUTS, ADAPTER, ARGS...>(inputs, sel, count, true_sel,
						                                                                 false_sel, adapter, indices);
					}
					break;
				case 4:
					if constexpr (N >= 3 && (SPECIALIZED_MASKS & (uint64_t(1) << 4))) {
						return SelectFlatSwitch<4, SPECIALIZE_OUTPUTS, ADAPTER, ARGS...>(inputs, sel, count, true_sel,
						                                                                 false_sel, adapter, indices);
					}
					break;
				case 5:
					if constexpr (N >= 3 && (SPECIALIZED_MASKS & (uint64_t(1) << 5))) {
						return SelectFlatSwitch<5, SPECIALIZE_OUTPUTS, ADAPTER, ARGS...>(inputs, sel, count, true_sel,
						                                                                 false_sel, adapter, indices);
					}
					break;
				case 6:
					if constexpr (N >= 3 && (SPECIALIZED_MASKS & (uint64_t(1) << 6))) {
						return SelectFlatSwitch<6, SPECIALIZE_OUTPUTS, ADAPTER, ARGS...>(inputs, sel, count, true_sel,
						                                                                 false_sel, adapter, indices);
					}
					break;
				default:
					throw InternalException("Invalid flat/constant scalar selection profile");
				}
			}
		}
		return SelectGeneric<SPECIALIZE_OUTPUTS, ADAPTER, ARGS...>(inputs, sel, count, true_sel, false_sel, adapter,
		                                                           indices);
	}

public:
	template <bool SPECIALIZE_FLAT, bool SPECIALIZE_GENERIC_SELECTIONS, bool PRESERVE_RESULT_VALIDITY,
	          class RESULT_TYPE, class ADAPTER, class... ARGS>
	static void Execute(const std::array<VectorRef, sizeof...(ARGS)> &inputs, Vector &result, idx_t count,
	                    ADAPTER &adapter) {
		ExecuteInternal<SPECIALIZE_FLAT, SPECIALIZE_GENERIC_SELECTIONS, PRESERVE_RESULT_VALIDITY, RESULT_TYPE, ADAPTER,
		                ARGS...>(inputs, result, count, adapter, std::index_sequence_for<ARGS...> {});
	}

	template <uint64_t SPECIALIZED_MASKS, bool SPECIALIZE_OUTPUTS, class ADAPTER, class... ARGS>
	static idx_t Select(const std::array<VectorRef, sizeof...(ARGS)> &inputs, const SelectionVector *sel, idx_t count,
	                    SelectionVector *true_sel, SelectionVector *false_sel, ADAPTER &adapter) {
		if (!true_sel && !false_sel) {
			throw InternalException("Either true or false sel must be set");
		}
		if (!sel) {
			sel = FlatVector::IncrementalSelectionVector();
		}
		return SelectInternal<SPECIALIZED_MASKS, SPECIALIZE_OUTPUTS, ADAPTER, ARGS...>(
		    inputs, *sel, count, true_sel, false_sel, adapter, std::index_sequence_for<ARGS...> {});
	}

	template <class... ARGS, size_t... Is>
	static bool InputsCanHaveNull(const std::array<VectorRef, sizeof...(ARGS)> &inputs, std::index_sequence<Is...>) {
		std::array<UnifiedVectorFormat, sizeof...(ARGS)> formats;
		for (idx_t i = 0; i < sizeof...(ARGS); i++) {
			inputs[i].get().ToUnifiedFormat(formats[i]);
		}
		return (... || formats[Is].validity.CanHaveNull());
	}

	template <class... ARGS>
	static bool InputsCanHaveNull(const std::array<VectorRef, sizeof...(ARGS)> &inputs) {
		return InputsCanHaveNull<ARGS...>(inputs, std::index_sequence_for<ARGS...> {});
	}
};

} // namespace duckdb
