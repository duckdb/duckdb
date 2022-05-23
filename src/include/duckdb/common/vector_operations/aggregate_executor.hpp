//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/aggregate_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {
struct FunctionData;
typedef std::pair<idx_t, idx_t> FrameBounds;

class AggregateExecutor {
private:
	template <class STATE_TYPE, class OP>
	static inline void NullaryFlatLoop(STATE_TYPE **__restrict states, FunctionData *bind_data, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			OP::template Operation<STATE_TYPE, OP>(states[i], bind_data, i);
		}
	}

	template <class STATE_TYPE, class OP>
	static inline void NullaryScatterLoop(STATE_TYPE **__restrict states, FunctionData *bind_data,
	                                      const SelectionVector &ssel, idx_t count) {

		for (idx_t i = 0; i < count; i++) {
			auto sidx = ssel.get_index(i);
			OP::template Operation<STATE_TYPE, OP>(states[sidx], bind_data, sidx);
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryFlatLoop(INPUT_TYPE *__restrict idata, FunctionData *bind_data,
	                                 STATE_TYPE **__restrict states, ValidityMask &mask, idx_t count) {
		if (!mask.AllValid()) {
			idx_t base_idx = 0;
			auto entry_count = ValidityMask::EntryCount(count);
			for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
				auto validity_entry = mask.GetValidityEntry(entry_idx);
				idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
				if (!OP::IgnoreNull() || ValidityMask::AllValid(validity_entry)) {
					// all valid: perform operation
					for (; base_idx < next; base_idx++) {
						OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[base_idx], bind_data, idata, mask,
						                                                   base_idx);
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
							OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[base_idx], bind_data, idata, mask,
							                                                   base_idx);
						}
					}
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[i], bind_data, idata, mask, i);
			}
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryScatterLoop(INPUT_TYPE *__restrict idata, FunctionData *bind_data,
	                                    STATE_TYPE **__restrict states, const SelectionVector &isel,
	                                    const SelectionVector &ssel, ValidityMask &mask, idx_t count) {
		if (OP::IgnoreNull() && !mask.AllValid()) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				auto idx = isel.get_index(i);
				auto sidx = ssel.get_index(i);
				if (mask.RowIsValid(idx)) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[sidx], bind_data, idata, mask, idx);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				auto idx = isel.get_index(i);
				auto sidx = ssel.get_index(i);
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[sidx], bind_data, idata, mask, idx);
			}
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryFlatUpdateLoop(INPUT_TYPE *__restrict idata, FunctionData *bind_data,
	                                       STATE_TYPE *__restrict state, idx_t count, ValidityMask &mask) {
		idx_t base_idx = 0;
		auto entry_count = ValidityMask::EntryCount(count);
		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			auto validity_entry = mask.GetValidityEntry(entry_idx);
			idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
			if (!OP::IgnoreNull() || ValidityMask::AllValid(validity_entry)) {
				// all valid: perform operation
				for (; base_idx < next; base_idx++) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(state, bind_data, idata, mask, base_idx);
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
						OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(state, bind_data, idata, mask, base_idx);
					}
				}
			}
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryUpdateLoop(INPUT_TYPE *__restrict idata, FunctionData *bind_data,
	                                   STATE_TYPE *__restrict state, idx_t count, ValidityMask &mask,
	                                   const SelectionVector &__restrict sel_vector) {
		if (OP::IgnoreNull() && !mask.AllValid()) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel_vector.get_index(i);
				if (mask.RowIsValid(idx)) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(state, bind_data, idata, mask, idx);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel_vector.get_index(i);
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(state, bind_data, idata, mask, idx);
			}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static inline void BinaryScatterLoop(A_TYPE *__restrict adata, FunctionData *bind_data, B_TYPE *__restrict bdata,
	                                     STATE_TYPE **__restrict states, idx_t count, const SelectionVector &asel,
	                                     const SelectionVector &bsel, const SelectionVector &ssel,
	                                     ValidityMask &avalidity, ValidityMask &bvalidity) {
		if (OP::IgnoreNull() && (!avalidity.AllValid() || !bvalidity.AllValid())) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				auto sidx = ssel.get_index(i);
				if (avalidity.RowIsValid(aidx) && bvalidity.RowIsValid(bidx)) {
					OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(states[sidx], bind_data, adata, bdata,
					                                                       avalidity, bvalidity, aidx, bidx);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				auto sidx = ssel.get_index(i);
				OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(states[sidx], bind_data, adata, bdata, avalidity,
				                                                       bvalidity, aidx, bidx);
			}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static inline void BinaryUpdateLoop(A_TYPE *__restrict adata, FunctionData *bind_data, B_TYPE *__restrict bdata,
	                                    STATE_TYPE *__restrict state, idx_t count, const SelectionVector &asel,
	                                    const SelectionVector &bsel, ValidityMask &avalidity, ValidityMask &bvalidity) {
		if (OP::IgnoreNull() && (!avalidity.AllValid() || !bvalidity.AllValid())) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				if (avalidity.RowIsValid(aidx) && bvalidity.RowIsValid(bidx)) {
					OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(state, bind_data, adata, bdata, avalidity,
					                                                       bvalidity, aidx, bidx);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(state, bind_data, adata, bdata, avalidity,
				                                                       bvalidity, aidx, bidx);
			}
		}
	}

public:
	template <class STATE_TYPE, class OP>
	static void NullaryScatter(Vector &states, FunctionData *bind_data, idx_t count) {
		if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			OP::template ConstantOperation<STATE_TYPE, OP>(*sdata, bind_data, count);
		} else if (states.GetVectorType() == VectorType::FLAT_VECTOR) {
			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			NullaryFlatLoop<STATE_TYPE, OP>(sdata, bind_data, count);
		} else {
			VectorData sdata;
			states.Orrify(count, sdata);
			NullaryScatterLoop<STATE_TYPE, OP>((STATE_TYPE **)sdata.data, bind_data, *sdata.sel, count);
		}
	}

	template <class STATE_TYPE, class OP>
	static void NullaryUpdate(data_ptr_t state, FunctionData *bind_data, idx_t count) {
		OP::template ConstantOperation<STATE_TYPE, OP>((STATE_TYPE *)state, bind_data, count);
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static void UnaryScatter(Vector &input, Vector &states, FunctionData *bind_data, idx_t count) {
		if (input.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			if (OP::IgnoreNull() && ConstantVector::IsNull(input)) {
				// constant NULL input in function that ignores NULL values
				return;
			}
			// regular constant: get first state
			auto idata = ConstantVector::GetData<INPUT_TYPE>(input);
			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			OP::template ConstantOperation<INPUT_TYPE, STATE_TYPE, OP>(*sdata, bind_data, idata,
			                                                           ConstantVector::Validity(input), count);
		} else if (input.GetVectorType() == VectorType::FLAT_VECTOR &&
		           states.GetVectorType() == VectorType::FLAT_VECTOR) {
			auto idata = FlatVector::GetData<INPUT_TYPE>(input);
			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			UnaryFlatLoop<STATE_TYPE, INPUT_TYPE, OP>(idata, bind_data, sdata, FlatVector::Validity(input), count);
		} else {
			VectorData idata, sdata;
			input.Orrify(count, idata);
			states.Orrify(count, sdata);
			UnaryScatterLoop<STATE_TYPE, INPUT_TYPE, OP>((INPUT_TYPE *)idata.data, bind_data, (STATE_TYPE **)sdata.data,
			                                             *idata.sel, *sdata.sel, idata.validity, count);
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static void UnaryUpdate(Vector &input, FunctionData *bind_data, data_ptr_t state, idx_t count) {
		switch (input.GetVectorType()) {
		case VectorType::CONSTANT_VECTOR: {
			if (OP::IgnoreNull() && ConstantVector::IsNull(input)) {
				return;
			}
			auto idata = ConstantVector::GetData<INPUT_TYPE>(input);
			OP::template ConstantOperation<INPUT_TYPE, STATE_TYPE, OP>((STATE_TYPE *)state, bind_data, idata,
			                                                           ConstantVector::Validity(input), count);
			break;
		}
		case VectorType::FLAT_VECTOR: {
			auto idata = FlatVector::GetData<INPUT_TYPE>(input);
			UnaryFlatUpdateLoop<STATE_TYPE, INPUT_TYPE, OP>(idata, bind_data, (STATE_TYPE *)state, count,
			                                                FlatVector::Validity(input));
			break;
		}
		default: {
			VectorData idata;
			input.Orrify(count, idata);
			UnaryUpdateLoop<STATE_TYPE, INPUT_TYPE, OP>((INPUT_TYPE *)idata.data, bind_data, (STATE_TYPE *)state, count,
			                                            idata.validity, *idata.sel);
			break;
		}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryScatter(FunctionData *bind_data, Vector &a, Vector &b, Vector &states, idx_t count) {
		VectorData adata, bdata, sdata;

		a.Orrify(count, adata);
		b.Orrify(count, bdata);
		states.Orrify(count, sdata);

		BinaryScatterLoop<STATE_TYPE, A_TYPE, B_TYPE, OP>((A_TYPE *)adata.data, bind_data, (B_TYPE *)bdata.data,
		                                                  (STATE_TYPE **)sdata.data, count, *adata.sel, *bdata.sel,
		                                                  *sdata.sel, adata.validity, bdata.validity);
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryUpdate(FunctionData *bind_data, Vector &a, Vector &b, data_ptr_t state, idx_t count) {
		VectorData adata, bdata;

		a.Orrify(count, adata);
		b.Orrify(count, bdata);

		BinaryUpdateLoop<STATE_TYPE, A_TYPE, B_TYPE, OP>((A_TYPE *)adata.data, bind_data, (B_TYPE *)bdata.data,
		                                                 (STATE_TYPE *)state, count, *adata.sel, *bdata.sel,
		                                                 adata.validity, bdata.validity);
	}

	template <class STATE_TYPE, class OP>
	static void Combine(Vector &source, Vector &target, FunctionData *bind_data, idx_t count) {
		D_ASSERT(source.GetType().id() == LogicalTypeId::POINTER && target.GetType().id() == LogicalTypeId::POINTER);
		auto sdata = FlatVector::GetData<const STATE_TYPE *>(source);
		auto tdata = FlatVector::GetData<STATE_TYPE *>(target);

		for (idx_t i = 0; i < count; i++) {
			OP::template Combine<STATE_TYPE, OP>(*sdata[i], tdata[i], bind_data);
		}
	}

	template <class STATE_TYPE, class RESULT_TYPE, class OP>
	static void Finalize(Vector &states, FunctionData *bind_data, Vector &result, idx_t count, idx_t offset) {
		if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);

			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			auto rdata = ConstantVector::GetData<RESULT_TYPE>(result);
			OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, bind_data, *sdata, rdata,
			                                               ConstantVector::Validity(result), 0);
		} else {
			D_ASSERT(states.GetVectorType() == VectorType::FLAT_VECTOR);
			result.SetVectorType(VectorType::FLAT_VECTOR);

			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
			for (idx_t i = 0; i < count; i++) {
				OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, bind_data, sdata[i], rdata,
				                                               FlatVector::Validity(result), i + offset);
			}
		}
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
	static void UnaryWindow(Vector &input, const ValidityMask &ifilter, FunctionData *bind_data, data_ptr_t state,
	                        const FrameBounds &frame, const FrameBounds &prev, Vector &result, idx_t rid, idx_t bias) {

		auto idata = FlatVector::GetData<const INPUT_TYPE>(input) - bias;
		const auto &ivalid = FlatVector::Validity(input);
		OP::template Window<STATE, INPUT_TYPE, RESULT_TYPE>(idata, ifilter, ivalid, bind_data, (STATE *)state, frame,
		                                                    prev, result, rid, bias);
	}

	template <class STATE_TYPE, class OP>
	static void Destroy(Vector &states, idx_t count) {
		auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
		for (idx_t i = 0; i < count; i++) {
			OP::template Destroy<STATE_TYPE>(sdata[i]);
		}
	}
};

} // namespace duckdb
