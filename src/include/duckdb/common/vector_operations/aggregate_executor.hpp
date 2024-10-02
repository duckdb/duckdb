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
#include "duckdb/function/aggregate_state.hpp"

namespace duckdb {

// structs
struct AggregateInputData;

// The bounds of a window frame
struct FrameBounds {
	FrameBounds() : start(0), end(0) {};
	FrameBounds(idx_t start, idx_t end) : start(start), end(end) {};
	idx_t start = 0;
	idx_t end = 0;
};

// A set of window subframes for windowed EXCLUDE
using SubFrames = vector<FrameBounds>;

class AggregateExecutor {
private:
	template <class STATE_TYPE, class OP>
	static inline void NullaryFlatLoop(STATE_TYPE **__restrict states, AggregateInputData &aggr_input_data,
	                                   idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			OP::template Operation<STATE_TYPE, OP>(*states[i], aggr_input_data, i);
		}
	}

	template <class STATE_TYPE, class OP>
	static inline void NullaryScatterLoop(STATE_TYPE **__restrict states, AggregateInputData &aggr_input_data,
	                                      const SelectionVector &ssel, idx_t count) {

		for (idx_t i = 0; i < count; i++) {
			auto sidx = ssel.get_index(i);
			OP::template Operation<STATE_TYPE, OP>(*states[sidx], aggr_input_data, sidx);
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryFlatLoop(const INPUT_TYPE *__restrict idata, AggregateInputData &aggr_input_data,
	                                 STATE_TYPE **__restrict states, ValidityMask &mask, idx_t count) {
		if (OP::IgnoreNull() && !mask.AllValid()) {
			AggregateUnaryInput input(aggr_input_data, mask);
			auto &base_idx = input.input_idx;
			base_idx = 0;
			auto entry_count = ValidityMask::EntryCount(count);
			for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
				auto validity_entry = mask.GetValidityEntry(entry_idx);
				idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
				if (ValidityMask::AllValid(validity_entry)) {
					// all valid: perform operation
					for (; base_idx < next; base_idx++) {
						OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*states[base_idx], idata[base_idx], input);
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
							OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*states[base_idx], idata[base_idx],
							                                                   input);
						}
					}
				}
			}
		} else {
			AggregateUnaryInput input(aggr_input_data, mask);
			auto &i = input.input_idx;
			for (i = 0; i < count; i++) {
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*states[i], idata[i], input);
			}
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryScatterLoop(const INPUT_TYPE *__restrict idata, AggregateInputData &aggr_input_data,
	                                    STATE_TYPE **__restrict states, const SelectionVector &isel,
	                                    const SelectionVector &ssel, ValidityMask &mask, idx_t count) {
		if (OP::IgnoreNull() && !mask.AllValid()) {
			// potential NULL values and NULL values are ignored
			AggregateUnaryInput input(aggr_input_data, mask);
			for (idx_t i = 0; i < count; i++) {
				input.input_idx = isel.get_index(i);
				auto sidx = ssel.get_index(i);
				if (mask.RowIsValid(input.input_idx)) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*states[sidx], idata[input.input_idx], input);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			AggregateUnaryInput input(aggr_input_data, mask);
			for (idx_t i = 0; i < count; i++) {
				input.input_idx = isel.get_index(i);
				auto sidx = ssel.get_index(i);
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*states[sidx], idata[input.input_idx], input);
			}
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryFlatUpdateLoop(const INPUT_TYPE *__restrict idata, AggregateInputData &aggr_input_data,
	                                       STATE_TYPE *__restrict state, idx_t count, ValidityMask &mask) {
		AggregateUnaryInput input(aggr_input_data, mask);
		auto &base_idx = input.input_idx;
		base_idx = 0;
		auto entry_count = ValidityMask::EntryCount(count);
		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			auto validity_entry = mask.GetValidityEntry(entry_idx);
			idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
			if (!OP::IgnoreNull() || ValidityMask::AllValid(validity_entry)) {
				// all valid: perform operation
				for (; base_idx < next; base_idx++) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*state, idata[base_idx], input);
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
						OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*state, idata[base_idx], input);
					}
				}
			}
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryUpdateLoop(const INPUT_TYPE *__restrict idata, AggregateInputData &aggr_input_data,
	                                   STATE_TYPE *__restrict state, idx_t count, ValidityMask &mask,
	                                   const SelectionVector &__restrict sel_vector) {
		AggregateUnaryInput input(aggr_input_data, mask);
		if (OP::IgnoreNull() && !mask.AllValid()) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				input.input_idx = sel_vector.get_index(i);
				if (mask.RowIsValid(input.input_idx)) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*state, idata[input.input_idx], input);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				input.input_idx = sel_vector.get_index(i);
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*state, idata[input.input_idx], input);
			}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static inline void BinaryScatterLoop(const A_TYPE *__restrict adata, AggregateInputData &aggr_input_data,
	                                     const B_TYPE *__restrict bdata, STATE_TYPE **__restrict states, idx_t count,
	                                     const SelectionVector &asel, const SelectionVector &bsel,
	                                     const SelectionVector &ssel, ValidityMask &avalidity,
	                                     ValidityMask &bvalidity) {
		AggregateBinaryInput input(aggr_input_data, avalidity, bvalidity);
		if (OP::IgnoreNull() && (!avalidity.AllValid() || !bvalidity.AllValid())) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				input.lidx = asel.get_index(i);
				input.ridx = bsel.get_index(i);
				auto sidx = ssel.get_index(i);
				if (avalidity.RowIsValid(input.lidx) && bvalidity.RowIsValid(input.ridx)) {
					OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(*states[sidx], adata[input.lidx],
					                                                       bdata[input.ridx], input);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				input.lidx = asel.get_index(i);
				input.ridx = bsel.get_index(i);
				auto sidx = ssel.get_index(i);
				OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(*states[sidx], adata[input.lidx],
				                                                       bdata[input.ridx], input);
			}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static inline void BinaryUpdateLoop(const A_TYPE *__restrict adata, AggregateInputData &aggr_input_data,
	                                    const B_TYPE *__restrict bdata, STATE_TYPE *__restrict state, idx_t count,
	                                    const SelectionVector &asel, const SelectionVector &bsel,
	                                    ValidityMask &avalidity, ValidityMask &bvalidity) {
		AggregateBinaryInput input(aggr_input_data, avalidity, bvalidity);
		if (OP::IgnoreNull() && (!avalidity.AllValid() || !bvalidity.AllValid())) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				input.lidx = asel.get_index(i);
				input.ridx = bsel.get_index(i);
				if (avalidity.RowIsValid(input.lidx) && bvalidity.RowIsValid(input.ridx)) {
					OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(*state, adata[input.lidx], bdata[input.ridx],
					                                                       input);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				input.lidx = asel.get_index(i);
				input.ridx = bsel.get_index(i);
				OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(*state, adata[input.lidx], bdata[input.ridx],
				                                                       input);
			}
		}
	}

public:
	template <class STATE_TYPE, class OP>
	static void NullaryScatter(Vector &states, AggregateInputData &aggr_input_data, idx_t count) {
		if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			OP::template ConstantOperation<STATE_TYPE, OP>(**sdata, aggr_input_data, count);
		} else if (states.GetVectorType() == VectorType::FLAT_VECTOR) {
			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			NullaryFlatLoop<STATE_TYPE, OP>(sdata, aggr_input_data, count);
		} else {
			UnifiedVectorFormat sdata;
			states.ToUnifiedFormat(count, sdata);
			NullaryScatterLoop<STATE_TYPE, OP>((STATE_TYPE **)sdata.data, aggr_input_data, *sdata.sel, count);
		}
	}

	template <class STATE_TYPE, class OP>
	static void NullaryUpdate(data_ptr_t state, AggregateInputData &aggr_input_data, idx_t count) {
		OP::template ConstantOperation<STATE_TYPE, OP>(*reinterpret_cast<STATE_TYPE *>(state), aggr_input_data, count);
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static void UnaryScatter(Vector &input, Vector &states, AggregateInputData &aggr_input_data, idx_t count) {
		if (input.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			if (OP::IgnoreNull() && ConstantVector::IsNull(input)) {
				// constant NULL input in function that ignores NULL values
				return;
			}
			// regular constant: get first state
			auto idata = ConstantVector::GetData<INPUT_TYPE>(input);
			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			AggregateUnaryInput input_data(aggr_input_data, ConstantVector::Validity(input));
			OP::template ConstantOperation<INPUT_TYPE, STATE_TYPE, OP>(**sdata, *idata, input_data, count);
		} else if (input.GetVectorType() == VectorType::FLAT_VECTOR &&
		           states.GetVectorType() == VectorType::FLAT_VECTOR) {
			auto idata = FlatVector::GetData<INPUT_TYPE>(input);
			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			UnaryFlatLoop<STATE_TYPE, INPUT_TYPE, OP>(idata, aggr_input_data, sdata, FlatVector::Validity(input),
			                                          count);
		} else {
			UnifiedVectorFormat idata, sdata;
			input.ToUnifiedFormat(count, idata);
			states.ToUnifiedFormat(count, sdata);
			UnaryScatterLoop<STATE_TYPE, INPUT_TYPE, OP>(UnifiedVectorFormat::GetData<INPUT_TYPE>(idata),
			                                             aggr_input_data, (STATE_TYPE **)sdata.data, *idata.sel,
			                                             *sdata.sel, idata.validity, count);
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static void UnaryUpdate(Vector &input, AggregateInputData &aggr_input_data, data_ptr_t state, idx_t count) {
		switch (input.GetVectorType()) {
		case VectorType::CONSTANT_VECTOR: {
			if (OP::IgnoreNull() && ConstantVector::IsNull(input)) {
				return;
			}
			auto idata = ConstantVector::GetData<INPUT_TYPE>(input);
			AggregateUnaryInput input_data(aggr_input_data, ConstantVector::Validity(input));
			OP::template ConstantOperation<INPUT_TYPE, STATE_TYPE, OP>(*reinterpret_cast<STATE_TYPE *>(state), *idata,
			                                                           input_data, count);
			break;
		}
		case VectorType::FLAT_VECTOR: {
			auto idata = FlatVector::GetData<INPUT_TYPE>(input);
			UnaryFlatUpdateLoop<STATE_TYPE, INPUT_TYPE, OP>(idata, aggr_input_data, (STATE_TYPE *)state, count,
			                                                FlatVector::Validity(input));
			break;
		}
		default: {
			UnifiedVectorFormat idata;
			input.ToUnifiedFormat(count, idata);
			UnaryUpdateLoop<STATE_TYPE, INPUT_TYPE, OP>(UnifiedVectorFormat::GetData<INPUT_TYPE>(idata),
			                                            aggr_input_data, (STATE_TYPE *)state, count, idata.validity,
			                                            *idata.sel);
			break;
		}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryScatter(AggregateInputData &aggr_input_data, Vector &a, Vector &b, Vector &states, idx_t count) {
		UnifiedVectorFormat adata, bdata, sdata;

		a.ToUnifiedFormat(count, adata);
		b.ToUnifiedFormat(count, bdata);
		states.ToUnifiedFormat(count, sdata);

		BinaryScatterLoop<STATE_TYPE, A_TYPE, B_TYPE, OP>(
		    UnifiedVectorFormat::GetData<A_TYPE>(adata), aggr_input_data, UnifiedVectorFormat::GetData<B_TYPE>(bdata),
		    (STATE_TYPE **)sdata.data, count, *adata.sel, *bdata.sel, *sdata.sel, adata.validity, bdata.validity);
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryUpdate(AggregateInputData &aggr_input_data, Vector &a, Vector &b, data_ptr_t state, idx_t count) {
		UnifiedVectorFormat adata, bdata;

		a.ToUnifiedFormat(count, adata);
		b.ToUnifiedFormat(count, bdata);

		BinaryUpdateLoop<STATE_TYPE, A_TYPE, B_TYPE, OP>(
		    UnifiedVectorFormat::GetData<A_TYPE>(adata), aggr_input_data, UnifiedVectorFormat::GetData<B_TYPE>(bdata),
		    (STATE_TYPE *)state, count, *adata.sel, *bdata.sel, adata.validity, bdata.validity);
	}

	template <class STATE_TYPE, class OP>
	static void Combine(Vector &source, Vector &target, AggregateInputData &aggr_input_data, idx_t count) {
		D_ASSERT(source.GetType().id() == LogicalTypeId::POINTER && target.GetType().id() == LogicalTypeId::POINTER);
		auto sdata = FlatVector::GetData<const STATE_TYPE *>(source);
		auto tdata = FlatVector::GetData<STATE_TYPE *>(target);

		for (idx_t i = 0; i < count; i++) {
			OP::template Combine<STATE_TYPE, OP>(*sdata[i], *tdata[i], aggr_input_data);
		}
	}

	template <class STATE_TYPE, class RESULT_TYPE, class OP>
	static void Finalize(Vector &states, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
	                     idx_t offset) {
		if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);

			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			auto rdata = ConstantVector::GetData<RESULT_TYPE>(result);
			AggregateFinalizeData finalize_data(result, aggr_input_data);
			OP::template Finalize<RESULT_TYPE, STATE_TYPE>(**sdata, *rdata, finalize_data);
		} else {
			D_ASSERT(states.GetVectorType() == VectorType::FLAT_VECTOR);
			result.SetVectorType(VectorType::FLAT_VECTOR);

			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
			AggregateFinalizeData finalize_data(result, aggr_input_data);
			for (idx_t i = 0; i < count; i++) {
				finalize_data.result_idx = i + offset;
				OP::template Finalize<RESULT_TYPE, STATE_TYPE>(*sdata[i], rdata[finalize_data.result_idx],
				                                               finalize_data);
			}
		}
	}

	template <class STATE_TYPE, class OP>
	static void VoidFinalize(Vector &states, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
	                         idx_t offset) {
		if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);

			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			AggregateFinalizeData finalize_data(result, aggr_input_data);
			OP::template Finalize<STATE_TYPE>(**sdata, finalize_data);
		} else {
			D_ASSERT(states.GetVectorType() == VectorType::FLAT_VECTOR);
			result.SetVectorType(VectorType::FLAT_VECTOR);

			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			AggregateFinalizeData finalize_data(result, aggr_input_data);
			for (idx_t i = 0; i < count; i++) {
				finalize_data.result_idx = i + offset;
				OP::template Finalize<STATE_TYPE>(*sdata[i], finalize_data);
			}
		}
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
	static void UnaryWindow(const Vector &input, const ValidityMask &ifilter, AggregateInputData &aggr_input_data,
	                        data_ptr_t state_p, const SubFrames &frames, Vector &result, idx_t ridx,
	                        const_data_ptr_t gstate_p) {

		auto idata = FlatVector::GetData<const INPUT_TYPE>(input);
		const auto &ivalid = FlatVector::Validity(input);
		auto &state = *reinterpret_cast<STATE *>(state_p);
		auto gstate = reinterpret_cast<const STATE *>(gstate_p);
		OP::template Window<STATE, INPUT_TYPE, RESULT_TYPE>(idata, ifilter, ivalid, aggr_input_data, state, frames,
		                                                    result, ridx, gstate);
	}

	template <typename OP>
	static void IntersectFrames(const SubFrames &lefts, const SubFrames &rights, OP &op) {
		const auto cover_start = MinValue(rights[0].start, lefts[0].start);
		const auto cover_end = MaxValue(rights.back().end, lefts.back().end);
		const FrameBounds last(cover_end, cover_end);

		//	Subframe indices
		idx_t l = 0;
		idx_t r = 0;
		for (auto i = cover_start; i < cover_end;) {
			uint8_t overlap = 0;

			// Are we in the previous frame?
			auto left = &last;
			if (l < lefts.size()) {
				left = &lefts[l];
				overlap |= uint8_t(left->start <= i && i < left->end) << 0;
			}

			// Are we in the current frame?
			auto right = &last;
			if (r < rights.size()) {
				right = &rights[r];
				overlap |= uint8_t(right->start <= i && i < right->end) << 1;
			}

			auto limit = i;
			switch (overlap) {
			case 0x00:
				// i ∉ F U P
				limit = MinValue(right->start, left->start);
				op.Neither(i, limit);
				break;
			case 0x01:
				// i ∈ P \ F
				limit = MinValue(left->end, right->start);
				op.Left(i, limit);
				break;
			case 0x02:
				// i ∈ F \ P
				limit = MinValue(right->end, left->start);
				op.Right(i, limit);
				break;
			case 0x03:
			default:
				D_ASSERT(overlap == 0x03);
				// i ∈ F ∩ P
				limit = MinValue(right->end, left->end);
				op.Both(i, limit);
				break;
			}

			// Advance  the subframe indices
			i = limit;
			l += (i == left->end);
			r += (i == right->end);
		}
	}

	template <class STATE_TYPE, class OP>
	static void Destroy(Vector &states, AggregateInputData &aggr_input_data, idx_t count) {
		auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
		for (idx_t i = 0; i < count; i++) {
			OP::template Destroy<STATE_TYPE>(*sdata[i], aggr_input_data);
		}
	}
};

} // namespace duckdb
