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

namespace duckdb {

class AggregateExecutor {
private:
	template <class STATE_TYPE, class INPUT_TYPE, class OP, bool HAS_SEL_VECTOR>
	static inline void UnaryScatterLoop(INPUT_TYPE *__restrict idata, STATE_TYPE **__restrict states, idx_t count,
	                                    nullmask_t &nullmask, SelectionVector *__restrict sel_vector) {
		if (OP::IgnoreNull() && nullmask.any()) {
			// potential NULL values and NULL values are ignored
			for(idx_t i = 0; i < count; i++) {
				auto idx = HAS_SEL_VECTOR ? sel_vector->get_index(i) : i;
				if (!nullmask[idx]) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[i], idata, nullmask, idx);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for(idx_t i = 0; i < count; i++) {
				auto idx = HAS_SEL_VECTOR ? sel_vector->get_index(i) : i;
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[i], idata, nullmask, idx);
			}
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP, bool HAS_SEL_VECTOR>
	static inline void UnaryUpdateLoop(INPUT_TYPE *__restrict idata, STATE_TYPE *__restrict state, idx_t count,
	                                   nullmask_t &nullmask, SelectionVector *__restrict sel_vector) {
		if (OP::IgnoreNull() && nullmask.any()) {
			// potential NULL values and NULL values are ignored
			for(idx_t i = 0; i < count; i++) {
				auto idx = HAS_SEL_VECTOR ? sel_vector->get_index(i) : i;
				if (!nullmask[idx]) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(state, idata, nullmask, idx);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for(idx_t i = 0; i < count; i++) {
				auto idx = HAS_SEL_VECTOR ? sel_vector->get_index(i) : i;
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(state, idata, nullmask, idx);
			}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static inline void BinaryScatterLoop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata,
	                                     STATE_TYPE **__restrict states, idx_t count, SelectionVector *__restrict asel, SelectionVector *__restrict bsel, nullmask_t &anullmask, nullmask_t &bnullmask) {
		if (OP::IgnoreNull() && (anullmask.any() || bnullmask.any())) {
			// potential NULL values and NULL values are ignored
			for(idx_t i = 0; i < count; i++) {
				auto aidx = asel->get_index(i);
				auto bidx = bsel->get_index(i);
				if (!anullmask[aidx] && !bnullmask[bidx]) {
					OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(states[i], adata, bdata, anullmask, bnullmask, aidx, bidx);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for(idx_t i = 0; i < count; i++) {
				auto aidx = asel->get_index(i);
				auto bidx = bsel->get_index(i);
				OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(states[i], adata, bdata, anullmask, bnullmask, aidx, bidx);
			}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static inline void BinaryUpdateLoop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata,
	                                    STATE_TYPE *__restrict state, idx_t count, SelectionVector *__restrict asel, SelectionVector *__restrict bsel,
										nullmask_t &anullmask, nullmask_t &bnullmask) {
		if (OP::IgnoreNull() && (anullmask.any() || bnullmask.any())) {
			// potential NULL values and NULL values are ignored
			for(idx_t i = 0; i < count; i++) {
				auto aidx = asel->get_index(i);
				auto bidx = bsel->get_index(i);
				if (!anullmask[aidx] && !bnullmask[bidx]) {
					OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(state, adata, bdata, anullmask, bnullmask, aidx, bidx);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for(idx_t i = 0; i < count; i++) {
				auto aidx = asel->get_index(i);
				auto bidx = bsel->get_index(i);
				OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(state, adata, bdata, anullmask, bnullmask, aidx, bidx);
			}
		}
	}

public:
	template <class STATE_TYPE, class INPUT_TYPE, class OP> static void UnaryScatter(Vector &input, Vector &states) {
		auto sdata = FlatVector::GetData<STATE_TYPE*>(states);
		switch(input.vector_type) {
		case VectorType::CONSTANT_VECTOR: {
			if (OP::IgnoreNull() && ConstantVector::IsNull(input)) {
				// constant NULL input in function that ignores NULL values
				return;
			}
			// regular constant: get first state
			assert(states.size() > 0);
			auto state = sdata[0];
			auto idata = ConstantVector::GetData<INPUT_TYPE>(input);
			OP::template ConstantOperation<INPUT_TYPE, STATE_TYPE, OP>(state, idata, ConstantVector::Nullmask(input), input.size());
			break;
		}
		case VectorType::DICTIONARY_VECTOR: {
			auto &sel = DictionaryVector::SelectionVector(input);
			auto &child = DictionaryVector::Child(input);
			child.Normalify();

			auto idata = FlatVector::GetData<INPUT_TYPE>(child);
			UnaryScatterLoop<STATE_TYPE, INPUT_TYPE, OP, true>(idata, sdata, input.size(), FlatVector::Nullmask(child), &sel);
			break;
		}
		default: {
			input.Normalify();

			auto idata = FlatVector::GetData<INPUT_TYPE>(input);
			UnaryScatterLoop<STATE_TYPE, INPUT_TYPE, OP, false>(idata, sdata, input.size(), FlatVector::Nullmask(input), nullptr);
			break;
		}
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP> static void UnaryUpdate(Vector &input, data_ptr_t state) {
		switch(input.vector_type) {
		case VectorType::CONSTANT_VECTOR: {
			if (OP::IgnoreNull() && ConstantVector::IsNull(input)) {
				return;
			}
			auto idata = ConstantVector::GetData<INPUT_TYPE>(input);
			OP::template ConstantOperation<INPUT_TYPE, STATE_TYPE, OP>((STATE_TYPE *)state, idata, ConstantVector::Nullmask(input),
			                                                           input.size());
			break;
		}
		case VectorType::DICTIONARY_VECTOR: {
			auto &sel = DictionaryVector::SelectionVector(input);
			auto &child = DictionaryVector::Child(input);
			child.Normalify();

			auto idata = FlatVector::GetData<INPUT_TYPE>(child);
			UnaryUpdateLoop<STATE_TYPE, INPUT_TYPE, OP, true>(idata, (STATE_TYPE *)state, input.size(), FlatVector::Nullmask(child), &sel);
			break;
		}
		default: {
			input.Normalify();
			auto idata = FlatVector::GetData<INPUT_TYPE>(input);
			UnaryUpdateLoop<STATE_TYPE, INPUT_TYPE, OP, false>(idata, (STATE_TYPE *)state, input.size(),
			                                            FlatVector::Nullmask(input), nullptr);
			break;
		}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryScatter(Vector &a, Vector &b, Vector &states) {
		VectorData adata, bdata;

		a.Orrify(adata);
		b.Orrify(bdata);

		auto sdata = (STATE_TYPE **) FlatVector::GetData(states);
		BinaryScatterLoop<STATE_TYPE, A_TYPE, B_TYPE, OP>((A_TYPE*) adata.data, (B_TYPE*) bdata.data, sdata, a.size(), adata.sel, bdata.sel, *adata.nullmask, *bdata.nullmask);
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryUpdate(Vector &a, Vector &b, data_ptr_t state) {
		VectorData adata, bdata;

		a.Orrify(adata);
		b.Orrify(bdata);

		BinaryUpdateLoop<STATE_TYPE, A_TYPE, B_TYPE, OP>((A_TYPE*) adata.data, (B_TYPE*) bdata.data, (STATE_TYPE *)state, a.size(), adata.sel, bdata.sel, *adata.nullmask, *bdata.nullmask);
	}

	template <class STATE_TYPE, class OP> static void Combine(Vector &source, Vector &target) {
		auto sdata = FlatVector::GetData<STATE_TYPE>(source);
		auto tdata = FlatVector::GetData<STATE_TYPE*>(target);

		for(idx_t i = 0; i < target.size(); i++) {
			OP::template Combine<STATE_TYPE, OP>(sdata[i], tdata[i]);
		}
	}

	template <class STATE_TYPE, class RESULT_TYPE, class OP> static void Finalize(Vector &states, Vector &result) {
		if (states.vector_type == VectorType::CONSTANT_VECTOR) {
			result.vector_type = VectorType::CONSTANT_VECTOR;

			auto sdata = ConstantVector::GetData<STATE_TYPE*>(states);
			auto rdata = ConstantVector::GetData<RESULT_TYPE>(result);
			OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, sdata[0], rdata, ConstantVector::Nullmask(result), 0);
		} else {
			assert(states.vector_type == VectorType::FLAT_VECTOR);
			result.vector_type = VectorType::FLAT_VECTOR;

			auto sdata = FlatVector::GetData<STATE_TYPE*>(states);
			auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
			for(idx_t i = 0; i < result.size(); i++) {
				OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, sdata[i], rdata, FlatVector::Nullmask(result), i);
			}
		}
	}

	template <class STATE_TYPE, class OP> static void Destroy(Vector &states) {
		auto sdata = FlatVector::GetData<STATE_TYPE*>(states);
		for(idx_t i = 0; i < states.size(); i++) {
			OP::template Destroy<STATE_TYPE>(sdata[i]);
		}
	}
};

} // namespace duckdb
