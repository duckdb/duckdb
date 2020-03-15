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
		assert(states.vector_type == VectorType::FLAT_VECTOR);

		auto sdata = (STATE_TYPE **)states.GetData();
		switch(input.vector_type) {
		case VectorType::CONSTANT_VECTOR: {
			if (OP::IgnoreNull() && input.nullmask[0]) {
				// constant NULL input in function that ignores NULL values
				return;
			}
			// regular constant: get first state
			assert(states.size() > 0);
			auto state = sdata[0];
			auto idata = (INPUT_TYPE *)input.GetData();
			OP::template ConstantOperation<INPUT_TYPE, STATE_TYPE, OP>(state, idata, input.nullmask, input.size());
			break;
		}
		case VectorType::DICTIONARY_VECTOR: {
			auto &sel = input.GetSelVector();
			auto &child = input.GetDictionaryChild();
			child.Normalify();

			auto idata = (INPUT_TYPE *) child.GetData();
			UnaryScatterLoop<STATE_TYPE, INPUT_TYPE, OP, true>(idata, sdata, input.size(), input.nullmask, &sel);
			break;
		}
		default: {
			input.Normalify();

			auto idata = (INPUT_TYPE *)input.GetData();
			UnaryScatterLoop<STATE_TYPE, INPUT_TYPE, OP, false>(idata, sdata, input.size(), input.nullmask, nullptr);
			break;
		}
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP> static void UnaryUpdate(Vector &input, data_ptr_t state) {
		switch(input.vector_type) {
		case VectorType::CONSTANT_VECTOR: {
			if (OP::IgnoreNull() && input.nullmask[0]) {
				return;
			}
			auto idata = (INPUT_TYPE *)input.GetData();
			OP::template ConstantOperation<INPUT_TYPE, STATE_TYPE, OP>((STATE_TYPE *)state, idata, input.nullmask,
			                                                           input.size());
			break;
		}
		case VectorType::DICTIONARY_VECTOR: {
			auto &sel = input.GetSelVector();
			auto &child = input.GetDictionaryChild();
			child.Normalify();

			auto idata = (INPUT_TYPE *) child.GetData();
			UnaryUpdateLoop<STATE_TYPE, INPUT_TYPE, OP, true>(idata, (STATE_TYPE *)state, input.size(), input.nullmask, &sel);
			break;
		}
		default: {
			input.Normalify();
			auto idata = (INPUT_TYPE *)input.GetData();
			UnaryUpdateLoop<STATE_TYPE, INPUT_TYPE, OP, false>(idata, (STATE_TYPE *)state, input.size(),
			                                            input.nullmask, nullptr);
			break;
		}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryScatter(Vector &a, Vector &b, Vector &states) {
		assert(states.vector_type == VectorType::FLAT_VECTOR);
		data_ptr_t adata, bdata;
		SelectionVector *asel, *bsel;

		a.Orrify(&asel, &adata);
		b.Orrify(&bsel, &bdata);

		auto sdata = (STATE_TYPE **)states.GetData();
		BinaryScatterLoop<STATE_TYPE, A_TYPE, B_TYPE, OP>((A_TYPE*) adata, (B_TYPE*) bdata, sdata, a.size(), asel, bsel, a.nullmask, b.nullmask);
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryUpdate(Vector &a, Vector &b, data_ptr_t state) {
		data_ptr_t adata, bdata;
		SelectionVector *asel, *bsel;

		a.Orrify(&asel, &adata);
		b.Orrify(&bsel, &bdata);

		BinaryUpdateLoop<STATE_TYPE, A_TYPE, B_TYPE, OP>((A_TYPE*) adata, (B_TYPE*) bdata, (STATE_TYPE *)state, a.size(), asel, bsel, a.nullmask, b.nullmask);
	}

	template <class STATE_TYPE, class OP> static void Combine(Vector &source, Vector &target) {
		assert(source.vector_type == VectorType::FLAT_VECTOR);
		assert(target.vector_type == VectorType::FLAT_VECTOR);
		auto sdata = (STATE_TYPE *)source.GetData();
		auto tdata = (STATE_TYPE **)target.GetData();

		for(idx_t i = 0; i < target.size(); i++) {
			OP::template Combine<STATE_TYPE, OP>(sdata[i], tdata[i]);
		}
	}

	template <class STATE_TYPE, class RESULT_TYPE, class OP> static void Finalize(Vector &states, Vector &result) {
		if (states.vector_type == VectorType::CONSTANT_VECTOR) {
			result.vector_type = VectorType::CONSTANT_VECTOR;

			auto sdata = (STATE_TYPE **)states.GetData();
			auto rdata = (RESULT_TYPE *)result.GetData();
			OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, sdata[0], rdata, result.nullmask, 0);
		} else {
			assert(states.vector_type == VectorType::FLAT_VECTOR);
			result.vector_type = VectorType::FLAT_VECTOR;

			auto sdata = (STATE_TYPE **)states.GetData();
			auto rdata = (RESULT_TYPE *)result.GetData();
			for(idx_t i = 0; i < result.size(); i++) {
				OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, sdata[i], rdata, result.nullmask, i);
			}
		}
	}

	template <class STATE_TYPE, class OP> static void Destroy(Vector &states) {
		assert(states.vector_type == VectorType::FLAT_VECTOR);
		auto sdata = (STATE_TYPE **)states.GetData();
		for(idx_t i = 0; i < states.size(); i++) {
			OP::template Destroy<STATE_TYPE>(sdata[i]);
		}
	}
};

} // namespace duckdb
