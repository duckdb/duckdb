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
	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryScatterLoop(INPUT_TYPE *__restrict idata, STATE_TYPE **__restrict states, idx_t count,
	                               sel_t *__restrict sel_vector, nullmask_t &nullmask) {
		if (OP::IgnoreNull() && nullmask.any()) {
			// potential NULL values and NULL values are ignored
			VectorOperations::Exec(sel_vector, count, [&](idx_t i, idx_t k) {
				if (!nullmask[i]) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[i], idata, nullmask, i);
				}
			});
		} else {
			// quick path: no NULL values or NULL values are not ignored
			VectorOperations::Exec(sel_vector, count, [&](idx_t i, idx_t k) {
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[i], idata, nullmask, i);
			});
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryUpdateLoop(INPUT_TYPE *__restrict idata, STATE_TYPE *__restrict state, idx_t count,
	                               sel_t *__restrict sel_vector, nullmask_t &nullmask) {
		if (OP::IgnoreNull() && nullmask.any()) {
			// potential NULL values and NULL values are ignored
			VectorOperations::Exec(sel_vector, count, [&](idx_t i, idx_t k) {
				if (!nullmask[i]) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(state, idata, nullmask, i);
				}
			});
		} else {
			// quick path: no NULL values or NULL values are not ignored
			VectorOperations::Exec(sel_vector, count, [&](idx_t i, idx_t k) {
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(state, idata, nullmask, i);
			});
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static inline void BinaryScatterLoop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata, STATE_TYPE **__restrict states, idx_t count,
	                               sel_t *__restrict sel_vector, nullmask_t &nullmask) {
		if (OP::IgnoreNull() && nullmask.any()) {
			// potential NULL values and NULL values are ignored
			VectorOperations::Exec(sel_vector, count, [&](idx_t i, idx_t k) {
				if (!nullmask[i]) {
					OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(states[i], adata, bdata, nullmask, i);
				}
			});
		} else {
			// quick path: no NULL values or NULL values are not ignored
			VectorOperations::Exec(sel_vector, count, [&](idx_t i, idx_t k) {
				OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(states[i], adata, bdata, nullmask, i);
			});
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static inline void BinaryUpdateLoop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata, STATE_TYPE *__restrict state, idx_t count,
	                               sel_t *__restrict sel_vector, nullmask_t &nullmask) {
		if (OP::IgnoreNull() && nullmask.any()) {
			// potential NULL values and NULL values are ignored
			VectorOperations::Exec(sel_vector, count, [&](idx_t i, idx_t k) {
				if (!nullmask[i]) {
					OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(state, adata, bdata, nullmask, i);
				}
			});
		} else {
			// quick path: no NULL values or NULL values are not ignored
			VectorOperations::Exec(sel_vector, count, [&](idx_t i, idx_t k) {
				OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(state, adata, bdata, nullmask, i);
			});
		}
	}
public:
	template<class STATE_TYPE, class INPUT_TYPE, class OP>
	static void UnaryScatter(Vector &input, Vector &states) {
		assert(states.vector_type == VectorType::FLAT_VECTOR);

		auto sdata = (STATE_TYPE**) states.GetData();
		if (input.vector_type == VectorType::CONSTANT_VECTOR) {
			if (OP::IgnoreNull() && input.nullmask[0]) {
				// constant NULL input in function that ignores NULL values
				return;
			}
			// regular constant NULL: get first state
			assert(states.size() > 0);
			auto state = sdata[states.sel_vector() ? states.sel_vector()[0] : 0];
			auto idata = (INPUT_TYPE*) input.GetData();
			OP::template ConstantOperation<INPUT_TYPE, STATE_TYPE, OP>(state, idata, input.nullmask, input.size());
		} else {
			input.Normalify();

			auto idata = (INPUT_TYPE*) input.GetData();
			UnaryScatterLoop<STATE_TYPE, INPUT_TYPE, OP>(idata, sdata, input.size(), input.sel_vector(), input.nullmask);
		}
	}

	template<class STATE_TYPE, class INPUT_TYPE, class OP>
	static void UnaryUpdate(Vector &input, data_ptr_t state) {
		if (input.vector_type == VectorType::CONSTANT_VECTOR) {
			if (OP::IgnoreNull() && input.nullmask[0]) {
				return;
			}
			auto idata = (INPUT_TYPE*) input.GetData();
			OP::template ConstantOperation<INPUT_TYPE, STATE_TYPE, OP>((STATE_TYPE *) state, idata, input.nullmask, input.size());
		} else {
			input.Normalify();
			auto idata = (INPUT_TYPE*) input.GetData();
			UnaryUpdateLoop<STATE_TYPE, INPUT_TYPE, OP>(idata, (STATE_TYPE *) state, input.size(), input.sel_vector(), input.nullmask);
		}
	}

	template<class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryScatter(Vector &a, Vector &b, Vector &states) {
		assert(states.vector_type == VectorType::FLAT_VECTOR);

		a.Normalify();
		b.Normalify();

		auto adata = (A_TYPE*) a.GetData();
		auto bdata = (B_TYPE*) b.GetData();
		auto sdata = (STATE_TYPE**) states.GetData();
		nullmask_t combined_mask = a.nullmask | b.nullmask;
		BinaryScatterLoop<STATE_TYPE, A_TYPE, B_TYPE, OP>(adata, bdata, sdata, a.size(), a.sel_vector(), combined_mask);
	}

	template<class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryUpdate(Vector &a, Vector &b, data_ptr_t state) {
		a.Normalify();
		b.Normalify();
		auto adata = (A_TYPE*) a.GetData();
		auto bdata = (B_TYPE*) b.GetData();
		nullmask_t combined_mask = a.nullmask | b.nullmask;
		BinaryUpdateLoop<STATE_TYPE, A_TYPE, B_TYPE, OP>(adata, bdata, (STATE_TYPE *) state, a.size(), a.sel_vector(), combined_mask);
	}

	template<class STATE_TYPE, class OP>
	static void Combine(Vector &source, Vector &target) {
		assert(source.vector_type == VectorType::FLAT_VECTOR);
		assert(target.vector_type == VectorType::FLAT_VECTOR);
		auto sdata = (STATE_TYPE *) source.GetData();
		auto tdata = (STATE_TYPE **) target.GetData();

		VectorOperations::Exec(target.sel_vector(), target.size(), [&](idx_t i, idx_t k) {
			OP::template Combine<STATE_TYPE>(sdata[i], tdata[i]);
		});
	}

	template<class STATE_TYPE, class RESULT_TYPE, class OP>
	static void Finalize(Vector &states, Vector &result) {
		if (states.vector_type == VectorType::CONSTANT_VECTOR) {
			result.vector_type = VectorType::CONSTANT_VECTOR;

			auto sdata = (STATE_TYPE **) states.GetData();
			auto rdata = (RESULT_TYPE *) result.GetData();
			OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, sdata[0], rdata, result.nullmask, 0);
		} else {
			states.Normalify();
			result.vector_type = VectorType::FLAT_VECTOR;

			auto sdata = (STATE_TYPE **) states.GetData();
			auto rdata = (RESULT_TYPE *) result.GetData();
			VectorOperations::Exec(result.sel_vector(), result.size(), [&](idx_t i, idx_t k) {
				OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, sdata[i], rdata, result.nullmask, i);
			});
		}
	}

	template<class STATE_TYPE, class OP>
	static void Destroy(Vector &states) {
		auto sdata = (STATE_TYPE **)states.GetData();
		VectorOperations::Exec(states, [&](idx_t i, idx_t k) {
			OP::template Destroy<STATE_TYPE>(sdata[i]);
		});
	}
};

} // namespace duckdb
