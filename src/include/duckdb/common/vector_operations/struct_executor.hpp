//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/struct_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <functional>

namespace duckdb {


template <class A_TYPE, class RESULT_TYPE, class FUNC>
struct StructExecutorUnary {
	using FUN = FUNC;
	using RESULT_T = RESULT_TYPE;

	static inline void Operation(UnifiedVectorFormat *vdata, idx_t i, RESULT_TYPE &result, FUNC &fun) {
		auto a_idx = vdata[0].sel->get_index(i);
		if (!vdata[0].validity.RowIsValid(a_idx)) {
			return;
		}
		auto a_ptr = (A_TYPE *)vdata[0].data;
		result = fun(a_ptr[a_idx]);
	}
};

template <class A_TYPE, class B_TYPE, class RESULT_TYPE, class FUNC>
struct StructExecutorBinary {
	using FUN = FUNC;
	using RESULT_T = RESULT_TYPE;

	static inline void Operation(UnifiedVectorFormat *vdata, idx_t i, RESULT_TYPE &result, FUNC &fun) {
		auto a_idx = vdata[0].sel->get_index(i);
		auto b_idx = vdata[1].sel->get_index(i);
		if (!vdata[0].validity.RowIsValid(a_idx) || !vdata[1].validity.RowIsValid(b_idx)) {
			return;
		}
		auto a_ptr = (A_TYPE *)vdata[0].data;
		auto b_ptr = (B_TYPE *)vdata[1].data;
		result = fun(a_ptr[a_idx], b_ptr[b_idx]);
	}
};

template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE, class FUNC>
struct StructExecutorTernary {
	using FUN = FUNC;
	using RESULT_T = RESULT_TYPE;

	static inline void Operation(UnifiedVectorFormat *vdata, idx_t i, RESULT_TYPE &result, FUNC &fun) {
		auto a_idx = vdata[0].sel->get_index(i);
		auto b_idx = vdata[1].sel->get_index(i);
		auto c_idx = vdata[2].sel->get_index(i);
		if (!vdata[0].validity.RowIsValid(a_idx) || !vdata[1].validity.RowIsValid(b_idx) ||
		    !vdata[2].validity.RowIsValid(c_idx)) {
			return;
		}
		auto a_ptr = (A_TYPE *)vdata[0].data;
		auto b_ptr = (B_TYPE *)vdata[1].data;
		auto c_ptr = (C_TYPE *)vdata[2].data;
		result = fun(a_ptr[a_idx], b_ptr[b_idx], c_ptr[c_idx]);
	}
};

struct StructExecutor {
private:
	template <idx_t CHILD_COUNT, class OP>
	static void Execute(Vector &input, Vector &result, idx_t count, typename OP::FUN &fun) {
		auto constant = input.GetVectorType() == VectorType::CONSTANT_VECTOR;
		auto &entries = StructVector::GetEntries(input);

		UnifiedVectorFormat struct_data;
		input.ToUnifiedFormat(count, struct_data);

		UnifiedVectorFormat vdata[CHILD_COUNT];
		for (idx_t i = 0; i < CHILD_COUNT; i++) {
			entries[i]->ToUnifiedFormat(count, vdata[i]);
		}

		auto result_data = FlatVector::GetData<typename OP::RESULT_T>(result);
		for (idx_t i = 0; i < (constant ? 1 : count); i++) {
			auto idx = struct_data.sel->get_index(i);
			if (!struct_data.validity.RowIsValid(idx)) {
				FlatVector::SetNull(result, i, true);
				continue;
			}
			OP::Operation(vdata, i, result_data[i], fun);
		}
		if (constant) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}

public:
	template <class A_TYPE, class RESULT_TYPE, class FUNC = std::function<RESULT_TYPE(A_TYPE)>>
	static void Execute(Vector &input, Vector &result, idx_t count, FUNC fun) {
		Execute<1, StructExecutorUnary<A_TYPE, RESULT_TYPE, FUNC>>(input, result, count, fun);
	}

	template <class A_TYPE, class B_TYPE, class RESULT_TYPE, class FUNC = std::function<RESULT_TYPE(A_TYPE, B_TYPE)>>
	static void Execute(Vector &input, Vector &result, idx_t count, FUNC fun) {
		Execute<2, StructExecutorBinary<A_TYPE, B_TYPE, RESULT_TYPE, FUNC>>(input, result, count, fun);
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE,
	          class FUNC = std::function<RESULT_TYPE(A_TYPE, B_TYPE, C_TYPE)>>
	static void Execute(Vector &input, Vector &result, idx_t count, FUNC fun) {
		Execute<3, StructExecutorTernary<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>>(input, result, count, fun);
	}
};

} // namespace duckdb
