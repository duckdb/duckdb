//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/generic_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <functional>

namespace duckdb {

struct PrimitiveTypeState {
	UnifiedVectorFormat main_data;

	void PrepareVector(Vector &input, idx_t count) {
		input.ToUnifiedFormat(count, main_data);
	}
};

template <class INPUT_TYPE>
struct PrimitiveType {
	PrimitiveType() = default;
	PrimitiveType(INPUT_TYPE val) : val(val) { // NOLINT: allow implicit cast
	}

	INPUT_TYPE val;
	bool is_null;

	using STRUCT_STATE = PrimitiveTypeState;

	bool ContainsNull() {
		return is_null;
	}

	static PrimitiveType<INPUT_TYPE> ConstructType(STRUCT_STATE &state, idx_t i) {
		auto &vdata = state.main_data;
		auto idx = vdata.sel->get_index(i);
		PrimitiveType<INPUT_TYPE> result;
		if (!vdata.validity.RowIsValid(idx)) {
			result.is_null = true;
			return result;
		}
		auto ptr = UnifiedVectorFormat::GetData<INPUT_TYPE>(vdata);
		result.val = ptr[idx];
		result.is_null = false;
		return result;
	}

	static void AssignResult(Vector &result, idx_t i, PrimitiveType<INPUT_TYPE> value) {
		if (value.is_null) {
			FlatVector::SetNull(result, i, true);
		}
		auto result_data = FlatVector::GetData<INPUT_TYPE>(result);
		result_data[i] = value.val;
	}
};

template <idx_t CHILD_COUNT>
struct StructTypeState {
	UnifiedVectorFormat main_data;
	UnifiedVectorFormat child_data[CHILD_COUNT];

	void PrepareVector(Vector &input, idx_t count) {
		auto &entries = StructVector::GetEntries(input);

		input.ToUnifiedFormat(count, main_data);

		for (idx_t i = 0; i < CHILD_COUNT; i++) {
			entries[i]->ToUnifiedFormat(count, child_data[i]);
		}
	}
};

template <class A_TYPE>
struct StructTypeUnary {
	A_TYPE a_val;
	bool is_null;

	using STRUCT_STATE = StructTypeState<1>;

	bool ContainsNull() {
		return is_null;
	}

	static StructTypeUnary<A_TYPE> ConstructType(STRUCT_STATE &state, idx_t i) {
		auto &a_data = state.child_data[0];
		auto a_idx = a_data.sel->get_index(i);
		StructTypeUnary<A_TYPE> result;
		result.is_null = !a_data.validity.RowIsValid(a_idx);
		if (!result.is_null) {
			auto a_ptr = UnifiedVectorFormat::GetData<A_TYPE>(a_data);
			result.a_val = a_ptr[a_idx];
		}
		return result;
	}

	static void AssignResult(Vector &result, idx_t i, StructTypeUnary<A_TYPE> value) {
		auto &entries = StructVector::GetEntries(result);

		if (value.is_null) {
			FlatVector::SetNull(*entries[0], i, true);
		}
		auto a_data = FlatVector::GetData<A_TYPE>(*entries[0]);
		a_data[i] = value.a_val;
	}
};

template <class A_TYPE, class B_TYPE>
struct StructTypeBinary {
	A_TYPE a_val;
	B_TYPE b_val;
	bool a_is_null;
	bool b_is_null;

	using STRUCT_STATE = StructTypeState<2>;

	bool ContainsNull() {
		return a_is_null || b_is_null;
	}

	static StructTypeBinary<A_TYPE, B_TYPE> ConstructType(STRUCT_STATE &state, idx_t i) {
		auto &a_data = state.child_data[0];
		auto &b_data = state.child_data[1];

		auto a_idx = a_data.sel->get_index(i);
		auto b_idx = b_data.sel->get_index(i);
		StructTypeBinary<A_TYPE, B_TYPE> result;
		result.a_is_null = !a_data.validity.RowIsValid(a_idx);
		result.b_is_null = !b_data.validity.RowIsValid(b_idx);
		if (!result.a_is_null) {
			auto a_ptr = UnifiedVectorFormat::GetData<A_TYPE>(a_data);
			result.a_val = a_ptr[a_idx];
		}
		if (!result.b_is_null) {
			auto b_ptr = UnifiedVectorFormat::GetData<B_TYPE>(b_data);
			result.b_val = b_ptr[b_idx];
		}
		return result;
	}

	static void AssignResult(Vector &result, idx_t i, StructTypeBinary<A_TYPE, B_TYPE> value) {
		auto &entries = StructVector::GetEntries(result);

		if (value.a_is_null) {
			FlatVector::SetNull(*entries[0], i, true);
		}
		if (value.b_is_null) {
			FlatVector::SetNull(*entries[1], i, true);
		}
		auto a_data = FlatVector::GetData<A_TYPE>(*entries[0]);
		auto b_data = FlatVector::GetData<B_TYPE>(*entries[1]);
		a_data[i] = value.a_val;
		b_data[i] = value.b_val;
	}
};

template <class A_TYPE, class B_TYPE, class C_TYPE>
struct StructTypeTernary {
	A_TYPE a_val;
	B_TYPE b_val;
	C_TYPE c_val;
	bool a_is_null;
	bool b_is_null;
	bool c_is_null;

	using STRUCT_STATE = StructTypeState<3>;

	bool ContainsNull() {
		return a_is_null || b_is_null || c_is_null;
	}

	static StructTypeTernary<A_TYPE, B_TYPE, C_TYPE> ConstructType(STRUCT_STATE &state, idx_t i) {
		auto &a_data = state.child_data[0];
		auto &b_data = state.child_data[1];
		auto &c_data = state.child_data[2];

		auto a_idx = a_data.sel->get_index(i);
		auto b_idx = b_data.sel->get_index(i);
		auto c_idx = c_data.sel->get_index(i);
		StructTypeTernary<A_TYPE, B_TYPE, C_TYPE> result;
		result.a_is_null = !a_data.validity.RowIsValid(a_idx);
		result.b_is_null = !b_data.validity.RowIsValid(b_idx);
		result.c_is_null = !c_data.validity.RowIsValid(c_idx);
		if (!result.a_is_null) {
			auto a_ptr = UnifiedVectorFormat::GetData<A_TYPE>(a_data);
			result.a_val = a_ptr[a_idx];
		}
		if (!result.b_is_null) {
			auto b_ptr = UnifiedVectorFormat::GetData<B_TYPE>(b_data);
			result.b_val = b_ptr[b_idx];
		}
		if (!result.c_is_null) {
			auto c_ptr = UnifiedVectorFormat::GetData<C_TYPE>(c_data);
			result.c_val = c_ptr[c_idx];
		}
		return result;
	}

	static void AssignResult(Vector &result, idx_t i, StructTypeTernary<A_TYPE, B_TYPE, C_TYPE> value) {
		auto &entries = StructVector::GetEntries(result);

		if (value.a_is_null) {
			FlatVector::SetNull(*entries[0], i, true);
		}
		if (value.b_is_null) {
			FlatVector::SetNull(*entries[1], i, true);
		}
		if (value.c_is_null) {
			FlatVector::SetNull(*entries[2], i, true);
		}
		auto a_data = FlatVector::GetData<A_TYPE>(*entries[0]);
		auto b_data = FlatVector::GetData<B_TYPE>(*entries[1]);
		auto c_data = FlatVector::GetData<C_TYPE>(*entries[2]);
		a_data[i] = value.a_val;
		b_data[i] = value.b_val;
		c_data[i] = value.c_val;
	}
};

template <class A_TYPE, class B_TYPE, class C_TYPE, class D_TYPE>
struct StructTypeQuaternary {
	A_TYPE a_val;
	B_TYPE b_val;
	C_TYPE c_val;
	D_TYPE d_val;
	bool a_is_null;
	bool b_is_null;
	bool c_is_null;
	bool d_is_null;

	using STRUCT_STATE = StructTypeState<4>;

	bool ContainsNull() {
		return a_is_null || b_is_null || c_is_null || d_is_null;
	}

	static StructTypeQuaternary<A_TYPE, B_TYPE, C_TYPE, D_TYPE> ConstructType(STRUCT_STATE &state, idx_t i) {
		auto &a_data = state.child_data[0];
		auto &b_data = state.child_data[1];
		auto &c_data = state.child_data[2];
		auto &d_data = state.child_data[3];

		auto a_idx = a_data.sel->get_index(i);
		auto b_idx = b_data.sel->get_index(i);
		auto c_idx = c_data.sel->get_index(i);
		auto d_idx = d_data.sel->get_index(i);
		StructTypeQuaternary<A_TYPE, B_TYPE, C_TYPE, D_TYPE> result;
		result.a_is_null = !a_data.validity.RowIsValid(a_idx);
		result.b_is_null = !b_data.validity.RowIsValid(b_idx);
		result.c_is_null = !c_data.validity.RowIsValid(c_idx);
		result.d_is_null = !d_data.validity.RowIsValid(d_idx);
		if (!result.a_is_null) {
			auto a_ptr = UnifiedVectorFormat::GetData<A_TYPE>(a_data);
			result.a_val = a_ptr[a_idx];
		}
		if (!result.b_is_null) {
			auto b_ptr = UnifiedVectorFormat::GetData<B_TYPE>(b_data);
			result.b_val = b_ptr[b_idx];
		}
		if (!result.c_is_null) {
			auto c_ptr = UnifiedVectorFormat::GetData<C_TYPE>(c_data);
			result.c_val = c_ptr[c_idx];
		}
		if (!result.d_is_null) {
			auto d_ptr = UnifiedVectorFormat::GetData<D_TYPE>(d_data);
			result.d_val = d_ptr[d_idx];
		}
		return result;
	}

	static void AssignResult(Vector &result, idx_t i, StructTypeQuaternary<A_TYPE, B_TYPE, C_TYPE, D_TYPE> value) {
		auto &entries = StructVector::GetEntries(result);

		if (value.a_is_null) {
			FlatVector::SetNull(*entries[0], i, true);
		}
		if (value.b_is_null) {
			FlatVector::SetNull(*entries[1], i, true);
		}
		if (value.c_is_null) {
			FlatVector::SetNull(*entries[2], i, true);
		}
		if (value.d_is_null) {
			FlatVector::SetNull(*entries[3], i, true);
		}

		auto a_data = FlatVector::GetData<A_TYPE>(*entries[0]);
		auto b_data = FlatVector::GetData<B_TYPE>(*entries[1]);
		auto c_data = FlatVector::GetData<C_TYPE>(*entries[2]);
		auto d_data = FlatVector::GetData<D_TYPE>(*entries[3]);

		a_data[i] = value.a_val;
		b_data[i] = value.b_val;
		c_data[i] = value.c_val;
		d_data[i] = value.d_val;
	}
};

template <class CHILD_TYPE>
struct GenericListType {
	vector<CHILD_TYPE> values;
	bool is_null;

	using STRUCT_STATE = PrimitiveTypeState;

	bool ContainsNull() {
		return is_null;
	}

	static GenericListType<CHILD_TYPE> ConstructType(STRUCT_STATE &state, idx_t i) {
		throw InternalException("FIXME: implement ConstructType for lists");
	}

	static void AssignResult(Vector &result, idx_t i, GenericListType<CHILD_TYPE> value) {
		auto &child = ListVector::GetEntry(result);
		auto current_size = ListVector::GetListSize(result);

		// reserve space in the child element
		auto list_size = value.values.size();
		ListVector::Reserve(result, current_size + list_size);

		if (value.is_null) {
			FlatVector::SetNull(result, i, true);
		}
		auto list_entries = FlatVector::GetData<list_entry_t>(result);
		list_entries[i].offset = current_size;
		list_entries[i].length = list_size;

		for (idx_t child_idx = 0; child_idx < list_size; child_idx++) {
			CHILD_TYPE::AssignResult(child, current_size + child_idx, value.values[child_idx]);
		}
		ListVector::SetListSize(result, current_size + list_size);
	}
};

//! The GenericExecutor can handle struct types in addition to primitive types
struct GenericExecutor {
private:
	template <class A_TYPE, class RESULT_TYPE, class FUNC>
	static void ExecuteUnaryInternal(Vector &input, Vector &result, idx_t count, FUNC &fun) {
		auto constant = input.GetVectorType() == VectorType::CONSTANT_VECTOR;

		typename A_TYPE::STRUCT_STATE state;
		state.PrepareVector(input, count);

		for (idx_t i = 0; i < (constant ? 1 : count); i++) {
			auto idx = state.main_data.sel->get_index(i);
			if (!state.main_data.validity.RowIsValid(idx)) {
				FlatVector::SetNull(result, i, true);
				continue;
			}
			auto input = A_TYPE::ConstructType(state, i);
			if (input.is_null) {
				FlatVector::SetNull(result, i, true);
				continue;
			}
			RESULT_TYPE::AssignResult(result, i, fun(input));
		}
		if (constant) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}

	template <class A_TYPE, class B_TYPE, class RESULT_TYPE, class FUNC>
	static void ExecuteBinaryInternal(Vector &a, Vector &b, Vector &result, idx_t count, FUNC &fun) {
		auto constant =
		    a.GetVectorType() == VectorType::CONSTANT_VECTOR && b.GetVectorType() == VectorType::CONSTANT_VECTOR;

		typename A_TYPE::STRUCT_STATE a_state;
		typename B_TYPE::STRUCT_STATE b_state;
		a_state.PrepareVector(a, count);
		b_state.PrepareVector(b, count);

		for (idx_t i = 0; i < (constant ? 1 : count); i++) {
			auto a_idx = a_state.main_data.sel->get_index(i);
			auto b_idx = b_state.main_data.sel->get_index(i);
			if (!a_state.main_data.validity.RowIsValid(a_idx) || !b_state.main_data.validity.RowIsValid(b_idx)) {
				FlatVector::SetNull(result, i, true);
				continue;
			}
			auto a_val = A_TYPE::ConstructType(a_state, i);
			auto b_val = B_TYPE::ConstructType(b_state, i);
			if (a_val.is_null || b_val.is_null) {
				FlatVector::SetNull(result, i, true);
				continue;
			}
			RESULT_TYPE::AssignResult(result, i, fun(a_val, b_val));
		}
		if (constant) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE, class FUNC>
	static void ExecuteTernaryInternal(Vector &a, Vector &b, Vector &c, Vector &result, idx_t count, FUNC &fun) {
		auto constant = a.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		                b.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		                c.GetVectorType() == VectorType::CONSTANT_VECTOR;

		typename A_TYPE::STRUCT_STATE a_state;
		typename B_TYPE::STRUCT_STATE b_state;
		typename C_TYPE::STRUCT_STATE c_state;

		a_state.PrepareVector(a, count);
		b_state.PrepareVector(b, count);
		c_state.PrepareVector(c, count);

		for (idx_t i = 0; i < (constant ? 1 : count); i++) {
			auto a_idx = a_state.main_data.sel->get_index(i);
			auto b_idx = b_state.main_data.sel->get_index(i);
			auto c_idx = c_state.main_data.sel->get_index(i);
			if (!a_state.main_data.validity.RowIsValid(a_idx) || !b_state.main_data.validity.RowIsValid(b_idx) ||
			    !c_state.main_data.validity.RowIsValid(c_idx)) {
				FlatVector::SetNull(result, i, true);
				continue;
			}
			auto a_val = A_TYPE::ConstructType(a_state, i);
			auto b_val = B_TYPE::ConstructType(b_state, i);
			auto c_val = C_TYPE::ConstructType(c_state, i);
			if (a_val.is_null || b_val.is_null || c_val.is_null) {
				FlatVector::SetNull(result, i, true);
				continue;
			}
			RESULT_TYPE::AssignResult(result, i, fun(a_val, b_val, c_val));
		}
		if (constant) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class D_TYPE, class RESULT_TYPE, class FUNC>
	static void ExecuteQuaternaryInternal(Vector &a, Vector &b, Vector &c, Vector &d, Vector &result, idx_t count,
	                                      FUNC &fun) {
		auto constant =
		    a.GetVectorType() == VectorType::CONSTANT_VECTOR && b.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    c.GetVectorType() == VectorType::CONSTANT_VECTOR && d.GetVectorType() == VectorType::CONSTANT_VECTOR;

		typename A_TYPE::STRUCT_STATE a_state;
		typename B_TYPE::STRUCT_STATE b_state;
		typename C_TYPE::STRUCT_STATE c_state;
		typename D_TYPE::STRUCT_STATE d_state;

		a_state.PrepareVector(a, count);
		b_state.PrepareVector(b, count);
		c_state.PrepareVector(c, count);
		d_state.PrepareVector(d, count);

		for (idx_t i = 0; i < (constant ? 1 : count); i++) {
			auto a_idx = a_state.main_data.sel->get_index(i);
			auto b_idx = b_state.main_data.sel->get_index(i);
			auto c_idx = c_state.main_data.sel->get_index(i);
			auto d_idx = d_state.main_data.sel->get_index(i);
			if (!a_state.main_data.validity.RowIsValid(a_idx) || !b_state.main_data.validity.RowIsValid(b_idx) ||
			    !c_state.main_data.validity.RowIsValid(c_idx) || !d_state.main_data.validity.RowIsValid(d_idx)) {
				FlatVector::SetNull(result, i, true);
				continue;
			}
			auto a_val = A_TYPE::ConstructType(a_state, i);
			auto b_val = B_TYPE::ConstructType(b_state, i);
			auto c_val = C_TYPE::ConstructType(c_state, i);
			auto d_val = D_TYPE::ConstructType(d_state, i);
			if (a_val.is_null || b_val.is_null || c_val.is_null || d_val.is_null) {
				FlatVector::SetNull(result, i, true);
				continue;
			}
			RESULT_TYPE::AssignResult(result, i, fun(a_val, b_val, c_val, d_val));
		}
		if (constant) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}

public:
	template <class A_TYPE, class RESULT_TYPE, class FUNC = std::function<RESULT_TYPE(A_TYPE)>>
	static void ExecuteUnary(Vector &input, Vector &result, idx_t count, FUNC fun) {
		ExecuteUnaryInternal<A_TYPE, RESULT_TYPE, FUNC>(input, result, count, fun);
	}
	template <class A_TYPE, class B_TYPE, class RESULT_TYPE, class FUNC = std::function<RESULT_TYPE(A_TYPE)>>
	static void ExecuteBinary(Vector &a, Vector &b, Vector &result, idx_t count, FUNC fun) {
		ExecuteBinaryInternal<A_TYPE, B_TYPE, RESULT_TYPE, FUNC>(a, b, result, count, fun);
	}
	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE,
	          class FUNC = std::function<RESULT_TYPE(A_TYPE)>>
	static void ExecuteTernary(Vector &a, Vector &b, Vector &c, Vector &result, idx_t count, FUNC fun) {
		ExecuteTernaryInternal<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(a, b, c, result, count, fun);
	}
	template <class A_TYPE, class B_TYPE, class C_TYPE, class D_TYPE, class RESULT_TYPE,
	          class FUNC = std::function<RESULT_TYPE(A_TYPE)>>
	static void ExecuteQuaternary(Vector &a, Vector &b, Vector &c, Vector &d, Vector &result, idx_t count, FUNC fun) {
		ExecuteQuaternaryInternal<A_TYPE, B_TYPE, C_TYPE, D_TYPE, RESULT_TYPE, FUNC>(a, b, c, d, result, count, fun);
	}
};

} // namespace duckdb
