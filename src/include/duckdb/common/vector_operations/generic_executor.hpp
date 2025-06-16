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

struct ExecutorBaseType {
	ExecutorBaseType() : is_null(false) {};

	bool is_null;
};

// Forward declaration
template <class INPUT_TYPE>
struct PrimitiveType;

template <typename T>
struct EnsureExecutorType {
	using type = T;
};

template <>
struct EnsureExecutorType<bool> {
	using type = PrimitiveType<bool>;
};
template <>
struct EnsureExecutorType<string_t> {
	using type = PrimitiveType<string_t>;
};
template <>
struct EnsureExecutorType<int32_t> {
	using type = PrimitiveType<int32_t>;
};
template <>
struct EnsureExecutorType<int64_t> {
	using type = PrimitiveType<int64_t>;
};
template <>
struct EnsureExecutorType<float> {
	using type = PrimitiveType<float>;
};
template <>
struct EnsureExecutorType<double> {
	using type = PrimitiveType<double>;
};

template <class INPUT_TYPE>
struct PrimitiveType : ExecutorBaseType {
	PrimitiveType() = default;
	PrimitiveType(INPUT_TYPE val) : val(val) { // NOLINT: allow implicit cast
	}

	INPUT_TYPE val;

	using STRUCT_STATE = PrimitiveTypeState;
	
	// Implicit conversion to INPUT_TYPE for transparent access
	operator INPUT_TYPE() const { return val; }
	
	// Pointer accessor for when addresses are needed
	INPUT_TYPE* ptr() { return &val; }
	const INPUT_TYPE* ptr() const { return &val; }

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
	Vector* child_vectors[CHILD_COUNT];
	idx_t count;

	void PrepareVector(Vector &input, idx_t input_count) {
		auto &entries = StructVector::GetEntries(input);
		count = input_count;

		input.ToUnifiedFormat(input_count, main_data);

		for (idx_t i = 0; i < CHILD_COUNT; i++) {
			child_vectors[i] = entries[i].get();
			entries[i]->ToUnifiedFormat(input_count, child_data[i]);
		}
	}
};

template <class A>
struct StructTypeUnary : ExecutorBaseType {
	using A_TYPE = typename EnsureExecutorType<A>::type;
	A_TYPE a_val;

	StructTypeUnary() = default;
	StructTypeUnary(A a) : a_val(a) {
	}

	using STRUCT_STATE = StructTypeState<1>;

	bool ContainsNull() {
		return is_null || a_val.is_null;
	}

	static StructTypeUnary<A> ConstructType(STRUCT_STATE &state, idx_t i) {
		StructTypeUnary<A> result;
		
		// Check main struct validity
		auto main_idx = state.main_data.sel->get_index(i);
		if (!state.main_data.validity.RowIsValid(main_idx)) {
			result.is_null = true;
			return result;
		}
		
		// Recursively construct child types using their ConstructType methods
		typename A_TYPE::STRUCT_STATE a_state;
		a_state.PrepareVector(*state.child_vectors[0], state.count);
		result.a_val = A_TYPE::ConstructType(a_state, i);
		
		return result;
	}

	static void AssignResult(Vector &result, idx_t i, StructTypeUnary<A> value) {
		auto &entries = StructVector::GetEntries(result);

		if (value.ContainsNull()) {
			FlatVector::SetNull(*entries[0], i, true);
		}
		auto a_data = FlatVector::GetData<A>(*entries[0]);
		a_data[i] = value.a_val;
	}
};

template <class A, class B>
struct StructTypeBinary : ExecutorBaseType {
	using A_TYPE = typename EnsureExecutorType<A>::type;
	using B_TYPE = typename EnsureExecutorType<B>::type;

	A_TYPE a_val;
	B_TYPE b_val;

	StructTypeBinary() = default;
	StructTypeBinary(A a, B b) : a_val(a), b_val(b) {
	}

	using STRUCT_STATE = StructTypeState<2>;

	bool ContainsNull() {
		return is_null || a_val.is_null || b_val.is_null;
	}

	static StructTypeBinary<A, B> ConstructType(STRUCT_STATE &state, idx_t i) {
		StructTypeBinary<A, B> result;
		
		// Check main struct validity
		auto main_idx = state.main_data.sel->get_index(i);
		if (!state.main_data.validity.RowIsValid(main_idx)) {
			result.is_null = true;
			return result;
		}
		
		// Recursively construct child types using their ConstructType methods
		typename A_TYPE::STRUCT_STATE a_state;
		a_state.PrepareVector(*state.child_vectors[0], state.count);
		result.a_val = A_TYPE::ConstructType(a_state, i);
		
		typename B_TYPE::STRUCT_STATE b_state;
		b_state.PrepareVector(*state.child_vectors[1], state.count);
		result.b_val = B_TYPE::ConstructType(b_state, i);
		
		return result;
	}

	static void AssignResult(Vector &result, idx_t i, StructTypeBinary<A, B> value) {
		auto &entries = StructVector::GetEntries(result);

		if (value.ContainsNull()) {
			FlatVector::SetNull(*entries[0], i, true);
			FlatVector::SetNull(*entries[1], i, true);
		}
		auto a_data = FlatVector::GetData<A>(*entries[0]);
		auto b_data = FlatVector::GetData<B>(*entries[1]);
		a_data[i] = value.a_val;
		b_data[i] = value.b_val;
	}
};

template <class A, class B, class C>
struct StructTypeTernary : ExecutorBaseType {
	using A_TYPE = typename EnsureExecutorType<A>::type;
	using B_TYPE = typename EnsureExecutorType<B>::type;
	using C_TYPE = typename EnsureExecutorType<C>::type;

	A_TYPE a_val;
	B_TYPE b_val;
	C_TYPE c_val;

	StructTypeTernary() = default;
	StructTypeTernary(A a, B b, C c) : a_val(a), b_val(b), c_val(c) {
	}

	using STRUCT_STATE = StructTypeState<3>;

	bool ContainsNull() {
		return is_null || a_val.is_null || b_val.is_null || c_val.is_null;
	}

	static StructTypeTernary<A, B, C> ConstructType(STRUCT_STATE &state, idx_t i) {
		StructTypeTernary<A, B, C> result;
		
		// Check main struct validity
		auto main_idx = state.main_data.sel->get_index(i);
		if (!state.main_data.validity.RowIsValid(main_idx)) {
			result.is_null = true;
			return result;
		}
		
		// Recursively construct child types using their ConstructType methods
		typename A_TYPE::STRUCT_STATE a_state;
		a_state.PrepareVector(*state.child_vectors[0], state.count);
		result.a_val = A_TYPE::ConstructType(a_state, i);
		
		typename B_TYPE::STRUCT_STATE b_state;
		b_state.PrepareVector(*state.child_vectors[1], state.count);
		result.b_val = B_TYPE::ConstructType(b_state, i);
		
		typename C_TYPE::STRUCT_STATE c_state;
		c_state.PrepareVector(*state.child_vectors[2], state.count);
		result.c_val = C_TYPE::ConstructType(c_state, i);
		
		return result;
	}

	static void AssignResult(Vector &result, idx_t i, StructTypeTernary<A, B, C> value) {
		auto &entries = StructVector::GetEntries(result);

		if (value.ContainsNull()) {
			FlatVector::SetNull(*entries[0], i, true);
			FlatVector::SetNull(*entries[1], i, true);
			FlatVector::SetNull(*entries[2], i, true);
		}
		auto a_data = FlatVector::GetData<A>(*entries[0]);
		auto b_data = FlatVector::GetData<B>(*entries[1]);
		auto c_data = FlatVector::GetData<C>(*entries[2]);
		a_data[i] = value.a_val;
		b_data[i] = value.b_val;
		c_data[i] = value.c_val;
	}
};

template <class A, class B, class C, class D>
struct StructTypeQuaternary : ExecutorBaseType {
	using A_TYPE = typename EnsureExecutorType<A>::type;
	using B_TYPE = typename EnsureExecutorType<B>::type;
	using C_TYPE = typename EnsureExecutorType<C>::type;
	using D_TYPE = typename EnsureExecutorType<D>::type;

	A_TYPE a_val;
	B_TYPE b_val;
	C_TYPE c_val;
	D_TYPE d_val;

	StructTypeQuaternary() = default;
	StructTypeQuaternary(A a, B b, C c, D d) : a_val(a), b_val(b), c_val(c), d_val(d) {
	}

	using STRUCT_STATE = StructTypeState<4>;

	bool ContainsNull() {
		return is_null || a_val.is_null || b_val.is_null || c_val.is_null || d_val.is_null;
	}

	static StructTypeQuaternary<A, B, C, D> ConstructType(STRUCT_STATE &state, idx_t i) {
		StructTypeQuaternary<A, B, C, D> result;
		
		// Check main struct validity
		auto main_idx = state.main_data.sel->get_index(i);
		if (!state.main_data.validity.RowIsValid(main_idx)) {
			result.is_null = true;
			return result;
		}
		
		// Recursively construct child types using their ConstructType methods
		typename A_TYPE::STRUCT_STATE a_state;
		a_state.PrepareVector(*state.child_vectors[0], state.count);
		result.a_val = A_TYPE::ConstructType(a_state, i);
		
		typename B_TYPE::STRUCT_STATE b_state;
		b_state.PrepareVector(*state.child_vectors[1], state.count);
		result.b_val = B_TYPE::ConstructType(b_state, i);
		
		typename C_TYPE::STRUCT_STATE c_state;
		c_state.PrepareVector(*state.child_vectors[2], state.count);
		result.c_val = C_TYPE::ConstructType(c_state, i);
		
		typename D_TYPE::STRUCT_STATE d_state;
		d_state.PrepareVector(*state.child_vectors[3], state.count);
		result.d_val = D_TYPE::ConstructType(d_state, i);
		
		return result;
	}

	static void AssignResult(Vector &result, idx_t i, StructTypeQuaternary<A, B, C, D> value) {
		auto &entries = StructVector::GetEntries(result);

		if (value.ContainsNull()) {
			FlatVector::SetNull(*entries[0], i, true);
			FlatVector::SetNull(*entries[1], i, true);
			FlatVector::SetNull(*entries[2], i, true);
			FlatVector::SetNull(*entries[3], i, true);
		}

		auto a_data = FlatVector::GetData<A>(*entries[0]);
		auto b_data = FlatVector::GetData<B>(*entries[1]);
		auto c_data = FlatVector::GetData<C>(*entries[2]);
		auto d_data = FlatVector::GetData<D>(*entries[3]);

		a_data[i] = value.a_val;
		b_data[i] = value.b_val;
		c_data[i] = value.c_val;
		d_data[i] = value.d_val;
	}
};

template <class CHILD_TYPE>
struct GenericListType : ExecutorBaseType {
	vector<CHILD_TYPE> values;

	GenericListType() = default;
	GenericListType(vector<CHILD_TYPE> values) : values(values) {
	}

	using STRUCT_STATE = PrimitiveTypeState;

	bool ContainsNull() {
		if (is_null) {
			return true;
		}

		for (idx_t i = 0; i < values.size(); i++) {
			if (values[i].is_null) {
				return true;
			}
		}

		return false;
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

		if (value.ContainsNull()) {
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
			auto input = A_TYPE::ConstructType(state, i);
			if (input.ContainsNull()) {
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
			auto a_val = A_TYPE::ConstructType(a_state, i);
			auto b_val = B_TYPE::ConstructType(b_state, i);
			if (a_val.ContainsNull() || b_val.ContainsNull()) {
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
			auto a_val = A_TYPE::ConstructType(a_state, i);
			auto b_val = B_TYPE::ConstructType(b_state, i);
			auto c_val = C_TYPE::ConstructType(c_state, i);
			if (a_val.ContainsNull() || b_val.ContainsNull() || c_val.ContainsNull()) {
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
			auto a_val = A_TYPE::ConstructType(a_state, i);
			auto b_val = B_TYPE::ConstructType(b_state, i);
			auto c_val = C_TYPE::ConstructType(c_state, i);
			auto d_val = D_TYPE::ConstructType(d_state, i);
			if (a_val.ContainsNull() || b_val.ContainsNull() || c_val.ContainsNull() || d_val.ContainsNull()) {
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
