#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/function/scalar/list/contains_or_position.hpp"

namespace duckdb {

template <class T, class RETURN_TYPE, bool FIND_NULLS>
static void TemplatedStructSearch(Vector &input_vector, vector<unique_ptr<Vector>> &members, Vector &target,
                                  const idx_t count, Vector &result) {
	// If the return type is not a bool, return the position
	const auto return_pos = std::is_same<RETURN_TYPE, int32_t>::value;

	const auto &target_type = target.GetType();

	UnifiedVectorFormat vector_format;
	input_vector.ToUnifiedFormat(count, vector_format);

	UnifiedVectorFormat target_format;
	target.ToUnifiedFormat(count, target_format);
	const auto target_data = UnifiedVectorFormat::GetData<T>(target_format);

	vector<const T *> member_datas;
	vector<UnifiedVectorFormat> member_vectors;
	idx_t total_matches = 0;
	for (const auto &member : members) {
		if (member->GetType().InternalType() == target_type.InternalType()) {
			UnifiedVectorFormat member_format;
			member->ToUnifiedFormat(count, member_format);
			member_datas.push_back(UnifiedVectorFormat::GetData<T>(member_format));
			member_vectors.push_back(std::move(member_format));
			total_matches++;
		} else {
			member_datas.push_back(nullptr);
			member_vectors.push_back(UnifiedVectorFormat());
		}
	}

	if (total_matches == 0 && return_pos) {
		// if there are no members that match the target type, we cannot return a position
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<RETURN_TYPE>(result);
	auto &result_validity = FlatVector::Validity(result);

	const auto member_count = members.size();
	for (idx_t row = 0; row < count; row++) {
		const auto &member_row_idx = vector_format.sel->get_index(row);

		if (!vector_format.validity.RowIsValid(member_row_idx)) {
			result_validity.SetInvalid(row);
			continue;
		}

		const auto &target_row_idx = target_format.sel->get_index(row);
		const bool target_valid = target_format.validity.RowIsValid(target_row_idx);

		// We are finished if we are not looking for NULL, and the target is NULL.
		const auto finished = !FIND_NULLS && !target_valid;
		// We did not find the target (finished, or struct is empty).
		if (finished) {
			if (!target_valid || return_pos) {
				result_validity.SetInvalid(row);
			} else {
				result_data[row] = false;
			}
			continue;
		}

		bool found = false;

		for (idx_t member_idx = 0; member_idx < member_count; member_idx++) {
			auto &member_data = member_datas[member_idx];
			if (!member_data) {
				continue; // skip if member data is not compatible with the target type
			}
			const auto &member_vector = member_vectors[member_idx];
			const auto member_data_idx = member_vector.sel->get_index(row);
			const auto col_valid = member_vector.validity.RowIsValid(member_data_idx);

			auto is_null = FIND_NULLS && !col_valid && !target_valid;
			auto both_valid_and_match = col_valid && target_valid &&
			                            Equals::Operation<T>(member_data[member_data_idx], target_data[target_row_idx]);

			if (is_null || both_valid_and_match) {
				found = true;
				if (return_pos) {
					result_data[row] = UnsafeNumericCast<int32_t>(member_idx + 1);
				} else {
					result_data[row] = true;
				}
			}
		}

		if (!found) {
			if (return_pos) {
				result_validity.SetInvalid(row);
			} else {
				result_data[row] = false;
			}
		}
	}
}

template <class RETURN_TYPE, bool FIND_NULLS>
static void StructNestedOp(Vector &input_vector, vector<unique_ptr<Vector>> &members, Vector &target, const idx_t count,
                           Vector &result) {
	const OrderModifiers order_modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);

	// Set up sort keys for nested types.
	const auto members_size = members.size();
	vector<unique_ptr<Vector>> member_sort_key_vectors;
	for (idx_t i = 0; i < members_size; i++) {
		Vector member_sort_key_vec(LogicalType::BLOB, count);
		CreateSortKeyHelpers::CreateSortKeyWithValidity(*members[i], member_sort_key_vec, order_modifiers, count);

		auto member_sort_key_ptr = make_uniq<Vector>(member_sort_key_vec);
		member_sort_key_vectors.push_back(std::move(member_sort_key_ptr));
	}

	Vector target_sort_key_vec(LogicalType::BLOB, count);
	CreateSortKeyHelpers::CreateSortKeyWithValidity(target, target_sort_key_vec, order_modifiers, count);

	TemplatedStructSearch<string_t, RETURN_TYPE, FIND_NULLS>(input_vector, member_sort_key_vectors, target_sort_key_vec,
	                                                         count, result);
}

template <class RETURN_TYPE, bool FIND_NULLS>
static void StructSearchOp(Vector &input_vector, vector<unique_ptr<Vector>> &members, Vector &target, const idx_t count,
                           Vector &result) {
	const auto &target_type = target.GetType().InternalType();
	switch (target_type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedStructSearch<int8_t, RETURN_TYPE, FIND_NULLS>(input_vector, members, target, count, result);
	case PhysicalType::INT16:
		return TemplatedStructSearch<int16_t, RETURN_TYPE, FIND_NULLS>(input_vector, members, target, count, result);
	case PhysicalType::INT32:
		return TemplatedStructSearch<int32_t, RETURN_TYPE, FIND_NULLS>(input_vector, members, target, count, result);
	case PhysicalType::INT64:
		return TemplatedStructSearch<int64_t, RETURN_TYPE, FIND_NULLS>(input_vector, members, target, count, result);
	case PhysicalType::INT128:
		return TemplatedStructSearch<hugeint_t, RETURN_TYPE, FIND_NULLS>(input_vector, members, target, count, result);
	case PhysicalType::UINT8:
		return TemplatedStructSearch<uint8_t, RETURN_TYPE, FIND_NULLS>(input_vector, members, target, count, result);
	case PhysicalType::UINT16:
		return TemplatedStructSearch<uint16_t, RETURN_TYPE, FIND_NULLS>(input_vector, members, target, count, result);
	case PhysicalType::UINT32:
		return TemplatedStructSearch<uint32_t, RETURN_TYPE, FIND_NULLS>(input_vector, members, target, count, result);
	case PhysicalType::UINT64:
		return TemplatedStructSearch<uint64_t, RETURN_TYPE, FIND_NULLS>(input_vector, members, target, count, result);
	case PhysicalType::UINT128:
		return TemplatedStructSearch<uhugeint_t, RETURN_TYPE, FIND_NULLS>(input_vector, members, target, count, result);
	case PhysicalType::FLOAT:
		return TemplatedStructSearch<float, RETURN_TYPE, FIND_NULLS>(input_vector, members, target, count, result);
	case PhysicalType::DOUBLE:
		return TemplatedStructSearch<double, RETURN_TYPE, FIND_NULLS>(input_vector, members, target, count, result);
	case PhysicalType::VARCHAR:
		return TemplatedStructSearch<string_t, RETURN_TYPE, FIND_NULLS>(input_vector, members, target, count, result);
	case PhysicalType::INTERVAL:
		return TemplatedStructSearch<interval_t, RETURN_TYPE, FIND_NULLS>(input_vector, members, target, count, result);
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
	case PhysicalType::ARRAY:
		return StructNestedOp<RETURN_TYPE, FIND_NULLS>(input_vector, members, target, count, result);
	default:
		throw NotImplementedException("This function has not been implemented for logical type %s",
		                              TypeIdToString(target_type));
	}
}

template <class RETURN_TYPE, bool FIND_NULLS = false>
static void StructSearchFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	if (result.GetType().id() == LogicalTypeId::SQLNULL) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	const auto count = args.size();
	auto &input_vector = args.data[0];
	auto &members = StructVector::GetEntries(input_vector);
	auto &target = args.data[1];

	StructSearchOp<RETURN_TYPE, FIND_NULLS>(input_vector, members, target, count, result);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> StructContainsBind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	auto &child_type = arguments[0]->return_type;
	if (child_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}

	if (child_type.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = LogicalTypeId::UNKNOWN;
		bound_function.arguments[1] = LogicalTypeId::UNKNOWN;
		bound_function.SetReturnType(LogicalType::SQLNULL);
		return nullptr;
	}

	auto &struct_children = StructType::GetChildTypes(arguments[0]->return_type);
	if (struct_children.empty()) {
		throw InternalException("Can't check for containment in an empty struct");
	}
	if (!StructType::IsUnnamed(child_type)) {
		throw BinderException("%s can only be used on unnamed structs", bound_function.name);
	}
	bound_function.arguments[0] = child_type;

	// the value type must match one of the struct's children
	LogicalType max_child_type = arguments[1]->return_type;
	vector<LogicalType> new_child_types;
	for (auto &child : struct_children) {
		if (!LogicalType::TryGetMaxLogicalType(context, child.second, max_child_type, max_child_type)) {
			new_child_types.push_back(child.second);
			continue;
		}

		new_child_types.push_back(max_child_type);
		bound_function.arguments[1] = max_child_type;
	}

	child_list_t<LogicalType> cast_children;
	for (idx_t i = 0; i < new_child_types.size(); i++) {
		cast_children.push_back(make_pair(struct_children[i].first, new_child_types[i]));
	}

	bound_function.arguments[0] = LogicalType::STRUCT(cast_children);

	return nullptr;
}

ScalarFunction StructContainsFun::GetFunction() {
	return ScalarFunction("struct_contains", {LogicalTypeId::STRUCT, LogicalType::ANY}, LogicalType::BOOLEAN,
	                      StructSearchFunction<bool>, StructContainsBind);
}

ScalarFunction StructPositionFun::GetFunction() {
	ScalarFunction fun("struct_contains", {LogicalTypeId::STRUCT, LogicalType::ANY}, LogicalType::INTEGER,
	                   StructSearchFunction<int32_t, true>, StructContainsBind);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace duckdb
