#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/bound_cast_data.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

unique_ptr<BoundCastData> ListBoundCastData::BindListToListCast(BindCastInput &input, const LogicalType &source,
                                                                const LogicalType &target) {
	vector<BoundCastInfo> child_cast_info;
	auto &source_child_type = ListType::GetChildType(source);
	auto &result_child_type = ListType::GetChildType(target);
	auto child_cast = input.GetCastFunction(source_child_type, result_child_type);
	return make_uniq<ListBoundCastData>(std::move(child_cast));
}

static unique_ptr<BoundCastData> BindListToArrayCast(BindCastInput &input, const LogicalType &source,
                                                     const LogicalType &target) {
	vector<BoundCastInfo> child_cast_info;
	auto &source_child_type = ListType::GetChildType(source);
	auto &result_child_type = ArrayType::GetChildType(target);
	auto child_cast = input.GetCastFunction(source_child_type, result_child_type);
	return make_uniq<ListBoundCastData>(std::move(child_cast));
}

unique_ptr<FunctionLocalState> ListBoundCastData::InitListLocalState(CastLocalStateParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<ListBoundCastData>();
	if (!cast_data.child_cast_info.init_local_state) {
		return nullptr;
	}
	CastLocalStateParameters child_parameters(parameters, cast_data.child_cast_info.cast_data);
	return cast_data.child_cast_info.init_local_state(child_parameters);
}

bool ListCast::ListToListCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<ListBoundCastData>();

	// only handle constant and flat vectors here for now
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(source.GetVectorType());
		const bool is_null = ConstantVector::IsNull(source);
		ConstantVector::SetNull(result, is_null);

		if (!is_null) {
			auto ldata = ConstantVector::GetData<list_entry_t>(source);
			auto tdata = ConstantVector::GetData<list_entry_t>(result);
			*tdata = *ldata;
		}
	} else {
		source.Flatten(count);
		result.SetVectorType(VectorType::FLAT_VECTOR);
		FlatVector::SetValidity(result, FlatVector::Validity(source));

		auto ldata = FlatVector::GetData<list_entry_t>(source);
		auto tdata = FlatVector::GetData<list_entry_t>(result);
		for (idx_t i = 0; i < count; i++) {
			tdata[i] = ldata[i];
		}
	}
	auto &source_cc = ListVector::GetEntry(source);
	auto source_size = ListVector::GetListSize(source);

	ListVector::Reserve(result, source_size);
	auto &append_vector = ListVector::GetEntry(result);

	CastParameters child_parameters(parameters, cast_data.child_cast_info.cast_data, parameters.local_state);
	bool all_succeeded = cast_data.child_cast_info.function(source_cc, append_vector, source_size, child_parameters);
	ListVector::SetListSize(result, source_size);
	D_ASSERT(ListVector::GetListSize(result) == source_size);
	return all_succeeded;
}

static bool ListToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;
	// first cast the child vector to varchar
	Vector varchar_list(LogicalType::LIST(LogicalType::VARCHAR), count);
	ListCast::ListToListCast(source, varchar_list, count, parameters);

	// now construct the actual varchar vector
	varchar_list.Flatten(count);
	auto &child = ListVector::GetEntry(varchar_list);
	auto list_data = FlatVector::GetData<list_entry_t>(varchar_list);
	auto &validity = FlatVector::Validity(varchar_list);

	child.Flatten(ListVector::GetListSize(varchar_list));
	auto child_data = FlatVector::GetData<string_t>(child);
	auto &child_validity = FlatVector::Validity(child);

	auto result_data = FlatVector::GetData<string_t>(result);
	static constexpr const idx_t SEP_LENGTH = 2;
	static constexpr const idx_t NULL_LENGTH = 4;
	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		auto list = list_data[i];
		// figure out how long the result needs to be
		idx_t list_length = 2; // "[" and "]"
		for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
			auto idx = list.offset + list_idx;
			if (list_idx > 0) {
				list_length += SEP_LENGTH; // ", "
			}
			// string length, or "NULL"
			list_length += child_validity.RowIsValid(idx) ? child_data[idx].GetSize() : NULL_LENGTH;
		}
		result_data[i] = StringVector::EmptyString(result, list_length);
		auto dataptr = result_data[i].GetDataWriteable();
		idx_t offset = 0;
		dataptr[offset++] = '[';
		for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
			auto idx = list.offset + list_idx;
			if (list_idx > 0) {
				memcpy(dataptr + offset, ", ", SEP_LENGTH);
				offset += SEP_LENGTH;
			}
			if (child_validity.RowIsValid(idx)) {
				auto len = child_data[idx].GetSize();
				memcpy(dataptr + offset, child_data[idx].GetData(), len);
				offset += len;
			} else {
				memcpy(dataptr + offset, "NULL", NULL_LENGTH);
				offset += NULL_LENGTH;
			}
		}
		dataptr[offset] = ']';
		result_data[i].Finalize();
	}

	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return true;
}

static bool ListToArrayCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<ListBoundCastData>();
	auto array_size = ArrayType::GetSize(result.GetType());

	// only handle constant and flat vectors here for now
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(source.GetVectorType());
		if (ConstantVector::IsNull(source)) {
			ConstantVector::SetNull(result, true);
			return true;
		}

		auto ldata = ConstantVector::GetData<list_entry_t>(source)[0];
		if (!ConstantVector::IsNull(source) && ldata.length != array_size) {
			// Cant cast to array, list size mismatch
			auto msg = StringUtil::Format("Cannot cast list with length %llu to array with length %u", ldata.length,
			                              array_size);
			HandleCastError::AssignError(msg, parameters.error_message);
			ConstantVector::SetNull(result, true);
			return false;
		}

		auto &source_cc = ListVector::GetEntry(source);
		auto &result_cc = ArrayVector::GetEntry(result);

		// Since the list was constant, there can only be one sequence of data in the child vector
		CastParameters child_parameters(parameters, cast_data.child_cast_info.cast_data, parameters.local_state);
		bool all_succeeded = cast_data.child_cast_info.function(source_cc, result_cc, array_size, child_parameters);
		return all_succeeded;
	} else {
		source.Flatten(count);
		result.SetVectorType(VectorType::FLAT_VECTOR);

		auto child_type = ArrayType::GetChildType(result.GetType());
		auto &source_cc = ListVector::GetEntry(source);
		auto &result_cc = ArrayVector::GetEntry(result);
		auto ldata = FlatVector::GetData<list_entry_t>(source);

		bool all_lengths_match = true;

		// Now, here's where things get funky.
		// We need to slice out the data from the child vector, since the list
		// wont store child data for null entries, which is the case for arrays.
		// So we need to preserve the "gaps" for null entries when we copy into
		// the result vector. In short 'len(source_child) <= len(result_child)'
		// depending on how many NULL's the source has. so we "unslice" the
		// child data so that len(payload_vector) == len(result_child).

		auto child_count = array_size * count;
		Vector payload_vector(source_cc.GetType(), child_count);
		SelectionVector sel(child_count);

		for (idx_t i = 0; i < count; i++) {
			// If the list is null, set the entire array to null
			if (FlatVector::IsNull(source, i)) {
				FlatVector::SetNull(result, i, true);
				for (idx_t array_elem = 0; array_elem < array_size; array_elem++) {
					FlatVector::SetNull(payload_vector, i * array_size + array_elem, true);
					// just select the first value, it won't be used anyway
					sel.set_index(i * array_size + array_elem, 0);
				}
			} else if (ldata[i].length != array_size) {
				if (all_lengths_match) {
					// Cant cast to array, list size mismatch
					all_lengths_match = false;
					auto msg = StringUtil::Format("Cannot cast list with length %llu to array with length %u",
					                              ldata[i].length, array_size);
					HandleCastError::AssignError(msg, parameters.error_message);
				}
				FlatVector::SetNull(result, i, true);
				for (idx_t array_elem = 0; array_elem < array_size; array_elem++) {
					FlatVector::SetNull(payload_vector, i * array_size + array_elem, true);
					// just select the first value, it won't be used anyway
					sel.set_index(i * array_size + array_elem, 0);
				}
			} else {
				// Set the selection vector to point to the correct offsets
				for (idx_t array_elem = 0; array_elem < array_size; array_elem++) {
					sel.set_index(i * array_size + array_elem, ldata[i].offset + array_elem);
				}
			}
		}

		// Perform the actual copy
		VectorOperations::Copy(source_cc, payload_vector, sel, child_count, 0, 0);

		// Take a last pass and null out the child elems
		for (idx_t i = 0; i < count; i++) {
			if (FlatVector::IsNull(source, i) || FlatVector::IsNull(result, i)) {
				for (idx_t array_elem = 0; array_elem < array_size; array_elem++) {
					FlatVector::SetNull(payload_vector, i * array_size + array_elem, true);
				}
			}
		}

		CastParameters child_parameters(parameters, cast_data.child_cast_info.cast_data, parameters.local_state);
		bool child_succeeded =
		    cast_data.child_cast_info.function(payload_vector, result_cc, child_count, child_parameters);

		if (!child_succeeded) {
			HandleCastError::AssignError(*child_parameters.error_message, parameters.error_message);
		}

		return all_lengths_match && child_succeeded;
	}
}

BoundCastInfo DefaultCasts::ListCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	switch (target.id()) {
	case LogicalTypeId::LIST:
		return BoundCastInfo(ListCast::ListToListCast, ListBoundCastData::BindListToListCast(input, source, target),
		                     ListBoundCastData::InitListLocalState);
	case LogicalTypeId::VARCHAR:
		return BoundCastInfo(
		    ListToVarcharCast,
		    ListBoundCastData::BindListToListCast(input, source, LogicalType::LIST(LogicalType::VARCHAR)),
		    ListBoundCastData::InitListLocalState);
	case LogicalTypeId::ARRAY:
		return BoundCastInfo(ListToArrayCast, BindListToArrayCast(input, source, target),
		                     ListBoundCastData::InitListLocalState);
	default:
		return DefaultCasts::TryVectorNullCast;
	}
}

} // namespace duckdb
