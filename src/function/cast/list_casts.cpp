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
			HandleCastError::AssignError(msg, parameters);
			ConstantVector::SetNull(result, true);
			return false;
		}

		auto &source_cc = ListVector::GetEntry(source);
		auto &result_cc = ArrayVector::GetEntry(result);

		CastParameters child_parameters(parameters, cast_data.child_cast_info.cast_data, parameters.local_state);

		if (ldata.offset == 0) {
			// Fast path: offset is zero, we can just cast `array_size` elements of the child vectors directly
			// Since the list was constant, there can only be one sequence of data in the child vector
			return cast_data.child_cast_info.function(source_cc, result_cc, array_size, child_parameters);
		}

		// Else, we need to copy the range we want to cast to a new vector and cast that
		// In theory we could slice the source child to create a dictionary, but we would then have to flatten the
		// result child which is going to allocate a temp vector and perform a copy anyway. Since we just want to copy a
		// single contiguous range with a single offset, this is simpler.

		Vector payload_vector(source_cc.GetType(), array_size);
		VectorOperations::Copy(source_cc, payload_vector, ldata.offset + array_size, ldata.offset, 0);
		return cast_data.child_cast_info.function(payload_vector, result_cc, array_size, child_parameters);

	} else {
		source.Flatten(count);
		result.SetVectorType(VectorType::FLAT_VECTOR);

		auto child_type = ArrayType::GetChildType(result.GetType());
		auto &source_cc = ListVector::GetEntry(source);
		auto &result_cc = ArrayVector::GetEntry(result);
		auto ldata = FlatVector::GetData<list_entry_t>(source);

		auto child_count = array_size * count;
		SelectionVector child_sel(child_count);

		bool all_ok = true;

		for (idx_t i = 0; i < count; i++) {
			if (FlatVector::IsNull(source, i)) {
				FlatVector::SetNull(result, i, true);
				for (idx_t array_elem = 0; array_elem < array_size; array_elem++) {
					FlatVector::SetNull(result_cc, i * array_size + array_elem, true);
					child_sel.set_index(i * array_size + array_elem, 0);
				}
			} else if (ldata[i].length != array_size) {
				if (all_ok) {
					all_ok = false;
					auto msg = StringUtil::Format("Cannot cast list with length %llu to array with length %u",
					                              ldata[i].length, array_size);
					HandleCastError::AssignError(msg, parameters);
				}
				FlatVector::SetNull(result, i, true);
				for (idx_t array_elem = 0; array_elem < array_size; array_elem++) {
					FlatVector::SetNull(result_cc, i * array_size + array_elem, true);
					child_sel.set_index(i * array_size + array_elem, 0);
				}
			} else {
				for (idx_t array_elem = 0; array_elem < array_size; array_elem++) {
					child_sel.set_index(i * array_size + array_elem, ldata[i].offset + array_elem);
				}
			}
		}

		CastParameters child_parameters(parameters, cast_data.child_cast_info.cast_data, parameters.local_state);

		// Fast path: No lists are null
		// We can just cast the child vector directly
		// Note: Its worth doing a CheckAllValid here, the slow path is significantly more expensive
		if (FlatVector::Validity(result).CheckAllValid(count)) {
			Vector payload_vector(result_cc.GetType(), child_count);

			bool ok = cast_data.child_cast_info.function(source_cc, payload_vector, child_count, child_parameters);
			if (all_ok && !ok) {
				all_ok = false;
				HandleCastError::AssignError(*child_parameters.error_message, parameters);
			}
			// Now do the actual copy onto the result vector, making sure to slice properly in case the lists are out of
			// order
			VectorOperations::Copy(payload_vector, result_cc, child_sel, child_count, 0, 0);
			return all_ok;
		}

		// Slow path: Some lists are null, so we need to copy the data list by list to the right place
		auto list_data = FlatVector::GetData<list_entry_t>(source);
		DataChunk cast_chunk;
		cast_chunk.Initialize(Allocator::DefaultAllocator(), {source_cc.GetType(), result_cc.GetType()}, array_size);

		for (idx_t i = 0; i < count; i++) {
			if (FlatVector::IsNull(result, i)) {
				// We've already failed to cast this list above (e.g. length mismatch), so theres nothing to do here.
				continue;
			} else {
				auto &list_cast_input = cast_chunk.data[0];
				auto &list_cast_output = cast_chunk.data[1];
				auto list_entry = list_data[i];

				VectorOperations::Copy(source_cc, list_cast_input, list_entry.offset + array_size, list_entry.offset,
				                       0);

				bool ok =
				    cast_data.child_cast_info.function(list_cast_input, list_cast_output, array_size, child_parameters);
				if (all_ok && !ok) {
					all_ok = false;
					HandleCastError::AssignError(*child_parameters.error_message, parameters);
				}
				VectorOperations::Copy(list_cast_output, result_cc, array_size, 0, i * array_size);

				// Reset the cast_chunk
				cast_chunk.Reset();
			}
		}

		return all_ok;
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

/*

 */

} // namespace duckdb
